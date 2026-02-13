# backend/app/services/indexing_service.py
from __future__ import annotations

import uuid
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import sqlalchemy as sa
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm
from app.services.qdrant_index_service import (
    ensure_collection,
    upsert_points,
    delete_points_by_doc_id,   # ✅ AJOUTE CETTE LIGNE
)

from app.core.settings import settings
from app.services.db_service import engine, documents
from app.services.minio_service import download_text
from app.services.chunking_service import build_chunks_from_structured
from app.services.embedding_service import embed_batch
from app.services.qdrant_index_service import ensure_collection, upsert_points

log = logging.getLogger("uvicorn.error")



def _purge_doc_points(doc_id: str) -> None:
    qc = QdrantClient(url=settings.qdrant_url)
    qc.delete(
        collection_name=settings.qdrant_collection,
        points_selector=qm.FilterSelector(
            filter=qm.Filter(
                must=[
                    qm.FieldCondition(
                        key="doc_id",
                        match=qm.MatchValue(value=doc_id),
                    )
                ]
            )
        ),
    )

def _load_structured(bucket: str, prefix: str, doc_type: str) -> tuple[dict, str]:
    """
    Charge le JSON structuré depuis MinIO.
    Stratégie:
      - si doc_type == ami -> ami_structured.json puis fallback tdr_structured.json
      - sinon -> tdr_structured.json puis fallback ami_structured.json
    """
    candidates: List[str] = []
    if doc_type == "ami":
        candidates = [
            f"{prefix}structured/ami_structured.json",
            f"{prefix}structured/tdr_structured.json",
        ]
    else:
        candidates = [
            f"{prefix}structured/tdr_structured.json",
            f"{prefix}structured/ami_structured.json",
        ]

    last_err: Optional[Exception] = None
    for key in candidates:
        try:
            log.info(f"[INDEX] try load structured key={key}")
            raw = download_text(bucket, key)
            obj = json.loads(raw)
            return obj, key
        except Exception as e:
            last_err = e
            log.warning(f"[INDEX] failed load key={key} err={type(e).__name__}: {e}")

    raise ValueError(f"Could not load structured JSON from MinIO (tried {candidates}). Last error: {last_err}")


def index_document(doc_id: str) -> Dict[str, Any]:
    log.info(f"[INDEX] start doc_id={doc_id}")

    # 1) Fetch pointers + doc_type
    with engine.begin() as conn:
        row = conn.execute(
            sa.select(
                documents.c.id,
                documents.c.doc_type,
                documents.c.processed_bucket,
                documents.c.processed_prefix,
            ).where(documents.c.id == doc_id)
        ).mappings().first()

        if not row:
            raise ValueError("doc_id not found")

        bucket = row["processed_bucket"]
        prefix = row["processed_prefix"]
        doc_type = (row.get("doc_type") or "unknown").lower()

    # 2) Load structured JSON (TDR/AMI)
    structured, structured_key = _load_structured(bucket=bucket, prefix=prefix, doc_type=doc_type)
    structured_doc_type = (structured.get("doc_type") or doc_type or "unknown").lower()

    log.info(f"[INDEX] structured loaded doc_type={structured_doc_type} key={structured_key}")

    # ✅ 2bis) Purge old points for this doc_id
    try:
        delete_points_by_doc_id(doc_id)
        log.info(f"[INDEX] purged old qdrant points doc_id={doc_id}")
    except Exception as e:
        # on ne casse pas l'indexation si purge échoue
        log.warning(f"[INDEX] purge failed doc_id={doc_id}: {e}")

    # 3) Build chunks
    chunks = build_chunks_from_structured(
        structured=structured,
        target_chars=settings.chunk_target_chars,
        max_chars=settings.chunk_max_chars,
        overlap_chars=settings.chunk_overlap_chars,
    )
    if not chunks:
        raise ValueError("No chunks produced")

    log.info(f"[INDEX] chunks={len(chunks)}")

    # 4) Embeddings + Upsert (streaming)
    bs = int(getattr(settings, "embed_batch_size", 8))
    qb = int(getattr(settings, "qdrant_upsert_batch", 32))

    vector_size: Optional[int] = None
    total_points = 0

    for i in range(0, len(chunks), bs):
        batch_chunks = chunks[i : i + bs]
        batch_texts = [c.text for c in batch_chunks]

        log.info(f"[INDEX] embed batch {i//bs + 1} size={len(batch_texts)}")
        batch_vectors: List[List[float]] = embed_batch(batch_texts)

        if not batch_vectors:
            raise ValueError("Empty embeddings batch")

        # sanity: all vectors same length
        if any(len(v) != len(batch_vectors[0]) for v in batch_vectors):
            raise ValueError("Inconsistent embedding vector sizes in batch")

        if vector_size is None:
            vector_size = len(batch_vectors[0])
            log.info(f"[INDEX] ensure_collection vector_size={vector_size}")
            ensure_collection(vector_size)

        points: List[qm.PointStruct] = []
        for c, v in zip(batch_chunks, batch_vectors):
            # ✅ MUST match rag_service._point_id()
            ns = uuid.UUID(c.doc_id)
            point_id = str(uuid.uuid5(ns, f"{c.doc_id}:{c.section}:{c.chunk_index}"))

            payload = {
                "chunk_id": f"{c.doc_id}:{c.section}:{c.chunk_index}",
                "doc_id": c.doc_id,
                "doc_type": c.doc_type,
                "section": c.section,
                "chunk_index": c.chunk_index,
                "text": c.text,
                "competences": c.competences,
                "metadata": c.metadata,
            }
            points.append(qm.PointStruct(id=point_id, vector=v, payload=payload))

        for j in range(0, len(points), qb):
            sub = points[j : j + qb]
            upsert_points(sub)
            total_points += len(sub)

        # libérer (important sur Windows)
        del points, batch_vectors, batch_chunks, batch_texts

    if vector_size is None:
        raise ValueError("vector_size could not be determined")

    # 5) Update DB status + stats
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            documents.update()
            .where(documents.c.id == doc_id)
            .values(
                status="indexed",
                indexed_at=now,
                chunk_count=len(chunks),
                vector_size=vector_size,
                qdrant_collection=settings.qdrant_collection,
            )
        )

    log.info(f"[INDEX] done doc_id={doc_id} points_upserted={total_points}")

    return {
        "doc_id": doc_id,
        "status": "indexed",
        "collection": settings.qdrant_collection,
        "structured_key": structured_key,
        "doc_type": structured_doc_type,
        "chunks": len(chunks),
        "vector_size": vector_size,
        "points_upserted": total_points,
    }
