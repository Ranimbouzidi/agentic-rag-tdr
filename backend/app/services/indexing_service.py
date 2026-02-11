# backend/app/services/indexing_service.py
from __future__ import annotations
import uuid
import json
from typing import Any, Dict, List
from datetime import datetime, timezone

import sqlalchemy as sa
from qdrant_client.http import models as qm

from app.core.settings import settings
from app.services.db_service import engine, documents
from app.services.minio_service import download_text
from app.services.chunking_service import build_chunks_from_structured
from app.services.embedding_service import embed_batch
from app.services.qdrant_index_service import ensure_collection, upsert_points



def index_document(doc_id: str) -> Dict[str, Any]:
    # 1) Fetch pointers
    with engine.begin() as conn:
        row = conn.execute(
            sa.select(
                documents.c.id,
                documents.c.processed_bucket,
                documents.c.processed_prefix,
            ).where(documents.c.id == doc_id)
        ).mappings().first()

        if not row:
            raise ValueError("doc_id not found")

        bucket = row["processed_bucket"]
        prefix = row["processed_prefix"]

    # 2) Load structured JSON
    structured_key = f"{prefix}structured/tdr_structured.json"
    structured_raw = download_text(bucket, structured_key)
    structured = json.loads(structured_raw)

    # 3) Build chunks (micro-chunks + tables séparées)
    chunks = build_chunks_from_structured(
        structured=structured,
        target_chars=settings.chunk_target_chars,
        max_chars=settings.chunk_max_chars,
        overlap_chars=settings.chunk_overlap_chars,
    )
    if not chunks:
        raise ValueError("No chunks produced")

    # 4) Embeddings (batch)
    texts = [c.text for c in chunks]
    vectors: List[List[float]] = []
    bs = settings.embed_batch_size

    for i in range(0, len(texts), bs):
        vectors.extend(embed_batch(texts[i : i + bs]))

    vector_size = len(vectors[0])

    # 5) Ensure collection + upsert points
    ensure_collection(vector_size)

    points: List[qm.PointStruct] = []
    for c, v in zip(chunks, vectors):
        ns = uuid.UUID(c.doc_id) 
        point_id = str(uuid.uuid5(ns, f"{c.section}:{c.chunk_index}"))
        chunk_id = f"{c.doc_id}:{c.section}:{c.chunk_index}"
        payload = {
            "chunk_id": chunk_id, 
            "doc_id": c.doc_id,
            "doc_type": c.doc_type,
            "section": c.section,
            "chunk_index": c.chunk_index,
            "text": c.text,
            "competences": c.competences,
            "metadata": c.metadata,
        }
        points.append(qm.PointStruct(id=point_id, vector=v, payload=payload))

    qb = settings.qdrant_upsert_batch
    for i in range(0, len(points), qb):
        upsert_points(points[i : i + qb])

    # 6) Update DB status + stats
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

    return {
        "doc_id": doc_id,
        "status": "indexed",
        "collection": settings.qdrant_collection,
        "chunks": len(chunks),
        "vector_size": vector_size,
    }
