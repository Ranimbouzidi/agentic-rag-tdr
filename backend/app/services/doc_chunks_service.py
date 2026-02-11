# backend/app/services/doc_chunks_service.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from app.core.settings import settings


def _get_qdrant() -> QdrantClient:
    return QdrantClient(url=settings.qdrant_url)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def get_doc_chunks(doc_id: str, limit: int = 200) -> Dict[str, Any]:
    doc_id = (doc_id or "").strip()
    if not doc_id:
        raise ValueError("doc_id is required")

    limit = max(1, min(int(limit or 200), 2000))

    qc = _get_qdrant()

    flt = qm.Filter(
        must=[qm.FieldCondition(key="doc_id", match=qm.MatchValue(value=doc_id))]
    )

    chunks: List[Dict[str, Any]] = []
    offset = None
    fetched = 0

    while fetched < limit:
        batch = min(256, limit - fetched)

        # Some qdrant-client versions use scroll_filter=, others filter=
        try:
            points, offset = qc.scroll(
                collection_name=settings.qdrant_collection,
                scroll_filter=flt,
                with_payload=True,
                with_vectors=False,
                limit=batch,
                offset=offset,
            )
        except TypeError:
            points, offset = qc.scroll(
                collection_name=settings.qdrant_collection,
                filter=flt,
                with_payload=True,
                with_vectors=False,
                limit=batch,
                offset=offset,
            )

        if not points:
            break

        for p in points:
            payload = getattr(p, "payload", None) or {}
            chunks.append(
                {
                    "doc_id": payload.get("doc_id"),
                    "doc_type": payload.get("doc_type"),
                    "section": payload.get("section"),
                    "chunk_index": _safe_int(payload.get("chunk_index"), 0),
                    "text": payload.get("text") or "",
                    "metadata": payload.get("metadata") or {},
                }
            )

        fetched += len(points)
        if offset is None:
            break

    # tri stable: section puis chunk_index
    chunks.sort(key=lambda c: (str(c.get("section") or ""), _safe_int(c.get("chunk_index"), 0)))

    return {"doc_id": doc_id, "count": len(chunks), "chunks": chunks}
