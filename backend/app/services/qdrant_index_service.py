from __future__ import annotations

from typing import List
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from app.core.settings import settings


def get_qdrant() -> QdrantClient:
    return QdrantClient(url=settings.qdrant_url)


def ensure_collection(vector_size: int) -> None:
    qc = get_qdrant()
    name = settings.qdrant_collection

    exists = True
    try:
        qc.get_collection(name)
    except Exception:
        exists = False

    if not exists:
        qc.create_collection(
            collection_name=name,
            vectors_config=qm.VectorParams(
                size=vector_size,
                distance=qm.Distance.COSINE,
            ),
            hnsw_config=qm.HnswConfigDiff(m=16, ef_construct=128),
            optimizers_config=qm.OptimizersConfigDiff(indexing_threshold=20000),
        )

    # Payload indexes (accélère filtres)
    for field in ["doc_id", "doc_type", "section"]:
        try:
            qc.create_payload_index(
                collection_name=name,
                field_name=field,
                field_schema=qm.PayloadSchemaType.KEYWORD,
            )
        except Exception:
            pass


def upsert_points(points: List[qm.PointStruct]) -> None:
    if not points:
        return
    qc = get_qdrant()
    qc.upsert(collection_name=settings.qdrant_collection, points=points)


def delete_points_by_doc_id(doc_id: str) -> None:
    """
    Supprime tous les points Qdrant dont payload.doc_id == doc_id.
    À faire avant ré-indexation pour éviter des chunks "fantômes".
    """
    qc = get_qdrant()
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
        wait=True,
    )
