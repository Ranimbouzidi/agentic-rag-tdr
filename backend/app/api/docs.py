# backend/app/api/docs.py
from fastapi import APIRouter, HTTPException

from app.services.doc_chunks_service import get_doc_chunks

router = APIRouter(prefix="/docs", tags=["docs"])


@router.get("/{doc_id}/chunks")
def doc_chunks(doc_id: str, limit: int = 200):
    try:
        return get_doc_chunks(doc_id=doc_id, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
