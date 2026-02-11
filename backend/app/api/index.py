# backend/app/api/index.py
from fastapi import APIRouter, HTTPException
from app.services.indexing_service import index_document

router = APIRouter(prefix="/index", tags=["indexing"])

@router.post("/{doc_id}")
def index(doc_id: str):
    try:
        return index_document(doc_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
