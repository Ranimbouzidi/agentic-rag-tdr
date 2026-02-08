from fastapi import APIRouter, HTTPException
from app.services.process_service import process_document

router = APIRouter(prefix="/process", tags=["processing"])

@router.post("/{doc_id}")
def process(doc_id: str):
    try:
        return process_document(doc_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
