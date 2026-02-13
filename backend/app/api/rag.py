# backend/app/api/rag.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
import logging

from app.services.rag_service import answer
from app.services.filters_utils import normalize_filters

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/rag", tags=["rag"])

class RagRequest(BaseModel):
    query: str
    top_k: int = 5
    filters: Optional[Dict[str, Any]] = None

@router.post("/", response_model=dict)
def rag(req: RagRequest):
    try:
        filters = normalize_filters(req.filters)
        return answer(query=req.query, top_k=req.top_k, filters=filters)
    except Exception as e:
        logger.exception("RAG failed")
        raise HTTPException(status_code=400, detail=str(e))
