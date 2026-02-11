from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional

from app.services.rag_service import answer

router = APIRouter(prefix="/rag", tags=["rag"])

class RagRequest(BaseModel):
    query: str
    top_k: int = 5
    filters: Optional[Dict[str, Any]] = None

@router.post("/")
def rag_api(req: RagRequest):
    try:
        return answer(query=req.query, top_k=req.top_k, filters=req.filters or {})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
