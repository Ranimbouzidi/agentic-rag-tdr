# backend/app/api/search.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from app.services.search_service import search
from app.services.filters_utils import normalize_filters

router = APIRouter(prefix="/search", tags=["search"])


class SearchRequest(BaseModel):
    query: str
    top_k: int = 8
    filters: Optional[Dict[str, Any]] = None


class Snippet(BaseModel):
    section: Optional[str] = None
    chunk_index: Optional[int] = None
    score_vector: Optional[float] = None
    score_lexical: Optional[float] = None
    score_bm25: Optional[float] = None
    score: Optional[float] = None
    snippet: str


class GroupedDocResult(BaseModel):
    doc_id: str
    doc_type: Optional[str] = None
    score: float
    metadata: Dict[str, Any] = {}
    snippets: List[Snippet] = []


class SearchResponse(BaseModel):
    mode: str
    query: str
    top_k: int
    filters: Dict[str, Any] = {}
    results: List[GroupedDocResult] = []

    # champs optionnels selon le mode
    weights: Optional[Dict[str, float]] = None
    pool_k: Optional[int] = None
    note: Optional[str] = None
    qdrant_error: Optional[str] = None

@router.post("/", response_model=SearchResponse)
def search_api(req: SearchRequest):
    try:
        filters = normalize_filters(req.filters)
        return search(query=req.query, top_k=req.top_k, filters=filters)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))    


