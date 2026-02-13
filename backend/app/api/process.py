# backend/app/api/process.py
from __future__ import annotations

import asyncio
import logging
from fastapi import APIRouter, HTTPException

from app.services.process_service import process_document

router = APIRouter(prefix="/process", tags=["processing"])
log = logging.getLogger("uvicorn.error")


@router.post("/{doc_id}")
def process(doc_id: str):
    """
    Process a document:
      - download raw from MinIO
      - extract text/markdown (docling/pymupdf/ocr/hybrid)
      - detect doc_type
      - upload extracted outputs to MinIO processed
      - update DB status/doc_type
    """
    log.info(f"[PROCESS] start doc_id={doc_id}")
    try:
        out = process_document(doc_id)
        log.info(f"[PROCESS] done doc_id={doc_id} status={out.get('status')} extractor={out.get('extractor')}")
        return out

    except asyncio.CancelledError:
        # happens when uvicorn reload/CTRL+C cancels ongoing work
        log.warning(f"[PROCESS] cancelled doc_id={doc_id}")
        raise HTTPException(status_code=499, detail="Client closed request / server shutdown")

    except Exception as e:
        log.exception(f"[PROCESS] failed doc_id={doc_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
