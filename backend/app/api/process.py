# backend/app/api/process.py
from __future__ import annotations

import asyncio
import logging
import time
from fastapi import APIRouter, HTTPException

from app.services.process_service import process_document
from app.services.metrics_service import PIPELINE_STEP_TOTAL, PIPELINE_STEP_DURATION

router = APIRouter(prefix="/process", tags=["processing"])
log = logging.getLogger("uvicorn.error")


@router.post("/{doc_id}")
def process(doc_id: str):
    t0 = time.time()
    log.info(f"[PROCESS] start doc_id={doc_id}")
    try:
        out = process_document(doc_id)
        log.info(f"[PROCESS] done doc_id={doc_id} status={out.get('status')} extractor={out.get('extractor')}")

        PIPELINE_STEP_TOTAL.labels(step="process", result="success").inc()
        PIPELINE_STEP_DURATION.labels(step="process", result="success").observe(time.time() - t0)
        return out

    except asyncio.CancelledError:
        log.warning(f"[PROCESS] cancelled doc_id={doc_id}")
        PIPELINE_STEP_TOTAL.labels(step="process", result="error").inc()
        PIPELINE_STEP_DURATION.labels(step="process", result="error").observe(time.time() - t0)
        raise HTTPException(status_code=499, detail="Client closed request / server shutdown")

    except Exception as e:
        log.exception(f"[PROCESS] failed doc_id={doc_id}: {e}")
        PIPELINE_STEP_TOTAL.labels(step="process", result="error").inc()
        PIPELINE_STEP_DURATION.labels(step="process", result="error").observe(time.time() - t0)
        raise HTTPException(status_code=400, detail=str(e))
