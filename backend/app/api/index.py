# backend/app/api/index.py
from __future__ import annotations

import asyncio
import logging
import time
from fastapi import APIRouter, HTTPException

from app.services.indexing_service import index_document
from app.services.metrics_service import PIPELINE_STEP_TOTAL, PIPELINE_STEP_DURATION

router = APIRouter(prefix="/index", tags=["indexing"])
log = logging.getLogger("uvicorn.error")


@router.post("/{doc_id}")
def index(doc_id: str):
    t0 = time.time()
    log.info(f"[INDEX] start doc_id={doc_id}")
    try:
        out = index_document(doc_id)
        log.info(
            f"[INDEX] done doc_id={doc_id} status={out.get('status')} "
            f"chunks={out.get('chunks')} vector_size={out.get('vector_size')} "
            f"embed_sec={out.get('embed_time_s')} upsert_sec={out.get('upsert_time_s')}"
        )

        PIPELINE_STEP_TOTAL.labels(step="index", result="success").inc()
        PIPELINE_STEP_DURATION.labels(step="index", result="success").observe(time.time() - t0)
        return out

    except asyncio.CancelledError:
        log.warning(f"[INDEX] cancelled doc_id={doc_id}")
        PIPELINE_STEP_TOTAL.labels(step="index", result="error").inc()
        PIPELINE_STEP_DURATION.labels(step="index", result="error").observe(time.time() - t0)
        raise HTTPException(status_code=499, detail="Client closed request / server shutdown")

    except Exception as e:
        log.exception(f"[INDEX] failed doc_id={doc_id}: {e}")
        PIPELINE_STEP_TOTAL.labels(step="index", result="error").inc()
        PIPELINE_STEP_DURATION.labels(step="index", result="error").observe(time.time() - t0)
        raise HTTPException(status_code=400, detail=str(e))
