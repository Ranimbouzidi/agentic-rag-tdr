from fastapi import APIRouter, UploadFile, File, HTTPException
from pathlib import Path
import shutil
import time

from app.services.ingestion_service import ingest_file
from app.services.metrics_service import PIPELINE_STEP_TOTAL, PIPELINE_STEP_DURATION

router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.post("/")
def ingest(file: UploadFile = File(...)):
    t0 = time.time()
    try:
        temp_dir = Path("tmp")
        temp_dir.mkdir(exist_ok=True)
        file_path = temp_dir / file.filename

        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        result = ingest_file(file_path)

        PIPELINE_STEP_TOTAL.labels(step="ingest", result="success").inc()
        PIPELINE_STEP_DURATION.labels(step="ingest", result="success").observe(time.time() - t0)
        return result

    except Exception as e:
        PIPELINE_STEP_TOTAL.labels(step="ingest", result="error").inc()
        PIPELINE_STEP_DURATION.labels(step="ingest", result="error").observe(time.time() - t0)
        raise HTTPException(status_code=400, detail=str(e))
