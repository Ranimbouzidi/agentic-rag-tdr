from fastapi import APIRouter, UploadFile, File
from pathlib import Path
import shutil

from app.services.ingestion_service import ingest_file

router = APIRouter(prefix="/ingest", tags=["ingestion"])

@router.post("/")
def ingest(file: UploadFile = File(...)):
    temp_dir = Path("tmp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / file.filename

    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = ingest_file(file_path)
    return result
