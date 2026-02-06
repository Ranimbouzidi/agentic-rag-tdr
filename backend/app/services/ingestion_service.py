import uuid
from pathlib import Path
import sqlalchemy as sa

from app.core.settings import settings
from app.services.minio_service import ensure_buckets, upload_file
from app.services.db_service import engine, documents

def ingest_file(file_path: Path) -> dict:
    doc_id = str(uuid.uuid4())
    ensure_buckets()

    # MinIO object keys (convention stable)
    raw_object_key = f"{doc_id}/source/{file_path.name}"
    processed_prefix = f"{doc_id}/"  # tout ce qui sera écrit en processed sous ce prefix

    # 1) Upload RAW
    upload_file(
        bucket=settings.minio_bucket_raw,
        object_name=raw_object_key,
        file_path=str(file_path),
    )

    # 2) Insert DB (curated)
    with engine.begin() as conn:
        conn.execute(
            documents.insert().values(
                id=doc_id,
                filename=file_path.name,
                status="uploaded",
                raw_bucket=settings.minio_bucket_raw,
                raw_object_key=raw_object_key,
                processed_bucket=settings.minio_bucket_processed,
                processed_prefix=processed_prefix,
            )
        )

    return {
        "doc_id": doc_id,
        "raw_bucket": settings.minio_bucket_raw,
        "raw_object_key": raw_object_key,
        "processed_bucket": settings.minio_bucket_processed,
        "processed_prefix": processed_prefix,
        "status": "uploaded",
    }
