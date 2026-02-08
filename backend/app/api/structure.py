from fastapi import APIRouter, HTTPException
import sqlalchemy as sa

from app.services.db_service import engine, documents
from app.services.minio_service import download_text, object_exists
from app.services.structuring_process_service import structure_document

router = APIRouter(prefix="/structure", tags=["structuring"])


@router.post("/{doc_id}")
def structure(doc_id: str):
    try:
        # 1) Get processed bucket/prefix from DB
        with engine.begin() as conn:
            row = conn.execute(
                sa.select(
                    documents.c.id,
                    documents.c.processed_bucket,
                    documents.c.processed_prefix,
                ).where(documents.c.id == doc_id)
            ).mappings().first()

            if not row:
                raise ValueError("doc_id not found")

        processed_bucket = row["processed_bucket"]
        processed_prefix = row["processed_prefix"]

        # 2) Fetch extracted text from MinIO (Phase 4A upgrade)
        txt_key = f"{processed_prefix}extracted/extracted.txt"
        try:
            extracted_text = download_text(processed_bucket, txt_key)
        except Exception as e:
            raise ValueError(
                f"Cannot read extracted text from MinIO: bucket={processed_bucket}, key={txt_key}. "
                f"Have you run /process/{doc_id} ? Original error: {e}"
            )

        if not extracted_text.strip():
            raise ValueError("Extracted text is empty (maybe scanned PDF needs OCR)")

        # 3) (Optional) detect markdown presence (Docling)
        md_key = f"{processed_prefix}extracted/extracted.md"
        has_markdown = object_exists(processed_bucket, md_key)

        # 4) Structure (Phase 4B unchanged)
        out_key = structure_document(
            doc_id=doc_id,
            extracted_text=extracted_text,
            processed_prefix=processed_prefix,
            processed_bucket=processed_bucket,
        )

        return {
            "doc_id": doc_id,
            "status": "structured",
            "structured_object_key": out_key,
            "extraction": {
                "txt_key": txt_key,
                "md_key": md_key if has_markdown else None,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
