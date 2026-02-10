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

        # 2) Read extracted.txt (required)
        txt_key = f"{processed_prefix}extracted/extracted.txt"
        try:
            extracted_text = download_text(processed_bucket, txt_key)
        except Exception as e:
            raise ValueError(
                f"Cannot read extracted text (missing extracted.txt). "
                f"bucket={processed_bucket}, key={txt_key}. "
                f"Run POST /process/{doc_id} first. Original error: {e}"
            )

        if not extracted_text.strip():
            raise ValueError("Extracted text is empty (OCR may have failed or PDF unreadable)")

        # 3) Read extracted.md (optional)
        md_key = f"{processed_prefix}extracted/extracted.md"
        extracted_markdown = None

        md_exists = object_exists(processed_bucket, md_key)
        if md_exists:
            try:
                extracted_markdown = download_text(processed_bucket, md_key)
            except Exception:
                extracted_markdown = None  # on n'échoue pas pour ça

        # 4) Structure (TXT + optional Markdown tables)
        out_key = structure_document(
            doc_id=doc_id,
            extracted_text=extracted_text,
            extracted_markdown=extracted_markdown,
            processed_prefix=processed_prefix,
            processed_bucket=processed_bucket,
        )

        return {
            "doc_id": doc_id,
            "status": "structured",
            "structured_object_key": out_key,
            "extraction": {
                "txt_key": txt_key,
                "md_key": md_key if md_exists else None,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
