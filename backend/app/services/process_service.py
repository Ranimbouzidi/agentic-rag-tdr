from pathlib import Path
import sqlalchemy as sa

from app.services.db_service import engine, documents
from app.services.minio_service import download_file, upload_text, upload_markdown
from app.services.extraction_service import extract_content


def process_document(doc_id: str) -> dict:
    with engine.begin() as conn:
        row = conn.execute(
            sa.select(
                documents.c.id,
                documents.c.filename,
                documents.c.raw_bucket,
                documents.c.raw_object_key,
                documents.c.processed_bucket,
                documents.c.processed_prefix,
            ).where(documents.c.id == doc_id)
        ).mappings().first()

        if not row:
            raise ValueError("doc_id not found in documents table")

        # 1) Download RAW to local tmp
        local_raw = Path("tmp") / doc_id / row["filename"]
        download_file(row["raw_bucket"], row["raw_object_key"], local_raw)

        # 2) Extract content (PDF: Docling + fallback, DOCX: python-docx)
        extracted = extract_content(local_raw)
        if not extracted.text.strip():
            raise ValueError("Extracted text is empty (maybe scanned PDF needs OCR)")

        # 3) Store extracted outputs in MinIO processed (Phase 4A upgrade)
        processed_bucket = row["processed_bucket"]
        processed_prefix = row["processed_prefix"]

        txt_key = f"{processed_prefix}extracted/extracted.txt"
        upload_text(processed_bucket, txt_key, extracted.text)

        md_key = None
        if extracted.markdown and extracted.markdown.strip():
            md_key = f"{processed_prefix}extracted/extracted.md"
            upload_markdown(processed_bucket, md_key, extracted.markdown)

        # 4) Update DB
        conn.execute(
            documents.update()
            .where(documents.c.id == doc_id)
            .values(status="extracted")
        )

    return {
        "doc_id": doc_id,
        "status": "extracted",
        "text_object_key": txt_key,
        "markdown_object_key": md_key,
    }
