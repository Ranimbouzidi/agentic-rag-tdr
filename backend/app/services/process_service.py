# backend/app/services/process_service.py
from __future__ import annotations

import time
import logging
from pathlib import Path

import sqlalchemy as sa

from app.services.tracing import span_step
from app.services.db_service import engine, documents
from app.services.minio_service import download_file, upload_text, upload_markdown
from app.services.extraction_service import extract_content, fix_mojibake
from app.services.doc_type_service import detect_doc_type

log = logging.getLogger("uvicorn.error")


def process_document(doc_id: str) -> dict:
    t0 = time.perf_counter()

    log.info(f"[PROCESS] step=load_db doc_id={doc_id}")
    with span_step("process.load_db", doc_id=doc_id):
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

            filename = row["filename"]
            raw_bucket = row["raw_bucket"]
            raw_object_key = row["raw_object_key"]
            processed_bucket = row["processed_bucket"]
            processed_prefix = row["processed_prefix"]

            # 1) Download RAW to local tmp
            log.info(f"[PROCESS] step=download_raw doc_id={doc_id} bucket={raw_bucket} key={raw_object_key}")
            local_dir = Path("tmp") / doc_id
            local_dir.mkdir(parents=True, exist_ok=True)
            local_raw = local_dir / filename

            t_dl0 = time.perf_counter()
            with span_step(
                "process.download_raw",
                doc_id=doc_id,
                bucket=raw_bucket,
                object_key=raw_object_key,
                local_path=str(local_raw),
            ):
                download_file(raw_bucket, raw_object_key, local_raw)
            log.info(
                f"[PROCESS] step=download_raw_done doc_id={doc_id} path={local_raw} "
                f"took={time.perf_counter()-t_dl0:.2f}s"
            )

            # 2) Extract content (smart PDF routing: docling/pymupdf/ocr/hybrid)
            log.info(f"[PROCESS] step=extract_start doc_id={doc_id} path={local_raw}")
            t_ex0 = time.perf_counter()
            with span_step(
                "process.extract",
                doc_id=doc_id,
                filename=filename,
                local_path=str(local_raw),
            ) as span:
                extracted = extract_content(local_raw)
                # ajout info utile dans trace
                span.set_attribute("extractor", getattr(extracted, "extractor", None))
            log.info(
                f"[PROCESS] step=extract_done doc_id={doc_id} extractor={getattr(extracted,'extractor',None)} "
                f"took={time.perf_counter()-t_ex0:.2f}s"
            )

            # Fix mojibake (encoding issues)
            log.info(f"[PROCESS] step=fix_encoding doc_id={doc_id}")
            with span_step("process.fix_encoding", doc_id=doc_id):
                extracted.text = fix_mojibake(extracted.text or "")
                if getattr(extracted, "markdown", None):
                    extracted.markdown = fix_mojibake(extracted.markdown)

            text_len = len((extracted.text or "").strip())
            md_len = len((extracted.markdown or "").strip()) if getattr(extracted, "markdown", None) else 0
            log.info(f"[PROCESS] step=extract_stats doc_id={doc_id} text_len={text_len} md_len={md_len}")

            if not (extracted.text or "").strip():
                if getattr(extracted, "extractor", "") == "failed":
                    raise ValueError("PDF is encrypted/protected or cannot be opened for extraction")
                raise ValueError("Extracted text is empty (OCR may have failed or PDF is unreadable)")

            # 2bis) Detect business doc_type (tdr/ami/other)
            log.info(f"[PROCESS] step=detect_doc_type doc_id={doc_id}")
            t_dt0 = time.perf_counter()
            with span_step(
                "process.detect_doc_type",
                doc_id=doc_id,
                text_len=text_len,
            ) as span:
                doc_type = detect_doc_type(extracted.text)
                span.set_attribute("doc_type", doc_type)
            log.info(
                f"[PROCESS] step=detect_doc_type_done doc_id={doc_id} doc_type={doc_type} "
                f"took={time.perf_counter()-t_dt0:.2f}s"
            )

            # 3) Store extracted outputs in MinIO processed
            log.info(f"[PROCESS] step=upload_extracted doc_id={doc_id} bucket={processed_bucket} prefix={processed_prefix}")

            txt_key = f"{processed_prefix}extracted/extracted.txt"
            t_up0 = time.perf_counter()
            with span_step(
                "process.upload_extracted_txt",
                doc_id=doc_id,
                bucket=processed_bucket,
                object_key=txt_key,
                text_len=text_len,
            ):
                upload_text(processed_bucket, txt_key, extracted.text)
            log.info(
                f"[PROCESS] step=upload_txt_done doc_id={doc_id} key={txt_key} "
                f"took={time.perf_counter()-t_up0:.2f}s"
            )

            md_key = None
            if getattr(extracted, "markdown", None) and extracted.markdown.strip():
                md_key = f"{processed_prefix}extracted/extracted.md"
                t_md0 = time.perf_counter()
                with span_step(
                    "process.upload_extracted_md",
                    doc_id=doc_id,
                    bucket=processed_bucket,
                    object_key=md_key,
                    markdown_len=md_len,
                ):
                    upload_markdown(processed_bucket, md_key, extracted.markdown)
                log.info(
                    f"[PROCESS] step=upload_md_done doc_id={doc_id} key={md_key} "
                    f"took={time.perf_counter()-t_md0:.2f}s"
                )

            # 4) Update DB (status + doc_type)
            log.info(f"[PROCESS] step=db_update doc_id={doc_id} status=extracted doc_type={doc_type}")
            with span_step(
                "process.db_update",
                doc_id=doc_id,
                status="extracted",
                doc_type=doc_type,
            ):
                conn.execute(
                    documents.update()
                    .where(documents.c.id == doc_id)
                    .values(
                        status="extracted",
                        doc_type=doc_type,
                    )
                )

    log.info(f"[PROCESS] finished doc_id={doc_id} total_took={time.perf_counter()-t0:.2f}s")
    return {
        "doc_id": doc_id,
        "status": "extracted",
        "doc_type": doc_type,
        "text_object_key": txt_key,
        "markdown_object_key": md_key,
        "extractor": getattr(extracted, "extractor", None),
        "text_len": text_len,
        "markdown_len": md_len,
    }
