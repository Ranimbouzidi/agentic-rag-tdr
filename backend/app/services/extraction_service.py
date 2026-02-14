from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal

import docx

from app.core.settings import settings
from app.services.tracing import span_step


# =========================
# Data model (return type)
# =========================
@dataclass
class ExtractedContent:
    markdown: Optional[str]
    text: str
    extractor: str  # "docling" | "pymupdf" | "ocr" | "hybrid" | "failed" | "docx"


# =========================
# Helpers
# =========================
def _normalize_text(s: Optional[str]) -> str:
    s = (s or "").replace("\r", "")
    while "\n\n\n" in s:
        s = s.replace("\n\n\n", "\n\n")
    return s.strip()


def fix_mojibake(s: str) -> str:
    """
    Répare le cas classique: texte UTF-8 décodé par erreur en latin-1/cp1252,
    ex: "CompÃ©tences" -> "Compétences".
    """
    if not s:
        return s
    if ("Ã" not in s) and ("Â" not in s):
        return s

    try:
        repaired = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired and (repaired.count("Ã") + repaired.count("Â")) < (s.count("Ã") + s.count("Â")):
            return repaired
    except Exception:
        pass
    return s


def _join_pages(pages: list[str]) -> str:
    return _normalize_text("\n\n".join(pages))


# =========================
# DOCX
# =========================
def extract_text_from_docx(docx_path: Path) -> ExtractedContent:
    with span_step("extract.docx", path=str(docx_path)):
        d = docx.Document(str(docx_path))
        txt = "\n".join(p.text for p in d.paragraphs).strip()
        return ExtractedContent(markdown=None, text=_normalize_text(txt), extractor="docx")


# =========================
# PDF: Docling / PyMuPDF / OCR
# =========================
def extract_from_pdf_docling(pdf_path: Path) -> ExtractedContent:
    with span_step("extract.pdf.docling", path=str(pdf_path)):
        from docling.document_converter import DocumentConverter

        converter = DocumentConverter()
        result = converter.convert(str(pdf_path))
        doc = result.document

        md: Optional[str] = None
        txt: Optional[str] = None

        if hasattr(doc, "export_to_markdown"):
            md = doc.export_to_markdown()
        else:
            md = doc.export_to_text()

        if hasattr(doc, "export_to_text"):
            txt = doc.export_to_text()
        else:
            txt = md or ""

        return ExtractedContent(
            markdown=_normalize_text(md),
            text=_normalize_text(txt),
            extractor="docling",
        )


def extract_from_pdf_pymupdf(pdf_path: Path) -> ExtractedContent:
    with span_step("extract.pdf.pymupdf", path=str(pdf_path)):
        import fitz

        doc = fitz.open(str(pdf_path))
        parts = []
        for page in doc:
            parts.append(page.get_text("text") or "")
        txt = _normalize_text("\n".join(parts))
        return ExtractedContent(markdown=None, text=txt, extractor="pymupdf")


# ---------- Classifier ----------
PdfKind = Literal["native_text", "scanned", "mixed", "encrypted_or_failed"]


@dataclass
class PdfStats:
    kind: PdfKind
    num_pages: int
    avg_text_len: float
    pct_low_text_pages: float
    avg_images_per_page: float


def classify_pdf(pdf_path: Path, low_text_threshold: int = 40) -> PdfStats:
    import fitz

    with span_step("extract.pdf.classify", path=str(pdf_path), low_text_threshold=low_text_threshold) as span:
        try:
            doc = fitz.open(str(pdf_path))
        except Exception:
            stats = PdfStats(
                kind="encrypted_or_failed",
                num_pages=0,
                avg_text_len=0.0,
                pct_low_text_pages=100.0,
                avg_images_per_page=0.0,
            )
            span.set_attribute("pdf.kind", stats.kind)
            return stats

        # PDF protégé
        try:
            if getattr(doc, "needs_pass", False):
                stats = PdfStats(
                    kind="encrypted_or_failed",
                    num_pages=len(doc),
                    avg_text_len=0.0,
                    pct_low_text_pages=100.0,
                    avg_images_per_page=0.0,
                )
                span.set_attribute("pdf.kind", stats.kind)
                span.set_attribute("pdf.num_pages", stats.num_pages)
                return stats
        except Exception:
            pass

        num_pages = len(doc)
        if num_pages == 0:
            stats = PdfStats(
                kind="encrypted_or_failed",
                num_pages=0,
                avg_text_len=0.0,
                pct_low_text_pages=100.0,
                avg_images_per_page=0.0,
            )
            span.set_attribute("pdf.kind", stats.kind)
            return stats

        text_lens = []
        img_counts = []
        low_text_pages = 0

        for i in range(num_pages):
            page = doc.load_page(i)
            t = page.get_text("text") or ""
            tl = len(t.strip())
            text_lens.append(tl)

            imgs = page.get_images(full=True)
            img_counts.append(len(imgs))

            if tl < low_text_threshold:
                low_text_pages += 1

        avg_text = sum(text_lens) / num_pages
        avg_imgs = sum(img_counts) / num_pages
        pct_low = (low_text_pages / num_pages) * 100.0

        if pct_low >= 90.0:
            kind: PdfKind = "scanned"
        elif pct_low <= 20.0:
            kind = "native_text"
        else:
            kind = "mixed"

        stats = PdfStats(
            kind=kind,
            num_pages=num_pages,
            avg_text_len=avg_text,
            pct_low_text_pages=pct_low,
            avg_images_per_page=avg_imgs,
        )

        span.set_attribute("pdf.kind", stats.kind)
        span.set_attribute("pdf.num_pages", stats.num_pages)
        span.set_attribute("pdf.pct_low_text_pages", stats.pct_low_text_pages)
        span.set_attribute("pdf.avg_text_len", stats.avg_text_len)
        span.set_attribute("pdf.avg_images_per_page", stats.avg_images_per_page)
        return stats


# ---------- OCR (RapidOCR) ----------
def _render_pdf_page_to_image(page, zoom: float = 2.0):
    from PIL import Image
    import fitz

    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return img


def ocr_page_rapidocr(img) -> list[str]:
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR

    # ⚠️ instancier RapidOCR par page est coûteux mais on ne change pas ta logique ici.
    # Tu pourras l’optimiser ensuite (engine global).
    engine = RapidOCR()
    res, _ = engine(np.array(img))
    lines: list[str] = []
    if res:
        for item in res:
            txt = item[1]
            if txt and txt.strip():
                lines.append(txt.strip())
    return lines


def ocr_pdf_whole_rapidocr(pdf_path: Path, max_pages: int = 50) -> str:
    import fitz

    with span_step("extract.pdf.ocr_whole", path=str(pdf_path), max_pages=max_pages) as span:
        doc = fitz.open(str(pdf_path))
        pages = min(len(doc), max_pages)
        span.set_attribute("ocr.pages", pages)

        all_lines: list[str] = []
        for i in range(pages):
            page = doc.load_page(i)
            img = _render_pdf_page_to_image(page, zoom=2.0)
            all_lines.extend(ocr_page_rapidocr(img))

        return _normalize_text("\n".join(all_lines))


def extract_pdf_hybrid(pdf_path: Path, low_text_threshold: int = 40, max_pages: int = 50) -> ExtractedContent:
    import fitz

    with span_step(
        "extract.pdf.hybrid",
        path=str(pdf_path),
        low_text_threshold=low_text_threshold,
        max_pages=max_pages,
    ) as span:
        doc = fitz.open(str(pdf_path))
        pages = min(len(doc), max_pages)
        span.set_attribute("hybrid.pages", pages)

        out_pages: list[str] = []
        for i in range(pages):
            page = doc.load_page(i)
            t = (page.get_text("text") or "").strip()
            if len(t) >= low_text_threshold:
                out_pages.append(t)
            else:
                img = _render_pdf_page_to_image(page, zoom=2.0)
                ocr_lines = ocr_page_rapidocr(img)
                out_pages.append("\n".join(ocr_lines).strip())

        text = _join_pages(out_pages)
        span.set_attribute("hybrid.text_len", len(text))
        return ExtractedContent(markdown=None, text=text, extractor="hybrid")


# ---------- Router ----------
def extract_pdf_smart(pdf_path: Path) -> ExtractedContent:
    # paramètres V1 (tu les as déjà dans settings)
    MIN_CHARS_TOTAL = settings.ocr_min_chars
    OCR_MAX_PAGES = settings.ocr_max_pages
    LOW_TEXT_PAGE_THRESHOLD = settings.ocr_page_text_threshold

    with span_step(
        "extract.pdf.smart_router",
        path=str(pdf_path),
        min_chars_total=MIN_CHARS_TOTAL,
        ocr_max_pages=OCR_MAX_PAGES,
        low_text_page_threshold=LOW_TEXT_PAGE_THRESHOLD,
    ) as span:
        stats = classify_pdf(pdf_path, low_text_threshold=LOW_TEXT_PAGE_THRESHOLD)
        span.set_attribute("pdf.kind", stats.kind)

        if stats.kind == "encrypted_or_failed":
            return ExtractedContent(markdown=None, text="", extractor="failed")

        # 1) DOC-LING FIRST
        try:
            extracted = extract_from_pdf_docling(pdf_path)
        except Exception:
            extracted = extract_from_pdf_pymupdf(pdf_path)

        span.set_attribute("router.initial_extractor", extracted.extractor)
        span.set_attribute("router.initial_text_len", len(extracted.text.strip()))

        # 3) Si le texte est “pauvre” => OCR/hybrid
        if len(extracted.text.strip()) < MIN_CHARS_TOTAL:
            # scanned => OCR global
            if stats.kind == "scanned":
                with span_step("extract.pdf.router_ocr_scanned", path=str(pdf_path)):
                    ocr_text = ocr_pdf_whole_rapidocr(pdf_path, max_pages=OCR_MAX_PAGES)
                if len(ocr_text.strip()) >= 50:
                    return ExtractedContent(
                        markdown=extracted.markdown,
                        text=ocr_text,
                        extractor="ocr",
                    )

            # mixed => hybrid puis OCR global fallback
            elif stats.kind == "mixed":
                with span_step("extract.pdf.router_hybrid_mixed", path=str(pdf_path)):
                    hybrid = extract_pdf_hybrid(
                        pdf_path,
                        low_text_threshold=LOW_TEXT_PAGE_THRESHOLD,
                        max_pages=OCR_MAX_PAGES,
                    )
                if len(hybrid.text.strip()) >= 50:
                    return hybrid

                with span_step("extract.pdf.router_ocr_fallback", path=str(pdf_path)):
                    ocr_text = ocr_pdf_whole_rapidocr(pdf_path, max_pages=OCR_MAX_PAGES)
                if len(ocr_text.strip()) >= 50:
                    return ExtractedContent(markdown=None, text=ocr_text, extractor="ocr")

            # native_text mais pauvre => OCR global (rare)
            else:
                with span_step("extract.pdf.router_ocr_native_poor", path=str(pdf_path)):
                    ocr_text = ocr_pdf_whole_rapidocr(pdf_path, max_pages=OCR_MAX_PAGES)
                if len(ocr_text.strip()) >= 50:
                    return ExtractedContent(
                        markdown=extracted.markdown,
                        text=ocr_text,
                        extractor="ocr",
                    )

        return extracted


# =========================
# Public entrypoint
# =========================
def extract_content(file_path: Path) -> ExtractedContent:
    suffix = file_path.suffix.lower()

    with span_step("extract.content", path=str(file_path), suffix=suffix) as span:
        if suffix == ".pdf":
            out = extract_pdf_smart(file_path)
            span.set_attribute("extractor", out.extractor)
            span.set_attribute("text_len", len((out.text or "").strip()))
            span.set_attribute("md_len", len((out.markdown or "").strip()) if out.markdown else 0)
            return out

        if suffix == ".docx":
            out = extract_text_from_docx(file_path)
            span.set_attribute("extractor", out.extractor)
            span.set_attribute("text_len", len((out.text or "").strip()))
            return out

        raise ValueError(f"Unsupported file type: {suffix}")
