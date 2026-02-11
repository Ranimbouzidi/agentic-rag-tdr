from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Literal
from app.core.settings import settings

import docx

# =========================
# Data model (return type)
# =========================
@dataclass
class ExtractedContent:
    markdown: Optional[str]
    text: str
    extractor: str  # "docling" | "pymupdf" | "ocr" | "hybrid" | "failed"


# =========================
# Helpers
# =========================
def _normalize_text(s: Optional[str]) -> str:
    s = (s or "").replace("\r", "")
    # éviter les énormes trous
    while "\n\n\n" in s:
        s = s.replace("\n\n\n", "\n\n")
    return s.strip()

def fix_mojibake(s: str) -> str:
    """
    Répare le cas classique: texte UTF-8 décodé par erreur en latin-1/cp1252,
    ex: "CompÃ©tences" -> "Compétences".
    Heuristique: on ne tente la réparation que si marqueurs présents.
    """
    if not s:
        return s
    if ("Ã" not in s) and ("Â" not in s):
        return s

    try:
        repaired = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        # garde seulement si ça améliore visiblement
        if repaired and (repaired.count("Ã") + repaired.count("Â")) < (s.count("Ã") + s.count("Â")):
            return repaired
    except Exception:
        pass
    return s



def _join_pages(pages: list[str]) -> str:
    return _normalize_text("\n\n".join(pages))


# =========================
# DOCX (optionnel, inchangé)
# =========================
def extract_text_from_docx(docx_path: Path) -> ExtractedContent:
    d = docx.Document(str(docx_path))
    txt = "\n".join(p.text for p in d.paragraphs).strip()
    return ExtractedContent(markdown=None, text=_normalize_text(txt), extractor="docx")


# =========================
# PDF: Docling / PyMuPDF / OCR
# =========================
def extract_from_pdf_docling(pdf_path: Path) -> ExtractedContent:
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
    """
    Détecte le sous-type PDF :
    - native_text : texte sur la majorité des pages
    - scanned     : quasi aucune page avec texte
    - mixed       : une partie texte, une partie vide (souvent scans)
    - encrypted_or_failed : chiffré ou erreur d’ouverture
    """
    import fitz

    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return PdfStats(
            kind="encrypted_or_failed",
            num_pages=0,
            avg_text_len=0.0,
            pct_low_text_pages=100.0,
            avg_images_per_page=0.0,
        )

    # PDF protégé
    try:
        if getattr(doc, "needs_pass", False):
            return PdfStats(
                kind="encrypted_or_failed",
                num_pages=len(doc),
                avg_text_len=0.0,
                pct_low_text_pages=100.0,
                avg_images_per_page=0.0,
            )
    except Exception:
        pass

    num_pages = len(doc)
    if num_pages == 0:
        return PdfStats(
            kind="encrypted_or_failed",
            num_pages=0,
            avg_text_len=0.0,
            pct_low_text_pages=100.0,
            avg_images_per_page=0.0,
        )

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

    # Heuristiques simples
    if pct_low >= 90.0:
        kind: PdfKind = "scanned"
    elif pct_low <= 20.0:
        kind = "native_text"
    else:
        kind = "mixed"

    return PdfStats(
        kind=kind,
        num_pages=num_pages,
        avg_text_len=avg_text,
        pct_low_text_pages=pct_low,
        avg_images_per_page=avg_imgs,
    )


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

    doc = fitz.open(str(pdf_path))
    pages = min(len(doc), max_pages)
    all_lines: list[str] = []

    for i in range(pages):
        page = doc.load_page(i)
        img = _render_pdf_page_to_image(page, zoom=2.0)
        all_lines.extend(ocr_page_rapidocr(img))

    return _normalize_text("\n".join(all_lines))


def extract_pdf_hybrid(pdf_path: Path, low_text_threshold: int = 40, max_pages: int = 50) -> ExtractedContent:
    """
    Mixte: texte natif quand dispo, OCR seulement sur pages faibles.
    """
    import fitz

    doc = fitz.open(str(pdf_path))
    pages = min(len(doc), max_pages)
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

    return ExtractedContent(markdown=None, text=_join_pages(out_pages), extractor="hybrid")


# ---------- Router ----------
def extract_pdf_smart(pdf_path: Path) -> ExtractedContent:
    # paramètres V1 (tu les as déjà dans settings)
    MIN_CHARS_TOTAL = settings.OCR_MIN_CHARS
    OCR_MAX_PAGES = settings.OCR_MAX_PAGES
    LOW_TEXT_PAGE_THRESHOLD = settings.OCR_PAGE_TEXT_THRESHOLD

    stats = classify_pdf(pdf_path, low_text_threshold=LOW_TEXT_PAGE_THRESHOLD)

    if stats.kind == "encrypted_or_failed":
        return ExtractedContent(markdown=None, text="", extractor="failed")

    # ✅ 1) DOC-LING FIRST (pour TOUS les cas)
    extracted: ExtractedContent
    try:
        extracted = extract_from_pdf_docling(pdf_path)
    except Exception:
        # ✅ 2) Fallback PyMuPDF si docling plante
        extracted = extract_from_pdf_pymupdf(pdf_path)

    # ✅ 3) Si le texte est “pauvre” => OCR en dernier recours
    # (surtout utile pour scanned/mixed, mais on le fait de manière générale)
    if len(extracted.text.strip()) < MIN_CHARS_TOTAL:
        # Cas scanned => OCR global
        if stats.kind == "scanned":
            ocr_text = ocr_pdf_whole_rapidocr(pdf_path, max_pages=OCR_MAX_PAGES)
            if len(ocr_text.strip()) >= 50:
                return ExtractedContent(
                    markdown=extracted.markdown,  # garde le md docling si dispo
                    text=ocr_text,
                    extractor="ocr",
                )

        # Cas mixed => hybrid (texte natif quand dispo + OCR pages faibles)
        elif stats.kind == "mixed":
            hybrid = extract_pdf_hybrid(
                pdf_path,
                low_text_threshold=LOW_TEXT_PAGE_THRESHOLD,
                max_pages=OCR_MAX_PAGES
            )
            if len(hybrid.text.strip()) >= 50:
                return hybrid

            # si hybrid encore trop pauvre => OCR global
            ocr_text = ocr_pdf_whole_rapidocr(pdf_path, max_pages=OCR_MAX_PAGES)
            if len(ocr_text.strip()) >= 50:
                return ExtractedContent(markdown=None, text=ocr_text, extractor="ocr")

        # Cas native_text mais docling/pymupdf ont renvoyé trop peu => OCR global (rare mais safe)
        else:
            ocr_text = ocr_pdf_whole_rapidocr(pdf_path, max_pages=OCR_MAX_PAGES)
            if len(ocr_text.strip()) >= 50:
                return ExtractedContent(
                    markdown=extracted.markdown,
                    text=ocr_text,
                    extractor="ocr",
                )

    # ✅ Sinon on retourne l’extraction docling/pymupdf
    return extracted


# =========================
# Public entrypoint
# =========================
def extract_content(file_path: Path) -> ExtractedContent:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return extract_pdf_smart(file_path)
    if suffix == ".docx":
        return extract_text_from_docx(file_path)
    raise ValueError(f"Unsupported file type: {suffix}")
