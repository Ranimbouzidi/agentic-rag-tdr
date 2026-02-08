from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import docx


# =========================
# Data model (return type)
# =========================
@dataclass
class ExtractedContent:
    markdown: Optional[str]
    text: str


# =========================
# Helpers
# =========================
def _normalize_text(s: Optional[str]) -> str:
    s = (s or "").replace("\r", "")
    # éviter les énormes trous
    while "\n\n\n" in s:
        s = s.replace("\n\n\n", "\n\n")
    return s.strip()


# =========================
# DOCX extraction (inchangé)
# =========================
def extract_text_from_docx(docx_path: Path) -> ExtractedContent:
    d = docx.Document(str(docx_path))
    txt = "\n".join(p.text for p in d.paragraphs).strip()
    return ExtractedContent(markdown=None, text=_normalize_text(txt))


# =========================
# PDF extraction (Docling + fallback)
# =========================
def extract_from_pdf_docling(pdf_path: Path) -> ExtractedContent:
    # import ici pour éviter d’imposer docling si pas utilisé
    from docling.document_converter import DocumentConverter

    converter = DocumentConverter()
    result = converter.convert(str(pdf_path))
    doc = result.document

    md: Optional[str] = None
    txt: Optional[str] = None

    # markdown propre si dispo
    if hasattr(doc, "export_to_markdown"):
        md = doc.export_to_markdown()
    else:
        # selon versions, export_to_text peut être “markdown-like”
        md = doc.export_to_text()

    # texte brut
    if hasattr(doc, "export_to_text"):
        txt = doc.export_to_text()
    else:
        txt = md or ""

    return ExtractedContent(
        markdown=_normalize_text(md),
        text=_normalize_text(txt),
    )


def extract_from_pdf_pymupdf(pdf_path: Path) -> ExtractedContent:
    # fallback rapide
    import fitz  # pymupdf

    doc = fitz.open(str(pdf_path))
    parts = []
    for page in doc:
        parts.append(page.get_text("text") or "")
    txt = _normalize_text("\n".join(parts))
    return ExtractedContent(markdown=None, text=txt)


def extract_pdf_with_fallback(pdf_path: Path) -> ExtractedContent:
    # Docling par défaut, fallback PyMuPDF si crash
    try:
        return extract_from_pdf_docling(pdf_path)
    except Exception:
        return extract_from_pdf_pymupdf(pdf_path)


# =========================
# Public entrypoint
# =========================
def extract_content(file_path: Path) -> ExtractedContent:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return extract_pdf_with_fallback(file_path)
    if suffix == ".docx":
        return extract_text_from_docx(file_path)
    raise ValueError(f"Unsupported file type: {suffix}")
