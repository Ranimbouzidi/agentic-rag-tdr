from app.core.settings import EXTRACTOR_BACKEND, EXTRACTOR_FALLBACK
from .base import BaseExtractor
from .docling_extractor import DoclingExtractor

def get_extractor() -> BaseExtractor:
    backend = (EXTRACTOR_BACKEND or "docling").lower()
    if backend == "docling":
        return DoclingExtractor()
    raise ValueError(f"Unknown extractor backend: {backend}")

def get_fallback_extractor() -> BaseExtractor | None:
    fb = (EXTRACTOR_FALLBACK or "none").lower()
    if fb in ("none", "", "null"):
        return None
    if fb == "pymupdf":
        from .pymupdf_extractor import PyMuPDFExtractor
        return PyMuPDFExtractor()
    raise ValueError(f"Unknown extractor fallback: {fb}")
