import fitz
from .base import BaseExtractor, ExtractedContent

def _normalize_text(s: str) -> str:
    s = (s or "").replace("\r", "")
    while "\n\n\n" in s:
        s = s.replace("\n\n\n", "\n\n")
    return s.strip()

class PyMuPDFExtractor(BaseExtractor):
    def extract(self, pdf_path: str) -> ExtractedContent:
        doc = fitz.open(pdf_path)
        parts = []
        for page in doc:
            parts.append(page.get_text("text"))
        txt = _normalize_text("\n".join(parts))
        return ExtractedContent(markdown=None, text=txt)
