from typing import Optional
from docling.document_converter import DocumentConverter
from .base import BaseExtractor, ExtractedContent

def _normalize_text(s: Optional[str]) -> str:
    s = (s or "").replace("\r", "")
    # évite les énormes trous
    while "\n\n\n" in s:
        s = s.replace("\n\n\n", "\n\n")
    return s.strip()

class DoclingExtractor(BaseExtractor):
    def extract(self, pdf_path: str) -> ExtractedContent:
        converter = DocumentConverter()
        result = converter.convert(pdf_path)
        doc = result.document

        md: Optional[str] = None
        txt: Optional[str] = None

        # markdown propre si dispo
        if hasattr(doc, "export_to_markdown"):
            md = doc.export_to_markdown()
        else:
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
