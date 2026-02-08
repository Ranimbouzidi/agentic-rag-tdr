from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

@dataclass
class ExtractedContent:
    markdown: Optional[str]
    text: str

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, pdf_path: str) -> ExtractedContent:
        ...
