from pydantic_settings import BaseSettings
import os


class Settings(BaseSettings):
    EXTRACTOR_BACKEND: str = os.getenv("EXTRACTOR_BACKEND", "docling")
    EXTRACTOR_FALLBACK: str = os.getenv("EXTRACTOR_FALLBACK", "pymupdf") 
    OCR_BACKEND: str = os.getenv("OCR_BACKEND", "rapidocr")  # rapidocr | none (future: tesseract | azure)
    OCR_MIN_CHARS: int = int(os.getenv("OCR_MIN_CHARS", "400"))
    OCR_MAX_PAGES: int = int(os.getenv("OCR_MAX_PAGES", "50"))
    OCR_PAGE_TEXT_THRESHOLD: int = int(os.getenv("OCR_PAGE_TEXT_THRESHOLD", "40"))

    
    
    # API
    api_port: int = 8000

    # Ollama
    ollama_base_url: str
    llm_model: str
    embed_model: str

    # Qdrant
    qdrant_url: str
    qdrant_collection: str

    # MinIO
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket_raw: str
    minio_bucket_processed: str

    

    # Database
    database_url: str

    class Config:
        env_file = "../.env"
        extra = "ignore"


settings = Settings()
