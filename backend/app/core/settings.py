from pydantic_settings import BaseSettings
from pydantic import Field
import os


class Settings(BaseSettings):
    # Extraction / OCR
    EXTRACTOR_BACKEND: str = os.getenv("EXTRACTOR_BACKEND", "docling")
    EXTRACTOR_FALLBACK: str = os.getenv("EXTRACTOR_FALLBACK", "pymupdf")
    OCR_BACKEND: str = os.getenv("OCR_BACKEND", "rapidocr")
    OCR_MIN_CHARS: int = int(os.getenv("OCR_MIN_CHARS", "400"))
    OCR_MAX_PAGES: int = int(os.getenv("OCR_MAX_PAGES", "50"))
    OCR_PAGE_TEXT_THRESHOLD: int = int(os.getenv("OCR_PAGE_TEXT_THRESHOLD", "40"))

    # API
    api_port: int = Field(default=8000, alias="API_PORT")

    # Ollama
    ollama_base_url: str = Field(..., alias="OLLAMA_BASE_URL")
    llm_model: str = Field(..., alias="LLM_MODEL")
    embed_model: str = Field(..., alias="EMBED_MODEL")

    # Qdrant
    qdrant_url: str = Field(..., alias="QDRANT_URL")
    qdrant_collection: str = Field(..., alias="QDRANT_COLLECTION")

    # MinIO
    minio_endpoint: str = Field(..., alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(..., alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(..., alias="MINIO_SECRET_KEY")
    minio_bucket_raw: str = Field(..., alias="MINIO_BUCKET_RAW")
    minio_bucket_processed: str = Field(..., alias="MINIO_BUCKET_PROCESSED")

    # Database
    database_url: str = Field(..., alias="DATABASE_URL")

    # Phase 5 - Chunking
    chunk_target_chars: int = Field(default=900, alias="CHUNK_TARGET_CHARS")
    chunk_max_chars: int = Field(default=1400, alias="CHUNK_MAX_CHARS")
    chunk_overlap_chars: int = Field(default=120, alias="CHUNK_OVERLAP_CHARS")

    # Phase 5 - Perf
    # Phase 5 - Perf
    embed_batch_size: int = Field(default=16, alias="EMBED_BATCH_SIZE")
    embed_concurrency: int = Field(default=2, alias="EMBED_CONCURRENCY")  # ✅ AJOUT
    qdrant_upsert_batch: int = Field(default=64, alias="QDRANT_UPSERT_BATCH")


    class Config:
        env_file = "../.env"
        extra = "ignore"
        populate_by_name = True  # important quand on utilise alias


settings = Settings()
