from pydantic_settings import BaseSettings
from pydantic import Field
import os


class Settings(BaseSettings):
    # Extraction / OCR
    extractor_backend: str = Field(default=os.getenv("EXTRACTOR_BACKEND", "docling"), alias="EXTRACTOR_BACKEND")
    extractor_fallback: str = Field(default=os.getenv("EXTRACTOR_FALLBACK", "pymupdf"), alias="EXTRACTOR_FALLBACK")
    ocr_backend: str = Field(default=os.getenv("OCR_BACKEND", "rapidocr"), alias="OCR_BACKEND")
    ocr_min_chars: int = Field(default=int(os.getenv("OCR_MIN_CHARS", "400")), alias="OCR_MIN_CHARS")
    ocr_max_pages: int = Field(default=int(os.getenv("OCR_MAX_PAGES", "50")), alias="OCR_MAX_PAGES")
    ocr_page_text_threshold: int = Field(default=int(os.getenv("OCR_PAGE_TEXT_THRESHOLD", "40")), alias="OCR_PAGE_TEXT_THRESHOLD")

    # API
    api_port: int = Field(default=8000, alias="API_PORT")

    # Ollama (LLM + embeddings)
    ollama_base_url: str = Field(default="http://localhost:11434", alias="OLLAMA_BASE_URL")

    # IMPORTANT: mets bien un modèle qui existe dans `ollama list`
    # ex: llama3.1:8b
    llm_model: str = Field(default="llama3.1:8b", alias="LLM_MODEL")
    embed_model: str = Field(default="nomic-embed-text:latest", alias="EMBED_MODEL")

    # LLM runtime safety (évite les timeouts CPU)
    llm_timeout_s: float = Field(default=300.0, alias="LLM_TIMEOUT_S")
    llm_num_predict: int = Field(default=512, alias="LLM_NUM_PREDICT")  # tokens générés max

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

    # Chunking
    chunk_target_chars: int = Field(default=900, alias="CHUNK_TARGET_CHARS")
    chunk_max_chars: int = Field(default=1400, alias="CHUNK_MAX_CHARS")
    chunk_overlap_chars: int = Field(default=120, alias="CHUNK_OVERLAP_CHARS")

    # Perf
    embed_batch_size: int = Field(default=16, alias="EMBED_BATCH_SIZE")
    embed_concurrency: int = Field(default=2, alias="EMBED_CONCURRENCY")
    qdrant_upsert_batch: int = Field(default=64, alias="QDRANT_UPSERT_BATCH")

    # Search hybrid
    hybrid_w_vec: float = Field(default=0.70, alias="HYBRID_W_VEC")
    hybrid_w_lex: float = Field(default=0.30, alias="HYBRID_W_LEX")
    hybrid_pool_mult: int = Field(default=8, alias="HYBRID_POOL_MULT")
    per_doc_snippets: int = Field(default=3, alias="PER_DOC_SNIPPETS")
    max_per_doc_chunks: int = Field(default=3, alias="MAX_PER_DOC_CHUNKS")
    fallback_max_docs: int = Field(default=50, alias="FALLBACK_MAX_DOCS")

    # RAG
    rag_top_docs: int = Field(default=3, alias="RAG_TOP_DOCS")
    rag_snippets_per_doc: int = Field(default=3, alias="RAG_SNIPPETS_PER_DOCS")
    rag_max_context_chars: int = Field(default=9000, alias="RAG_MAX_CONTEXT_CHARS")
    rag_temperature: float = Field(default=0.2, alias="RAG_TEMPERATURE")
    rag_max_chunk_chars: int = Field(default=2500, alias="RAG_MAX_CHUNK_CHARS")


    class Config:
        env_file = "../.env"
        extra = "ignore"
        populate_by_name = True


settings = Settings()
