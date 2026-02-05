from pydantic_settings import BaseSettings


class Settings(BaseSettings):
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
