from fastapi import FastAPI, Response
import requests
import sqlalchemy as sa
from qdrant_client import QdrantClient
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.core.settings import settings

app = FastAPI(title="Agentic RAG TdR API", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.get("/ready")
def ready():
    checks = {}

    # Qdrant
    try:
        qc = QdrantClient(url=settings.qdrant_url)
        qc.get_collections()
        checks["qdrant"] = "ok"
    except Exception as e:
        checks["qdrant"] = str(e)

    # Ollama
    try:
        r = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=3)
        checks["ollama"] = "ok" if r.status_code == 200 else f"status {r.status_code}"
    except Exception as e:
        checks["ollama"] = str(e)

    # MinIO (HTTP check)
    try:
        r = requests.get(settings.minio_endpoint, timeout=3)
        checks["minio"] = "ok" if r.status_code in (200, 403) else f"status {r.status_code}"
    except Exception as e:
        checks["minio"] = str(e)

    # Postgres
    try:
        engine = sa.create_engine(settings.database_url)
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = str(e)

    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": status, "checks": checks}
