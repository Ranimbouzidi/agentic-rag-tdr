from fastapi import FastAPI, Response
import requests
import sqlalchemy as sa
from qdrant_client import QdrantClient
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.core.settings import settings
from app.api.ingest import router as ingest_router

app = FastAPI(title="Agentic RAG TdR API", version="0.1.0")
app.include_router(ingest_router)

from app.services.db_service import init_db

init_db()

#hedhy l endpoint mtei loula c est pour verifier la disponibilité de l api 

@app.get("/health")
def health():
    return {"status": "ok"}

#hedhy l endpoint mtei li bch yakra biha prometheus l metrics mta3na w yjibha b format eli yefhamha prometheus w hneya generate_latest() mta3 prometheus client library li tgeneri l metrics b format eli yefhamha prometheus w CONTENT_TYPE_LATEST houwa l content type eli yest3mlou prometheus bch y3rfou format mta3 l data eli jeyha
@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)

#hedhy l endpoint mtei pour verifier la disponibilite les services eli hatithom fil docker compose lkol o mnhom ollama zeda 
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
