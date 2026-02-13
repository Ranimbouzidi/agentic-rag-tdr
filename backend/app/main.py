# backend/app/main.py
from __future__ import annotations

from contextlib import asynccontextmanager

import requests
import sqlalchemy as sa
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from qdrant_client import QdrantClient

from app.core.settings import settings
from app.services.db_service import init_db

from app.api.ingest import router as ingest_router
from app.api.process import router as process_router
from app.api.structure import router as structure_router
from app.api.index import router as index_router
from app.api.search import router as search_router
from app.api.docs import router as docs_router
from app.api.rag import router as rag_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    yield
    # Shutdown (optional)


app = FastAPI(
    title="Agentic RAG TdR API",
    version="0.1.0",
    lifespan=lifespan,
)

# ✅ CORS (pour le front Vite / lovable)
# - allow_methods=["*"] est CRITIQUE pour que OPTIONS (preflight) marche
# - si tu déploies plus tard, tu ajouteras ton domaine ici
cors_origins = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:8081",
    "http://127.0.0.1:8081",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ routers
app.include_router(ingest_router)
app.include_router(process_router)
app.include_router(structure_router)
app.include_router(index_router)
app.include_router(search_router)
app.include_router(docs_router)
app.include_router(rag_router)


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
        r = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=5)
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
