# backend/app/main.py
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import requests
import sqlalchemy as sa
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from qdrant_client import QdrantClient

from app.api.docs import router as docs_router
from app.api.index import router as index_router
from app.api.ingest import router as ingest_router
from app.api.process import router as process_router
from app.api.rag import router as rag_router
from app.api.search import router as search_router
from app.api.structure import router as structure_router
from app.core.settings import settings
from app.services.db_service import init_db

# ✅ Metrics service
from app.services.metrics_service import (
    API_UP,
    check_dependencies,
    refresh_documents_status_counts,
)

# ✅ OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

# -----------------------
# HTTP metrics (generic)
# -----------------------
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
)

# -----------------------
# OpenTelemetry init guard
# -----------------------
_TRACING_INITIALIZED = False


def init_tracing() -> None:
    """Init OTel tracer provider + exporter (Jaeger OTLP gRPC).

    Backend is running locally (Windows), Jaeger is running in Docker
    and port 4317 is published, so we send to localhost:4317.
    """
    global _TRACING_INITIALIZED
    if _TRACING_INITIALIZED:
        return

    resource = Resource.create(
        {
            "service.name": "agentic-rag-backend",
            "service.version": "0.1.0",
            "deployment.environment": "local",
        }
    )

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    exporter = OTLPSpanExporter(
        endpoint="http://localhost:4317",
        insecure=True,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))

    _TRACING_INITIALIZED = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    # -----------------------
    # Startup
    # -----------------------
    init_db()
    API_UP.set(1)

    # ✅ Tracing OpenTelemetry
    init_tracing()

    # Auto-instrument FastAPI + requests
    # (exclude /metrics to avoid noise)
    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls="/metrics",
    )
    RequestsInstrumentor().instrument()

    # Optional: SQLAlchemy instrumentation (best effort)
    # If you have a global engine object somewhere, you can pass it:
    # SQLAlchemyInstrumentor().instrument(engine=engine)
    try:
        SQLAlchemyInstrumentor().instrument()
    except Exception:
        pass

    async def tick():
        # refresh deps + doc counts every 10s
        while True:
            check_dependencies()
            refresh_documents_status_counts()
            await asyncio.sleep(10)

    task = asyncio.create_task(tick())

    try:
        yield
    finally:
        # -----------------------
        # Shutdown
        # -----------------------
        API_UP.set(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Agentic RAG TdR API",
    version="0.1.0",
    lifespan=lifespan,
)

# -----------------------
# CORS
# -----------------------
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

# -----------------------
# Routers
# -----------------------
app.include_router(ingest_router)
app.include_router(process_router)
app.include_router(structure_router)
app.include_router(index_router)
app.include_router(search_router)
app.include_router(docs_router)
app.include_router(rag_router)

# -----------------------
# Basic endpoints
# -----------------------
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

    # MinIO (HTTP)
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


# -----------------------
# HTTP metrics middleware
# -----------------------
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    process_time = time.time() - start_time

    REQUEST_COUNT.labels(
        request.method,
        request.url.path,
        str(response.status_code),
    ).inc()

    REQUEST_LATENCY.labels(
        request.method,
        request.url.path,
    ).observe(process_time)

    return response
