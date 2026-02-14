from __future__ import annotations

import time
import sqlalchemy as sa
import requests
from prometheus_client import Counter, Gauge, Histogram
from qdrant_client import QdrantClient

from app.core.settings import settings
from app.services.db_service import engine  # <-- adapte si ton engine est ailleurs

# --------------------
# API / Dependencies
# --------------------
API_UP = Gauge("api_up", "API is up (1 = running)")

DEPENDENCY_UP = Gauge(
    "dependency_up",
    "Dependency availability (1 = ok, 0 = down)",
    ["dep"],
)

DEPENDENCY_LAST_CHECK = Gauge(
    "dependency_last_check_seconds",
    "Unix time of last dependency check",
    ["dep"],
)

# --------------------
# Pipeline (business)
# --------------------
PIPELINE_STEP_TOTAL = Counter(
    "pipeline_step_total",
    "Pipeline step executions",
    ["step", "result"],  # success|error
)

PIPELINE_STEP_DURATION = Histogram(
    "pipeline_step_duration_seconds",
    "Pipeline step duration",
    ["step", "result"],
)

# --------------------
# Documents status
# --------------------
DOCUMENTS_STATUS_TOTAL = Gauge(
    "documents_status_total",
    "Number of documents by status",
    ["status"],
)

KNOWN_STATUSES = ["uploaded", "extracted", "structured", "indexed", "error"]


def check_dependencies() -> dict[str, bool]:
    result: dict[str, bool] = {}
    now = time.time()

    # Postgres
    ok = False
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        ok = True
    except Exception:
        ok = False
    result["postgres"] = ok

    # Qdrant
    ok = False
    try:
        qc = QdrantClient(url=settings.qdrant_url)
        qc.get_collections()
        ok = True
    except Exception:
        ok = False
    result["qdrant"] = ok

    # MinIO (HTTP check)
    ok = False
    try:
        r = requests.get(settings.minio_endpoint, timeout=3)
        ok = r.status_code in (200, 403)
    except Exception:
        ok = False
    result["minio"] = ok

    # Ollama
    ok = False
    try:
        r = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=5)
        ok = (r.status_code == 200)
    except Exception:
        ok = False
    result["ollama"] = ok

    # Export gauges
    for dep, is_ok in result.items():
        DEPENDENCY_UP.labels(dep=dep).set(1 if is_ok else 0)
        DEPENDENCY_LAST_CHECK.labels(dep=dep).set(now)

    return result


def refresh_documents_status_counts() -> None:
    # reset known statuses (avoid stale values)
    for s in KNOWN_STATUSES:
        DOCUMENTS_STATUS_TOTAL.labels(status=s).set(0)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text("""
                SELECT status, COUNT(*) AS c
                FROM documents
                GROUP BY status
            """)).fetchall()

        for status, c in rows:
            DOCUMENTS_STATUS_TOTAL.labels(status=str(status)).set(int(c))
    except Exception:
        # keep last values if DB temporarily unreachable
        pass
