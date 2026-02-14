# backend/app/core/tracing.py
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Optional

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
# tes métriques pipeline existantes
from app.services.metrics_service import PIPELINE_STEP_TOTAL, PIPELINE_STEP_DURATION

tracer = trace.get_tracer("pipeline")


@contextmanager
def span_step(step: str, **attrs):
    """
    Crée un span OTel + alimente les métriques pipeline_step_*.
    Usage:
        with span_step("process.extract", doc_id=doc_id):
            ...
    """
    t0 = time.time()
    with tracer.start_as_current_span(step) as span:
        # attributs utiles pour filtrer dans Jaeger
        span.set_attribute("pipeline.step", step)
        for k, v in attrs.items():
            if v is not None:
                span.set_attribute(str(k), v)

        try:
            yield span
            result = "success"
        except Exception as e:
            result = "error"
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR))
            span.set_attribute("error", True)
            raise
        finally:
            dur = time.time() - t0
            PIPELINE_STEP_TOTAL.labels(step=step, result=result).inc()
            PIPELINE_STEP_DURATION.labels(step=step, result=result).observe(dur)
            span.set_attribute("pipeline.result", result)
            span.set_attribute("pipeline.duration_s", dur)
