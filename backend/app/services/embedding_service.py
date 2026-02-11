from __future__ import annotations

from typing import List
import httpx

from app.core.settings import settings


class EmbeddingError(RuntimeError):
    pass


def _ollama_embed_single(client: httpx.Client, text: str) -> List[float]:
    """
    Call Ollama embeddings endpoint for a single text.
    Ollama expects: POST /api/embeddings with {"model": "...", "prompt": "..."}
    Returns: {"embedding": [..]}
    """
    url = settings.ollama_base_url.rstrip("/") + "/api/embeddings"
    payload = {"model": settings.embed_model, "prompt": text}

    r = client.post(url, json=payload)
    if r.status_code != 200:
        raise EmbeddingError(f"Ollama embeddings failed: {r.status_code} {r.text}")

    data = r.json()
    emb = data.get("embedding")
    if not emb or not isinstance(emb, list):
        raise EmbeddingError(f"Invalid embedding response: {data}")

    return emb


def embed_batch(texts: List[str]) -> List[List[float]]:
    """
    Batch embedding (app-level).
    Ollama embeddings endpoint is single-prompt, so we parallelize with a small pool.
    We keep it safe for CPU by limiting concurrency.
    """
    if not texts:
        return []

    cleaned = []
    for t in texts:
        s = (t or "").strip()
        cleaned.append(s if s else " ")

    # ✅ Use the proper setting name
    concurrency = max(1, int(settings.embed_concurrency))

    timeout = httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=30.0)
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )

    results: List[List[float]] = [None] * len(cleaned)  # type: ignore

    def worker(i: int, txt: str, client: httpx.Client):
        results[i] = _ollama_embed_single(client, txt)

    import concurrent.futures

    with httpx.Client(timeout=timeout, limits=limits) as client:
        i = 0
        while i < len(cleaned):
            wave = list(enumerate(cleaned[i : i + concurrency], start=i))

            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
                futs = [ex.submit(worker, idx, txt, client) for idx, txt in wave]
                for f in futs:
                    f.result()

            i += concurrency

    return results  # type: ignore
