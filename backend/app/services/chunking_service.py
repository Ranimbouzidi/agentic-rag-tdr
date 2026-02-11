# backend/app/services/chunking_service.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import re
import hashlib


# =========================
# Data model
# =========================
@dataclass
class Chunk:
    doc_id: str
    doc_type: str                 # "tdr" | "ami" | "other" | "unknown"
    section: str                  # e.g. "contexte" | "mission" | "livrables" | "profil" | "taches" | "competences" | "ami:criteres_selection"
    chunk_index: int
    text: str
    metadata: Dict[str, Any]
    competences: List[str]


# =========================
# Helpers
# =========================
def _norm_text(s: Optional[str]) -> str:
    s = (s or "").replace("\r", "")
    # compact blank lines
    s = re.sub(r"\n{3,}", "\n\n", s)
    # compact spaces
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()


def _looks_like_table_md(block: str) -> bool:
    """
    Heuristique Markdown table:
    - contains header separator line like | --- | --- |
    """
    b = block or ""
    if "|" not in b:
        return False
    # header separator row
    return bool(re.search(r"(?m)^\s*\|?[\s:\-]+\|[\s:\-\|]+\s*$", b))


def _extract_md_tables(text: str) -> List[str]:
    """
    Extract markdown tables (as text blocks) from a markdown-ish string.
    Keeps them as their own blocks so they don't dominate other sections.
    """
    if not text:
        return []

    lines = text.splitlines()
    tables: List[str] = []
    cur: List[str] = []
    in_table = False

    def flush():
        nonlocal cur
        if cur:
            t = "\n".join(cur).strip()
            if t and _looks_like_table_md(t):
                tables.append(t)
        cur = []

    for line in lines:
        # table-ish line if it has pipes and is not insanely long
        is_table_line = ("|" in line) and (len(line) <= 500)
        if is_table_line:
            in_table = True
            cur.append(line)
        else:
            if in_table:
                flush()
                in_table = False

    flush()
    return tables


def _remove_tables_from_text(text: str) -> str:
    """
    Remove markdown tables from a markdown-ish string to avoid polluting narrative chunks.
    (We index tables separately.)
    """
    if not text:
        return ""

    tables = _extract_md_tables(text)
    out = text
    for t in tables:
        # remove exact block once (safe)
        out = out.replace(t, "\n")
    return _norm_text(out)


def _split_sentences_fallback(text: str) -> List[str]:
    """
    Very light sentence splitter for OCR/noisy text.
    """
    t = _norm_text(text)
    if not t:
        return []
    # split on blank lines first (best signal)
    parts = [p.strip() for p in re.split(r"\n\s*\n", t) if p.strip()]
    if len(parts) >= 2:
        return parts
    # then on punctuation
    parts = [p.strip() for p in re.split(r"(?<=[\.\?!;:])\s+", t) if p.strip()]
    return parts


def _build_windows(text: str, target_chars: int, max_chars: int, overlap_chars: int) -> List[str]:
    """
    Greedy chunker with overlap based on paragraph/sentence units.
    """
    units = _split_sentences_fallback(text)
    if not units:
        return []

    chunks: List[str] = []
    cur: List[str] = []
    cur_len = 0

    def flush():
        nonlocal cur, cur_len
        if not cur:
            return
        chunk = _norm_text("\n\n".join(cur))
        if chunk:
            # hard cap
            if len(chunk) > max_chars:
                chunk = chunk[:max_chars].rstrip()
            chunks.append(chunk)
        cur = []
        cur_len = 0

    for u in units:
        u = u.strip()
        if not u:
            continue

        # if a single unit is huge, split it hard
        if len(u) > max_chars:
            # flush current
            flush()
            start = 0
            while start < len(u):
                end = min(start + max_chars, len(u))
                chunks.append(_norm_text(u[start:end]))
                start = max(0, end - overlap_chars)
            continue

        if cur_len + len(u) + 2 <= target_chars or not cur:
            cur.append(u)
            cur_len += len(u) + 2
        else:
            flush()
            # overlap: bring tail from previous chunk
            if overlap_chars > 0 and chunks:
                prev = chunks[-1]
                tail = prev[-overlap_chars:].strip()
                if tail:
                    cur = [tail, u]
                    cur_len = len(tail) + len(u) + 2
                else:
                    cur = [u]
                    cur_len = len(u) + 2
            else:
                cur = [u]
                cur_len = len(u) + 2

    flush()
    return chunks


def _section_priority(doc_type: str) -> List[str]:
    """
    Order matters: we want the most query-relevant sections to be indexed early.
    (Doesn't change correctness; helps debugging & consistent chunk ids.)
    """
    if doc_type == "ami":
        return [
            "contexte",
            "mission",
            "profil",
            "livrables",
            "taches",
            "ami:criteres_selection",
            "ami:deadline",
        ]
    # tdr / other
    return [
        "contexte",
        "mission",
        "livrables",
        "profil",
        "taches",
        "competences",
    ]


def _stable_hash(text: str, size: int = 10) -> str:
    h = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()
    return h[:size]


# =========================
# Public API
# =========================
def build_chunks_from_structured(
    structured: Dict[str, Any],
    target_chars: int = 900,
    max_chars: int = 1400,
    overlap_chars: int = 120,
) -> List[Chunk]:
    """
    Input: your structured JSON (TDR or AMI), example:
      {
        "doc_id": "...",
        "doc_type": "tdr"/"ami"/...,
        "metadata": {...},
        "sections": {...},
        "ami_fields": {...}  # optional
      }

    Output: List[Chunk] ready for embeddings + Qdrant payload.

    Strategy (optimal V1):
    - "Section-first" chunking: each section chunked independently
    - Tables are extracted (from markdown-ish sections) and indexed separately with "table:" prefix
    - "taches" list is indexed as (a) whole list summary + (b) per-item short chunks
    - competences list is indexed as a compact "skills" chunk
    """
    doc_id = str(structured.get("doc_id") or "").strip()
    if not doc_id:
        raise ValueError("structured missing doc_id")

    doc_type = (structured.get("doc_type") or structured.get("metadata", {}).get("doc_type") or "unknown").strip().lower()
    if not doc_type:
        doc_type = "unknown"

    metadata = structured.get("metadata") or {}
    sections = structured.get("sections") or {}
    competences_list = sections.get("competences") or []
    if isinstance(competences_list, str):
        competences_list = [competences_list]
    if not isinstance(competences_list, list):
        competences_list = []

    # Normalize skills
    competences: List[str] = []
    seen = set()
    for c in competences_list:
        s = str(c).strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        competences.append(s)

    chunks: List[Chunk] = []
    idx = 0

    # ---------
    # Helper to add chunk
    # ---------
    def add(section_name: str, text: str):
        nonlocal idx
        t = _norm_text(text)
        if not t:
            return
        chunks.append(
            Chunk(
                doc_id=doc_id,
                doc_type=doc_type,
                section=section_name,
                chunk_index=idx,
                text=t,
                metadata=metadata,
                competences=competences,
            )
        )
        idx += 1

    # ---------
    # 1) Index core narrative sections
    # ---------
    order = _section_priority(doc_type)
    for sec in order:
        if sec.startswith("ami:"):
            continue  # handled below

        raw = sections.get(sec)
        # special: lists
        if sec == "taches":
            continue
        if sec == "competences":
            continue

        if not isinstance(raw, str):
            raw = str(raw) if raw is not None else ""

        raw = _norm_text(raw)
        if not raw:
            continue

        # table-first: if section contains markdown tables, split them out
        tables = _extract_md_tables(raw)
        narrative = _remove_tables_from_text(raw)

        # narrative windows
        for w in _build_windows(narrative, target_chars, max_chars, overlap_chars):
            add(sec, w)

        # tables as separate chunks (small cap)
        for ti, tb in enumerate(tables[:8]):  # avoid exploding
            tb = _norm_text(tb)
            if tb:
                add(f"table:{sec}", tb)

    # ---------
    # 2) Tasks: list → chunks
    # ---------
    taches = sections.get("taches") or []
    if isinstance(taches, str):
        # sometimes OCR gives a block string
        taches = [t.strip() for t in taches.splitlines() if t.strip()]
    if not isinstance(taches, list):
        taches = []

    # summary chunk (query-friendly)
    if taches:
        summary = "Tâches / activités principales :\n- " + "\n- ".join([_norm_text(str(x)) for x in taches[:25] if _norm_text(str(x))])
        add("taches", summary)

        # per-item chunks (better recall for specific task queries)
        for item in taches[:40]:
            it = _norm_text(str(item))
            if it and len(it) >= 8:
                # stable micro id inside text helps debug
                add("tache:item", f"[task:{_stable_hash(it)}] {it}")

    # ---------
    # 3) Competences: compact chunk
    # ---------
    if competences:
        add("competences", "Compétences / mots-clés détectés : " + ", ".join(competences[:80]))

    # ---------
    # 4) AMI fields (if doc_type ami)
    # ---------
    ami_fields = structured.get("ami_fields") or {}
    if isinstance(ami_fields, dict) and doc_type == "ami":
        deadline = _norm_text(str(ami_fields.get("deadline") or ""))
        if deadline:
            add("ami:deadline", deadline)

        selection_method = _norm_text(str(ami_fields.get("selection_method") or ""))
        if selection_method:
            add("ami:selection_method", selection_method)

        emails = ami_fields.get("emails") or []
        if isinstance(emails, list) and emails:
            em = [str(e).strip() for e in emails if str(e).strip()]
            if em:
                add("ami:emails", "Contacts / emails : " + ", ".join(em[:20]))

        criteres = _norm_text(str(ami_fields.get("criteres_selection") or ""))
        if criteres:
            # same table-first logic
            tables = _extract_md_tables(criteres)
            narrative = _remove_tables_from_text(criteres)

            for w in _build_windows(narrative, target_chars, max_chars, overlap_chars):
                add("ami:criteres_selection", w)

            for tb in tables[:10]:
                add("table:ami_criteres_selection", tb)

    # Final guard: avoid empty
    return chunks
