# backend/app/services/chunking_service.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import re
import hashlib
from collections import defaultdict


# =========================
# Data model
# =========================
@dataclass
class Chunk:
    doc_id: str
    doc_type: str
    section: str
    chunk_index: int          # ✅ IMPORTANT: index PAR SECTION
    text: str
    metadata: Dict[str, Any]
    competences: List[str]


# =========================
# Helpers
# =========================
def _norm_text(s: Optional[str]) -> str:
    s = (s or "").replace("\r", "")
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()


def _looks_like_table_md(block: str) -> bool:
    b = block or ""
    if "|" not in b:
        return False
    return bool(re.search(r"(?m)^\s*\|?[\s:\-]+\|[\s:\-\|]+\s*$", b))


def _extract_md_tables(text: str) -> List[str]:
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
    if not text:
        return ""
    tables = _extract_md_tables(text)
    out = text
    for t in tables:
        out = out.replace(t, "\n")
    return _norm_text(out)


def _split_sentences_fallback(text: str) -> List[str]:
    t = _norm_text(text)
    if not t:
        return []
    parts = [p.strip() for p in re.split(r"\n\s*\n", t) if p.strip()]
    if len(parts) >= 2:
        return parts
    parts = [p.strip() for p in re.split(r"(?<=[\.\?!;:])\s+", t) if p.strip()]
    return parts


def _build_windows(text: str, target_chars: int, max_chars: int, overlap_chars: int) -> List[str]:
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
            if len(chunk) > max_chars:
                chunk = chunk[:max_chars].rstrip()
            chunks.append(chunk)
        cur = []
        cur_len = 0

    for u in units:
        u = u.strip()
        if not u:
            continue

        # unit huge => hard split (safe overlap)
        if len(u) > max_chars:
            flush()
            start = 0
            n = len(u)
            while start < n:
                end = min(start + max_chars, n)
                chunks.append(_norm_text(u[start:end]))
                if end >= n:
                    break
                next_start = end - overlap_chars if overlap_chars > 0 else end
                if next_start <= start:
                    next_start = end
                start = next_start
            continue

        if cur_len + len(u) + 2 <= target_chars or not cur:
            cur.append(u)
            cur_len += len(u) + 2
        else:
            flush()
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


def _stable_hash(text: str, size: int = 10) -> str:
    h = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()
    return h[:size]


def _section_priority(doc_type: str) -> List[str]:
    # ✅ inclut les nouvelles sections
    if doc_type == "ami":
        return [
            "contexte",
            "mission",
            "profil",
            "livrables",
            "taches",          # texte section
            "evaluation",
            "candidature",
            "planning",
            "taches_table",
            "ami:criteres_selection",
            "ami:deadline",
        ]
    return [
        "contexte",
        "mission",
        "taches",          # texte section
        "livrables",
        "planning",
        "profil",
        "evaluation",
        "candidature",
        "taches_table",
        "competences",     # texte section (si jamais)
    ]


def _normalize_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        return [s]
    return [str(x).strip()] if str(x).strip() else []


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
    Build chunks for embeddings/Qdrant from structured JSON.

    - Section-first chunking
    - Markdown tables extracted and indexed separately as "table:<section>"
    - Tasks indexed as:
        (1) "taches" summary chunk
        (2) per-item "tache:item"
    - Skills indexed as "competences"
    - AMI extra fields indexed as "ami:*"
    - chunk_index is PER SECTION (stable neighbors within a section)
    """

    doc_id = str(structured.get("doc_id") or "").strip()
    if not doc_id:
        raise ValueError("structured missing doc_id")

    doc_type = (
        (structured.get("doc_type") or structured.get("metadata", {}).get("doc_type") or "unknown")
        .strip()
        .lower()
    )
    if not doc_type:
        doc_type = "unknown"

    metadata = structured.get("metadata") or {}
    sections = structured.get("sections") or {}

    # -------------------------
    # Normalize competences list
    # -------------------------
    competences_list = sections.get("competences") or []
    if isinstance(competences_list, str):
        competences_list = [competences_list]
    if not isinstance(competences_list, list):
        competences_list = []

    competences: List[str] = []
    seen = set()
    for c in competences_list:
        s = str(c).strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        competences.append(s)

    chunks: List[Chunk] = []

    # ✅ chunk_index stable per section
    section_counters: Dict[str, int] = {}

    def _normalize_list(x: Any) -> List[str]:
        if x is None:
            return []
        if isinstance(x, list):
            out = []
            for it in x:
                s = str(it or "").strip()
                if s:
                    out.append(s)
            return out
        if isinstance(x, str):
            # if OCR gave one big block
            lines = [l.strip() for l in x.splitlines() if l.strip()]
            return lines
        return []

    # -------------------------
    # Helper: add chunk
    # -------------------------
    def add(section_name: str, text: str):
        t = _norm_text(text)
        if not t:
            return

        # ✅ skip junk remnants like "|" or "--|" or tiny table separators
        if re.fullmatch(r"[\s\|\-:–—_]+", t):
            return
        pipe_count = t.count("|")
        alpha_count = len(re.findall(r"[A-Za-zÀ-ÿ]", t))
        digit_count = len(re.findall(r"[0-9]", t))
        alnum_count = alpha_count + digit_count

        if pipe_count >= 8 and alnum_count < 40:
            return
        if t.lstrip().startswith("|") and alnum_count < 60:
            return
        
        if alnum_count < 12 and not re.search(r"[A-Za-zÀ-ÿ]{4,}", t):
            return

        ci = section_counters.get(section_name, 0)
        section_counters[section_name] = ci + 1

        chunks.append(
            Chunk(
                doc_id=doc_id,
                doc_type=doc_type,
                section=section_name,
                chunk_index=ci,
                text=t,
                metadata=metadata,
                competences=competences,
            )
        )

    # -------------------------
    # 1) Index narrative sections (inclut nouvelles sections)
    # -------------------------
    order = _section_priority(doc_type)

    for sec in order:
        if sec.startswith("ami:"):
            continue  # handled in AMI block below

        raw = sections.get(sec)
        if raw is None:
            continue

        # sections expected as strings
        if not isinstance(raw, str):
            raw = str(raw)

        raw = _norm_text(raw)
        if not raw:
            continue

        # extract tables out of section content
        tables = _extract_md_tables(raw)
        narrative = _remove_tables_from_text(raw)

        # narrative windows
        for w in _build_windows(narrative, target_chars, max_chars, overlap_chars):
            add(sec, w)

        # tables separately
        for tb in tables[:10]:
            tb = _norm_text(tb)
            if tb:
                add(f"table:{sec}", tb)

    # -------------------------
    # 2) Tasks list (top-level) -> chunks
    # -------------------------
    # ✅ nouveau payload: structured["taches"] = liste (prioritaire)
    taches_list = structured.get("taches")
    if taches_list is None:
        # fallback ancien format
        taches_list = sections.get("taches")

    taches = _normalize_list(taches_list)

    if taches:
        summary_items = [_norm_text(x) for x in taches[:25] if _norm_text(x)]
        if summary_items:
            summary = "Tâches / activités principales :\n- " + "\n- ".join(summary_items)
            add("taches", summary)

        # per-item chunks
        for item in taches[:40]:
            it = _norm_text(item)
            if it and len(it) >= 8:
                add("tache:item", f"[task:{_stable_hash(it)}] {it}")

    # -------------------------
    # 3) Competences list -> compact chunk
    # -------------------------
    if competences:
        add("competences", "Compétences / mots-clés détectés : " + ", ".join(competences[:80]))

    # -------------------------
    # 4) AMI fields
    # -------------------------
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
            tables = _extract_md_tables(criteres)
            narrative = _remove_tables_from_text(criteres)

            for w in _build_windows(narrative, target_chars, max_chars, overlap_chars):
                add("ami:criteres_selection", w)

            for tb in tables[:10]:
                tb = _norm_text(tb)
                if tb:
                    add("table:ami_criteres_selection", tb)

    return chunks
