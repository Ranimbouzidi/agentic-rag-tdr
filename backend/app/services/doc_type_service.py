from __future__ import annotations

import re
from typing import Literal

DocType = Literal["tdr", "ami", "other", "unknown"]


def _norm(text: str) -> str:
    """
    Normalisation légère pour matcher les marqueurs même en OCR:
    - minuscule
    - enlever accents simples (optionnel minimal)
    - garder les espaces
    """
    t = (text or "").lower()
    # normaliser apostrophes
    t = t.replace("’", "'")
    return t


def detect_doc_type(text: str) -> DocType:
    """
    Détecte le type métier du document (pas le sous-type technique PDF).
    Retour: "tdr" | "ami" | "other"
    """
    t = _norm(text)

    # Marqueurs AMI (très fréquents Banque Mondiale / Tunisie)
    ami_markers = [
        "appel à manifestations d’intérêts",
        "appel à manifestation d’intérêt",
        "appel a manifestation d'interet",
        "appel a manifestations d'interets",
        "manifestations d'intérêt",
        "manifestations d’interêt",
        "manifester leur intérêt",
        "manifester leur interet",
        "invite les firmes",
        "invite les consultants",
        "les consultants intéressés doivent fournir",
        "les consultants interesses doivent fournir",
        "critères d’analyse des dossiers",
        "criteres d'analyse des dossiers",
        "barème de notation",
        "poids",
        "qcbs",
        "sfqc",
        "sélection fondée sur la qualité",
        "selection fondee sur la qualite",
        "règlement de passation des marchés",
        "reglement de passation des marches",
        "conflits d'intérêts",
        "conflits d'interets",
        "manifestations d'intérêt doivent être envoyées",
        "manifestations d'interet doivent etre envoyees",
        "\bappel\s+[àa]\s+manifestations?\s+d[’']int[eé]r[eê]t\b",
        "\bmanifestations?\s+d[’']int[eé]r[eê]t\b",
        "\bqcbs\b|\bqcb[s]?\b|\bsfqc\b",
        "\bliste\s+restreinte\b|\bshort\s*list\b|\bshortlist\b",
        "\bfirmes?\s+de\s+consultants?\b|\bfirms?\s+of\s+consultants?\b",
        "\bdoivent\s+[êe]tre\s+envoy[ée]es?\b|\bau\s+plus\s+tard\b|\bavant\s+le\b",
        "\bint[eé]r[eê]ss[ée]s?\b.*\bdoivent\s+fournir\b",

    ]

    # Marqueurs TdR
    tdr_markers = [
        "termes de référence",
        "termes de reference",
        "tdr",
        "cahier des charges",
        "description de la prestation",
        "profil du consultant",
        "livrables attendus",
        "objectifs de la mission",
        "\btermes?\s+de\s+r[eé]f[eé]rence\b",
        "\bterms?\s+of\s+reference\b",
        "\btdr\b",
        "\bterme\s+de\s+r[eé]f[eé]rence\b",
        "\bconsultant\b",
        "\bprestation\b",
        "\bmission\b",
        "\bobjectifs?\b",
        "\blivrables?\b",
        "\bprofil\b",
        "\bqualifications?\b",
        "\bm[eé]thodologie\b",
        "\bplanning\b|\bcalendrier\b",
        "\bmodalit[eé]s?\b",
    ]

    ami_score = sum(1 for m in ami_markers if m in t)
    tdr_score = sum(1 for m in tdr_markers if m in t)

    # Heuristique V1
    if ami_score >= 2 and ami_score >= tdr_score:
        return "ami"
    if tdr_score >= 2:
        return "tdr"

    # Cas où il y a "TdR détaillés peuvent être téléchargés" dans un AMI :
    # => on privilégie AMI si présence d'un vocabulaire de soumission
    if "manifestations d'intérêt" in t or "qcbs" in t or "sfqc" in t:
        return "ami"

    return "other"
