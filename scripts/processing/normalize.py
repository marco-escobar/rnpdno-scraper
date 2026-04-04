"""Shared text-normalization utilities for the RNPDNO cleaning pipeline.

Two tiers
---------
normalize_text          – standard: lowercase, strip accents, strip punctuation,
                          collapse whitespace.  Used as the primary join key.
normalize_text_aggressive – applies normalize_text, then expands common
                          abbreviations and removes the filler word "de".
                          Used as a second-pass join key when simple fails.
"""

import re
import unicodedata

# ---------------------------------------------------------------------------
# Abbreviation table – applied only during aggressive normalization.
# All patterns are word-boundary-anchored; replacements are lowercase.
# ---------------------------------------------------------------------------
_ABBREV_MAP = [
    (r"\bdr\b", "doctor"),
    (r"\bgral\b", "general"),
    (r"\bcd\b", "ciudad"),
    (r"\bsta\b", "santa"),
    (r"\bsto\b", "santo"),
    (r"\bmto\b", "maestro"),
    (r"\bprof\b", "profesor"),
    (r"\bing\b", "ingeniero"),
    (r"\blic\b", "licenciado"),
]


def normalize_text(text) -> str:
    """Standard normalization.

    Steps
    -----
    1. str()  + strip
    2. NFD decomposition -> drop non-spacing marks (accents)
    3. lowercase
    4. replace every non-[a-z0-9] char with a space
    5. collapse runs of whitespace to a single space

    Returns "" for None / NaN / empty input.
    """
    if text is None:
        return ""
    text = str(text).strip()
    if not text or text.lower() == "nan":
        return ""
    text = "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = re.sub(r" +", " ", text).strip()
    return text


def normalize_text_aggressive(text) -> str:
    """Aggressive normalization – second-pass fallback for geo-matching.

    Applies normalize_text then:
      - expands abbreviations (Dr -> doctor, Gral -> general, ...)
      - removes the standalone word ``de``
    """
    text = normalize_text(text)
    if not text:
        return text
    for pattern, expansion in _ABBREV_MAP:
        text = re.sub(pattern, expansion, text)
    text = re.sub(r"\bde\b", " ", text)
    text = re.sub(r" +", " ", text).strip()
    return text
