"""
English name normalization for approximate association matching.

Approach: lowercase -> strip non-alphanumeric -> tokenize -> drop stopwords
-> stem with NLTK SnowballStemmer. Two names match iff their normalized token
lists are equal.
"""

import re

from nltk.stem.snowball import SnowballStemmer

STOPWORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "and",
        "at",
        "by",
        "for",
        "in",
        "of",
        "on",
        "or",
        "the",
        "to",
        "with",
    }
)

_TOKEN_RE = re.compile(r"[a-z0-9]+")

_STEMMER = SnowballStemmer("english")


def normalize_name(value: str) -> list[str]:
    if not value:
        return []
    tokens = _TOKEN_RE.findall(value.lower())
    return [_STEMMER.stem(t) for t in tokens if t not in STOPWORDS]


def name_match(a: str, b: str) -> bool:
    na = normalize_name(a)
    nb = normalize_name(b)
    return bool(na) and na == nb
