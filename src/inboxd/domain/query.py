import shlex
from datetime import UTC, datetime
from typing import Literal

SearchOrder = Literal["relevance", "newest", "oldest"]
MatchMode = Literal["all", "any"]
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def search_terms(text: str) -> list[str]:
    lexer = shlex.shlex(text, posix=True)
    lexer.quotes = '"'
    lexer.whitespace_split = True
    lexer.commenters = ""
    terms = list(lexer)

    if any(len(term) < 2 for term in terms):
        raise ValueError("Search terms must contain at least two characters")

    return terms


def time_value(value: str) -> int:
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError("Timestamp requires a timezone offset")

    delta = parsed.astimezone(UTC) - EPOCH

    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
