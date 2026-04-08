"""
Extractor registry.

Maps URL patterns to custom extractor functions.
Each extractor returns List[Dict] with keys:
  club_name, league_name, city, state, source_url

When a URL matches a registered extractor, run.py uses it instead of
the generic table/list/link fallback.
"""

from __future__ import annotations

import re
from typing import Callable, List, Dict, Optional

_registry: list[tuple[re.Pattern, Callable]] = []


def register(pattern: str):
    """Decorator: register a function as the extractor for URLs matching pattern."""
    def decorator(fn: Callable):
        _registry.append((re.compile(pattern, re.IGNORECASE), fn))
        return fn
    return decorator


def get_extractor(url: str) -> Optional[Callable]:
    """Return the custom extractor for this URL, or None to use the generic path."""
    for pattern, fn in _registry:
        if pattern.search(url):
            return fn
    return None


# Import all extractors so their @register decorators fire
from extractors import girls_academy   # noqa: E402, F401
from extractors import norcal          # noqa: E402, F401
from extractors import ecnl            # noqa: E402, F401
from extractors import dpl             # noqa: E402, F401
from extractors import edp             # noqa: E402, F401
