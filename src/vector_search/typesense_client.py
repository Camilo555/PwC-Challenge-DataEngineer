from __future__ import annotations

from typing import Any

import typesense

from core.config import settings


def get_typesense_client() -> Any:  # typesense.Client
    """Get configured Typesense client."""
    cfg = settings.typesense_config
    return typesense.Client(cfg)  # type: ignore[arg-type]
