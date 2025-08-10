from __future__ import annotations

from typing import Any, Dict

import typesense

from de_challenge.core.config import settings


def get_typesense_client() -> typesense.Client:  # type: ignore[name-defined]
    cfg: Dict[str, Any] = settings.typesense_config
    return typesense.Client(cfg)  # type: ignore[no-untyped-call]
