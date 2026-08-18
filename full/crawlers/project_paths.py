#!/usr/bin/env python3
"""Shared filesystem locations for the full legal-text repository.

Environment variables keep the Git checkout portable while providing the
Dropbox defaults used for this project.
"""

import os
from pathlib import Path


def configured_path(variable, default):
    """Return an expanded user path from *variable* or its default."""
    value = os.environ.get(variable)
    return Path(value).expanduser() if value else default


DATA_ROOT = configured_path(
    "EU_REGULATION_DATA_DIR",
    Path.home() / "Dropbox" / "Projects" / "EU_Regulation" / "Data",
)
SHARED_ROOT = configured_path(
    "EU_REGULATION_SHARED_DIR",
    Path.home() / "Dropbox" / "Papers" / "00_Ideas" / "EU_Regulation",
)
CRAWLER_LOG_ROOT = configured_path(
    "EU_REGULATION_LOG_DIR",
    SHARED_ROOT / "logs" / "crawlers",
)
