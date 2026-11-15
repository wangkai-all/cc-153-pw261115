"""Shared helpers for the local object-store ingest demo."""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from typing import Any

# Object keys are validated so the local filesystem store cannot escape its root.
_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$")


def valid_key(key: str) -> bool:
    if not _KEY_RE.match(key or ""):
        return False
    if key.endswith("/") or "//" in key:
        return False
    return all(part not in ("", ".", "..") for part in key.split("/"))


def utcnow() -> str:
    """ISO-8601 UTC timestamp with millisecond precision."""
    now = time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + ".%03dZ" % int(now % 1 * 1000)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_loads(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"))


def json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def new_uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
