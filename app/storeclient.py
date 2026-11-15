"""HTTP client for the local demo object store.

Only the standard library is used so the demo runs without installing anything.
The client returns raw responses plus per-attempt byte counters so the ingest
layer can persist resume offsets after every chunk.
"""
from __future__ import annotations

import http.client
import json
import os
import socket
import time
import urllib.parse
from typing import Any, Callable, Iterator

from .common import json_loads


class StoreError(Exception):
    def __init__(self, message: str, status: int | None = None, body: Any = None):
        super().__init__(message)
        self.status = status
        self.body = body


class StoreClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("store URL must be http(s)")
        self.scheme = parsed.scheme
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.base_path = parsed.path.rstrip("/")
        self.timeout = timeout

    def _conn(self, timeout: float | None = None) -> http.client.HTTPConnection:
        if self.scheme == "https":
            import ssl

            return http.client.HTTPSConnection(
                self.host, self.port, timeout=timeout or self.timeout,
                context=ssl.create_default_context(),
            )
        return http.client.HTTPConnection(self.host, self.port, timeout=timeout or self.timeout)

    def _url(self, path: str, params: dict | None = None) -> str:
        url = self.base_path + path
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            url += "?" + urllib.parse.urlencode(clean)
        return url

    # ---------------------------------------------------------------- JSON API
    def request_json(self, method: str, path: str, params: dict | None = None,
                     body: Any = None, headers: dict | None = None) -> tuple[int, dict]:
        raw = json.dumps(body).encode() if body is not None else None
        hdrs = {"Accept": "application/json"}
        if raw is not None:
            hdrs["Content-Type"] = "application/json"
        hdrs.update(headers or {})
        conn = self._conn()
        try:
            conn.request(method, self._url(path, params), body=raw, headers=hdrs)
            resp = conn.getresponse()
            data = resp.read()
            status = resp.status
        except (OSError, http.client.HTTPException) as exc:
            raise StoreError(f"connection failed: {exc}") from exc
        finally:
            conn.close()
        try:
            payload = json_loads(data) if data else {}
        except Exception:
            payload = {"raw": data[:500].decode("utf-8", "replace")}
        if status >= 400:
            raise StoreError(str(payload.get("message") or payload.get("error") or f"HTTP {status}"),
                             status, payload)
        return status, payload

    def list_page(self, prefix: str, cursor: str | None, limit: int) -> dict:
        _, payload = self.request_json(
            "GET", "/api/list",
            params={"prefix": prefix, "cursor": cursor, "limit": limit},
        )
        return payload

    def head(self, key: str, version: str | None = None) -> dict:
        _, payload = self.request_json(
            "GET", "/api/head", params={"key": key, "version": version}
        )
        return payload

    def versions(self, key: str) -> dict:
        _, payload = self.request_json("GET", "/api/versions", params={"key": key})
        return payload

    # --------------------------------------------------------------- download
    def download(self, key: str, version: str, offset: int = 0,
                 expected_etag: str | None = None,
                 on_chunk: Callable[[int], None] | None = None,
                 stop_flag: Callable[[], bool] | None = None,
                 chunk_timeout: float = 30.0,
                 read_limit: int = 64 * 1024) -> Iterator[dict]:
        """Stream a version, resuming from ``offset`` bytes.

        Yields dict events:
          {"type": "meta", "status": , "headers": {...}}
          {"type": "chunk", "data": b"...", "received": n}
          {"type": "done", "received": n}
        Raises StoreError on a non-2xx response. A mid-stream disconnect simply
        ends the generator without a "done" event; callers persist what they
        received.
        """
        headers = {"Accept": "*/*"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        if expected_etag:
            headers["If-Match"] = f'"{expected_etag}"'
        conn = self._conn(timeout=chunk_timeout)
        meta_sent = False
        try:
            conn.request("GET", self._url("/api/download",
                                          {"key": key, "version": version}), headers=headers)
            resp = conn.getresponse()
            if resp.status >= 400:
                data = resp.read()
                try:
                    payload = json_loads(data) if data else {}
                except Exception:
                    payload = {}
                raise StoreError(
                    str(payload.get("message") or payload.get("code") or f"HTTP {resp.status}"),
                    resp.status, payload,
                )
            meta = {
                "status": resp.status,
                "headers": {k.lower(): v for k, v in resp.getheaders()},
            }
            etag_header = meta["headers"].get("etag", "").strip('"')
            if expected_etag and etag_header and etag_header != expected_etag:
                raise StoreError("server etag changed before transfer", 412, {
                    "code": "PRECONDITION_FAILED", "etag": etag_header,
                })
            meta_sent = True
            yield {"type": "meta", **meta}
            received = 0
            while True:
                if stop_flag and stop_flag():
                    return
                chunk = resp.read(read_limit)
                if not chunk:
                    break
                received += len(chunk)
                if on_chunk:
                    on_chunk(received)
                yield {"type": "chunk", "data": chunk, "received": received}
            yield {"type": "done", "received": received}
        except StoreError:
            raise
        except (OSError, http.client.HTTPException, socket.timeout) as exc:
            if meta_sent:
                # The body ended early. This is a normal, recoverable condition
                # for resumable ingest; tell the caller how far we got.
                yield {"type": "aborted", "message": f"{type(exc).__name__}: {exc}"}
                return
            raise StoreError(f"transfer could not start: {exc}") from exc
        finally:
            try:
                conn.close()
            except OSError:
                pass


def wait_for_store(base_url: str, attempts: int = 50, delay: float = 0.1) -> bool:
    client = StoreClient(base_url, timeout=2)
    for _ in range(attempts):
        try:
            status, _ = client.request_json("GET", "/healthz")
            if status == 200:
                return True
        except StoreError:
            pass
        time.sleep(delay)
    return False
