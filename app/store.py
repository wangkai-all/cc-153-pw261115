"""A tiny local, versioned object store used as the demo source system.

It deliberately mirrors the parts of S3-style object storage this demo cares
about: lexicographic prefix listing with opaque, expirable continuation cursors,
immutable versions with delete markers, ETag/range downloads, and fault
injection for interrupted transfers.
"""
from __future__ import annotations

import base64
import json
import os
import re
import shutil
import socket
import sqlite3
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .common import json_dumps, sha256_bytes, utcnow, valid_key

DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 100
MAX_BODY = 8 * 1024 * 1024

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS versions (
    version_id TEXT PRIMARY KEY,
    key TEXT NOT NULL,
    seq INTEGER NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    etag TEXT,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    is_delete_marker INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    blob TEXT
);
CREATE INDEX IF NOT EXISTS idx_versions_key_seq ON versions(key, seq DESC);
CREATE TABLE IF NOT EXISTS heads (
    key TEXT PRIMARY KEY,
    current_version TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS faults (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    mode TEXT NOT NULL,
    after_bytes INTEGER NOT NULL DEFAULT 0,
    times_left INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT ''
);
"""


def _db(data_dir: str) -> str:
    return os.path.join(data_dir, "store.db")


def _connect(data_dir: str) -> sqlite3.Connection:
    conn = sqlite3.connect(_db(data_dir), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _prefix_bounds(prefix: str) -> tuple[str, str | None]:
    if not prefix:
        return "", None
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    return prefix, upper


def _encode_cursor(key: str, epoch: int) -> str:
    raw = json.dumps({"e": epoch, "k": key}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(token: str) -> dict[str, Any] | None:
    try:
        pad = "=" * (-len(token) % 4)
        data = json.loads(base64.urlsafe_b64decode(token + pad))
        if not isinstance(data, dict) or not isinstance(data.get("k"), str):
            return None
        return data
    except Exception:
        return None


class CursorExpired(Exception):
    pass


class Store:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.blob_root = os.path.join(self.data_dir, "blobs")
        os.makedirs(self.blob_root, exist_ok=True)
        with _connect(self.data_dir) as conn:
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO meta(k, v) VALUES ('cursor_epoch', '0')"
            )
        self._put_lock = threading.Lock()

    # ---------------------------------------------------------------- blobs
    def _blob_path(self, etag: str) -> str:
        return os.path.join(self.blob_root, etag[:2], etag[2:4], etag)

    def _store_blob(self, data: bytes, etag: str) -> str:
        path = self._blob_path(etag)
        if os.path.exists(path):
            return path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp-" + str(os.getpid()) + "-" + str(threading.get_ident())
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        return path

    # ------------------------------------------------------------- versions
    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> dict:
        if not valid_key(key):
            raise ValueError("invalid object key")
        etag = sha256_bytes(data)
        blob = self._store_blob(data, etag)
        now = utcnow()
        with self._put_lock, _connect(self.data_dir) as conn:
            conn.execute("BEGIN IMMEDIATE")
            seq = int(conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM versions").fetchone()[0])
            version_id = "v%08d" % seq
            conn.execute(
                "INSERT INTO versions(version_id, key, seq, size, etag, content_type,"
                " is_delete_marker, created_at, blob) VALUES (?,?,?,?,?,?,0,?,?)",
                (version_id, key, seq, len(data), etag, content_type, now, blob),
            )
            conn.execute(
                "INSERT INTO heads(key, current_version, updated_at) VALUES (?,?,?)"
                " ON CONFLICT(key) DO UPDATE SET current_version=excluded.current_version,"
                " updated_at=excluded.updated_at",
                (key, version_id, now),
            )
            conn.execute("COMMIT")
        return self.head(key)

    def delete(self, key: str) -> dict:
        """Create a delete marker, S3 versioning style. The key stays in history."""
        if not valid_key(key):
            raise ValueError("invalid object key")
        now = utcnow()
        with self._put_lock, _connect(self.data_dir) as conn:
            conn.execute("BEGIN IMMEDIATE")
            seq = int(conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM versions").fetchone()[0])
            version_id = "v%08d" % seq
            conn.execute(
                "INSERT INTO versions(version_id, key, seq, size, etag, content_type,"
                " is_delete_marker, created_at, blob) VALUES (?,?,?,0,NULL,"
                "'application/x-directory',1,?,NULL)",
                (version_id, key, seq, now),
            )
            conn.execute(
                "INSERT INTO heads(key, current_version, updated_at) VALUES (?,?,?)"
                " ON CONFLICT(key) DO UPDATE SET current_version=excluded.current_version,"
                " updated_at=excluded.updated_at",
                (key, version_id, now),
            )
            conn.execute("COMMIT")
        return self.head(key)

    def head(self, key: str, version_id: str | None = None) -> dict:
        with _connect(self.data_dir) as conn:
            if version_id:
                row = conn.execute(
                    "SELECT * FROM versions WHERE key=? AND version_id=?",
                    (key, version_id),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT v.* FROM heads h JOIN versions v ON v.version_id=h.current_version"
                    " WHERE h.key=?",
                    (key,),
                ).fetchone()
        return self._version_row(row) if row else {"exists": False, "key": key}

    @staticmethod
    def _version_row(row: sqlite3.Row | None, current: bool = True) -> dict:
        if row is None:
            return {"exists": False}
        return {
            "exists": True,
            "key": row["key"],
            "version_id": row["version_id"],
            "seq": row["seq"],
            "size": row["size"],
            "etag": row["etag"],
            "content_type": row["content_type"],
            "is_delete_marker": bool(row["is_delete_marker"]),
            "created_at": row["created_at"],
            "is_current": current,
        }

    def versions(self, key: str) -> dict:
        with _connect(self.data_dir) as conn:
            head = conn.execute(
                "SELECT current_version FROM heads WHERE key=?", (key,)
            ).fetchone()
            if not head:
                return {"exists": False, "key": key, "versions": []}
            current = head["current_version"]
            rows = conn.execute(
                "SELECT * FROM versions WHERE key=? ORDER BY seq DESC", (key,)
            ).fetchall()
        return {
            "exists": True,
            "key": key,
            "current_version_id": current,
            "versions": [
                self._version_row(r, current=r["version_id"] == current) for r in rows
            ],
        }

    def list_page(
        self, prefix: str = "", cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE
    ) -> dict:
        limit = max(1, min(int(limit or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))
        prefix = prefix or ""
        if prefix and (not re.match(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$", prefix)):
            raise ValueError("invalid prefix")
        with _connect(self.data_dir) as conn:
            epoch = int(conn.execute("SELECT v FROM meta WHERE k='cursor_epoch'").fetchone()[0])
            after_key = ""
            if cursor:
                decoded = _decode_cursor(cursor)
                if decoded is None:
                    raise CursorExpired("malformed cursor")
                if int(decoded.get("e", -1)) != epoch:
                    raise CursorExpired("cursor epoch expired")
                after_key = decoded["k"]
            lo, hi = _prefix_bounds(prefix)
            if after_key and after_key >= lo:
                lo = after_key
                exclusive = True
            else:
                exclusive = False
            clauses = []
            params: list[Any] = []
            if lo:
                clauses.append("h.key > ?" if exclusive else "h.key >= ?")
                params.append(lo)
            if hi:
                clauses.append("h.key < ?")
                params.append(hi)
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            rows = conn.execute(
                "SELECT h.key, v.*, "
                " (SELECT COUNT(*) FROM versions v2 WHERE v2.key=h.key) AS version_count"
                " FROM heads h JOIN versions v ON v.version_id=h.current_version"
                + where
                + " ORDER BY h.key LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = []
        for row in rows:
            item = self._version_row(row)
            item["version_count"] = row["version_count"]
            items.append(item)
        next_cursor = None
        if has_more and items:
            next_cursor = _encode_cursor(items[-1]["key"], epoch)
        return {
            "prefix": prefix,
            "items": items,
            "next_cursor": next_cursor,
            "cursor_epoch": epoch,
            "page_size": limit,
        }

    # --------------------------------------------------------------- faults
    def add_fault(self, key: str, mode: str, after_bytes: int, times: int, note: str = "") -> dict:
        if mode not in ("truncate",):
            raise ValueError("unsupported fault mode")
        if key != "*":
            if not valid_key(key):
                raise ValueError("invalid fault key")
        after_bytes = max(1, int(after_bytes))
        times = max(1, min(int(times), 50))
        now = utcnow()
        with _connect(self.data_dir) as conn:
            cur = conn.execute(
                "INSERT INTO faults(key, mode, after_bytes, times_left, created_at, note)"
                " VALUES (?,?,?,?,?,?)",
                (key, mode, after_bytes, times, now, note),
            )
            fault_id = cur.lastrowid
        return {
            "id": fault_id,
            "key": key,
            "mode": mode,
            "after_bytes": after_bytes,
            "times_left": times,
            "created_at": now,
            "note": note,
        }

    def list_faults(self) -> list[dict]:
        with _connect(self.data_dir) as conn:
            rows = conn.execute(
                "SELECT * FROM faults WHERE times_left > 0 ORDER BY id DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def clear_faults(self) -> int:
        with _connect(self.data_dir) as conn:
            cur = conn.execute("DELETE FROM faults")
            return cur.rowcount

    def _take_fault(self, key: str) -> dict | None:
        with _connect(self.data_dir) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM faults WHERE times_left > 0 AND (key=? OR key='*')"
                " ORDER BY (key = '*') ASC, id DESC LIMIT 1",
                (key,),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE faults SET times_left=times_left-1 WHERE id=?", (row["id"],)
                )
                conn.execute("DELETE FROM faults WHERE times_left <= 0")
            conn.execute("COMMIT")
        return dict(row) if row else None

    # --------------------------------------------------------------- cursor
    def invalidate_cursors(self) -> int:
        with _connect(self.data_dir) as conn:
            conn.execute("BEGIN IMMEDIATE")
            epoch = int(
                conn.execute("SELECT v FROM meta WHERE k='cursor_epoch'").fetchone()[0]
            ) + 1
            conn.execute("UPDATE meta SET v=? WHERE k='cursor_epoch'", (str(epoch),))
            conn.execute("COMMIT")
        return epoch

    def stats(self) -> dict:
        with _connect(self.data_dir) as conn:
            keys = conn.execute("SELECT COUNT(*) FROM heads").fetchone()[0]
            versions = conn.execute("SELECT COUNT(*) FROM versions").fetchone()[0]
            markers = conn.execute(
                "SELECT COUNT(*) FROM versions WHERE is_delete_marker=1"
            ).fetchone()[0]
            live = conn.execute(
                "SELECT COUNT(*) FROM heads h JOIN versions v ON v.version_id=h.current_version"
                " WHERE v.is_delete_marker=0"
            ).fetchone()[0]
            deleted = keys - live
            bytes_total = conn.execute(
                "SELECT COALESCE(SUM(size),0) FROM versions WHERE is_delete_marker=0"
            ).fetchone()[0]
            epoch = conn.execute(
                "SELECT v FROM meta WHERE k='cursor_epoch'"
            ).fetchone()[0]
        return {
            "keys": keys,
            "live_keys": live,
            "deleted_keys": deleted,
            "versions": versions,
            "delete_markers": markers,
            "bytes": bytes_total,
            "cursor_epoch": int(epoch),
            "blobs": sum(len(files) for _, _, files in os.walk(self.blob_root)),
        }

    def reset(self) -> None:
        with self._put_lock:
            with _connect(self.data_dir) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM faults")
                conn.execute("DELETE FROM heads")
                conn.execute("DELETE FROM versions")
                conn.execute("UPDATE meta SET v='0' WHERE k='cursor_epoch'")
                conn.execute("COMMIT")
            if os.path.isdir(self.blob_root):
                for name in os.listdir(self.blob_root):
                    target = os.path.join(self.blob_root, name)
                    if os.path.isdir(target):
                        shutil.rmtree(target, ignore_errors=True)
                    else:
                        try:
                            os.unlink(target)
                        except OSError:
                            pass
        self._seed_if_empty()

    def _seed_if_empty(self) -> None:
        from .seed import seed_store

        seed_store(self)


class StoreHandler(BaseHTTPRequestHandler):
    server_version = "DemoObjectStore/1.0"
    protocol_version = "HTTP/1.1"

    @property
    def store(self) -> Store:
        return self.server.store  # type: ignore[attr-defined]

    # ------------------------------------------------------------ utilities
    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("DEMO_HTTP_LOG", "1") == "1":
            print(f"[store] {self.address_string()} {fmt % args}", flush=True)

    def _send_json(self, status: int, payload: Any, extra_headers: dict[str, str] | None = None) -> None:
        body = json_dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > MAX_BODY:
            raise ValueError("request body too large")
        raw = self.rfile.read(length)
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("JSON object required")
        return data

    def _query(self) -> dict[str, list[str]]:
        return urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)

    def _path(self) -> str:
        return urllib.parse.urlsplit(self.path).path

    # --------------------------------------------------------------- routes
    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,Range,If-Match,X-Expected-Version")
        self.send_header("Access-Control-Expose-Headers", "ETag,X-Object-Version,Content-Range,Accept-Ranges,X-Demo-Fault")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = self._path()
        try:
            if path == "/healthz":
                self._send_json(200, {"ok": True, "service": "store", "stats": self.store.stats()})
            elif path == "/api/list":
                self._handle_list()
            elif path == "/api/versions":
                q = self._query()
                key = (q.get("key") or [""])[0]
                if not valid_key(key):
                    self._send_json(400, {"error": "invalid key"})
                    return
                self._send_json(200, self.store.versions(key))
            elif path == "/api/head":
                self._handle_head()
            elif path == "/api/download":
                self._handle_download()
            elif path == "/api/admin/stats":
                self._send_json(200, self.store.stats())
            elif path == "/api/admin/faults":
                self._send_json(200, {"faults": self.store.list_faults()})
            else:
                self._send_json(404, {"error": "not found", "path": path})
        except CursorExpired as exc:
            self._send_json(
                410,
                {
                    "code": "CURSOR_EXPIRED",
                    "message": str(exc),
                    "cursor_epoch": self.store.stats()["cursor_epoch"],
                },
            )
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self) -> None:  # noqa: N802
        path = self._path()
        try:
            body = self._read_json()
            if path == "/api/admin/put":
                key = str(body.get("key") or "")
                content = body.get("content")
                if content is None and "content_b64" in body:
                    content = base64.b64decode(body["content_b64"])
                    data = content
                elif isinstance(content, str):
                    data = content.encode("utf-8")
                else:
                    data = b""
                result = self.store.put(
                    key, data, str(body.get("content_type") or "application/octet-stream")
                )
                self._send_json(200, {"ok": True, "object": result})
            elif path == "/api/admin/delete":
                key = str(body.get("key") or "")
                marker = self.store.delete(key)
                result: dict[str, Any] = {"ok": True, "delete_marker": marker}
                if body.get("recreate"):
                    content = body.get("content", "")
                    if not isinstance(content, str):
                        content = str(content)
                    rebuilt = self.store.put(
                        key,
                        content.encode("utf-8"),
                        str(body.get("content_type") or "text/plain; charset=utf-8"),
                    )
                    result["recreated"] = rebuilt
                self._send_json(200, result)
            elif path == "/api/admin/fault":
                fault = self.store.add_fault(
                    key=str(body.get("key") or "*"),
                    mode=str(body.get("mode") or "truncate"),
                    after_bytes=int(body.get("after_bytes") or 1024),
                    times=int(body.get("times") or 1),
                    note=str(body.get("note") or ""),
                )
                self._send_json(200, {"ok": True, "fault": fault})
            elif path == "/api/admin/invalidate-cursors":
                epoch = self.store.invalidate_cursors()
                self._send_json(200, {"ok": True, "cursor_epoch": epoch})
            elif path == "/api/admin/seed":
                from .seed import seed_store

                seed_store(self.store, reset=bool(body.get("reset")))
                self._send_json(200, {"ok": True, "stats": self.store.stats()})
            else:
                self._send_json(404, {"error": "not found", "path": path})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_DELETE(self) -> None:  # noqa: N802
        path = self._path()
        if path == "/api/admin/faults":
            count = self.store.clear_faults()
            self._send_json(200, {"ok": True, "cleared": count})
        else:
            self._send_json(404, {"error": "not found", "path": path})

    # -------------------------------------------------------------- handlers
    def _handle_list(self) -> None:
        q = self._query()
        prefix = (q.get("prefix") or [""])[0]
        cursor = (q.get("cursor") or [None])[0] or None
        limit_raw = (q.get("limit") or [str(DEFAULT_PAGE_SIZE)])[0]
        try:
            limit = int(limit_raw)
        except ValueError:
            limit = DEFAULT_PAGE_SIZE
        self._send_json(200, self.store.list_page(prefix, cursor, limit))

    def _handle_head(self) -> None:
        q = self._query()
        key = (q.get("key") or [""])[0]
        version = (q.get("version") or [None])[0] or None
        if not valid_key(key):
            self._send_json(400, {"error": "invalid key"})
            return
        info = self.store.head(key, version)
        if not info.get("exists"):
            self._send_json(404, {"code": "NOT_FOUND", "key": key, "version_id": version})
            return
        headers = {
            "X-Object-Version": info["version_id"],
            "X-Object-Size": str(info["size"]),
            "Accept-Ranges": "bytes",
        }
        if info["is_delete_marker"]:
            headers["X-Delete-Marker"] = "true"
            self._send_json(404, {"code": "DELETE_MARKER", "object": info}, headers)
            return
        if info["etag"]:
            headers["ETag"] = '"%s"' % info["etag"]
        self._send_json(200, info, headers)

    def _parse_range(self, size: int) -> tuple[int, int] | None:
        header = self.headers.get("Range")
        if not header:
            return None
        match = re.match(r"^bytes=(\d*)-(\d*)$", header.strip())
        if not match:
            raise ValueError("unsupported range")
        start_s, end_s = match.groups()
        if start_s == "" and end_s == "":
            raise ValueError("unsupported range")
        if start_s == "":
            suffix = int(end_s)
            start = max(0, size - suffix)
            end = size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else size - 1
        end = min(end, size - 1)
        if start >= size or start > end:
            raise RangeNotSatisfiable(size)
        return start, end

    def _handle_download(self) -> None:
        q = self._query()
        key = (q.get("key") or [""])[0]
        version = (q.get("version") or [None])[0] or None
        if not valid_key(key):
            self._send_json(400, {"error": "invalid key"})
            return
        info = self.store.head(key, version)
        if not info.get("exists"):
            self._send_json(404, {"code": "NOT_FOUND", "key": key, "version_id": version})
            return
        base_headers = {
            "X-Object-Version": info["version_id"],
            "X-Object-Size": str(info["size"]),
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "ETag,X-Object-Version,Content-Range,Accept-Ranges,X-Demo-Fault",
        }
        if info["is_delete_marker"]:
            base_headers["X-Delete-Marker"] = "true"
            self._send_json(404, {"code": "DELETE_MARKER", "object": info}, base_headers)
            return

        # Conditional checks used by resumable clients.
        if_match = self.headers.get("If-Match")
        if if_match and if_match.strip().strip('"') not in (info["etag"], "*"):
            self._send_json(
                412,
                {
                    "code": "PRECONDITION_FAILED",
                    "message": "server object version/etag no longer matches",
                    "expected_version": info["version_id"],
                    "etag": info["etag"],
                },
                {"X-Object-Version": info["version_id"]},
            )
            return
        expected_version = self.headers.get("X-Expected-Version")
        if expected_version and expected_version != info["version_id"]:
            self._send_json(
                412,
                {
                    "code": "PRECONDITION_FAILED",
                    "message": "server object version changed",
                    "expected_version": info["version_id"],
                },
                {"X-Object-Version": info["version_id"]},
            )
            return

        etag = info["etag"]
        path = self.store._blob_path(etag) if etag else None
        if not path or not os.path.exists(path):
            self._send_json(410, {"code": "BLOB_MISSING", "message": "stored blob is missing"})
            return
        size = info["size"]
        try:
            byte_range = self._parse_range(size)
        except RangeNotSatisfiable as exc:
            self.send_response(416)
            self.send_header("Content-Range", "bytes */%d" % exc.size)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if byte_range:
            start, end = byte_range
            status = 206
            length = end - start + 1
            base_headers["Content-Range"] = "bytes %d-%d/%d" % (start, end, size)
        else:
            start, length = 0, size
            status = 200
        base_headers["ETag"] = '"%s"' % etag
        base_headers["Content-Type"] = info["content_type"]
        base_headers["Content-Length"] = str(length)

        fault = self.store._take_fault(key)
        cut = None
        if fault and fault["mode"] == "truncate":
            cut = min(int(fault["after_bytes"]), length)
            base_headers["X-Demo-Fault"] = "truncate-after-%d" % cut
        if cut is not None and cut < 0:
            cut = 0

        self.send_response(status)
        for hk, hv in base_headers.items():
            self.send_header(hk, hv)
        if cut is not None:
            self.send_header("Connection", "close")
            self.close_connection = True
        self.end_headers()

        remaining_fault = cut
        with open(path, "rb") as fh:
            fh.seek(start)
            left = length
            try:
                while left > 0:
                    chunk_size = min(64 * 1024, left)
                    if remaining_fault is not None:
                        if remaining_fault <= 0:
                            break
                        chunk_size = min(chunk_size, remaining_fault)
                    chunk = fh.read(chunk_size)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    left -= len(chunk)
                    if remaining_fault is not None:
                        remaining_fault -= len(chunk)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
                return
        if cut is not None:
            # Deliberately close mid-body: the client sees a short read.
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            try:
                self.connection.close()
            except OSError:
                pass


class RangeNotSatisfiable(Exception):
    def __init__(self, size: int):
        self.size = size


def create_server(host: str, port: int, data_dir: str) -> ThreadingHTTPServer:
    store = Store(data_dir)
    httpd = ThreadingHTTPServer((host, port), StoreHandler)
    httpd.daemon_threads = True
    httpd.store = store  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Local versioned object store (demo)")
    parser.add_argument("--host", default=os.environ.get("STORE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("STORE_PORT", "9001")))
    parser.add_argument("--data", default=os.environ.get("STORE_DATA", "var/store"))
    parser.add_argument("--seed", action="store_true", default=os.environ.get("STORE_SEED") == "1")
    args = parser.parse_args()
    os.makedirs(args.data, exist_ok=True)
    httpd = create_server(args.host, args.port, args.data)
    if args.seed:
        from .seed import seed_store

        seed_store(httpd.store)  # type: ignore[attr-defined]
    print(f"Object store: http://{args.host}:{args.port}  data={os.path.abspath(args.data)}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
