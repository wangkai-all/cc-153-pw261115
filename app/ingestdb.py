"""Persistence for the ingest service: jobs, discoveries, transfers, events."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Iterable

from .common import utcnow

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    prefix TEXT NOT NULL,
    page_size INTEGER NOT NULL DEFAULT 10,
    page_delay_ms INTEGER NOT NULL DEFAULT 600,
    include_history INTEGER NOT NULL DEFAULT 1,
    auto_import INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'created',
    phase TEXT NOT NULL DEFAULT 'idle',
    generation INTEGER NOT NULL DEFAULT 0,
    pages INTEGER NOT NULL DEFAULT 0,
    keys_count INTEGER NOT NULL DEFAULT 0,
    versions_count INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    cursor TEXT,
    error TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    hold INTEGER NOT NULL DEFAULT 0,
    rescan_requested INTEGER NOT NULL DEFAULT 0,
    scan_active INTEGER NOT NULL DEFAULT 0,
    import_active INTEGER NOT NULL DEFAULT 0,
    import_again INTEGER NOT NULL DEFAULT 0,
    options_json TEXT NOT NULL DEFAULT '{}',
    preselected_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS discoveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    etag TEXT,
    size INTEGER NOT NULL DEFAULT 0,
    content_type TEXT,
    is_delete_marker INTEGER NOT NULL DEFAULT 0,
    is_current INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'listing',
    generation INTEGER NOT NULL DEFAULT 0,
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    UNIQUE(job_id, key, version_id)
);
CREATE INDEX IF NOT EXISTS idx_disc_job ON discoveries(job_id, key, version_id);
CREATE TABLE IF NOT EXISTS transfers (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    key TEXT NOT NULL,
    version_id TEXT NOT NULL,
    etag TEXT,
    expected_sha256 TEXT,
    size INTEGER NOT NULL DEFAULT 0,
    content_type TEXT,
    state TEXT NOT NULL,
    bytes INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    follow_current INTEGER NOT NULL DEFAULT 1,
    staging_path TEXT,
    final_path TEXT,
    sha256 TEXT,
    origin TEXT NOT NULL DEFAULT 'import',
    reused_from TEXT,
    note TEXT,
    error TEXT,
    superseded_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_tr_job ON transfers(job_id, created_at);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    key TEXT,
    version_id TEXT,
    transfer_id TEXT,
    message TEXT,
    data_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_ev_job ON events(job_id, id);
"""


def db_path(data_dir: str) -> str:
    return os.path.join(data_dir, "ingest.db")


class IngestDB:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self._lock = threading.RLock()
        with self.conn() as conn:
            conn.executescript(_SCHEMA)

    def conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path(self.data_dir), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # ----------------------------------------------------------------- jobs
    def create_job(self, job_id: str, prefix: str, page_size: int, page_delay_ms: int,
                   include_history: bool, auto_import: bool, preselected: list[dict] | None = None) -> dict:
        now = utcnow()
        with self.conn() as conn:
            conn.execute(
                "INSERT INTO jobs(id, prefix, page_size, page_delay_ms, include_history,"
                " auto_import, options_json, preselected_json, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (job_id, prefix, page_size, page_delay_ms, int(include_history),
                 int(auto_import), "{}", json.dumps(preselected or [], ensure_ascii=False),
                 now, now),
            )
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict | None:
        with self.conn() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def list_jobs(self) -> list[dict]:
        with self.conn() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utcnow()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.conn() as conn:
            conn.execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))

    def request_cancel(self, job_id: str) -> None:
        self.update_job(job_id, cancel_requested=1, phase="cancelling")

    def request_rescan(self, job_id: str) -> None:
        self.update_job(job_id, rescan_requested=1)

    def take_rescan(self, job_id: str) -> bool:
        with self._lock, self.conn() as conn:
            row = conn.execute("SELECT rescan_requested FROM jobs WHERE id=?",
                               (job_id,)).fetchone()
            if not row or not row["rescan_requested"]:
                return False
            conn.execute("UPDATE jobs SET rescan_requested=0 WHERE id=?", (job_id,))
            return True

    def set_hold(self, job_id: str, hold: bool) -> None:
        self.update_job(job_id, hold=int(hold))

    def claim_scan(self, job_id: str) -> bool:
        with self._lock, self.conn() as conn:
            cur = conn.execute(
                "UPDATE jobs SET scan_active=1, updated_at=? WHERE id=? AND scan_active=0",
                (utcnow(), job_id),
            )
            return cur.rowcount == 1

    def release_scan(self, job_id: str) -> None:
        self.update_job(job_id, scan_active=0)

    def claim_import(self, job_id: str) -> bool:
        """Set import_active only if it was false. Guards duplicate import loops."""
        with self._lock, self.conn() as conn:
            cur = conn.execute(
                "UPDATE jobs SET import_active=1, updated_at=? WHERE id=? AND import_active=0",
                (utcnow(), job_id),
            )
            return cur.rowcount == 1

    def request_import_again(self, job_id: str) -> None:
        """A second import request arrived while an import loop is running."""
        self.update_job(job_id, import_again=1)

    def take_import_again(self, job_id: str) -> bool:
        with self._lock, self.conn() as conn:
            row = conn.execute("SELECT import_again FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row or not row["import_again"]:
                return False
            conn.execute("UPDATE jobs SET import_again=0 WHERE id=?", (job_id,))
            return True

    def release_import(self, job_id: str) -> None:
        self.update_job(job_id, import_active=0)

    def is_cancelled(self, job_id: str) -> bool:
        job = self.get_job(job_id)
        return bool(job and job["cancel_requested"])

    def job_summary(self, job: dict) -> dict:
        return {
            "id": job["id"],
            "prefix": job["prefix"],
            "page_size": job["page_size"],
            "page_delay_ms": job["page_delay_ms"],
            "include_history": bool(job["include_history"]),
            "auto_import": bool(job["auto_import"]),
            "status": job["status"],
            "phase": job["phase"],
            "generation": job["generation"],
            "pages": job["pages"],
            "keys": job["keys_count"],
            "versions": job["versions_count"],
            "changed": job["changed_count"],
            "error": job["error"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "started_at": job["started_at"],
            "finished_at": job["finished_at"],
            "cancel_requested": bool(job["cancel_requested"]),
            "hold": bool(job["hold"]),
            "rescan_requested": bool(job["rescan_requested"]),
            "preselected": json.loads(job["preselected_json"] or "[]"),
        }

    # ---------------------------------------------------------- discoveries
    def upsert_discovery(self, job_id: str, key: str, version_id: str, etag: str | None,
                         size: int, content_type: str | None, is_delete_marker: bool, *,
                         is_current: bool, source: str, generation: int) -> bool:
        """Insert or refresh a discovered version. Returns True when newly inserted."""
        now = utcnow()
        with self.conn() as conn:
            row = conn.execute(
                "SELECT id FROM discoveries WHERE job_id=? AND key=? AND version_id=?",
                (job_id, key, version_id),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE discoveries SET last_seen_at=?, is_current=?, source=?, generation=?"
                    " WHERE id=?",
                    (now, int(is_current), source, generation, row["id"]),
                )
                return False
            conn.execute(
                "INSERT INTO discoveries(job_id, key, version_id, etag, size, content_type,"
                " is_delete_marker, is_current, source, generation, discovered_at, last_seen_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, key, version_id, etag, size, content_type, int(is_delete_marker),
                 int(is_current), source, generation, now, now),
            )
        return True

    def mark_not_current(self, job_id: str, key: str, except_version: str) -> int:
        with self.conn() as conn:
            cur = conn.execute(
                "UPDATE discoveries SET is_current=0 WHERE job_id=? AND key=? AND version_id<>?",
                (job_id, key, except_version),
            )
            return cur.rowcount

    def list_discoveries(self, job_id: str, include_history: bool = True) -> list[dict]:
        with self.conn() as conn:
            rows = conn.execute(
                "SELECT * FROM discoveries WHERE job_id=? ORDER BY key, id", (job_id,)
            ).fetchall()
        out = []
        for r in rows:
            if not include_history and not r["is_current"]:
                continue
            out.append({
                "id": r["id"],
                "key": r["key"],
                "version_id": r["version_id"],
                "etag": r["etag"],
                "sha256": r["etag"],
                "size": r["size"],
                "content_type": r["content_type"],
                "is_delete_marker": bool(r["is_delete_marker"]),
                "is_current": bool(r["is_current"]),
                "source": r["source"],
                "generation": r["generation"],
                "discovered_at": r["discovered_at"],
                "last_seen_at": r["last_seen_at"],
            })
        return out

    def count_keys(self, job_id: str) -> int:
        with self.conn() as conn:
            row = conn.execute(
                "SELECT COUNT(DISTINCT key) FROM discoveries WHERE job_id=?", (job_id,)
            ).fetchone()
        return int(row[0])

    def discovery_keys_scope(self, job_id: str, generation: int) -> list[dict]:
        with self.conn() as conn:
            rows = conn.execute(
                "SELECT key, MAX(generation) AS gen FROM discoveries WHERE job_id=?"
                " GROUP BY key", (job_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- transfers
    def create_transfer(self, transfer_id: str, job_id: str, key: str, version_id: str,
                        etag: str | None, expected_sha256: str | None, size: int,
                        content_type: str | None, state: str, staging_path: str,
                        origin: str, note: str | None = None) -> dict:
        now = utcnow()
        with self.conn() as conn:
            conn.execute(
                "INSERT INTO transfers(id, job_id, key, version_id, etag, expected_sha256,"
                " size, content_type, state, staging_path, origin, note, created_at,"
                " updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (transfer_id, job_id, key, version_id, etag, expected_sha256, size,
                 content_type, state, staging_path, origin, note, now, now),
            )
        return self.get_transfer(transfer_id)

    def get_transfer(self, transfer_id: str) -> dict | None:
        with self.conn() as conn:
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        return dict(row) if row else None

    def update_transfer(self, transfer_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utcnow()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.conn() as conn:
            conn.execute(f"UPDATE transfers SET {cols} WHERE id=?", (*fields.values(), transfer_id))

    def list_transfers(self, job_id: str) -> list[dict]:
        with self.conn() as conn:
            rows = conn.execute(
                "SELECT * FROM transfers WHERE job_id=? ORDER BY created_at, id", (job_id,)
            ).fetchall()
        return [dict(r) for r in rows]

    def completed_match(self, job_id: str, key: str, version_id: str, etag: str | None,
                        size: int) -> dict | None:
        with self.conn() as conn:
            row = conn.execute(
                "SELECT * FROM transfers WHERE job_id=? AND key=? AND version_id=? AND state='complete'"
                " ORDER BY completed_at DESC LIMIT 1",
                (job_id, key, version_id),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        if item["etag"] != etag or item["size"] != size:
            return None
        if item["final_path"] and os.path.exists(item["final_path"]):
            return item
        return None

    def find_landed_any_job(self, key: str, version_id: str, etag: str | None,
                            size: int) -> dict | None:
        """Find a fully landed identical version, even from an earlier job.

        Identity is (key, version_id, etag, size); the ETag is a SHA-256 of the
        content in this demo, so equal ETags mean byte-identical content.
        """
        with self.conn() as conn:
            row = conn.execute(
                "SELECT * FROM transfers WHERE key=? AND version_id=? AND etag=? AND size=?"
                " AND state='complete' AND final_path IS NOT NULL"
                " ORDER BY completed_at DESC LIMIT 1",
                (key, version_id, etag, size),
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        if item["final_path"] and os.path.exists(item["final_path"]):
            if item["sha256"] in (None, etag):
                return item
        return None

    def active_transfer_same_version(self, job_id: str, key: str, version_id: str) -> dict | None:
        with self.conn() as conn:
            row = conn.execute(
                "SELECT * FROM transfers WHERE job_id=? AND key=? AND version_id=?"
                " AND state IN ('pending','queued','downloading','interrupted','resuming')"
                " ORDER BY created_at DESC LIMIT 1",
                (job_id, key, version_id),
            ).fetchone()
        return dict(row) if row else None

    def reuse_record(self, job_id: str, key: str, version_id: str, source_id: str) -> dict | None:
        with self.conn() as conn:
            row = conn.execute(
                "SELECT * FROM transfers WHERE job_id=? AND key=? AND version_id=?"
                " AND origin='reused' AND reused_from=? ORDER BY created_at DESC LIMIT 1",
                (job_id, key, version_id, source_id),
            ).fetchone()
        return dict(row) if row else None

    # ---------------------------------------------------------------- events
    def add_event(self, job_id: str, kind: str, message: str = "", *,
                  key: str | None = None, version_id: str | None = None,
                  transfer_id: str | None = None, data: Any = None) -> dict:
        now = utcnow()
        with self.conn() as conn:
            cur = conn.execute(
                "INSERT INTO events(job_id, ts, kind, key, version_id, transfer_id, message,"
                " data_json) VALUES (?,?,?,?,?,?,?,?)",
                (job_id, now, kind, key, version_id, transfer_id, message,
                 json.dumps(data, ensure_ascii=False) if data is not None else None),
            )
            event_id = cur.lastrowid
        return {"id": event_id, "ts": now, "kind": kind, "message": message, "key": key,
                "version_id": version_id, "transfer_id": transfer_id, "data": data}

    def list_events(self, job_id: str, after: int = 0, limit: int = 2000) -> list[dict]:
        with self.conn() as conn:
            rows = conn.execute(
                "SELECT * FROM events WHERE job_id=? AND id>? ORDER BY id LIMIT ?",
                (job_id, after, limit),
            ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r["id"], "ts": r["ts"], "kind": r["kind"], "message": r["message"],
                "key": r["key"], "version_id": r["version_id"],
                "transfer_id": r["transfer_id"],
                "data": json.loads(r["data_json"]) if r["data_json"] else None,
            })
        return out

    def reset_all(self) -> None:
        with self._lock, self.conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for table in ("events", "transfers", "discoveries", "jobs"):
                conn.execute(f"DELETE FROM {table}")
            conn.execute("COMMIT")

    # ------------------------------------------------------------- dashboard
    def stats(self) -> dict:
        with self.conn() as conn:
            jobs = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            jobs_active = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('scanning','importing')"
            ).fetchone()[0]
            transfers = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
            complete = conn.execute(
                "SELECT COUNT(*) FROM transfers WHERE state='complete'"
            ).fetchone()[0]
            interrupted = conn.execute(
                "SELECT COUNT(*) FROM transfers WHERE state='interrupted'"
            ).fetchone()[0]
            conflicts = conn.execute(
                "SELECT COUNT(*) FROM transfers WHERE state='superseded_version_changed'"
            ).fetchone()[0]
            bytes_total = conn.execute(
                "SELECT COALESCE(SUM(bytes),0) FROM transfers"
            ).fetchone()[0]
        return {"jobs": jobs, "active_jobs": jobs_active, "transfers": transfers,
                "ingested": complete, "interrupted": interrupted,
                "version_conflicts": conflicts, "bytes_downloaded": bytes_total}


def reset_db(data_dir: str, keep: Iterable[str] = ()) -> None:
    path = db_path(data_dir)
    if os.path.exists(path):
        os.unlink(path)
