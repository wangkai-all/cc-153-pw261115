"""Ingest worker: paged discovery, verification, resumable transfer, manifest.

Key correctness rules implemented here:

1. Identity of a source object is ``(key, version_id)``. The server's version
   id can be reused only if the generation changes, so each generation records
   the ETag observed at discovery. A version that is "complete" is only reused
   if key+version+ETag+size all match and the landed file still exists.
2. Nothing is ever appended to a file from a different source version. A partial
   download stores its ETag next to its bytes; before appending, the server is
   re-HEADed and the ETag must still match. If it changed, the partial file is
   quarantined, the old transfer is marked ``superseded_version_changed`` and a
   new transfer (and file) is started.
3. Skip/queue decisions never rely on filename or timestamp. They rely on the
   observed version id + content fingerprint.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import time
from typing import Any

from .common import json_dumps, sha256_bytes, utcnow, new_uid
from .ingestdb import IngestDB
from .storeclient import StoreClient, StoreError


class DownloadInterrupted(Exception):
    """The connection closed before the full body arrived."""


def safe_filename(key: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", key.replace("/", "__"))
    return name or "object"


def etag_matches(etag: str | None, header_etag: str | None) -> bool:
    if etag is None or header_etag is None:
        return True
    return etag.strip('"') == header_etag.strip('"')


class IngestService:
    def __init__(self, data_dir: str, store_url: str, page_delay_ms: int = 600,
                 chunk_timeout: float = 30.0):
        self.data_dir = os.path.abspath(data_dir)
        self.landing_root = os.path.join(self.data_dir, "landing")
        self.staging_root = os.path.join(self.data_dir, "partial")
        self.quarantine_root = os.path.join(self.data_dir, "quarantine")
        for root in (self.landing_root, self.staging_root, self.quarantine_root):
            os.makedirs(root, exist_ok=True)
        self.db = IngestDB(self.data_dir)
        self.store = StoreClient(store_url, timeout=chunk_timeout)
        self.page_delay_ms = page_delay_ms
        self.chunk_timeout = chunk_timeout
        self._threads: dict[str, list[threading.Thread]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ jobs
    def create_job(self, prefix: str, *, page_size: int = 10, page_delay_ms: int | None = None,
                   include_history: bool = True, auto_import: bool = False,
                   preselected: list[dict] | None = None, start: bool = True) -> dict:
        job_id = new_uid("job")
        job = self.db.create_job(
            job_id, prefix, max(1, min(int(page_size), 100)),
            self.page_delay_ms if page_delay_ms is None else max(0, int(page_delay_ms)),
            include_history, auto_import, preselected or [],
        )
        self.db.add_event(job_id, "job.created", f"scan prefix '{prefix or '(all)'}'", data={
            "prefix": prefix, "page_size": page_size,
        })
        if start:
            self.start_scan(job_id)
        return self.db.job_summary(self.db.get_job(job_id))

    def start_scan(self, job_id: str) -> bool:
        if not self.db.claim_scan(job_id):
            self.db.request_rescan(job_id)
            return False
        thread = threading.Thread(target=self._scan_loop, args=(job_id,),
                                  name=f"scan-{job_id}", daemon=True)
        with self._lock:
            self._threads.setdefault(job_id, []).append(thread)
        thread.start()
        return True

    def start_import(self, job_id: str) -> bool:
        if not self.db.claim_import(job_id):
            # A loop is already running. Remember the request so the running
            # loop performs another pass (e.g. resume after an interruption)
            # instead of silently dropping it.
            self.db.request_import_again(job_id)
            return False
        thread = threading.Thread(target=self._import_loop, args=(job_id,),
                                  name=f"import-{job_id}", daemon=True)
        with self._lock:
            self._threads.setdefault(job_id, []).append(thread)
        thread.start()
        return True

    def _wait_threads(self, job_id: str) -> None:
        with self._lock:
            threads = list(self._threads.get(job_id, []))
        for t in threads:
            if t is threading.current_thread():
                continue
            t.join(timeout=120)
        with self._lock:
            self._threads.pop(job_id, None)

    def shutdown(self) -> None:
        """Best-effort: ask loops to stop and wait for their threads."""
        for job in self.db.list_jobs():
            if job["status"] in ("scanning", "importing"):
                self.db.update_job(job["id"], cancel_requested=1)
        with self._lock:
            threads = [t for group in self._threads.values() for t in group]
        for t in threads:
            if t.is_alive():
                t.join(timeout=5)

    # ------------------------------------------------------------------ scan
    def _scan_loop(self, job_id: str) -> None:
        try:
            while True:
                self._run_scan(job_id)
                if not self.db.take_rescan(job_id):
                    break
                self.db.update_job(job_id, cancel_requested=0)
                self.db.add_event(job_id, "scan.extra_pass",
                                  "another rescan was requested; scanning again")
        except Exception as exc:  # pragma: no cover - defensive
            self.db.update_job(job_id, status="error", phase="error", error=repr(exc),
                               finished_at=utcnow())
            self.db.add_event(job_id, "scan.error", f"unexpected error: {exc!r}")
        finally:
            self.db.release_scan(job_id)
            if self.db.get_job(job_id) and self.db.get_job(job_id)["rescan_requested"]:
                self.start_scan(job_id)
            else:
                self._forget_thread(job_id, threading.current_thread())

    def take_rescan(self, job_id: str) -> bool:
        return self.db.take_rescan(job_id)

    def _run_scan(self, job_id: str) -> None:
        job = self.db.get_job(job_id)
        if not job:
            return
        blocking_import = bool(job["import_active"])
        generation = int(job["generation"]) + 1
        self.db.update_job(
            job_id,
            status="importing" if blocking_import else "scanning",
            phase="listing",
            generation=generation,
            started_at=job["started_at"] or utcnow(),
            error=None,
            pages=0,
        )
        self.db.add_event(job_id, "scan.started", f"generation {generation}",
                          data={"generation": generation, "prefix": job["prefix"]})
        cursor = None
        pages = 0
        try:
            while True:
                if self.db.is_cancelled(job_id):
                    self.db.update_job(job_id, status="cancelled", phase="cancelled",
                                       finished_at=utcnow())
                    self.db.add_event(job_id, "scan.cancelled", "cancelled by operator")
                    return
                while self.db.get_job(job_id)["hold"]:
                    time.sleep(0.1)
                self.db.update_job(job_id, phase="listing")
                delay = int(self.db.get_job(job_id)["page_delay_ms"]) / 1000.0
                if delay > 0 and pages > 0:
                    time.sleep(delay)
                try:
                    page = self.store.list_page(job["prefix"], cursor, job["page_size"])
                except StoreError as exc:
                    if exc.status == 410:
                        # Cursor expired: restart the listing from the beginning
                        # under a new generation. Dedupe by (key, version) keeps
                        # the earlier partial pass harmless.
                        generation += 1
                        self.db.update_job(job_id, generation=generation)
                        self.db.add_event(
                            job_id, "scan.cursor_expired",
                            "listing cursor expired; restarting scan from the start"
                            f" (generation {generation})",
                            data={"generation": generation, "page": pages + 1},
                        )
                        cursor = None
                        pages = 0
                        time.sleep(min(0.25, delay))
                        continue
                    raise
                pages += 1
                items = page.get("items") or []
                for item in items:
                    self._record_listing_item(job_id, item, generation)
                cursor = page.get("next_cursor")
                distinct_keys = self.db.count_keys(job_id)
                self.db.update_job(job_id, pages=pages, cursor=cursor,
                                   keys_count=distinct_keys, phase="listing",
                                   versions_count=len(self.db.list_discoveries(job_id)))
                self.db.add_event(
                    job_id, "scan.page",
                    f"page {pages}: {len(items)} key(s)" + (" • more" if cursor else " • end of listing"),
                    data={"page": pages, "count": len(items), "next_cursor": cursor,
                          "generation": generation,
                          "keys": [i["key"] for i in items]},
                )
                if not cursor:
                    break

            self.db.update_job(job_id, phase="resolving_history")
            self.db.add_event(job_id, "scan.listing_done",
                              f"listing finished after {pages} page(s)")
            if self.db.get_job(job_id)["include_history"]:
                self._resolve_history(job_id, generation)
            else:
                self.db.add_event(job_id, "scan.history_skipped",
                                  "history resolution disabled; current versions only")

            if self.db.is_cancelled(job_id):
                self.db.update_job(job_id, status="cancelled", phase="cancelled",
                                   finished_at=utcnow())
                self.db.add_event(job_id, "scan.cancelled", "cancelled by operator")
                return

            keys_seen = self.db.count_keys(job_id)
            versions = len(self.db.list_discoveries(job_id))
            final_importing = bool((self.db.get_job(job_id) or {}).get("import_active"))
            self.db.update_job(
                job_id,
                status="importing" if final_importing else "ready",
                phase="importing" if final_importing else "ready",
                versions_count=versions, keys_count=keys_seen,
                finished_at=None if final_importing else utcnow(), cursor=None,
            )
            self.db.add_event(job_id, "scan.complete",
                              f"discovered {keys_seen} key(s) / {versions} version(s)",
                              data={"keys": keys_seen, "versions": versions,
                                    "generation": generation})
            job_now = self.db.get_job(job_id) or {}
            if job_now.get("auto_import"):
                self.start_import(job_id)
        except StoreError as exc:
            self.db.update_job(job_id, status="error", phase="error",
                               error=str(exc), finished_at=utcnow())
            self.db.add_event(job_id, "scan.error", f"store error: {exc}",
                              data={"status": exc.status})
        except Exception as exc:  # pragma: no cover - defensive
            self.db.update_job(job_id, status="error", phase="error", error=repr(exc),
                               finished_at=utcnow())
            self.db.add_event(job_id, "scan.error", f"unexpected error: {exc!r}")

    def _record_listing_item(self, job_id: str, item: dict, generation: int) -> None:
        key = item["key"]
        version_id = item["version_id"]
        is_new = self.db.upsert_discovery(
            job_id, key, version_id, item.get("etag"), int(item.get("size") or 0),
            item.get("content_type"), bool(item.get("is_delete_marker")),
            is_current=True, source="listing", generation=generation,
        )
        self.db.mark_not_current(job_id, key, version_id)
        if is_new:
            self.db.add_event(
                job_id, "discovery.new",
                f"new version {version_id} discovered for {key}",
                key=key, version_id=version_id,
                data={"size": item.get("size"), "etag": item.get("etag"),
                      "is_delete_marker": bool(item.get("is_delete_marker"))},
            )

    def _resolve_history(self, job_id: str, generation: int) -> None:
        """Enumerate older versions for every discovered key.

        This makes recreated keys and prior revisions visible without touching
        file timestamps. The listing already gives every key, so this is one
        request per *distinct* key; failures are logged, not fatal.
        """
        keys = list(dict.fromkeys(d["key"] for d in self.db.list_discoveries(job_id)))
        failures = 0
        for key in keys:
            if self.db.is_cancelled(job_id):
                return
            try:
                payload = self.store.versions(key)
            except StoreError as exc:
                failures += 1
                self.db.add_event(job_id, "history.error",
                                  f"version history unavailable for {key}: {exc}", key=key)
                continue
            current = payload.get("current_version_id")
            for version in payload.get("versions") or []:
                self.db.upsert_discovery(
                    job_id, key, version["version_id"], version.get("etag"),
                    int(version.get("size") or 0), version.get("content_type"),
                    bool(version.get("is_delete_marker")),
                    is_current=version["version_id"] == current,
                    source="history", generation=generation,
                )
        if failures:
            self.db.add_event(job_id, "history.partial",
                              f"{failures} key(s) could not provide history")
        self.db.update_job(job_id, versions_count=len(self.db.list_discoveries(job_id)))

    # ---------------------------------------------------------------- imports
    def _import_loop(self, job_id: str) -> None:
        try:
            while True:
                self._run_import(job_id)
                if not self.db.take_import_again(job_id):
                    break
                self.db.update_job(job_id, cancel_requested=0)
                self.db.add_event(job_id, "import.extra_pass",
                                  "another import pass requested; re-verifying"
                                  " partial and pending transfers")
        except Exception as exc:  # pragma: no cover - defensive
            self.db.update_job(job_id, status="error", phase="error", error=repr(exc))
            self.db.add_event(job_id, "import.error", f"unexpected error: {exc!r}")
        finally:
            self.db.release_import(job_id)
            # Close the race: a request that landed between the last check and
            # release_import must still be honoured.
            if self.db.take_import_again(job_id):
                self.start_import(job_id)
            else:
                self._forget_thread(job_id, threading.current_thread())

    def _forget_thread(self, job_id: str, thread: threading.Thread) -> None:
        with self._lock:
            group = self._threads.get(job_id) or []
            if thread in group:
                group.remove(thread)
            if not group:
                self._threads.pop(job_id, None)

    def _selected_targets(self, job_id: str) -> list[dict]:
        job = self.db.get_job(job_id)
        if not job:
            return []
        preselected = json.loads(job["preselected_json"] or "[]")
        discoveries = self.db.list_discoveries(job_id, include_history=bool(job["include_history"]))
        wanted: list[dict] = []
        seen: set[str] = set()
        by_identity = {(d["key"], d["version_id"]): d for d in discoveries}

        def add(d: dict, origin: str, follow_current: bool):
            ident = d["key"] + "\x00" + d["version_id"]
            if ident in seen:
                return
            seen.add(ident)
            item = dict(d)
            item["origin"] = origin
            item["follow_current"] = follow_current
            wanted.append(item)

        for entry in preselected:
            kind = entry.get("kind") or "version"
            key = entry.get("key")
            if not isinstance(key, str):
                continue
            if kind == "current":
                match = next((d for d in discoveries
                              if d["key"] == key and d["is_current"]), None)
                if match:
                    add(match, "browse", True)
                continue
            version_id = entry.get("version_id")
            match = by_identity.get((key, version_id)) if version_id else None
            if match:
                # An explicit version selection is pinned by default; following
                # the head is opt-in only.
                add(match, "browse", bool(entry.get("follow_current", False)))
                continue
            # The preselection refers to a version the listing has not seen yet.
            try:
                head = self.store.head(key, version_id)
            except StoreError:
                head = {"exists": False}
            if head.get("exists"):
                self.db.upsert_discovery(
                    job_id, key, head["version_id"], head.get("etag"),
                    int(head.get("size") or 0), head.get("content_type"),
                    bool(head.get("is_delete_marker")), is_current=head.get("version_id") == version_id,
                    source="browse", generation=int(job["generation"]),
                )
                found = dict(head)
                found["origin"] = "browse"
                follow = bool(entry.get("follow_current", False))
                found["follow_current"] = follow
                add(found, "browse", follow)

        if job["auto_import"]:
            # Auto-import means "the current version of every discovered key".
            # Historical versions stay opt-in (they are still listed in the
            # manifest as discovered-but-not-selected).
            for d in discoveries:
                if d["is_current"] and not d["is_delete_marker"]:
                    add(d, "auto", True)
        return wanted

    def _run_import(self, job_id: str) -> None:
        job = self.db.get_job(job_id)
        if not job:
            return
        if job["cancel_requested"]:
            # An import can only be restarted after an explicit resume, which
            # clears the cancellation flag.
            self.db.release_import(job_id)
            return
        targets = self._selected_targets(job_id)
        self.db.update_job(job_id, status="importing", phase="importing", error=None,
                           finished_at=None, hold=0)
        self.db.add_event(job_id, "import.started",
                          f"{len(targets)} selected version(s)",
                          data={"count": len(targets)})

        processed = 0
        skipped = 0
        for target in targets:
            if self.db.is_cancelled(job_id):
                break
            processed += 1
            self.db.update_job(job_id, phase="importing")
            plan = self._plan_target(job_id, target)
            if plan["action"] == "skip":
                skipped += 1
                self.db.add_event(
                    job_id, "import.skip",
                    f"{target['key']} {target['version_id']} already fully landed"
                    " (version + fingerprint match); no download",
                    key=target["key"], version_id=target["version_id"],
                    data={"reason": plan["reason"], "transfer_id": plan.get("transfer_id"),
                          "reused_from": plan.get("reused_from")},
                )
                continue
            if plan["action"] == "delete_marker":
                skipped += 1
                self.db.add_event(
                    job_id, "import.tombstone",
                    f"{target['key']} {target['version_id']} is a delete marker; recorded only",
                    key=target["key"], version_id=target["version_id"],
                )
                self._write_manifest(job_id, final=False)
                continue
            transfer = plan["transfer"]
            self.db.add_event(
                job_id, "import.plan",
                f"{transfer['key']} {transfer['version_id']}: {plan['reason']}",
                key=transfer["key"], version_id=transfer["version_id"],
                transfer_id=transfer["id"],
                data={"action": plan["action"], "reason": plan["reason"],
                      "transfer_id": transfer["id"]},
            )
            self._run_transfer(job_id, transfer,
                               target.get("follow_current") or transfer.get("follow_current", True))

        if self.db.is_cancelled(job_id):
            self.db.update_job(job_id, status="cancelled", phase="cancelled",
                               finished_at=utcnow())
            self.db.add_event(job_id, "import.cancelled", "cancelled by operator")
        else:
            scanning = bool((self.db.get_job(job_id) or {}).get("scan_active"))
            self.db.update_job(
                job_id,
                status="scanning" if scanning else "ready",
                phase="listing" if scanning else "ready",
                finished_at=None if scanning else utcnow(),
                changed_count=skipped,
            )
            self.db.add_event(job_id, "import.complete",
                              f"{processed} version(s) processed, {skipped} skipped as"
                              " already landed")
        self._write_manifest(job_id, final=not self.db.is_cancelled(job_id))

    def _plan_target(self, job_id: str, target: dict) -> dict:
        key = target["key"]
        version_id = target["version_id"]
        etag = target.get("etag")
        size = int(target.get("size") or 0)
        if target.get("is_delete_marker"):
            return {"action": "delete_marker", "reason": "delete marker"}

        # For "follow current" selections, decide against the live head rather
        # than the (possibly stale) listing entry. Otherwise a version that was
        # complete when the listing was taken would be skipped even though the
        # key has since been overwritten.
        if target.get("follow_current") and target.get("is_current"):
            try:
                live = self.store.head(key)
            except StoreError:
                live = None
            if live is not None:
                if not live.get("exists") or live.get("is_delete_marker"):
                    return {"action": "delete_marker",
                            "reason": "key no longer has live content"}
                if live.get("version_id") != version_id:
                    self.db.upsert_discovery(
                        job_id, key, live["version_id"], live.get("etag"),
                        int(live.get("size") or 0), live.get("content_type"), False,
                        is_current=True, source="import", generation=int(
                            (self.db.get_job(job_id) or {}).get("generation") or 0),
                    )
                    self.db.mark_not_current(job_id, key, live["version_id"])
                    self.db.add_event(
                        job_id, "import.retarget",
                        f"{key}: live version moved {version_id} → {live['version_id']}"
                        " before import; targeting the live version",
                        key=key, version_id=live["version_id"],
                        data={"old_version": version_id,
                              "new_version": live["version_id"],
                              "old_etag": etag, "new_etag": live.get("etag")},
                    )
                    # Any partial bytes of the vanished version are abandoned
                    # explicitly: they must never be resumed as part of the new
                    # version, even though the transfer file paths differ.
                    stale = [t for t in self.db.list_transfers(job_id)
                             if t["key"] == key and t["version_id"] == version_id
                             and t["state"] in ("pending", "queued", "downloading",
                                                "interrupted", "resuming")]
                    for t in stale:
                        self._quarantine_partial(job_id, t, live.get("etag"))
                    target = {
                        "key": key, "version_id": live["version_id"],
                        "etag": live.get("etag"), "size": live.get("size"),
                        "content_type": live.get("content_type"),
                        "is_current": True, "follow_current": True,
                        "origin": target.get("origin") or "import",
                        "supersedes": [t["id"] for t in stale],
                    }
                    version_id = target["version_id"]
                    etag = target["etag"]
                    size = int(live.get("size") or 0)

        landed = self.db.completed_match(job_id, key, version_id, etag, size)
        if landed:
            return {"action": "skip", "reason": "already landed in this job",
                    "transfer_id": landed["id"]}
        other = self.db.find_landed_any_job(key, version_id, etag, size)
        if other:
            # A previous job already land-ded this exact (version, fingerprint).
            # Reference it instead of downloading again; the manifest still gets
            # a complete row so this job's ledger is self-contained.
            if not self.db.reuse_record(job_id, key, version_id, other["id"]):
                transfer_id = new_uid("tr")
                record = self.db.create_transfer(
                    transfer_id, job_id, key, version_id, etag,
                    other.get("expected_sha256"), size, target.get("content_type"),
                    "complete", other.get("staging_path"), origin="reused",
                    note=f"reused landed copy from transfer {other['id']}",
                )
                self.db.update_transfer(
                    transfer_id, bytes=size, sha256=other.get("sha256"),
                    final_path=other.get("final_path"), reused_from=other["id"],
                    follow_current=int(bool(target.get("follow_current", True))),
                    completed_at=utcnow(),
                )
                self.db.add_event(
                    job_id, "import.reuse",
                    f"{key} {version_id}: identical version already landed as"
                    f" {other['id']}; reused, no download",
                    key=key, version_id=version_id, transfer_id=transfer_id,
                    data={"reused_from": other["id"], "sha256": other.get("sha256")},
                )
                return {"action": "skip", "reason": "reused already-landed copy",
                        "transfer_id": transfer_id, "reused_from": other["id"]}
            return {"action": "skip", "reason": "already landed (reused)",
                    "transfer_id": other["id"]}

        active = self.db.active_transfer_same_version(job_id, key, version_id)
        if active and active["etag"] == etag and active["size"] == size:
            state = active["state"]
            if state in ("interrupted", "pending", "resuming", "queued", "downloading"):
                if state == "downloading":
                    # The worker that owns it is gone (we persist per chunk and
                    # only one import loop runs per job), so treat as resumable.
                    self.db.update_transfer(active["id"], state="interrupted")
                return {"action": "resume", "reason": f"resume partial {active['bytes']}B",
                        "transfer": self.db.get_transfer(active["id"])}
        staged = self._staging_path(job_id, key, version_id, etag)
        if active and not (active["etag"] == etag and active["size"] == size):
            self._quarantine_partial(job_id, active, etag)
        return self._new_transfer(job_id, target, staged_existing=staged)

    def _adopt_live_version(self, job_id: str, transfer: dict, live: dict,
                            offset: int, follow_current: bool) -> None:
        """The live version differs from what this transfer expects.

        Any bytes already fetched belong to the *old* fingerprint, so they are
        quarantined and never appended. The old transfer is closed as
        ``superseded_version_changed`` and a new transfer is opened for the live
        version. This is the "new task, never concatenate" rule.
        """
        key = transfer["key"]
        old_version = transfer["version_id"]
        new_version = live["version_id"]
        if offset:
            self._quarantine_partial(job_id, transfer, live.get("etag"))
        else:
            self.db.update_transfer(
                transfer["id"], state="superseded_version_changed",
                error="source version changed before bytes were transferred",
                note="retargeted to the live version",
            )
        self.db.add_event(
            job_id, "transfer.retargeted",
            f"{key}: live version is {new_version} (was {old_version});"
            f" bytes from the old version discarded (had {offset}B),"
            " new transfer created for the new version",
            key=key, version_id=new_version, transfer_id=transfer["id"],
            data={"old_version": old_version, "new_version": new_version,
                  "bytes_discarded": offset, "old_etag": transfer.get("etag"),
                  "new_etag": live.get("etag")},
        )
        replacement = self._new_transfer(job_id, {
            "key": key, "version_id": new_version, "etag": live.get("etag"),
            "size": live.get("size"), "content_type": live.get("content_type"),
            "origin": transfer.get("origin") or "import", "follow_current": True,
        })
        self.db.update_transfer(transfer["id"], superseded_by=replacement["transfer"]["id"])
        replacement_transfer = replacement["transfer"]
        self.db.update_transfer(replacement_transfer["id"],
                                note=f"replaces transfer {transfer['id']} (version changed)")
        self._run_transfer(job_id, replacement_transfer, True)

    def _new_transfer(self, job_id: str, target: dict, staged_existing: str | None = None) -> dict:
        key = target["key"]
        version_id = target["version_id"]
        etag = target.get("etag")
        size = int(target.get("size") or 0)
        transfer_id = new_uid("tr")
        staging = self._staging_path(job_id, key, version_id, etag)
        state = "pending"
        if staged_existing and os.path.exists(staged_existing):
            state = "interrupted"
            staging = staged_existing
        self.db.create_transfer(
            transfer_id, job_id, key, version_id, etag,
            etag if self._etag_is_checksum(etag) else None,
            size, target.get("content_type"), state, staging,
            origin=str(target.get("origin") or "import"),
            note="fingerprint pending",
        )
        fields: dict[str, Any] = {
            "follow_current": int(bool(target.get("follow_current", True))),
        }
        supersedes = target.get("supersedes") or []
        if supersedes:
            fields["note"] = "replaces " + ", ".join(supersedes)
            for old_id in supersedes:
                self.db.update_transfer(old_id, superseded_by=transfer_id)
        self.db.update_transfer(transfer_id, **fields)
        return {"action": "transfer", "reason": "new transfer",
                "transfer": self.db.get_transfer(transfer_id)}

    @staticmethod
    def _etag_is_checksum(etag: str | None) -> bool:
        return bool(etag) and len(etag) == 64 and re.fullmatch(r"[0-9a-f]{64}", etag) is not None

    def _staging_path(self, job_id: str, key: str, version_id: str, etag: str | None) -> str:
        digest = hashlib.sha256(f"{key}\x00{version_id}\x00{etag}\x00{job_id}".encode()).hexdigest()[:24]
        return os.path.join(self.staging_root, f"{safe_filename(key)}.{version_id}.{digest}.part")

    def _final_path(self, key: str, version_id: str, etag: str | None) -> str:
        tag = (etag or "etag-unknown")[:16]
        return os.path.join(self.landing_root, safe_filename(key), f"{version_id}.{tag}.bin")

    def _quarantine_partial(self, job_id: str, transfer: dict, new_etag: str | None) -> None:
        source = transfer.get("staging_path")
        self.db.update_transfer(
            transfer["id"], state="superseded_version_changed",
            note="source version/fingerprint changed while partial download existed",
            error="etag or size changed at the source",
            superseded_by=None,
        )
        if source and os.path.exists(source):
            dest_dir = os.path.join(self.quarantine_root, transfer["id"])
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, os.path.basename(source))
            try:
                os.replace(source, dest)
                self.db.update_transfer(transfer["id"], staging_path=None)
            except OSError:
                pass
        self.db.add_event(
            job_id, "transfer.superseded",
            f"{transfer['key']} {transfer['version_id']}: source changed mid-download;"
            " partial bytes quarantined, a new transfer will start from byte 0",
            key=transfer["key"], version_id=transfer["version_id"],
            transfer_id=transfer["id"],
            data={"old_etag": transfer.get("etag"), "new_etag": new_etag,
                  "bytes_discarded": transfer.get("bytes")},
        )

    # ------------------------------------------------------------- transfer
    def _run_transfer(self, job_id: str, transfer: dict, follow_current: bool) -> None:
        key, version_id = transfer["key"], transfer["version_id"]
        staging = transfer["staging_path"]
        assert staging
        os.makedirs(os.path.dirname(staging), exist_ok=True)
        offset = os.path.getsize(staging) if os.path.exists(staging) else 0
        if offset > transfer["size"]:
            # A partial larger than the declared object can never be valid
            # (e.g. a previous transfer was truncated and the source shrank).
            # Quarantine it rather than trust or truncate the bytes in place.
            self._quarantine_partial(job_id, transfer, None)
            replacement = self._new_transfer(job_id, {
                "key": key, "version_id": version_id, "etag": transfer.get("etag"),
                "size": transfer["size"], "content_type": transfer.get("content_type"),
                "origin": transfer.get("origin") or "import",
                "follow_current": follow_current,
            })
            self.db.update_transfer(transfer["id"],
                                    superseded_by=replacement["transfer"]["id"])
            self._run_transfer(job_id, replacement["transfer"], follow_current)
            return

        # 1. Confirm the server still serves the bytes this transfer describes.
        #    A "follow current" selection is retargeted when the live version
        #    changed; an explicitly pinned version is only checked for
        #    fingerprint drift. Either way, partial bytes are never appended to
        #    a different content fingerprint.
        try:
            live = self.store.head(key) if follow_current else self.store.head(key, version_id)
        except StoreError as exc:
            if exc.status == 404:
                self.db.update_transfer(transfer["id"], state="failed_source_gone",
                                        error="version no longer exists on the server",
                                        note="source version gone; nothing downloaded")
                self.db.add_event(job_id, "transfer.source_gone",
                                  f"{key} {version_id} disappeared before download",
                                  key=key, version_id=version_id, transfer_id=transfer["id"])
                self._write_manifest(job_id, final=False)
                return
            raise
        if not live.get("exists") or live.get("is_delete_marker"):
            self.db.update_transfer(transfer["id"], state="failed_source_gone",
                                    error="version is a delete marker or missing",
                                    note="delete marker / missing; nothing downloaded")
            self.db.add_event(job_id, "transfer.source_gone",
                              f"{key} {version_id} is no longer downloadable"
                              + (" (delete marker)" if live.get("is_delete_marker") else ""),
                              key=key, version_id=version_id, transfer_id=transfer["id"])
            self._write_manifest(job_id, final=False)
            return

        retarget = (
            follow_current and live.get("version_id") != version_id
        ) or (
            live.get("version_id") == version_id
            and (not etag_matches(transfer.get("etag"), live.get("etag"))
                 or live.get("size") != transfer["size"])
        )
        if retarget:
            self._adopt_live_version(job_id, transfer, live, offset, follow_current)
            return

        self.db.update_transfer(transfer["id"], state="downloading" if offset == 0 else "resuming",
                               bytes=offset, attempts=int(transfer["attempts"]) + 1,
                               error=None)
        self.db.add_event(
            job_id, "transfer.start",
            f"{key} {version_id}: " + (f"resuming at {offset}B" if offset else "starting at 0B"),
            key=key, version_id=version_id, transfer_id=transfer["id"],
            data={"offset": offset, "size": transfer["size"], "etag": transfer.get("etag"),
                  "server_version": live.get("version_id"),
                  "fingerprint_ok": True},
        )

        hasher = hashlib.sha256()
        if offset:
            with open(staging, "rb") as fh:
                while True:
                    block = fh.read(256 * 1024)
                    if not block:
                        break
                    hasher.update(block)

        mode = "ab" if offset else "wb"
        last_flushed = offset
        state = {"bytes": offset, "cancelled": False, "truncated": False}

        def stop_flag() -> bool:
            if self.db.is_cancelled(job_id):
                state["cancelled"] = True
                return True
            return False

        def flush_progress(force: bool = False) -> None:
            nonlocal last_flushed
            if force or state["bytes"] - last_flushed >= 256 * 1024:
                self.db.update_transfer(transfer["id"], bytes=state["bytes"])
                last_flushed = state["bytes"]

        def on_chunk(received: int) -> None:
            state["bytes"] = offset + received
            flush_progress()

        done = False
        error: str | None = None
        try:
            with open(staging, mode) as fh:
                for event in self.store.download(
                    key, version_id, offset,
                    expected_etag=transfer.get("etag") if self._etag_is_checksum(transfer.get("etag")) else None,
                    on_chunk=on_chunk, stop_flag=stop_flag, chunk_timeout=self.chunk_timeout,
                ):
                    if event["type"] == "meta":
                        headers = event["headers"]
                        if offset and event["status"] != 206:
                            self.db.add_event(
                                job_id, "transfer.resume_mismatch",
                                f"{key} {version_id}: server ignored the range request;"
                                " quarantining this transfer's partial bytes",
                                key=key, version_id=version_id, transfer_id=transfer["id"],
                            )
                            fh.close()
                            self._quarantine_partial(job_id, transfer, live.get("etag"))
                            # Re-planning creates a brand new transfer rather than
                            # mutating the file in place.
                            replacement = self._new_transfer(job_id, {
                                "key": key, "version_id": version_id,
                                "etag": transfer.get("etag"), "size": transfer["size"],
                                "content_type": transfer.get("content_type"),
                                "origin": transfer.get("origin") or "import",
                                "follow_current": follow_current,
                            })
                            self.db.update_transfer(transfer["id"],
                                                    superseded_by=replacement["transfer"]["id"])
                            self._run_transfer(job_id, replacement["transfer"], follow_current)
                            return
                        if not etag_matches(transfer.get("etag"), headers.get("etag")):
                            raise StoreError("etag mismatch at download start", 412, {
                                "code": "PRECONDITION_FAILED", "etag": headers.get("etag")})
                        continue
                    if event["type"] == "chunk":
                        fh.write(event["data"])
                        hasher.update(event["data"])
                        state["bytes"] += len(event["data"])
                        fh.flush()
                    elif event["type"] == "aborted":
                        state["truncated"] = True
                        error = event.get("message") or "connection closed mid-stream"
                    elif event["type"] == "done":
                        done = True
                flush_progress(force=True)
                if done:
                    fh.flush()
                    os.fsync(fh.fileno())
        except StoreError as exc:
            if exc.status == 412 or (exc.body or {}).get("code") == "PRECONDITION_FAILED":
                new_etag = (exc.body or {}).get("etag") or live.get("etag")
                new_head = live
                try:
                    if not new_etag or new_etag == transfer.get("etag"):
                        new_head = self.store.head(key, version_id)
                        new_etag = new_head.get("etag")
                except StoreError:
                    pass
                self._quarantine_partial(job_id, transfer, new_etag)
                replacement = self._new_transfer(job_id, {
                    "key": key, "version_id": version_id,
                    "etag": new_etag,
                    "size": new_head.get("size") or transfer["size"],
                    "content_type": new_head.get("content_type") or transfer.get("content_type"),
                    "origin": transfer.get("origin") or "import",
                    "follow_current": follow_current,
                })
                self.db.update_transfer(transfer["id"],
                                        superseded_by=replacement["transfer"]["id"])
                self.db.add_event(
                    job_id, "transfer.restarted_new_version",
                    f"{key} {version_id}: server fingerprint changed during download;"
                    " partial bytes quarantined and a fresh transfer created",
                    key=key, version_id=version_id, transfer_id=replacement["transfer"]["id"],
                    data={"old_etag": transfer.get("etag"), "new_etag": new_etag,
                          "bytes_discarded": offset},
                )
                self._run_transfer(job_id, replacement["transfer"], follow_current)
                return
            error = str(exc)
            self.db.update_transfer(transfer["id"], state="failed", error=error,
                                    bytes=state["bytes"])
            self.db.add_event(job_id, "transfer.failed",
                              f"{key} {version_id}: download failed: {error}",
                              key=key, version_id=version_id, transfer_id=transfer["id"])
            self._write_manifest(job_id, final=False)
            return

        if state["cancelled"]:
            self.db.update_transfer(transfer["id"], state="interrupted",
                                    bytes=os.path.getsize(staging) if os.path.exists(staging) else 0,
                                    note="cancelled by operator; partial kept for resume")
            self.db.add_event(job_id, "transfer.interrupted",
                              f"{key} {version_id}: stopped at byte {state['bytes']}; progress kept",
                              key=key, version_id=version_id, transfer_id=transfer["id"],
                              data={"bytes": state["bytes"]})
            return

        actual_size = os.path.getsize(staging) if os.path.exists(staging) else 0
        if not done or actual_size != transfer["size"]:
            self.db.update_transfer(transfer["id"], state="interrupted", bytes=actual_size,
                                    note=f"stream ended early ({actual_size}/{transfer['size']} bytes)")
            self.db.add_event(
                job_id, "transfer.interrupted",
                f"{key} {version_id}: stream ended at {actual_size}/{transfer['size']}B;"
                " progress kept for resume",
                key=key, version_id=version_id, transfer_id=transfer["id"],
                data={"bytes": actual_size, "expected": transfer["size"]},
            )
            # An explicit fault is often consumed at the connection layer; do
            # not auto-resume here so the UI can show the interrupted state.
            self._write_manifest(job_id, final=False)
            return

        digest = hasher.hexdigest()
        if transfer.get("expected_sha256") and digest != transfer["expected_sha256"]:
            self.db.update_transfer(transfer["id"], state="failed_integrity",
                                    sha256=digest, error="sha256 mismatch")
            self.db.add_event(job_id, "transfer.integrity_failed",
                              f"{key} {version_id}: sha256 mismatch, file not imported",
                              key=key, version_id=version_id, transfer_id=transfer["id"])
            return

        # Last-moment guard: the version could have been overwritten while the
        # final chunks were arriving. Re-check identity before publishing.
        try:
            final_head = self.store.head(key, version_id)
        except StoreError:
            final_head = {"exists": False}
        if (not final_head.get("exists")
                or final_head.get("is_delete_marker")
                or not etag_matches(transfer.get("etag"), final_head.get("etag"))
                or final_head.get("size") != transfer["size"]
                or digest != (transfer.get("etag") or digest)):
            self._quarantine_landed_staging(job_id, transfer, final_head, digest)
            return

        final = self._final_path(key, version_id, transfer.get("etag"))
        os.makedirs(os.path.dirname(final), exist_ok=True)
        os.replace(staging, final)
        self.db.update_transfer(transfer["id"], state="complete", bytes=transfer["size"],
                                sha256=digest, final_path=final, completed_at=utcnow(),
                                error=None, note="verified")
        self.db.add_event(
            job_id, "transfer.complete",
            f"{key} {version_id}: landed {transfer['size']}B, sha256 {digest[:12]}…",
            key=key, version_id=version_id, transfer_id=transfer["id"],
            data={"size": transfer["size"], "sha256": digest, "path": final},
        )
        self._write_manifest(job_id, final=False)

    def _quarantine_landed_staging(self, job_id: str, transfer: dict, final_head: dict,
                                   digest: str) -> None:
        """Full download completed but the source no longer matches it."""
        self._quarantine_partial(job_id, transfer, final_head.get("etag"))
        self.db.update_transfer(transfer["id"], sha256=digest)
        live_version = final_head.get("version_id")
        if final_head.get("exists") and not final_head.get("is_delete_marker"):
            replacement = self._new_transfer(job_id, {
                "key": transfer["key"], "version_id": live_version,
                "etag": final_head.get("etag"), "size": final_head.get("size"),
                "content_type": final_head.get("content_type"),
                "origin": transfer.get("origin") or "import", "follow_current": True,
            })
            self.db.update_transfer(transfer["id"],
                                    superseded_by=replacement["transfer"]["id"])
            self.db.add_event(
                job_id, "transfer.changed_before_publish",
                f"{transfer['key']} {transfer['version_id']}: all bytes arrived but the"
                " source changed before publish; downloaded bytes quarantined,"
                f" new transfer for {live_version}",
                key=transfer["key"], version_id=transfer["version_id"],
                transfer_id=transfer["id"],
                data={"sha256": digest, "live_version": live_version},
            )
            self._run_transfer(job_id, replacement["transfer"], True)
        else:
            self.db.add_event(
                job_id, "transfer.changed_before_publish",
                f"{transfer['key']} {transfer['version_id']}: source disappeared/changed"
                " before publish; downloaded bytes quarantined",
                key=transfer["key"], version_id=transfer["version_id"],
                transfer_id=transfer["id"],
            )
        self._write_manifest(job_id, final=False)

    # ---------------------------------------------------------------- manifest
    def manifest(self, job_id: str, include_history: bool | None = None) -> dict:
        job = self.db.get_job(job_id)
        if not job:
            return {"error": "unknown job"}
        if include_history is None:
            include_history = bool(job["include_history"])
        discoveries = self.db.list_discoveries(job_id, include_history=include_history)
        transfers = self.db.list_transfers(job_id)
        events = self.db.list_events(job_id)
        latest_by_identity: dict[tuple[str, str], dict] = {}
        for t in transfers:
            ident = (t["key"], t["version_id"])
            latest_by_identity[ident] = t
        event_count: dict[str, int] = {}
        for e in events:
            if e["transfer_id"]:
                event_count[e["transfer_id"]] = event_count.get(e["transfer_id"], 0) + 1

        rows = []
        for d in discoveries:
            t = latest_by_identity.get((d["key"], d["version_id"]))
            status, reason, landed_sha = "not_selected", "", None
            if d["is_delete_marker"]:
                status, reason = "tombstone", "delete marker; recorded, never downloaded"
            elif t is None:
                if job["status"] in ("ready", "complete") and not job["auto_import"]:
                    reason = "discovered, waiting for selection"
                if self.db.completed_match(job_id, d["key"], d["version_id"], d["etag"], d["size"]):
                    status, reason = "complete", "landed earlier"
            if t:
                status = t["state"]
                if status == "complete":
                    reason = "content fingerprint verified"
                    landed_sha = t["sha256"] or t["etag"]
                elif status in ("interrupted", "pending", "queued", "downloading", "resuming"):
                    reason = f"partial at {t['bytes']}B of {t['size']}B"
                elif status == "superseded_version_changed":
                    reason = "source version changed; superseded by a later transfer"
                elif status == "failed_source_gone":
                    reason = "version disappeared from the source before download"
                elif status == "failed_integrity":
                    reason = "downloaded bytes did not match the expected fingerprint"
                elif status == "failed":
                    reason = t["error"] or "download failed"
            transfer_followers = [x for x in transfers
                                  if x["key"] == d["key"] and x["version_id"] == d["version_id"]]
            rows.append({
                "key": d["key"],
                "version_id": d["version_id"],
                "etag": d["etag"],
                "size": d["size"],
                "content_type": d["content_type"],
                "is_current": d["is_current"],
                "is_delete_marker": d["is_delete_marker"],
                "discovered_by": d["source"],
                "discovered_at": d["discovered_at"],
                "last_seen_at": d["last_seen_at"],
                "generation": d["generation"],
                "import_status": status,
                "reason": reason,
                "transfer_id": t["id"] if t else None,
                "bytes_downloaded": t["bytes"] if t else 0,
                "sha256": (t["sha256"] if t and t["sha256"] else None) or landed_sha,
                "file": t["final_path"] if t and t["state"] == "complete" else None,
                "attempts": len(transfer_followers),
                "transfer_chain": [
                    {"id": x["id"], "state": x["state"], "bytes": x["bytes"],
                     "attempts": x["attempts"], "created_at": x["created_at"],
                     "completed_at": x["completed_at"], "note": x["note"],
                     "error": x["error"], "events": event_count.get(x["id"], 0)}
                    for x in transfer_followers
                ],
            })

        # Duplicate landed files from other jobs are reported, not re-downloaded.
        manifest = {
            "job": {
                "id": job["id"], "prefix": job["prefix"], "status": job["status"],
                "phase": job["phase"], "generation": job["generation"],
                "pages": job["pages"], "created_at": job["created_at"],
                "finished_at": job["finished_at"], "include_history": include_history,
            },
            "generated_at": utcnow(),
            "summary": self._summarize(rows),
            "objects": rows,
            "events": events,
            "totals": self.db.stats(),
        }
        return manifest

    @staticmethod
    def _summarize(rows: list[dict]) -> dict:
        out: dict[str, int] = {}
        for r in rows:
            out[r["import_status"]] = out.get(r["import_status"], 0) + 1
        return out

    def _write_manifest(self, job_id: str, final: bool) -> None:
        try:
            payload = self.manifest(job_id)
            path = os.path.join(self.data_dir, "manifests")
            os.makedirs(path, exist_ok=True)
            target = os.path.join(path, f"{job_id}.json")
            tmp = target + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, target)
            if final:
                self.db.add_event(job_id, "manifest.written", f"manifest written: {target}")
        except Exception as exc:  # pragma: no cover - defensive
            self.db.add_event(job_id, "manifest.error", f"could not write manifest: {exc!r}")

    def cancel_job(self, job_id: str) -> None:
        self.db.request_cancel(job_id)
        self.db.add_event(job_id, "job.cancel_requested", "operator requested cancellation")

    def rescan(self, job_id: str) -> dict:
        self.db.request_rescan(job_id)
        self.start_scan(job_id)
        return self.db.job_summary(self.db.get_job(job_id))

    def import_selected(self, job_id: str, selections: list[dict] | None = None) -> dict:
        if selections is not None:
            job = self.db.get_job(job_id)
            if not job:
                raise ValueError("unknown job")
            self.db.update_job(job_id, preselected_json=json.dumps(selections, ensure_ascii=False),
                               hold=1)
        self.start_import(job_id)
        return self.db.job_summary(self.db.get_job(job_id))

    def set_auto_import(self, job_id: str, enabled: bool) -> dict:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError("unknown job")
        self.db.update_job(job_id, auto_import=int(enabled))
        self.db.add_event(job_id, "job.auto_import",
                          f"auto-import {'enabled' if enabled else 'disabled'}")
        if enabled:
            self.db.update_job(job_id, cancel_requested=0)
            self.start_import(job_id)
        return self.db.job_summary(self.db.get_job(job_id))

    def reset_all(self) -> None:
        """Remove ingest state (jobs, transfers, landed files) for the demo."""
        with self._lock:
            threads = [t for group in self._threads.values() for t in group]
        for t in threads:
            if t.is_alive() and t is not threading.current_thread():
                t.join(timeout=10)
        for root in (self.landing_root, self.staging_root, self.quarantine_root,
                     os.path.join(self.data_dir, "manifests")):
            if not os.path.isdir(root):
                continue
            for name in os.listdir(root):
                target = os.path.join(root, name)
                if os.path.isdir(target):
                    shutil.rmtree(target, ignore_errors=True)
                else:
                    try:
                        os.unlink(target)
                    except OSError:
                        pass
        self.db.reset_all()
