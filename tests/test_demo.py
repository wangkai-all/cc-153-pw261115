"""End-to-end tests for the local object ingest demo (stdlib unittest only)."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.common import json_dumps  # noqa: E402
from app.ingest import IngestService  # noqa: E402
from app.seed import seed_store  # noqa: E402
from app.store import create_server as create_store_server  # noqa: E402
from app.storeclient import StoreClient, StoreError, wait_for_store  # noqa: E402
from app.webapp import create_app_server, manifest_to_csv  # noqa: E402


def free_port() -> int:
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_until(fn, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s (last={last!r})")


class DemoHarness:
    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="ingest-demo-test-")
        self.store_dir = os.path.join(self.root, "store")
        self.ingest_dir = os.path.join(self.root, "ingest")
        os.makedirs(self.store_dir)
        os.makedirs(self.ingest_dir)
        self.store_port = free_port()
        self.app_port = free_port()
        self.store_httpd = create_store_server("127.0.0.1", self.store_port, self.store_dir)
        seed_store(self.store_httpd.store)
        self.store_thread = threading.Thread(target=self.store_httpd.serve_forever, daemon=True)
        self.store_thread.start()
        self.store_url = f"http://127.0.0.1:{self.store_port}"
        assert wait_for_store(self.store_url)
        self.store = StoreClient(self.store_url, timeout=10)
        self.service = IngestService(self.ingest_dir, self.store_url, page_delay_ms=5,
                                     chunk_timeout=10)
        self.app_httpd = create_app_server("127.0.0.1", self.app_port, self.service,
                                          self.store, self.store_dir, self.ingest_dir)
        self.app_thread = threading.Thread(target=self.app_httpd.serve_forever, daemon=True)
        self.app_thread.start()
        self.base = f"http://127.0.0.1:{self.app_port}"

    def close(self):
        self.app_httpd.shutdown()
        self.store_httpd.shutdown()
        shutil.rmtree(self.root, ignore_errors=True)

    # --------------------------------------------------------------- helpers
    def api(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, json.loads(raw) if raw else None

    def wait_job(self, job_id, statuses=("ready", "error", "cancelled"), timeout=30):
        def check():
            _, job = self.api("GET", f"/api/jobs/{job_id}")
            return job if job["status"] in statuses else None
        return wait_until(check, timeout=timeout)

    def create_job(self, prefix="", **kwargs):
        body = {"prefix": prefix, "page_size": kwargs.pop("page_size", 5),
                "page_delay_ms": kwargs.pop("page_delay_ms", 5)}
        body.update(kwargs)
        status, payload = self.api("POST", "/api/jobs", body)
        assert status == 201, payload
        return payload["job"]["id"]

    def events(self, job_id):
        _, payload = self.api("GET", f"/api/jobs/{job_id}/events")
        return payload["events"]

    def transfers(self, job_id):
        _, payload = self.api("GET", f"/api/jobs/{job_id}/transfers")
        return payload["items"]

    def manifest(self, job_id):
        _, payload = self.api("GET", f"/api/jobs/{job_id}/manifest")
        return payload


class IngestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.h = DemoHarness()

    @classmethod
    def tearDownClass(cls):
        cls.h.close()

    def setUp(self):
        # Each test gets a fresh ingest ledger; the store persists so demos of
        # cross-job reuse are possible inside a single test.
        self.h.api("POST", "/api/demo/reset", {})
        seed_store(self.h.store_httpd.store)  # type: ignore[attr-defined]

    # ------------------------------------------------------------ discovery
    def test_prefix_listing_paginates_and_lists_versions(self):
        job_id = self.h.create_job("incoming/", page_size=2)
        job = self.h.wait_job(job_id)
        self.assertEqual(job["status"], "ready")
        self.assertGreaterEqual(job["pages"], 3)
        _, page = self.h.api("GET", "/api/store/list?prefix=incoming/&limit=2")
        self.assertEqual(len(page["items"]), 2)
        self.assertIsNotNone(page["next_cursor"])
        _, versions = self.h.api(
            "GET", "/api/store/versions?key=incoming/batch-c/notes.txt")
        ids = [v["version_id"] for v in versions["versions"]]
        self.assertEqual(len(ids), 3)
        self.assertEqual(versions["current_version_id"], ids[0])

    def test_delete_and_recreate_produces_distinct_versions(self):
        _, versions = self.h.api(
            "GET", "/api/store/versions?key=incoming/batch-b/report.txt")
        states = [(v["version_id"], v["is_delete_marker"]) for v in versions["versions"]]
        self.assertTrue(any(marker for _, marker in states),
                        "expected a delete marker in history")
        live = [v for v in versions["versions"] if not v["is_delete_marker"]]
        self.assertEqual(len(live), 2, "original + recreated version expected")
        self.assertNotEqual(live[0]["etag"], live[1]["etag"])

    def test_deleted_key_current_version_is_tombstone(self):
        _, versions = self.h.api(
            "GET", "/api/store/versions?key=archive/removed-object.txt")
        current = [v for v in versions["versions"] if v["is_current"]]
        self.assertEqual(len(current), 1)
        self.assertTrue(current[0]["is_delete_marker"])

    # -------------------------------------------------------------- importing
    def test_import_lands_verified_file(self):
        job_id = self.h.create_job("incoming/batch-c/", selections=[
            {"kind": "current", "key": "incoming/batch-c/notes.txt"}])
        self.h.wait_job(job_id)
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        wait_until(lambda: any(t["state"] == "complete" for t in self.h.transfers(job_id)),
                   timeout=15)
        t = next(x for x in self.h.transfers(job_id) if x["state"] == "complete")
        with open(t["final_path"], "rb") as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()
        self.assertEqual(digest, t["sha256"])
        self.assertEqual(digest, t["etag"])
        manifest = self.h.manifest(job_id)
        row = next(r for r in manifest["objects"] if r["key"] == "incoming/batch-c/notes.txt")
        self.assertEqual(row["import_status"], "complete")
        self.assertEqual(row["etag"], digest)

    def test_second_import_of_same_version_does_not_download(self):
        job_id = self.h.create_job("incoming/batch-c/", selections=[
            {"kind": "current", "key": "incoming/batch-c/notes.txt"}])
        self.h.wait_job(job_id)
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        wait_until(lambda: any(t["state"] == "complete" for t in self.h.transfers(job_id)))
        first = self.h.transfers(job_id)
        marker = max(e["id"] for e in self.h.events(job_id))
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        wait_until(lambda: any(e["kind"] == "import.skip" and e["id"] > marker
                               for e in self.h.events(job_id)))
        skips = [e for e in self.h.events(job_id) if e["kind"] == "import.skip"]
        self.assertTrue(skips)
        self.assertIn("already fully landed", skips[-1]["message"])
        # No new transfer record should have been created.
        self.assertEqual(len(first), len(self.h.transfers(job_id)))

    def test_identical_version_reused_across_jobs(self):
        first_job = self.h.create_job("incoming/batch-c/", selections=[
            {"kind": "current", "key": "incoming/batch-c/notes.txt"}])
        self.h.wait_job(first_job)
        self.h.api("POST", f"/api/jobs/{first_job}/import", {})
        wait_until(lambda: any(t["state"] == "complete" for t in self.h.transfers(first_job)))
        second_job = self.h.create_job("incoming/batch-c/", selections=[
            {"kind": "current", "key": "incoming/batch-c/notes.txt"}])
        self.h.wait_job(second_job)
        self.h.api("POST", f"/api/jobs/{second_job}/import", {})
        wait_until(lambda: any(t["state"] == "reused" for t in self.h.transfers(second_job))
                   or any(e["kind"] == "import.skip" for e in self.h.events(second_job)))
        reused = [t for t in self.h.transfers(second_job) if t["origin"] == "reused"]
        self.assertTrue(reused, "expected reuse record rather than a second download")
        self.assertEqual(reused[0]["bytes"], reused[0]["size"])
        self.assertTrue(os.path.exists(reused[0]["final_path"]))

    def test_only_versions_present_in_manifest_are_selected(self):
        job_id = self.h.create_job("datasets/", selections=[
            {"kind": "version", "key": "datasets/2025/events-01.csv",
             "version_id": "v00000008"}])
        self.h.wait_job(job_id)
        _, payload = self.h.api("GET", f"/api/jobs/{job_id}/discoveries")
        matching = [d for d in payload["items"]
                    if d["key"] == "datasets/2025/events-01.csv"
                    and d["version_id"] == "v00000008"]
        self.assertEqual(len(matching), 1)

    # ------------------------------------------------------------ interruption
    def test_interrupted_transfer_resumes_from_offset(self):
        content = "RESUME-TEST\n" + ("x" * 400_000)
        self.h.api("POST", "/api/demo/put", {"key": "scratch/resume.txt", "content": content})
        job_id = self.h.create_job("scratch/", selections=[
            {"kind": "current", "key": "scratch/resume.txt"}])
        self.h.wait_job(job_id)
        self.h.api("POST", "/api/demo/fault", {
            "key": "scratch/resume.txt", "after_bytes": 120_000, "times": 1})
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        partial = wait_until(lambda: next(
            (t for t in self.h.transfers(job_id) if t["state"] == "interrupted"), None))
        self.assertLess(partial["bytes"], partial["size"])
        self.assertGreater(partial["bytes"], 0)
        offset = partial["bytes"]
        self.h.api("POST", f"/api/jobs/{job_id}/resume", {})
        done = wait_until(lambda: next(
            (t for t in self.h.transfers(job_id) if t["state"] == "complete"), None))
        self.assertEqual(done["id"], partial["id"], "resume must continue the same transfer")
        self.assertEqual(done["attempts"], 2)
        start = [e for e in self.h.events(job_id)
                 if e["kind"] == "transfer.start" and e["transfer_id"] == done["id"]]
        self.assertEqual(start[-1]["data"]["offset"], offset)

    def test_overwrite_during_partial_creates_new_transfer(self):
        v1 = "VERSION-ONE\n" + ("A" * 400_000)
        self.h.api("POST", "/api/demo/put", {"key": "scratch/change.txt", "content": v1})
        job_id = self.h.create_job("scratch/", selections=[
            {"kind": "current", "key": "scratch/change.txt"}])
        self.h.wait_job(job_id)
        self.h.api("POST", "/api/demo/fault", {
            "key": "scratch/change.txt", "after_bytes": 100_000, "times": 1})
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        partial = wait_until(lambda: next(
            (t for t in self.h.transfers(job_id) if t["state"] == "interrupted"), None))
        old_id = partial["id"]
        old_version = partial["version_id"]

        v2 = "VERSION-TWO-DIFFERENT\n" + ("B" * 400_000)
        self.h.api("POST", "/api/demo/put", {"key": "scratch/change.txt", "content": v2})
        self.h.api("POST", f"/api/jobs/{job_id}/resume", {})
        wait_until(lambda: any(t["state"] == "complete" for t in self.h.transfers(job_id))
                   and not any(t["state"] == "interrupted" for t in self.h.transfers(job_id)))
        transfers = self.h.transfers(job_id)
        old = next(t for t in transfers if t["id"] == old_id)
        new = [t for t in transfers if t["id"] != old_id]
        self.assertTrue(new)
        self.assertEqual(old["state"], "superseded_version_changed")
        self.assertEqual(old["superseded_by"], new[0]["id"])
        self.assertEqual(new[0]["state"], "complete")
        self.assertNotEqual(new[0]["version_id"], old_version)
        with open(new[0]["final_path"], "rb") as fh:
            data = fh.read()
        self.assertIn(b"VERSION-TWO-DIFFERENT", data)
        # The old bytes must never appear in the new file.
        self.assertEqual(data.count(b"VERSION-ONE"), 0)
        self.assertEqual(data.count(b"A" * 64), 0)

    def test_cursor_expiry_restarts_scan(self):
        job_id = self.h.create_job("datasets/", page_size=2, page_delay_ms=300)
        # Invalidate while the scan is between pages.
        wait_until(lambda: self.h.api("GET", f"/api/jobs/{job_id}")[1]["pages"] >= 1)
        self.h.api("POST", "/api/demo/invalidate-cursors", {})
        job = self.h.wait_job(job_id)
        events = self.h.events(job_id)
        kinds = [e["kind"] for e in events]
        self.assertIn("scan.cursor_expired", kinds)
        complete = next(e for e in events if e["kind"] == "scan.complete")
        self.assertEqual(complete["data"]["keys"], 7)
        self.assertGreaterEqual(complete["data"]["generation"], 2)

    def test_deleted_after_discovery_is_recorded_not_downloaded(self):
        self.h.api("POST", "/api/demo/put", {
            "key": "scratch/volatile.txt", "content": "temporary-version\n"})
        # A concrete version selection is pinned. Even if the key is deleted
        # afterwards, the immutable historical bytes remain downloadable.
        pinned = {"kind": "version", "key": "scratch/volatile.txt",
                  "version_id": self.h.store.head("scratch/volatile.txt")["version_id"]}
        job_id = self.h.create_job("scratch/", selections=[pinned])
        self.h.wait_job(job_id)
        self.h.api("POST", "/api/demo/delete", {"key": "scratch/volatile.txt"})
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        wait_until(lambda: any(
            e["kind"] == "import.complete" for e in self.h.events(job_id)))
        transfers = self.h.transfers(job_id)
        done = [t for t in transfers if t["state"] == "complete"]
        self.assertEqual(len(done), 1, transfers)
        with open(done[0]["final_path"], "rb") as fh:
            data = fh.read()
        self.assertEqual(data, b"temporary-version\n")
        manifest = self.h.manifest(job_id)
        row = next(r for r in manifest["objects"]
                   if r["key"] == "scratch/volatile.txt"
                   and r["version_id"] == pinned["version_id"])
        self.assertEqual(row["import_status"], "complete")

        # A "current" selection of a key that later got a delete marker must be
        # recorded as a tombstone instead of attempting a download.
        status, payload = self.h.api("POST", "/api/jobs", {
            "prefix": "scratch/", "page_size": 5,
            "selections": [{"kind": "current", "key": "scratch/volatile.txt"}],
        })
        self.assertEqual(status, 201, payload)
        tomb_job = payload["job"]["id"]
        self.h.wait_job(tomb_job)
        self.h.api("POST", f"/api/jobs/{tomb_job}/import", {})
        wait_until(lambda: any(
            e["kind"] == "import.complete" for e in self.h.events(tomb_job)))
        self.assertEqual(self.h.transfers(tomb_job), [])
        tomb_rows = [r for r in self.h.manifest(tomb_job)["objects"]
                     if r["key"] == "scratch/volatile.txt"]
        self.assertTrue(any(r["import_status"] == "tombstone" for r in tomb_rows), tomb_rows)

    def test_manifest_csv_contains_provenance_columns(self):
        job_id = self.h.create_job("incoming/batch-c/", selections=[
            {"kind": "current", "key": "incoming/batch-c/notes.txt"}])
        self.h.wait_job(job_id)
        self.h.api("POST", f"/api/jobs/{job_id}/import", {})
        wait_until(lambda: any(t["state"] == "complete" for t in self.h.transfers(job_id)))
        manifest = self.h.manifest(job_id)
        csv_text = manifest_to_csv(manifest)
        header = csv_text.splitlines()[0]
        for col in ("object_key", "version_id", "etag_sha256", "content_sha256",
                    "transfer_chain", "import_status"):
            self.assertIn(col, header)
        self.assertIn("incoming/batch-c/notes.txt", csv_text)

    def test_web_page_and_static_assets(self):
        with urllib.request.urlopen(self.h.base + "/", timeout=10) as resp:
            html = resp.read().decode()
        self.assertIn("对象采集", html)
        with urllib.request.urlopen(self.h.base + "/static/app.js", timeout=10) as resp:
            self.assertEqual(resp.status, 200)

    def test_path_traversal_is_rejected(self):
        status, _ = self.h.api("POST", "/api/demo/put",
                               {"key": "../../etc/passwd", "content": "x"})
        self.assertEqual(status, 400)
        status, _ = self.h.api("POST", "/api/admin/put", {"key": "a/../../b", "content": "x"})
        self.assertEqual(status, 404)

if __name__ == "__main__":
    unittest.main(verbosity=2)
