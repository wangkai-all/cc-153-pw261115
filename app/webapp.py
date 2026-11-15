"""Web entry point + JSON API for the object ingest demo."""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .common import json_dumps, utcnow
from .ingest import IngestService
from .ingestdb import IngestDB
from .storeclient import StoreClient, StoreError

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
ALLOWED_STATIC = {"index.html", "app.js", "styles.css", "favicon.svg"}

CSV_COLUMNS = [
    "object_key", "version_id", "size", "etag_sha256", "content_type", "is_current",
    "is_delete_marker", "discovery_source", "discovered_at", "last_seen_at",
    "generation", "import_status", "bytes_downloaded", "content_sha256", "file",
    "reason", "transfer_id", "transfer_attempts", "transfer_chain",
]


class AppContext:
    def __init__(self, service: IngestService, store: StoreClient, store_data_dir: str,
                 data_dir: str):
        self.service = service
        self.store = store
        self.store_data_dir = store_data_dir
        self.data_dir = data_dir

    def reset_demo(self) -> dict:
        self.service.reset_all()
        try:
            self.store.request_json("POST", "/api/admin/seed", body={"reset": True})
        except StoreError as exc:
            return {"ok": False, "error": f"store reset failed: {exc}"}
        return {"ok": True, "stats": self.store_stats()}

    def store_stats(self) -> dict:
        try:
            _, payload = self.store.request_json("GET", "/api/admin/stats")
            return payload
        except StoreError as exc:
            return {"error": str(exc)}


class Handler(BaseHTTPRequestHandler):
    server_version = "IngestDemo/1.0"
    protocol_version = "HTTP/1.1"

    @property
    def ctx(self) -> AppContext:
        return self.server.ctx  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("DEMO_HTTP_LOG", "1") == "1":
            print(f"[web] {self.address_string()} {fmt % args}", flush=True)

    # ------------------------------------------------------------- utilities
    def _send(self, status: int, body: bytes, ctype: str,
              extra: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, status: int, payload: Any) -> None:
        self._send(status, json_dumps(payload), "application/json; charset=utf-8")

    def _error(self, status: int, message: str, **extra: Any) -> None:
        self._json(status, {"error": message, **extra})

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > 4 * 1024 * 1024:
            raise ValueError("request body too large")
        data = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("JSON object required")
        return data

    def _path(self) -> str:
        return urllib.parse.urlsplit(self.path).path

    def _query(self) -> dict[str, str]:
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
        return {k: v[-1] for k, v in q.items()}

    # ------------------------------------------------------------------ GET
    def do_GET(self) -> None:  # noqa: N802
        path = self._path()
        service = self.ctx.service
        try:
            if path == "/" or path == "/index.html":
                self._serve_static("index.html")
                return
            if path.startswith("/static/"):
                self._serve_static(path[len("/static/"):])
                return
            if path == "/healthz":
                self._json(200, {"ok": True, "service": "web", "stats": service.db.stats()})
                return

            if path == "/api/jobs":
                self._json(200, {"jobs": [service.db.job_summary(j) for j in service.db.list_jobs()],
                                 "stats": service.db.stats()})
                return
            if path == "/api/dashboard":
                self._json(200, {
                    "jobs": [service.db.job_summary(j) for j in service.db.list_jobs()],
                    "ingest": service.db.stats(), "store": self.ctx.store_stats(),
                })
                return
            if path.startswith("/api/jobs/"):
                self._job_get(path)
                return
            if path == "/api/store/list":
                q = self._query()
                page = self.ctx.store.list_page(
                    q.get("prefix", ""), q.get("cursor") or None,
                    int(q.get("limit") or 10),
                )
                self._json(200, page)
                return
            if path == "/api/store/versions":
                q = self._query()
                key = q.get("key", "")
                if not key:
                    self._error(400, "key required")
                    return
                self._json(200, self.ctx.store.versions(key))
                return
            if path == "/api/store/head":
                q = self._query()
                self._json(200, self.ctx.store.head(q.get("key", ""), q.get("version") or None))
                return
            if path == "/api/store/download":
                self._proxy_download()
                return
            if path == "/api/store/stats":
                self._json(200, self.ctx.store_stats())
                return
            if path == "/api/store/faults":
                _, payload = self.ctx.store.request_json("GET", "/api/admin/faults")
                self._json(200, payload)
                return
            self._error(404, "not found")
        except StoreError as exc:
            self._error(exc.status or 502, str(exc), upstream=exc.body)
        except ValueError as exc:
            self._error(400, str(exc))
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _job_get(self, path: str) -> None:
        service = self.ctx.service
        parts = [p for p in path[len("/api/jobs/"):].split("/") if p]
        if not parts:
            self._error(404, "not found")
            return
        job_id = parts[0]
        job = service.db.get_job(job_id)
        if not job:
            self._error(404, "unknown job")
            return
        summary = service.db.job_summary(job)
        tail = parts[1] if len(parts) > 1 else ""
        if not tail:
            summary["discoveries"] = service.db.list_discoveries(
                job_id, include_history=bool(job["include_history"]))
            summary["transfers"] = service.db.list_transfers(job_id)
            summary["manifest_summary"] = {}
            try:
                manifest = service.manifest(job_id)
                summary["manifest_summary"] = manifest.get("summary", {})
            except Exception:
                pass
            self._json(200, summary)
            return
        if tail == "discoveries":
            self._json(200, {"job": summary, "items": service.db.list_discoveries(
                job_id, include_history=bool(job["include_history"]))})
            return
        if tail == "transfers":
            self._json(200, {"job": summary, "items": service.db.list_transfers(job_id)})
            return
        if tail == "events":
            q = self._query()
            after = int(q.get("after") or 0)
            self._json(200, {
                "job": summary,
                "events": service.db.list_events(job_id, after=after),
                "stats": service.db.stats(),
            })
            return
        if tail == "manifest":
            manifest = service.manifest(job_id)
            fmt = (self._query().get("format") or "json").lower()
            if fmt == "csv":
                data = manifest_to_csv(manifest)
                filename = f"manifest-{job_id}.csv"
                self._send(200, data.encode("utf-8"), "text/csv; charset=utf-8", {
                    "Content-Disposition": f'attachment; filename="{filename}"',
                })
            else:
                pretty = json.dumps(manifest, ensure_ascii=False, indent=2).encode()
                self._send(200, pretty, "application/json; charset=utf-8", {
                    "Content-Disposition": f'attachment; filename="manifest-{job_id}.json"',
                })
            return
        if tail == "manifest.csv":
            data = manifest_to_csv(service.manifest(job_id))
            self._send(200, data.encode("utf-8"), "text/csv; charset=utf-8", {
                "Content-Disposition": f'attachment; filename="manifest-{job_id}.csv"',
            })
            return
        self._error(404, "not found")

    def _serve_static(self, name: str) -> None:
        if name not in ALLOWED_STATIC:
            self._error(404, "not found")
            return
        path = os.path.join(STATIC_DIR, name)
        if not os.path.exists(path):
            self._error(404, "not found")
            return
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(os.path.splitext(name)[1], "application/octet-stream")
        with open(path, "rb") as fh:
            self._send(200, fh.read(), ctype)

    def _proxy_download(self) -> None:
        """Stream a demo object through the web app (for the preview panel)."""
        q = self._query()
        key = q.get("key", "")
        version = q.get("version") or None
        try:
            head = self.ctx.store.head(key, version)
        except StoreError as exc:
            self._error(exc.status or 502, str(exc))
            return
        if head.get("is_delete_marker"):
            self._error(409, "delete marker has no content")
            return
        headers = {"Content-Type": head.get("content_type") or "application/octet-stream",
                   "Content-Length": str(head.get("size") or 0)}
        if head.get("etag"):
            headers["ETag"] = '"%s"' % head["etag"]
        headers["X-Object-Version"] = head["version_id"]
        self.send_response(200)
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        for event in self.ctx.store.download(key, head["version_id"]):
            if event["type"] == "chunk":
                try:
                    self.wfile.write(event["data"])
                except (BrokenPipeError, ConnectionResetError):
                    return

    # ----------------------------------------------------------------- POST
    def do_POST(self) -> None:  # noqa: N802
        path = self._path()
        service = self.ctx.service
        try:
            body = self._body()
            if path == "/api/jobs":
                prefix = str(body.get("prefix") or "")
                selections = body.get("selections") or []
                if not isinstance(selections, list):
                    raise ValueError("selections must be a list")
                job = service.create_job(
                    prefix,
                    page_size=int(body.get("page_size") or 10),
                    page_delay_ms=body.get("page_delay_ms"),
                    include_history=bool(body.get("include_history", True)),
                    auto_import=bool(body.get("auto_import", False)),
                    preselected=selections,
                    start=bool(body.get("start", True)),
                )
                self._json(201, {"job": job})
                return
            if path.startswith("/api/jobs/"):
                self._job_post(path, body)
                return
            if path == "/api/demo/put":
                key = str(body.get("key") or "")
                content = body.get("content")
                if content is None:
                    content = "@@ overwritten by demo operator at " + utcnow() + "\n"
                if not isinstance(content, str):
                    content = json.dumps(content)
                _, payload = self.ctx.store.request_json(
                    "POST", "/api/admin/put",
                    body={"key": key, "content": content,
                          "content_type": body.get("content_type") or "text/plain; charset=utf-8"},
                )
                self._json(200, payload)
                return
            if path == "/api/demo/delete":
                key = str(body.get("key") or "")
                payload = {"recreate": bool(body.get("recreate"))}
                if body.get("recreate"):
                    payload["content"] = body.get("content") or (
                        "@@ recreated by demo operator at " + utcnow() + "\n")
                _, out = self.ctx.store.request_json("POST", "/api/admin/delete",
                                                     body={"key": key, **payload})
                self._json(200, out)
                return
            if path == "/api/demo/fault":
                _, out = self.ctx.store.request_json("POST", "/api/admin/fault", body={
                    "key": body.get("key") or "*",
                    "mode": "truncate",
                    "after_bytes": int(body.get("after_bytes") or 1024),
                    "times": int(body.get("times") or 1),
                    "note": body.get("note") or "demo truncation",
                })
                self._json(200, out)
                return
            if path == "/api/demo/faults/clear":
                _, out = self.ctx.store.request_json("DELETE", "/api/admin/faults")
                self._json(200, out)
                return
            if path == "/api/demo/invalidate-cursors":
                _, out = self.ctx.store.request_json("POST", "/api/admin/invalidate-cursors",
                                                     body={})
                self._json(200, out)
                return
            if path == "/api/demo/reset":
                self._json(200, self.ctx.reset_demo())
                return
            if path == "/api/demo/seed":
                _, out = self.ctx.store.request_json("POST", "/api/admin/seed",
                                                     body={"reset": bool(body.get("reset"))})
                self._json(200, out)
                return
            self._error(404, "not found")
        except StoreError as exc:
            self._error(exc.status or 502, str(exc), upstream=exc.body)
        except ValueError as exc:
            self._error(400, str(exc))
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _job_post(self, path: str, body: dict) -> None:
        service = self.ctx.service
        parts = [p for p in path[len("/api/jobs/"):].split("/") if p]
        if len(parts) < 2:
            self._error(404, "not found")
            return
        job_id, action = parts[0], parts[1]
        job = service.db.get_job(job_id)
        if not job:
            self._error(404, "unknown job")
            return
        if action == "import":
            selections = body.get("selections")
            if selections is not None and not isinstance(selections, list):
                raise ValueError("selections must be a list")
            service.db.update_job(job_id, cancel_requested=0)
            summary = service.import_selected(job_id, selections)
            self._json(200, {"job": summary})
            return
        if action == "rescan":
            summary = service.rescan(job_id)
            self._json(200, {"job": summary})
            return
        if action == "cancel":
            service.cancel_job(job_id)
            self._json(200, {"job": service.db.job_summary(service.db.get_job(job_id))})
            return
        if action == "resume":
            service.db.update_job(job_id, cancel_requested=0)
            service.db.add_event(job_id, "job.resume",
                                 "operator resumed; existing partial downloads will be verified"
                                 " against the source before continuing")
            summary = service.import_selected(job_id)
            self._json(200, {"job": summary})
            return
        if action == "hold":
            service.db.set_hold(job_id, bool(body.get("hold", True)))
            self._json(200, {"job": service.db.job_summary(service.db.get_job(job_id))})
            return
        if action == "auto-import":
            summary = service.set_auto_import(job_id, bool(body.get("enabled", True)))
            self._json(200, {"job": summary})
            return
        self._error(404, "not found")

    # --------------------------------------------------------------- DELETE
    def do_DELETE(self) -> None:  # noqa: N802
        path = self._path()
        if path == "/api/demo/faults":
            try:
                _, out = self.ctx.store.request_json("DELETE", "/api/admin/faults")
                self._json(200, out)
            except StoreError as exc:
                self._error(exc.status or 502, str(exc))
            return
        self._error(404, "not found")


def manifest_to_csv(manifest: dict) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in manifest.get("objects") or []:
        chain = " | ".join(
            f"{t['id']}:{t['state']}({t['bytes']}B,attempts={t['attempts']})"
            for t in row.get("transfer_chain") or []
        )
        writer.writerow({
            "object_key": row.get("key"),
            "version_id": row.get("version_id"),
            "size": row.get("size"),
            "etag_sha256": row.get("etag"),
            "content_type": row.get("content_type"),
            "is_current": row.get("is_current"),
            "is_delete_marker": row.get("is_delete_marker"),
            "discovery_source": row.get("discovered_by"),
            "discovered_at": row.get("discovered_at"),
            "last_seen_at": row.get("last_seen_at"),
            "generation": row.get("generation"),
            "import_status": row.get("import_status"),
            "bytes_downloaded": row.get("bytes_downloaded"),
            "content_sha256": row.get("sha256"),
            "file": row.get("file"),
            "reason": row.get("reason"),
            "transfer_id": row.get("transfer_id"),
            "transfer_attempts": row.get("attempts"),
            "transfer_chain": chain,
        })
    return buf.getvalue()


def create_app_server(host: str, port: int, service: IngestService, store: StoreClient,
                      store_data_dir: str, data_dir: str) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    httpd.ctx = AppContext(service, store, store_data_dir, data_dir)  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Ingest demo web app")
    parser.add_argument("--host", default=os.environ.get("APP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("APP_PORT", "8000")))
    parser.add_argument("--data", default=os.environ.get("APP_DATA", "var/ingest"))
    parser.add_argument("--store-url", default=os.environ.get("STORE_URL", "http://127.0.0.1:9001"))
    parser.add_argument("--store-data", default=os.environ.get("STORE_DATA", "var/store"))
    args = parser.parse_args()
    service = IngestService(args.data, args.store_url)
    httpd = create_app_server(args.host, args.port, service, service.store,
                              args.store_data, args.data)
    print(f"Ingest web: http://{args.host}:{args.port}  store={args.store_url}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
