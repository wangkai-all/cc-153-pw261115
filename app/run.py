"""Start the demo store and ingest web app in one process (local demo launcher)."""
from __future__ import annotations

import os
import signal
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.ingest import IngestService  # noqa: E402
from app.seed import seed_store  # noqa: E402
from app.store import create_server as create_store_server  # noqa: E402
from app.storeclient import wait_for_store  # noqa: E402
from app.webapp import create_app_server  # noqa: E402


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Local object ingest demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--app-port", type=int, default=int(os.environ.get("APP_PORT", "8000")))
    parser.add_argument("--store-port", type=int, default=int(os.environ.get("STORE_PORT", "9001")))
    parser.add_argument("--data", default=os.environ.get("DEMO_DATA", "var"))
    parser.add_argument("--no-seed", action="store_true")
    args = parser.parse_args()

    store_dir = os.path.join(args.data, "store")
    ingest_dir = os.path.join(args.data, "ingest")
    os.makedirs(store_dir, exist_ok=True)
    os.makedirs(ingest_dir, exist_ok=True)

    store_httpd = create_store_server(args.host, args.store_port, store_dir)
    if not args.no_seed:
        seed_store(store_httpd.store)  # type: ignore[attr-defined]
    store_thread = threading.Thread(target=store_httpd.serve_forever, daemon=True,
                                    name="demo-store")
    store_thread.start()

    store_url = f"http://{args.host}:{args.store_port}"
    if not wait_for_store(store_url):
        print("store failed to start", file=sys.stderr)
        raise SystemExit(1)

    service = IngestService(ingest_dir, store_url, page_delay_ms=900)
    app_httpd = create_app_server(args.host, args.app_port, service, service.store,
                                  store_dir, ingest_dir)
    print("")
    print("  本地对象采集演示")
    print(f"  网页入口   http://{args.host}:{args.app_port}")
    print(f"  模拟存储   {store_url}")
    print(f"  数据目录   {os.path.abspath(args.data)}")
    print("")

    def shutdown(*_args: object) -> None:
        def stop() -> None:
            service.shutdown()
            app_httpd.shutdown()
            store_httpd.shutdown()

        threading.Thread(target=stop, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    try:
        app_httpd.serve_forever()
    finally:
        store_httpd.shutdown()
        print("stopped", flush=True)


if __name__ == "__main__":
    main()
