"""Deterministic demo data for the local object store.

The data is intentionally shaped to exercise the ingest flow: several prefixes
that page, objects with multiple immutable versions, an object whose current
version is a delete marker, and an object that was deleted and then recreated
under the same key.
"""
from __future__ import annotations

import random
from typing import Iterable

TEXT = "text/plain; charset=utf-8"
BIN = "application/octet-stream"


def _paragraph(topic: str, revision: int, lines: int) -> str:
    rng = random.Random(f"{topic}:{revision}")
    words = [
        "ingest", "manifest", "version", "checksum", "cursor", "prefix", "object",
        "storage", "resume", "offset", "etag", "snapshot", "verify", "batch",
        "schema", "record", "stream", "integrity", "boundary", "ledger",
    ]
    out = [f"# {topic} — revision {revision}", ""]
    for i in range(lines):
        sentence = " ".join(rng.choice(words) for _ in range(12))
        out.append(f"{i + 1:03d}. {sentence.capitalize()}.")
    return "\n".join(out) + "\n"


def _binary(topic: str, revision: int, size: int) -> bytes:
    rng = random.Random(f"bin:{topic}:{revision}")
    header = f"DEMO-BLOB {topic} revision={revision}\n".encode()
    body = bytes(rng.randrange(256) for _ in range(max(0, size - len(header))))
    return header + body


def seed_store(store, reset: bool = False) -> None:
    if reset:
        store.reset()
        return
    if store.stats()["keys"] > 0:
        return

    plan: Iterable[tuple[str, int, str]] = [
        # key, revision, content type
        ("README.md", 1, TEXT),
        ("catalog/index.json", 1, "application/json"),
        ("datasets/2024/events-01.csv", 1, "text/csv"),
        ("datasets/2024/events-02.csv", 1, "text/csv"),
        ("datasets/2024/events-03.csv", 1, "text/csv"),
        ("datasets/2024/summary.md", 1, TEXT),
        ("datasets/2025/events-01.csv", 1, "text/csv"),
        ("datasets/2025/events-01.csv", 2, "text/csv"),
        ("datasets/2025/events-02.csv", 1, "text/csv"),
        ("datasets/2025/summary.md", 1, TEXT),
        ("incoming/batch-a/report.txt", 1, TEXT),
        ("incoming/batch-a/payload.bin", 1, BIN),
        ("incoming/batch-b/report.txt", 1, TEXT),
        ("incoming/batch-b/payload.bin", 1, BIN),
        ("incoming/batch-b/payload.bin", 2, BIN),
        ("incoming/batch-c/notes.txt", 1, TEXT),
        ("incoming/batch-c/notes.txt", 2, TEXT),
        ("incoming/batch-c/notes.txt", 3, TEXT),
        ("logs/2025-01-01.log", 1, TEXT),
        ("logs/2025-01-02.log", 1, TEXT),
        ("logs/2025-01-03.log", 1, TEXT),
        ("logs/2025-01-04.log", 1, TEXT),
        ("logs/2025-01-05.log", 1, TEXT),
        ("archive/old-export.csv", 1, "text/csv"),
        ("archive/old-export.csv", 2, "text/csv"),
        ("archive/legacy-readme.txt", 1, TEXT),
    ]

    rendered: dict[tuple[str, int], bytes] = {}
    for key, rev, ctype in plan:
        if ctype == "text/csv":
            lines = ["id,key,revision,value"]
            for i in range(40):
                lines.append(f"{i},{key},{rev},{rev * 1000 + i}")
            payload = ("\n".join(lines) + "\n").encode()
        elif ctype == "application/json":
            payload = (
                '{"catalog": "demo", "rev": %d, "entries": ['
                '"datasets/", "incoming/", "logs/", "archive/"'
                "]}\n" % rev
            ).encode()
        elif ctype == BIN:
            payload = _binary(key, rev, 220_000)
        else:
            payload = _paragraph(key, rev, 25).encode()
        rendered[(key, rev)] = payload
        store.put(key, payload, ctype)

    # A key whose current version is a delete marker (tombstone, still listed).
    store.delete("archive/removed-object.txt")

    # A key deleted and recreated later under the same name. Its content
    # changes but the key stays identical.
    store.delete("incoming/batch-b/report.txt")
    store.put(
        "incoming/batch-b/report.txt",
        _paragraph("incoming/batch-b/report.txt", 7, 25).encode(),
        TEXT,
    )

    # A large object, useful for the resume demo.
    large = _binary("incoming/batch-a/payload.bin", 9, 1_400_000)
    store.put("incoming/batch-a/payload.bin", large, BIN)
