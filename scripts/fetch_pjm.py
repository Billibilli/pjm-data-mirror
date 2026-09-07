#!/usr/bin/env python3
"""Fetch PJM market feeds into an auditable Git snapshot (GitHub Actions mirror).

Why mirror: pjm.com / dataminer2.pjm.com is unreachable from the local network
(DNS resolution fails directly, proxy times out). GitHub Actions (Azure egress)
reaches it, so this repo snapshots feeds weekly; downstream research consumes
stable raw.githubusercontent.com URLs (B1 DC-vs-power chain, price baseline).

Feeds (probe order; dataminer2 public feeds, no key):
  - rt_da_price  : real-time + day-ahead price, 5-min rows
  - da_hrl_lmps  : day-ahead hourly LMP (price formation per node/zone)
  - archive check: meta/errors.json records any feed that refused.

Snapshot philosophy: no full-history backfill; weekly immutable commits grow a
history that becomes a time series (git log = audit trail). Compare snapshots
after N weeks to answer "is PJM pricing up YoY due to DC load?".
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
META_DIR = ROOT / "meta"
USER_AGENT = "Billibilli/pjm-data-mirror/1.0"

FEEDS = ["rt_da_price", "da_hrl_lmps"]
BASE = "https://dataminer2.pjm.com/feed"


def fetch(url: str, attempts: int = 3, timeout: int = 120) -> bytes:
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/csv,*/*"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as exc:  # noqa: BLE001
            last = exc
            if "429" in str(exc):
                import time
                time.sleep(20)
            else:
                time.sleep(3)
    raise last or RuntimeError(f"fetch failed: {url}")


def canonical_csv(raw: bytes) -> bytes:
    """Parse CSV rows, re-serialize with sorted column order + stable row sort."""
    text = raw.decode("utf-8", "replace")
    if "<!doctype html" in text[:500].lower() or "<html" in text[:500].lower() or "bundle.js" in text[:2000]:
        raise RuntimeError("feed returned HTML shell (no API key?) not CSV — refusing to snapshot")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise RuntimeError("feed returned no CSV columns — refusing to snapshot")
    rows = [dict(r) for r in reader if any((v or "").strip() for v in r.values())]
    # stable sort by first 2 columns (datetime-like first)
    cols = list(reader.fieldnames)
    sort_keys = [c for c in cols[:2] if c]
    rows.sort(key=lambda r: tuple(str(r.get(c, "")) for c in sort_keys))
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return out.getvalue().encode("utf-8")


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    errors = {}
    manifest = {"status": "ok", "generated_at": now, "datasets": [], "failures": []}
    for feed in FEEDS:
        url = f"{BASE}/{feed}?rowCount=5000"
        try:
            raw = fetch(url)
            body = canonical_csv(raw)
            rows = body.count(b"\n") - 1 if body else 0
            h = hashlib.sha256(body).hexdigest()
            (DATA_DIR / f"{feed}.csv").write_bytes(body)
            (META_DIR / f"{feed}.sha256").write_text(h + "\n")
            manifest["datasets"].append({"id": feed, "rows": max(rows, 0), "sha256": h})
        except Exception as exc:  # noqa: BLE001
            errors[feed] = str(exc)[:300]
            manifest["failures"].append({"id": feed, "error": str(exc)[:300]})
    if errors:
        manifest["status"] = "degraded"
        (META_DIR / "errors.json").write_text(json.dumps(errors, indent=2))
    (META_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
