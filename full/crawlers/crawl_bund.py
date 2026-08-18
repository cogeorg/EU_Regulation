#!/usr/bin/env python3
"""Crawler for German federal legal texts from gesetze-im-internet.de.

Categories:
  verfassungen  - Grundgesetz (XML zip + HTML Gesamtausgabe + PDF)
  stammgesetze  - all current federal laws/ordinances listed in gii-toc.xml
                  (authoritative XML zip per norm, extracted in place)

Usage:
  python3 crawl_bund.py [--category {verfassungen,stammgesetze,all}]
                        [--dry-run] [--limit N]

Idempotent: norms whose folder already contains an extracted .xml file are
skipped. Manifests are written incrementally so interruptions lose little.
"""

import argparse
import io
import json
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

from project_paths import CRAWLER_LOG_ROOT, DATA_ROOT

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = DATA_ROOT / "bund"
VERF_DIR = DATA_DIR / "verfassungen"
STAMM_DIR = DATA_DIR / "stammgesetze"
ERROR_LOG = CRAWLER_LOG_ROOT / "bund_errors.log"

TOC_URL = "https://www.gesetze-im-internet.de/gii-toc.xml"
USER_AGENT = "EU-Regulation-Research/1.0 (academic research)"
SLEEP_SECONDS = 0.25
MAX_RETRIES = 4
RETRY_BACKOFF = 2.0  # seconds, doubled on each retry
MANIFEST_FLUSH_EVERY = 100

GG_SOURCES = [
    # (filename, url, format)
    ("gg/xml.zip", "https://www.gesetze-im-internet.de/gg/xml.zip", "xml"),
    ("gg/BJNR000010949.html",
     "https://www.gesetze-im-internet.de/gg/BJNR000010949.html", "html"),
    ("gg/GG.pdf", "https://www.gesetze-im-internet.de/gg/GG.pdf", "pdf"),
]

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_error(message):
    ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(ERROR_LOG, "a", encoding="utf-8") as fh:
        fh.write("[%s] %s\n" % (now_iso(), message))
    print("ERROR: %s" % message, file=sys.stderr)


def fetch(url, stream=False):
    """GET with retries and exponential backoff. Returns Response or raises."""
    delay = RETRY_BACKOFF
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=60, stream=stream)
            if resp.status_code == 200:
                return resp
            # 404 will not get better on retry
            if resp.status_code == 404:
                raise requests.HTTPError("404 Not Found: %s" % url,
                                         response=resp)
            last_exc = requests.HTTPError(
                "HTTP %d: %s" % (resp.status_code, url), response=resp)
        except requests.HTTPError:
            raise
        except requests.RequestException as exc:
            last_exc = exc
        if attempt < MAX_RETRIES:
            time.sleep(delay)
            delay *= 2
    raise last_exc


def polite_sleep():
    time.sleep(SLEEP_SECONDS)


# Slugs become directory names; only accept a safe charset and never
# "." / ".." so a malformed/malicious TOC link cannot escape STAMM_DIR.
SAFE_SLUG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]*$")


def slug_from_link(link):
    """http://www.gesetze-im-internet.de/1-dm-goldm_nzg/xml.zip -> 1-dm-goldm_nzg"""
    path = urlparse(link.strip()).path
    parts = [p for p in path.split("/") if p]
    if len(parts) != 2 or parts[-1] != "xml.zip":
        return None
    slug = parts[0]
    if not SAFE_SLUG_RE.match(slug):
        return None
    return slug


def load_manifest(path):
    if path.exists():
        try:
            with open(path, encoding="utf-8") as fh:
                entries = json.load(fh)
            return {e["id"]: e for e in entries}
        except (ValueError, KeyError) as exc:
            log_error("Could not parse existing manifest %s: %s" % (path, exc))
    return {}


def save_manifest(path, entries_by_id):
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = sorted(entries_by_id.values(), key=lambda e: e["id"])
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=1)
    tmp.replace(path)


def download_to(url, dest, extract_zip=False):
    """Download url to dest (Path). If extract_zip, also extract next to it."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = fetch(url)
    data = resp.content
    if extract_zip:
        # Validate the zip before writing anything.
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            bad = zf.testzip()
            if bad is not None:
                raise zipfile.BadZipFile("corrupt member %s in %s" % (bad, url))
            dest.write_bytes(data)
            zf.extractall(dest.parent)
    else:
        dest.write_bytes(data)
    return len(data)


def enumerate_toc():
    """Return list of (slug, title, link) from gii-toc.xml."""
    print("Fetching TOC %s ..." % TOC_URL)
    resp = fetch(TOC_URL)
    root = ET.fromstring(resp.content)
    items = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        slug = slug_from_link(link)
        if not slug:
            log_error("Skipping TOC item with unexpected link: %r" % link)
            continue
        items.append((slug, title, link))
    print("TOC lists %d norms." % len(items))
    return items


def has_extracted_xml(folder):
    return folder.is_dir() and any(folder.glob("*.xml"))


def crawl_stammgesetze(dry_run=False, limit=None):
    manifest_path = STAMM_DIR / "manifest.json"
    manifest = load_manifest(manifest_path)
    items = enumerate_toc()

    downloaded = skipped = failed = 0
    since_flush = 0
    for idx, (slug, title, link) in enumerate(items, 1):
        # Always use https to avoid one redirect round-trip per norm.
        url = link.replace("http://", "https://", 1)
        folder = STAMM_DIR / slug
        entry = manifest.get(slug, {})
        entry.update({
            "id": slug,
            "title": title,
            "source_url": url,
            "file": "%s/xml.zip" % slug,
            "format": "xml",
        })

        if dry_run:
            entry.setdefault("status", "listed")
            entry.setdefault("downloaded_at", None)
            manifest[slug] = entry
            continue

        if limit is not None and downloaded >= limit:
            print("Download limit %d reached, stopping." % limit)
            break

        if has_extracted_xml(folder):
            if not entry.get("downloaded_at"):
                entry["downloaded_at"] = now_iso()
            entry["status"] = "ok"
            manifest[slug] = entry
            skipped += 1
        else:
            try:
                download_to(url, folder / "xml.zip", extract_zip=True)
                entry["downloaded_at"] = now_iso()
                entry["status"] = "ok"
                downloaded += 1
            except Exception as exc:  # noqa: BLE001 - log and continue
                entry["status"] = "error: %s" % exc
                failed += 1
                log_error("stammgesetze/%s: %s" % (slug, exc))
            manifest[slug] = entry
            polite_sleep()

        since_flush += 1
        if idx % 100 == 0:
            print("[%d/%d] downloaded=%d skipped=%d failed=%d (last: %s)"
                  % (idx, len(items), downloaded, skipped, failed, slug))
        if since_flush >= MANIFEST_FLUSH_EVERY:
            save_manifest(manifest_path, manifest)
            since_flush = 0

    save_manifest(manifest_path, manifest)
    print("Stammgesetze done: %d listed, %d downloaded, %d skipped, %d failed."
          % (len(items), downloaded, skipped, failed))
    if dry_run:
        print("(dry run: manifest written, nothing downloaded)")


def crawl_verfassungen(dry_run=False, limit=None):
    manifest_path = VERF_DIR / "manifest.json"
    manifest = load_manifest(manifest_path)

    downloaded = 0
    for relpath, url, fmt in GG_SOURCES:
        if limit is not None and downloaded >= limit:
            break
        entry_id = "gg:%s" % fmt
        entry = manifest.get(entry_id, {})
        entry.update({
            "id": entry_id,
            "title": "Grundgesetz für die Bundesrepublik Deutschland",
            "source_url": url,
            "file": relpath,
            "format": fmt,
        })
        if dry_run:
            entry.setdefault("status", "listed")
            entry.setdefault("downloaded_at", None)
            manifest[entry_id] = entry
            continue

        dest = VERF_DIR / relpath
        is_zip = relpath.endswith(".zip")
        already = (has_extracted_xml(dest.parent) if is_zip
                   else dest.exists() and dest.stat().st_size > 0)
        if already:
            if not entry.get("downloaded_at"):
                entry["downloaded_at"] = now_iso()
            entry["status"] = "ok"
        else:
            try:
                download_to(url, dest, extract_zip=is_zip)
                entry["downloaded_at"] = now_iso()
                entry["status"] = "ok"
                downloaded += 1
            except Exception as exc:  # noqa: BLE001
                entry["status"] = "error: %s" % exc
                log_error("verfassungen/%s: %s" % (relpath, exc))
            polite_sleep()
        manifest[entry_id] = entry

    save_manifest(manifest_path, manifest)
    print("Verfassungen done (%d entries in manifest)." % len(manifest))


def main():
    parser = argparse.ArgumentParser(
        description="Crawl German federal legal texts from gesetze-im-internet.de")
    parser.add_argument("--category",
                        choices=["verfassungen", "stammgesetze", "all"],
                        default="all")
    parser.add_argument("--dry-run", action="store_true",
                        help="enumerate and write manifests only, no downloads")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="cap number of downloads (for testing)")
    args = parser.parse_args()

    start = time.time()
    if args.category in ("verfassungen", "all"):
        crawl_verfassungen(dry_run=args.dry_run, limit=args.limit)
    if args.category in ("stammgesetze", "all"):
        crawl_stammgesetze(dry_run=args.dry_run, limit=args.limit)
    print("Total elapsed: %.1f s" % (time.time() - start))


if __name__ == "__main__":
    main()
