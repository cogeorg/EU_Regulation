#!/usr/bin/env python3
"""Crawler for state law (Stammgesetze: Gesetze + Verordnungen) of HB, NI, NW.

Sources
  hb  Bremen        Transparenzportal (www.transparenz.bremen.de), section
                    "Vorschriften", facet "Gesetze und Rechtsverordnungen".
                    Full consolidated text lives on the /metainformationen/
                    page (template=20_gp_ifg_meta_detail_d), juris-style.
  ni  Niedersachsen VORIS on voris.wolterskluwer-online.de. Paginated search
                    (12 results/page) with the publication-form facets
                    ATS_Rechtsvorschriften_NI_G (Gesetze) and
                    ATS_Rechtsvorschriften_NI_VO (Verordnungen). Documents
                    are paragraph-level (/browse/document/<uuid>).
  nw  NRW           recht.nrw.de. The public OpenSearch middleware
                    (/search-middleware/opensearch_internet/_search) lists all
                    current SGV norms as type=state_law_and_regulations; the
                    full text of each norm is one HTML page under /lrgv/....

Usage
  python3 crawl_laender_hb_ni_nw.py [--state {hb,ni,nw,all}] [--dry-run]
                                    [--limit N] [--refresh-index]

Enumeration is cached to <state>/index.json (use --refresh-index to redo it).
Idempotent: existing non-empty .html files are skipped. The manifest is
flushed incrementally, so interruptions lose little.
"""

import argparse
import email.utils
import json
import math
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from project_paths import CRAWLER_LOG_ROOT, DATA_ROOT

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = DATA_ROOT / "laender"
STAMM_DIR = DATA_DIR / "stammgesetze"
ERROR_LOG = CRAWLER_LOG_ROOT / "laender_hb_ni_nw_errors.log"

USER_AGENT = "EU-Regulation-Research/1.0 (academic research)"
SLEEP_SECONDS = 0.5
MAX_RETRIES = 4
RETRY_BACKOFF = 2.0  # seconds, doubled on each retry
MANIFEST_FLUSH_EVERY = 25

HB_BASE = "https://www.transparenz.bremen.de"
HB_SEARCH = (HB_BASE + "/vorschriften-72741?id=72741&fulltext=&dosubmit=true"
             "&vorschriften%5B0%5D=Gesetze+und+Rechtsverordnungen"
             "&skip={skip}&max={page_size}&sort=titel&order=asc")
HB_PAGE_SIZE = 50
HB_FULLTEXT_TEMPLATE = "template=20_gp_ifg_meta_detail_d"

NI_BASE = "https://voris.wolterskluwer-online.de"
NI_SEARCH = (NI_BASE + "/search?query="
             "&publicationtype=publicationform-ats-filter%21{facet}"
             "&page={page}")
NI_FACETS = ["ATS_Rechtsvorschriften_NI_G", "ATS_Rechtsvorschriften_NI_VO"]
NI_PAGE_SIZE = 12  # fixed by the portal, items_per_page is ignored

NW_BASE = "https://recht.nrw.de"
NW_SEARCH_API = NW_BASE + "/search-middleware/opensearch_internet/_search"
NW_PAGE_SIZE = 500

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT


class SourceBlocked(RuntimeError):
    """Expected policy block, distinct from a crawler or document failure."""


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_error(message):
    ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(ERROR_LOG, "a", encoding="utf-8") as fh:
        fh.write("[%s] %s\n" % (now_iso(), message))
    print("ERROR: %s" % message, file=sys.stderr)


def polite_sleep():
    time.sleep(SLEEP_SECONDS)


def fetch(url, method="GET", json_body=None, timeout=90):
    """Request with retries and exponential backoff. Returns Response."""
    delay = RETRY_BACKOFF
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == "POST":
                resp = session.post(url, json=json_body, timeout=timeout)
            else:
                resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 404):
                raise requests.HTTPError(
                    "HTTP %d: %s" % (resp.status_code, url), response=resp)
            last_exc = requests.HTTPError(
                "HTTP %d: %s" % (resp.status_code, url), response=resp)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "").strip()
                retry_delay = None
                if retry_after.isdigit():
                    retry_delay = float(retry_after)
                elif retry_after:
                    try:
                        retry_at = email.utils.parsedate_to_datetime(retry_after)
                        retry_delay = max(0.0, retry_at.timestamp() - time.time())
                    except (TypeError, ValueError, OverflowError):
                        pass
                delay = max(delay, retry_delay or 30.0)
        except requests.HTTPError:
            raise
        except requests.RequestException as exc:
            last_exc = exc
        if attempt < MAX_RETRIES:
            time.sleep(delay)
            delay *= 2
    raise last_exc


def load_json(path):
    if path.exists():
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except ValueError as exc:
            log_error("Could not parse %s: %s" % (path, exc))
    return None


def save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1)
    tmp.replace(path)


def load_manifest(path):
    entries = load_json(path)
    if entries:
        try:
            return {e["id"]: e for e in entries}
        except (KeyError, TypeError) as exc:
            log_error("Bad manifest %s: %s" % (path, exc))
    return {}


def save_manifest(path, entries_by_id):
    save_json(path, sorted(entries_by_id.values(), key=lambda e: e["id"]))


def clean_text(node_text):
    return re.sub(r"\s+", " ", node_text).strip()


# --------------------------------------------------------------- enumeration

def enumerate_hb():
    """Bremen: paginate the faceted Vorschriften search, 50 hits per page.

    Each hit exposes a /metainformationen/<slug> link whose
    template=20_gp_ifg_meta_detail_d view holds the full consolidated text.
    """
    items = {}
    expected = None
    skip = 0
    while True:
        url = HB_SEARCH.format(skip=skip, page_size=HB_PAGE_SIZE)
        resp = fetch(url)
        soup = BeautifulSoup(resp.text, "lxml")

        if expected is None:
            # The page heading is authoritative (e.g. "Ergebnisse 1 - 50
            # von (2.164)").  The old code took the first matching facet
            # anchor, often reading an unrelated "(1)" and truncating at 100.
            m = re.search(
                r"Ergebnisse\s+\d+\s*-\s*\d+\s+von\s+\(?([\d.]+)\)?",
                soup.get_text(" ", strip=True), re.I,
            )
            if m:
                expected = int(m.group(1).replace(".", ""))

        new = 0
        for card in soup.select("li.search-result-item"):
            a = card.find("a", href=re.compile(r"/metainformationen/"))
            if a is None:
                continue
            href = a["href"].split("?")[0]
            slug = href.rstrip("/").rsplit("/", 1)[-1]
            doc_id = card.get("data-id")
            if not doc_id:
                id_match = re.search(r"-(\d+)$", slug)
                doc_id = id_match.group(1) if id_match else None
            if not doc_id:
                continue
            if doc_id in items:
                continue
            heading = card.find("h2")
            title = clean_text(heading.get_text(" ")) if heading else None
            if not title:
                title = slug.rsplit("-", 1)[0].replace("-", " ")
            items[doc_id] = {
                "id": doc_id,
                "title": title,
                "source_url": "%s/metainformationen/%s?%s"
                              % (HB_BASE, slug, HB_FULLTEXT_TEMPLATE),
            }
            new += 1
        print("  hb: skip=%d -> %d new (total %d, expected %s)"
              % (skip, new, len(items), expected))
        if new == 0 and (expected is None or skip >= expected):
            break
        skip += HB_PAGE_SIZE
        if expected is not None and skip >= expected:
            break
        polite_sleep()
    if expected is not None and len(items) != expected:
        raise RuntimeError(
            "HB enumeration incomplete: portal reported %d results, collected %d unique IDs"
            % (expected, len(items))
        )
    return list(items.values()), expected


def enumerate_ni():
    """VORIS: paginate the search for the Gesetze and Verordnungen facets."""
    items = {}
    expected_sum = 0
    for facet in NI_FACETS:
        kind = facet.rsplit("_", 1)[-1]  # G / VO
        expected = None
        page = 0
        empty_streak = 0
        while True:
            url = NI_SEARCH.format(facet=facet, page=page)
            resp = fetch(url)
            if expected is None:
                m = re.search(r"([\d.]+)\s+Suchergebnisse", resp.text)
                expected = int(m.group(1).replace(".", "")) if m else None
                print("  ni[%s]: portal reports %s results" % (kind, expected))
            soup = BeautifulSoup(resp.text, "lxml")
            new = 0
            for a in soup.find_all("a", href=re.compile(
                    r"^/browse/document/[a-f0-9-]{36}$")):
                title = clean_text(a.get_text())
                if not title:
                    continue  # thumbnails etc.
                uuid = a["href"].rsplit("/", 1)[-1]
                if uuid in items:
                    continue
                items[uuid] = {
                    "id": uuid,
                    "title": title,
                    "source_url": NI_BASE + a["href"],
                    "category": kind,
                }
                new += 1
            if new == 0:
                empty_streak += 1
            else:
                empty_streak = 0
            if page % 25 == 0 or new == 0:
                print("  ni[%s]: page=%d -> %d new (total %d)"
                      % (kind, page, new, len(items)))
            page += 1
            last_page = (math.ceil(expected / float(NI_PAGE_SIZE))
                         if expected else None)
            if empty_streak >= 2:
                break
            if last_page is not None and page > last_page:
                break
            polite_sleep()
        if expected:
            expected_sum += expected
    return list(items.values()), expected_sum or None


def enumerate_nw():
    """NRW: page through the public OpenSearch middleware for all current
    SGV norms (type=state_law_and_regulations)."""
    items = {}
    expected = None
    offset = 0
    while True:
        body = {
            "query": {"terms": {"type": ["state_law_and_regulations"]}},
            "size": NW_PAGE_SIZE,
            "from": offset,
            "sort": ["_doc"],
            "_source": ["url", "title", "field_abbreviation"],
            "track_total_hits": True,
        }
        resp = fetch(NW_SEARCH_API, method="POST", json_body=body)
        data = resp.json()
        hits = data.get("hits", {})
        if expected is None:
            expected = hits.get("total", {}).get("value")
            print("  nw: index reports %s current SGV norms" % expected)
        batch = hits.get("hits", [])
        for hit in batch:
            src = hit.get("_source", {})
            urls = src.get("url") or []
            if not urls:
                continue
            rel = urls[0]
            # id = "<type>--<slug>" since a few docs share a slug under
            # both /lrgv/gesetz/ and /lrgv/bekanntmachung/
            parts = rel.strip("/").split("/")
            slug = "--".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            if slug in items:
                continue
            titles = src.get("title") or []
            items[slug] = {
                "id": slug,
                "title": titles[0] if titles else slug,
                "source_url": NW_BASE + rel.rstrip("/") + "/",
            }
        print("  nw: from=%d -> %d hits (total %d)"
              % (offset, len(batch), len(items)))
        if not batch:
            break
        offset += NW_PAGE_SIZE
        if expected is not None and offset >= expected:
            break
        polite_sleep()
    if expected is not None and len(items) != expected:
        raise RuntimeError(
            "NW enumeration incomplete: index reports %d but parsed %d unique items"
            % (expected, len(items))
        )
    return list(items.values()), expected


ENUMERATORS = {"hb": enumerate_hb, "ni": enumerate_ni, "nw": enumerate_nw}


def get_index(state, refresh):
    state_dir = STAMM_DIR / state
    index_path = state_dir / "index.json"
    if not refresh:
        cached = load_json(index_path)
        if cached and cached.get("items"):
            print("%s: using cached index (%d items, fetched %s)"
                  % (state, len(cached["items"]), cached.get("fetched_at")))
            return cached
    print("%s: enumerating ..." % state)
    items, expected = ENUMERATORS[state]()
    index = {
        "state": state,
        "fetched_at": now_iso(),
        "expected_total": expected,
        "items": items,
    }
    save_json(index_path, index)
    print("%s: index written (%d items, expected %s)"
          % (state, len(items), expected))
    return index


# ----------------------------------------------------------------- downloads

def looks_like_html(content):
    head = content[:400].lstrip().lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html")


def validate_html_payload(state, content):
    if not looks_like_html(content):
        return False
    lower = content.lower()
    if any(marker in lower for marker in (
            b"<title>error", b"<title>fehler", b"access denied",
            b"seite nicht gefunden", b"page not found")):
        return False
    if state == "hb":
        return b"jgwstoc" in lower and b"<title" in lower
    if state == "nw":
        base_valid = (b"recht.nrw.de" in lower and b"<h1" in lower
                      and b"<title" in lower)
        has_inline_text = b"legaldoc-article" in lower
        has_legacy_fallback = (
            (b"/system/files/bh/" in lower and b"<iframe" in lower)
            or (
                b"dokument herunterladen" in lower
                and b"/system/files/pdf/" in lower
            )
        )
        return base_valid and (has_inline_text or has_legacy_fallback)
    return False


def payload_manifest_status(state, content):
    lower = content.lower()
    if state == "hb" and (
        b"wegen einer bevorstehenden neuregelung nur mit" in lower
        or b"[in vorbereitung]" in lower
    ):
        return "source_fulltext_unavailable_metadata_only"
    if state == "nw" and b"legaldoc-article" not in lower:
        return "wrapper_with_fulltext_fallback"
    return "ok"


def record_ni_robots_block():
    """Persist the NI gap without deleting a formerly cached inventory."""
    state_dir = STAMM_DIR / "ni"
    state_dir.mkdir(parents=True, exist_ok=True)
    index_path = state_dir / "index.json"
    index = load_json(index_path) or {"state": "ni", "items": []}
    cached_items = index.get("items", [])
    cached_expected = index.get("expected_total")
    index.update({
        "state": "ni",
        "fetched_at": index.get("fetched_at"),
        "expected_total": None,
        "inventory_status": "blocked_untrusted_search_index",
        "robots_url": NI_BASE + "/robots.txt",
        "robots_disallowed_path": "/search/",
        "count": None,
        "untrusted_cached_count": len(cached_items),
        "untrusted_cached_expected_total": cached_expected,
        "notes": (
            "NI-VORIS robots.txt disallows /search/. Any cached items were "
            "enumerated through that path before the compliance audit and are "
            "retained only as untrusted evidence; no downloads use them."
        ),
    })
    save_json(index_path, index)
    manifest_path = state_dir / "manifest.json"
    manifest = load_manifest(manifest_path)
    for entry in manifest.values():
        entry["status"] = "untrusted_robots_disallowed_enumeration"
    save_manifest(manifest_path, manifest)


def crawl_state(state, dry_run=False, limit=None, refresh_index=False):
    if state == "ni":
        record_ni_robots_block()
        raise SourceBlocked(
            "NI-VORIS robots.txt disallows /search/; the former cached index "
            "was produced through that path and is not trusted. A compliant "
            "bulk enumeration source is required before NI can run."
        )
    state_dir = STAMM_DIR / state
    state_dir.mkdir(parents=True, exist_ok=True)
    index = get_index(state, refresh_index)
    items = index["items"]
    selected_items = items[:max(limit, 0)] if limit is not None else items

    manifest_path = state_dir / "manifest.json"
    manifest = load_manifest(manifest_path)
    current_ids = {item["id"] for item in items}
    for entry in manifest.values():
        old_file = entry.get("file")
        if isinstance(old_file, str) and old_file.startswith(state + "/"):
            entry["file"] = old_file[len(state) + 1:]
    stale = {doc_id: entry for doc_id, entry in manifest.items()
             if doc_id not in current_ids}
    if stale:
        archive_path = state_dir / "manifest_stale.json"
        archive = load_manifest(archive_path)
        for doc_id, entry in stale.items():
            entry["status"] = "stale_not_in_current_index"
            archive[doc_id] = entry
        save_manifest(archive_path, archive)
        manifest = {doc_id: entry for doc_id, entry in manifest.items()
                    if doc_id in current_ids}

    downloaded = skipped = failed = 0
    since_flush = 0
    for idx, item in enumerate(selected_items, 1):
        doc_id = item["id"]
        filename = "%s.html" % doc_id
        entry = manifest.get(doc_id, {})
        previous_source_url = entry.get("source_url")
        entry.update({
            "id": doc_id,
            "title": item["title"],
            "source_url": item["source_url"],
            "file": filename,
            "format": "html",
        })

        if dry_run:
            entry.setdefault("status", "listed")
            entry.setdefault("downloaded_at", None)
            manifest[doc_id] = entry
            continue

        dest = state_dir / filename
        existing_valid = False
        existing_content = None
        if dest.exists() and dest.stat().st_size > 0:
            try:
                existing_content = dest.read_bytes()
                existing_valid = validate_html_payload(state, existing_content)
            except OSError:
                existing_valid = False
        if existing_valid and previous_source_url == item["source_url"]:
            if not entry.get("downloaded_at"):
                entry["downloaded_at"] = datetime.fromtimestamp(
                    dest.stat().st_mtime, timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
            entry["status"] = payload_manifest_status(state, existing_content)
            manifest[doc_id] = entry
            skipped += 1
        else:
            try:
                resp = fetch(item["source_url"])
                if not validate_html_payload(state, resp.content):
                    raise ValueError("response failed portal-specific HTML validation")
                tmp_dest = dest.with_suffix(dest.suffix + ".tmp")
                tmp_dest.write_bytes(resp.content)
                tmp_dest.replace(dest)
                entry["downloaded_at"] = now_iso()
                entry["status"] = payload_manifest_status(state, resp.content)
                downloaded += 1
            except Exception as exc:  # noqa: BLE001 - log and continue
                entry["status"] = "error: %s" % exc
                failed += 1
                log_error("%s/%s: %s" % (state, doc_id, exc))
            manifest[doc_id] = entry
            polite_sleep()

        since_flush += 1
        if idx % 100 == 0:
            print("%s: [%d/%d] downloaded=%d skipped=%d failed=%d"
                  % (state, idx, len(selected_items), downloaded, skipped, failed))
        if since_flush >= MANIFEST_FLUSH_EVERY:
            save_manifest(manifest_path, manifest)
            since_flush = 0

    save_manifest(manifest_path, manifest)
    print("%s done: %d selected of %d listed (expected %s), %d downloaded, %d skipped, "
          "%d failed." % (state, len(selected_items), len(items), index.get("expected_total"),
                          downloaded, skipped, failed))
    if dry_run:
        print("(%s dry run: manifest written, nothing downloaded)" % state)
    return failed


def main():
    parser = argparse.ArgumentParser(
        description="Crawl state law (Gesetze+Verordnungen) for HB, NI, NW")
    parser.add_argument("--state", choices=["hb", "ni", "nw", "all"],
                        default="all")
    parser.add_argument("--dry-run", action="store_true",
                        help="enumerate and write manifests only")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="cap number of downloads per state")
    parser.add_argument("--refresh-index", action="store_true",
                        help="re-run enumeration even if index.json exists")
    args = parser.parse_args()

    states = ["hb", "ni", "nw"] if args.state == "all" else [args.state]
    start = time.time()
    failed_states = []
    blocked_states = []
    for state in states:
        try:
            failed_documents = crawl_state(
                state, dry_run=args.dry_run, limit=args.limit,
                refresh_index=args.refresh_index
            )
            if failed_documents:
                failed_states.append(state)
        except SourceBlocked as exc:
            blocked_states.append(state)
            print("BLOCKED: %s" % exc, file=sys.stderr)
        except Exception as exc:  # keep other states resumable when one portal is blocked
            failed_states.append(state)
            log_error("%s fatal: %s" % (state, exc))
    print("Total elapsed: %.1f s" % (time.time() - start))
    if failed_states:
        return 1
    if blocked_states and not args.dry_run:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
