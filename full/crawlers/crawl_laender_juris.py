#!/usr/bin/env python3
"""Inventory the ten juris-hosted German state-law portals safely.

The portals for BW, BE, HH, HE, MV, RP, SL, ST, SH and TH all reserve text
and data mining (``tdm-reservation: 1``) and publish robots.txt with
``User-agent: * / Disallow: /``.  Their robots files advertise sitemap
indexes, so this crawler may use the sitemap metadata to build a reproducible
inventory, but it deliberately does not request the listed document pages.

Normal mode writes index.json and manifest.json, reports the robots block, and
exits non-zero because no legal texts were downloaded.  ``--dry-run`` performs
the same safe inventory operation but exits zero for launcher smoke tests.
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

from project_paths import DATA_ROOT


SCRIPT_DIR = Path(__file__).resolve().parent
STAMM_DIR = DATA_ROOT / "laender" / "stammgesetze"
USER_AGENT = "EU-Regulation-Research/1.0 (academic research)"
TIMEOUT = 120
MAX_RETRIES = 3
SLEEP_SECONDS = 1.0
CLASSIFICATION_VERSION = 3

PORTALS = {
    "bw": ("https://www.landesrecht-bw.de", "bsbw"),
    "be": ("https://gesetze.berlin.de", "bsbe"),
    "hh": ("https://www.landesrecht-hamburg.de", "bsha"),
    "he": ("https://www.rv.hessenrecht.hessen.de", "bshe"),
    "mv": ("https://www.landesrecht-mv.de", "bsmv"),
    "rp": ("https://www.landesrecht.rlp.de", "bsrp"),
    "sl": ("https://recht.saarland.de", "bssl"),
    "st": ("https://www.landesrecht.sachsen-anhalt.de", "bsst"),
    "sh": ("https://www.gesetze-rechtsprechung.sh.juris.de", "bssh"),
    "th": ("https://landesrecht.thueringen.de", "bsth"),
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch(session, url, timeout=TIMEOUT, max_retries=MAX_RETRIES):
    last = None
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last = exc
            if attempt + 1 < max_retries:
                time.sleep(2 ** attempt)
    raise last


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    tmp.replace(path)


def read_json(path):
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def xml_locs(content):
    root = ET.fromstring(content)
    return [
        (element.text or "").strip()
        for element in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if (element.text or "").strip()
    ]


def is_act_level_id(doc_id):
    """Separate act/root records from provision/version records in jlr IDs.

    Current juris sitemaps use either an opaque root identifier (for example,
    jlr-NNLBW00007BCC) or a classic identifier ending ``rahmen``.  The split is
    structural and does not claim that every root is a currently-in-force
    statute; the robots block prevents metadata inspection.
    """
    # Across the ten current state sitemaps, act-level records use either an
    # opaque NNL identifier (jlr-NNL + two-letter state code + hex payload) or
    # the classic juris ``...rahmen`` suffix.  A negative suffix filter let a
    # handful of provision (``...pP1``) and FFN register pages leak through;
    # use the observed positive grammar instead.
    return bool(
        re.match(r"^jlr-NNL[A-Z]{2}[0-9A-F]+$", doc_id, re.I)
        or re.search(r"rahmen$", doc_id, re.I)
    )


def classify_cached_index(index):
    """Migrate the first inventory format without re-fetching sitemaps."""
    if index.get("classification_version") == CLASSIFICATION_VERSION:
        return index, False
    candidate_items = index.get("items", [])
    act_items = [item for item in candidate_items if is_act_level_id(item["id"])]
    all_count = index.get("all_state_norm_url_count")
    if all_count is None:
        all_count = len(candidate_items)
    index["all_state_norm_url_count"] = all_count
    index["provision_or_version_url_count"] = all_count - len(act_items)
    index["act_level_count"] = len(act_items)
    index["count"] = len(act_items)
    index["items"] = act_items
    index["classification_version"] = CLASSIFICATION_VERSION
    index["classification_method"] = (
        "act/root candidate = opaque jlr-NNL<state><hex> identifier or "
        "classic juris identifier ending in rahmen"
    )
    return index, True


def inventory_state(session, state, refresh):
    base, portal = PORTALS[state]
    state_dir = STAMM_DIR / state
    index_path = state_dir / "index.json"
    if not refresh:
        cached = read_json(index_path)
        if cached and cached.get("inventory_status") == "blocked_sitemap_unavailable":
            print("%s: cached inventory status: advertised sitemap payload unavailable" % state)
            return cached
        if cached and cached.get("inventory_status") in {
            "sitemap_inventory_only", "sitemap_inventory_incomplete",
            "sitemap_inventory_cached_fallback"
        }:
            cached, changed = classify_cached_index(cached)
            if changed:
                write_json(index_path, cached)
            act_count = cached.get("act_level_count")
            total_count = cached.get("all_state_norm_url_count")
            cache_note = (
                " (cached fallback)" if cached.get("sitemap_cache_used")
                else " (incomplete)" if cached.get("sitemap_errors") else ""
            )
            print("%s: cached sitemap inventory: %s act-level candidates; %s total norm URLs%s" %
                  (state, "unknown" if act_count is None else act_count,
                   "unknown" if total_count is None else total_count,
                   cache_note))
            return cached

    robots_url = base + "/robots.txt"
    robots_response = fetch(session, robots_url)
    robots_text = robots_response.text
    robots_disallows_documents = bool(re.search(
        r"(?ims)^User-agent:\s*\*\s*$.*?^Disallow:\s*/\s*$", robots_text
    ))
    if not robots_text.lstrip().lower().startswith("user-agent"):
        index = {
            "state": state,
            "portal": portal,
            "generated_at": now_iso(),
            "inventory_status": "blocked_no_advertised_xml_sitemap",
            "robots_url": robots_url,
            "robots_disallows_documents": None,
            "tdm_reservation": "1",
            "count": None,
            "act_level_count": None,
            "all_state_norm_url_count": None,
            "provision_or_version_url_count": None,
            "classification_version": CLASSIFICATION_VERSION,
            "items": [],
            "notes": (
                "Root robots.txt redirects to the JavaScript application and no XML "
                "sitemap is advertised. Search/API enumeration was not attempted."
            ),
        }
        write_json(index_path, index)
        print("%s: BLOCKED - no advertised XML sitemap" % state)
        return index

    advertised = re.findall(r"(?im)^Sitemap:\s*(\S+)\s*$", robots_text)
    if not advertised:
        index = {
            "state": state,
            "portal": portal,
            "generated_at": now_iso(),
            "inventory_status": "blocked_no_advertised_xml_sitemap",
            "robots_url": robots_url,
            "robots_disallows_documents": robots_disallows_documents,
            "tdm_reservation": "1",
            "count": None,
            "act_level_count": None,
            "all_state_norm_url_count": None,
            "provision_or_version_url_count": None,
            "classification_version": CLASSIFICATION_VERSION,
            "items": [],
        }
        write_json(index_path, index)
        return index

    sitemap_urls = []
    for sitemap_index_url in advertised:
        response = fetch(session, sitemap_index_url)
        sitemap_urls.extend(xml_locs(response.content))
        time.sleep(SLEEP_SECONDS)

    document_urls = set()
    sitemap_counts = {}
    sitemap_errors = {}
    sitemap_cache_used = []
    sitemap_cache_dir = state_dir / "sitemap_cache"
    pattern = re.compile(r"/%s/document/(jlr-[^/?#]+)$" % re.escape(portal), re.I)
    for sitemap_url in sorted(set(sitemap_urls)):
        cache_name = re.sub(r"[^A-Za-z0-9._-]+", "_", urlparse(sitemap_url).path)
        cache_path = sitemap_cache_dir / (cache_name or "sitemap.xml")
        response_content = None
        try:
            # Hessen's current sitemap generator intermittently accepts the
            # request but sends no response body.  One bounded attempt per
            # advertised file keeps the inventory run finite and records the
            # outage explicitly instead of reverting to the obsolete host.
            if state == "he":
                response = fetch(session, sitemap_url, timeout=60, max_retries=1)
            else:
                response = fetch(session, sitemap_url)
            response_content = response.content
            sitemap_cache_dir.mkdir(parents=True, exist_ok=True)
            cache_tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
            cache_tmp.write_bytes(response_content)
            cache_tmp.replace(cache_path)
        except requests.RequestException as exc:
            sitemap_errors[sitemap_url] = str(exc)
            if cache_path.exists() and cache_path.stat().st_size:
                response_content = cache_path.read_bytes()
                sitemap_cache_used.append(sitemap_url)
                print("%s: %s -> LIVE UNAVAILABLE; USING CACHE (%s)" %
                      (state, sitemap_url.rsplit("/", 1)[-1], exc), flush=True)
            else:
                print("%s: %s -> UNAVAILABLE (%s)" %
                      (state, sitemap_url.rsplit("/", 1)[-1], exc), flush=True)
                continue
        locs = xml_locs(response_content)
        matched = 0
        for url in locs:
            if pattern.search(urlparse(url).path):
                document_urls.add(url)
                matched += 1
        sitemap_counts[sitemap_url] = {
            "urls": len(locs),
            "state_norm_urls": matched,
            "cache_used": sitemap_url in sitemap_cache_used,
        }
        print("%s: %s -> %d URLs, %d state-norm URLs" %
              (state, sitemap_url.rsplit("/", 1)[-1], len(locs), matched))
        time.sleep(SLEEP_SECONDS)

    all_items = []
    for url in sorted(document_urls):
        doc_id = urlparse(url).path.rsplit("/", 1)[-1]
        all_items.append({
            "id": doc_id,
            "title": doc_id,
            "source_url": url,
            "category": "Landesnorm (jlr identifier; sitemap metadata)",
        })
    items = [item for item in all_items if is_act_level_id(item["id"])]
    advertised_sitemap_count = len(set(sitemap_urls))
    no_sitemap_payload = bool(sitemap_errors) and not sitemap_counts
    missing_sitemap_payload = len(sitemap_counts) < advertised_sitemap_count
    inventory_status = (
        "blocked_sitemap_unavailable" if no_sitemap_payload
        else "sitemap_inventory_incomplete" if missing_sitemap_payload
        else "sitemap_inventory_cached_fallback" if sitemap_cache_used
        else "sitemap_inventory_only"
    )
    count_value = None if no_sitemap_payload else len(items)
    total_value = None if no_sitemap_payload else len(all_items)
    provision_value = None if no_sitemap_payload else len(all_items) - len(items)
    if no_sitemap_payload:
        items = []
    index = {
        "state": state,
        "portal": portal,
        "generated_at": now_iso(),
        "inventory_status": inventory_status,
        "robots_url": robots_url,
        "robots_disallows_documents": robots_disallows_documents,
        "tdm_reservation": "1",
        "sitemaps": sitemap_counts,
        "advertised_sitemaps": sorted(set(sitemap_urls)),
        "sitemap_errors": sitemap_errors,
        "sitemap_cache_used": sitemap_cache_used,
        "all_state_norm_url_count": total_value,
        "provision_or_version_url_count": provision_value,
        "act_level_count": count_value,
        "count": count_value,
        "items": items,
        "classification_version": CLASSIFICATION_VERSION,
        "classification_method": (
            "act/root candidate = opaque jlr-NNL<state><hex> identifier or "
            "classic juris identifier ending in rahmen"
        ),
        "notes": (
            "The robots-advertised sitemap metadata was enumerated where the sitemap server "
            "responded; sitemap_errors identifies any advertised file that was unavailable. "
            "all_state_norm_url_count "
            "includes provision/version URLs; count and act_level_count are structural root "
            "candidates, not a verified in-force-statute count. Document pages were not "
            "requested because robots.txt disallows User-agent: * at /."
        ),
    }
    write_json(index_path, index)
    return index


def write_blocked_manifest(state, index, limit):
    state_dir = STAMM_DIR / state
    manifest_path = state_dir / "manifest.json"
    items = index.get("items", [])
    # There are no document requests to cap.  Keep the inventory complete so
    # a launcher smoke test with --limit cannot truncate an existing manifest.
    entries = [{
        "id": item["id"],
        "title": item["title"],
        "source_url": item["source_url"],
        "file": None,
        "format": "html",
        "downloaded_at": None,
        "status": "listed_robots_blocked",
    } for item in items]
    write_json(manifest_path, entries)
    return len(entries)


def main():
    parser = argparse.ArgumentParser(
        description="Inventory the ten juris Länder portals without violating robots.txt"
    )
    parser.add_argument("--state", choices=list(PORTALS) + ["all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh-index", action="store_true")
    args = parser.parse_args()

    states = list(PORTALS) if args.state == "all" else [args.state]
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    blocked = []
    for state in states:
        try:
            index = inventory_state(session, state, args.refresh_index)
            listed = write_blocked_manifest(state, index, args.limit)
            print("%s: %d inventory entries; 0 documents downloaded (robots/TDM block)" %
                  (state, listed))
            blocked.append(state)
        except Exception as exc:
            print("%s: ERROR %s" % (state, exc), file=sys.stderr)
            blocked.append(state)
    if blocked:
        print("Blocked document downloads: %s" % ", ".join(blocked), file=sys.stderr)
    return 0 if args.dry_run else (2 if blocked else 0)


if __name__ == "__main__":
    sys.exit(main())
