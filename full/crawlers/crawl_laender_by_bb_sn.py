#!/usr/bin/env python3
"""Crawler for German state legislation (Stammgesetze) for BY, BB, SN.

Portals:
  by  Bayern      https://www.gesetze-bayern.de/   (BAYERN.RECHT, session-based
                  hitlist: /Search/Filter/DOKTYP/norm + /Search/Page/N,
                  documents at /Content/Document/<id>)
  bb  Brandenburg https://bravors.brandenburg.de/  (BRAVORS, chronological
                  Fundstellennachweis index by year, documents at
                  /gesetze/<slug> and /verordnungen/<slug>)
  sn  Sachsen     https://www.revosax.sachsen.de/  (REVOSAX, sitemap.xml
                  enumerates all /vorschrift/<slug> pages)

Usage:
  python3 crawl_laender_by_bb_sn.py --state by            # crawl Bayern
  python3 crawl_laender_by_bb_sn.py --state all --limit 5 # smoke test
  python3 crawl_laender_by_bb_sn.py --state sn --dry-run  # enumerate only

Idempotent and resumable: enumeration is cached in <state>/index.json
(refresh with --refresh-index), already-downloaded files are skipped, and
the manifest is updated incrementally.
"""

import argparse
import email.utils
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from project_paths import CRAWLER_LOG_ROOT, DATA_ROOT as PROJECT_DATA_ROOT

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.fspath(PROJECT_DATA_ROOT / "laender" / "stammgesetze")
ERROR_LOG = os.fspath(CRAWLER_LOG_ROOT / "laender_by_bb_sn_errors.log")

USER_AGENT = "EU-Regulation-Research/1.0 (academic research)"
MAX_RETRIES = 4
BACKOFF_BASE = 3.0  # seconds; grows 3, 6, 12, 24

# Per-state politeness delay (seconds). robots.txt of bravors.brandenburg.de
# requests Crawl-delay: 20.  Never go below that published minimum.
DEFAULT_DELAYS = {"by": (0.4, 0.6), "bb": (20.0, 21.0), "sn": (0.4, 0.6)}

# Four legacy /sixcms/detail.php links currently return the literal response
# "invalid template".  Two have exact official BRAVORS publication-PDF
# replacements; the two nonofficial translations do not have a recoverable
# official payload and remain truthful inventory-only records.
BB_SUPPLEMENTAL_RECOVERY = {
    "supp_detail_231722": {
        "url": "https://bravors.brandenburg.de/fm/76/GVBl_I_03_2011.pdf",
        "reason": (
            "Recovered from the official BRAVORS 2011 publication index; "
            "the discovered detail endpoint returns 'invalid template'."
        ),
    },
    "supp_detail_241254": {
        "url": (
            "https://bravors.brandenburg.de/sixcms/media.php/"
            "land_bb_bravors_01.a.111.de/gvbl_ii_14_2004.pdf"
        ),
        "reason": (
            "Recovered from the official BRAVORS GVBl II 14/2004 PDF; "
            "the discovered detail endpoint returns 'invalid template'."
        ),
    },
}

log = logging.getLogger("crawl_laender_by_bb_sn")


def setup_logging():
    log.setLevel(logging.DEBUG)
    con = logging.StreamHandler(sys.stdout)
    con.setLevel(logging.INFO)
    con.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    log.addHandler(con)
    os.makedirs(os.path.dirname(ERROR_LOG), exist_ok=True)
    err = logging.FileHandler(ERROR_LOG)
    err.setLevel(logging.WARNING)
    err.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(err)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


_LAST_REQUEST_AT = {}


def polite_get(session, url, state, **kwargs):
    """Issue one GET while enforcing the portal delay between real requests.

    The timestamp is process-global per state, so enumeration, retries, root
    downloads, supplemental downloads, and redirect probes all share one
    throttle.  Cached/skipped records never call this function and therefore
    do not sleep.
    """
    previous = _LAST_REQUEST_AT.get(state)
    if previous is not None:
        lo, hi = DEFAULT_DELAYS[state]
        target_delay = random.uniform(lo, hi)
        remaining = target_delay - (time.monotonic() - previous)
        if remaining > 0:
            time.sleep(remaining)
    _LAST_REQUEST_AT[state] = time.monotonic()
    return session.get(url, **kwargs)


def fetch(session, url, state, expect_html=True):
    """GET with retries and exponential backoff. Returns Response or None."""
    for attempt in range(MAX_RETRIES):
        retry_after_delay = None
        try:
            resp = polite_get(session, url, state, timeout=60)
            if resp.status_code == 200:
                if expect_html and not resp.text.strip():
                    raise requests.RequestException("empty body")
                return resp
            if resp.status_code in (404, 410):
                log.warning("[%s] HTTP %s (permanent) %s", state, resp.status_code, url)
                return None
            if resp.status_code in (429, 503):
                retry_after = resp.headers.get("Retry-After", "").strip()
                if retry_after.isdigit():
                    retry_after_delay = float(retry_after)
                elif retry_after:
                    try:
                        retry_at = email.utils.parsedate_to_datetime(retry_after)
                        retry_after_delay = max(
                            0.0, retry_at.timestamp() - time.time()
                        )
                    except (TypeError, ValueError, OverflowError):
                        pass
            raise requests.RequestException("HTTP %s" % resp.status_code)
        except requests.TooManyRedirects as exc:
            # A REVOSax sitemap occasionally retains a historical instrument
            # that redirects through a long chain of successors. Repeating the
            # entire chain cannot repair it and imposes needless load.
            log.warning("[%s] redirect chain exceeds limit for %s: %s",
                        state, url, exc)
            return None
        except requests.RequestException as exc:
            wait = BACKOFF_BASE * (2 ** attempt)
            if retry_after_delay is not None:
                wait = max(wait, retry_after_delay)
            log.warning("[%s] attempt %d/%d failed for %s: %s (backoff %.0fs)",
                        state, attempt + 1, MAX_RETRIES, url, exc, wait)
            if attempt + 1 < MAX_RETRIES:
                time.sleep(wait)
    log.error("[%s] giving up on %s", state, url)
    return None


def safe_filename(doc_id):
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", doc_id).strip("_")
    return (name[:180] or "doc") + ".html"


# ---------------------------------------------------------------- enumeration

def enumerate_by(session):
    """Bayern: apply DOKTYP/norm facet, walk paginated hitlist (10 hits/page)."""
    base = "https://www.gesetze-bayern.de"
    # Establish session cookie, then apply the "Vorschriften" facet.
    fetch(session, base + "/", "by")
    first = fetch(session, base + "/Search/Filter/DOKTYP/norm", "by")
    if first is None:
        raise RuntimeError("BY: could not load filtered hitlist")
    soup = BeautifulSoup(first.text, "lxml")
    m = re.search(r"([\d.]+)\s+Treffer", soup.get_text())
    total = int(m.group(1).replace(".", "")) if m else None
    log.info("[by] portal reports %s Treffer", total)

    items, seen = [], set()

    def collect(soup_):
        new = 0
        hitlist = soup_.find(id="hitlist") or soup_
        for a in hitlist.select("p.hltitel a[href]"):
            href = a["href"].split("?")[0]
            m_ = re.match(r"^/Content/Document/([A-Za-z0-9._-]+)$", href)
            if not m_:
                continue
            doc_id = m_.group(1)
            if doc_id in seen:
                continue
            seen.add(doc_id)
            b = a.find("b")
            title = (b.get_text(" ", strip=True) if b else a.get_text(" ", strip=True))
            items.append({"id": doc_id, "title": title,
                          "source_url": base + "/Content/Document/" + doc_id})
            new += 1
        return new

    collect(soup)
    n_pages = (total + 9) // 10 if total else 1
    for page in range(2, n_pages + 1):
        resp = fetch(session, base + "/Search/Page/%d" % page, "by")
        if resp is None:
            continue
        collect(BeautifulSoup(resp.text, "lxml"))
        if page % 25 == 0:
            log.info("[by] enumerated %d/%s (page %d/%d)", len(items), total, page, n_pages)
    if total is not None and len(items) != total:
        raise RuntimeError(
            "BY enumeration incomplete: portal reported %d results, collected %d unique IDs"
            % (total, len(items))
        )
    log.info("[by] enumeration done: %d items (portal count: %s)", len(items), total)
    return items, total


def enumerate_bb(session):
    """Brandenburg: chronological Fundstellennachweis, one page per year."""
    base = "https://bravors.brandenburg.de"
    index_url = base + "/de/vorschriften_fundstellennachweis_gesetzte_und_verordnungen_chronologisch"
    resp = fetch(session, index_url, "bb")
    if resp is None:
        raise RuntimeError("BB: could not load chronological index")
    soup = BeautifulSoup(resp.text, "lxml")
    years = sorted({a["href"] for a in soup.select('a[href*="chronologisch/year/"]')})
    log.info("[bb] found %d year pages", len(years))

    items, seen = [], set()
    failed_years = []
    for year_href in years:
        resp = fetch(session, base + year_href, "bb")
        if resp is None:
            failed_years.append(year_href)
            continue
        ysoup = BeautifulSoup(resp.text, "lxml")
        for a in ysoup.select('a[href^="/gesetze/"], a[href^="/verordnungen/"]'):
            href = a["href"].split("#")[0].split("?")[0].rstrip("/")
            m = re.match(r"^/(gesetze|verordnungen)/([A-Za-z0-9._-]+)$", href)
            if not m:
                continue
            doc_id = "%s_%s" % (m.group(1)[:3], m.group(2))
            if doc_id in seen:
                continue
            seen.add(doc_id)
            title = a.get_text(" ", strip=True) or m.group(2)
            items.append({"id": doc_id, "title": title, "source_url": base + href})
        log.info("[bb] %s -> total %d items", year_href.rsplit("/", 1)[-1], len(items))
    if failed_years:
        raise RuntimeError("BB enumeration incomplete; failed year pages: %s" %
                           ", ".join(failed_years))
    log.info("[bb] enumeration done: %d items", len(items))
    return items, None


def enumerate_sn(session):
    """Sachsen: sitemap.xml enumerates every /vorschrift/<slug> page."""
    resp = fetch(session, "https://www.revosax.sachsen.de/sitemap.xml", "sn")
    if resp is None:
        raise RuntimeError("SN: could not load sitemap.xml")
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    root = ET.fromstring(resp.content)
    items, seen = [], set()
    for loc in root.findall(".//sm:loc", ns):
        url = (loc.text or "").strip()
        m = re.match(r"^https://www\.revosax\.sachsen\.de/vorschrift/([^/?#]+)$", url)
        if not m:
            continue
        slug = m.group(1)
        if slug in seen:
            continue
        seen.add(slug)
        title = re.sub(r"[-_]+", " ", re.sub(r"^\d+-", "", slug)).strip() or slug
        items.append({"id": slug, "title": title, "source_url": url})
    log.info("[sn] enumeration done: %d items from sitemap", len(items))
    return items, None


ENUMERATORS = {"by": enumerate_by, "bb": enumerate_bb, "sn": enumerate_sn}
SCOPE_NOTES = {
    "by": ("Inclusive BAYERN.RECHT Vorschriften inventory. It mixes laws, "
           "ordinances, administrative provisions and other norm documents; "
           "the count is not a Stammgesetze-only count."),
    "bb": ("Inclusive BRAVORS chronological inventory of /gesetze and "
           "/verordnungen documents; amendment and historical instruments may "
           "be included."),
    "sn": ("Inclusive inventory of every REVOSAX /vorschrift sitemap entry. "
           "It mixes full acts, regulations, administrative provisions, "
           "amendment instruments and short component/reference documents; "
           "slugs alone do not support a reliable document-level subtype."),
}


# ------------------------------------------------------------------ downloads

def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return default


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def get_index(session, state, refresh):
    state_dir = os.path.join(DATA_ROOT, state)
    os.makedirs(state_dir, exist_ok=True)
    index_path = os.path.join(state_dir, "index.json")
    if not refresh and os.path.exists(index_path):
        idx = load_json(index_path, None)
        if idx and idx.get("items"):
            changed = False
            if idx.get("inventory_scope") != SCOPE_NOTES[state]:
                idx["inventory_scope"] = SCOPE_NOTES[state]
                changed = True
            if idx.get("count") != len(idx["items"]):
                idx["count"] = len(idx["items"])
                changed = True
            if "expected_total" not in idx:
                idx["expected_total"] = None
                changed = True
            if "enumeration_complete" not in idx:
                # This cache could only have been written after its enumerator
                # returned successfully.  The BB enumerator raises if any
                # chronological year page fails, so its 967-item cache is a
                # complete enumeration even though the portal publishes no
                # independent total.
                idx["enumeration_complete"] = True
                changed = True
            if changed:
                write_json(index_path, idx)
            log.info("[%s] using cached index.json: %d items (generated %s)",
                     state, len(idx["items"]), idx.get("generated_at"))
            return idx["items"]
    result = ENUMERATORS[state](session)
    items, expected_total = result
    write_json(index_path, {"state": state, "generated_at": now_iso(),
                            "count": len(items),
                            "expected_total": expected_total,
                            "enumeration_complete": (
                                expected_total is None or len(items) == expected_total
                            ),
                            "inventory_scope": SCOPE_NOTES[state],
                            "items": items})
    return items


def extract_supplemental_items(state, root_item, text):
    """Return documents linked from a root portal record.

    BRAVORS stores heterogeneous documents behind /sixcms/detail.php links,
    including agreements, regulations, gazette issues, annexes, and explicitly
    nonofficial text versions. REVOSAX component records name their parent
    immediately after the marker ``Bestandteil der Vorschrift``. Neither class
    is reliably present in the portals' primary bulk inventory.
    """
    discovered = []
    if state == "bb" and "/sixcms/detail.php/" not in text:
        return discovered
    if state == "sn" and "Bestandteil der Vorschrift" not in text:
        return discovered
    soup = BeautifulSoup(text, "lxml")
    if state == "bb":
        for anchor in soup.select('a[href^="/sixcms/detail.php/"]'):
            href = anchor.get("href", "").split("#")[0].split("?")[0]
            match = re.match(r"^/sixcms/detail\.php/(\d+)$", href)
            if not match:
                continue
            title = (
                anchor.get_text(" ", strip=True)
                or str(anchor.get("title", "")).strip()
                or "Linked BRAVORS document"
            )
            is_nonofficial = bool(re.search(r"\bnicht\s*amtlich", title, re.I))
            discovered.append({
                "id": "supp_detail_%s" % match.group(1),
                "title": title,
                "source_url": "https://bravors.brandenburg.de" + href,
                "record_role": (
                    "supplemental_nonofficial_text_version"
                    if is_nonofficial
                    else "supplemental_linked_document"
                ),
                "category": (
                    "Non-official linked text version disclosed by BRAVORS."
                    if is_nonofficial
                    else (
                        "Document linked by a BRAVORS root record; outside the "
                        "primary chronological inventory."
                    )
                ),
                "discovered_from_ids": [root_item["id"]],
            })
    elif state == "sn":
        law = soup.select_one(".law_show")
        if law is not None:
            marker = law.find(string=re.compile(r"Bestandteil\s+der\s+Vorschrift"))
            anchor = marker.find_next("a", href=True) if marker is not None else None
            if anchor is not None and anchor in law.descendants:
                href = anchor.get("href", "").split("#")[0].split("?")[0]
                match = re.match(r"^/vorschrift/([^/?#]+)$", href)
                if match:
                    slug = match.group(1)
                    discovered.append({
                        "id": slug,
                        "title": anchor.get_text(" ", strip=True) or slug,
                        "source_url": "https://www.revosax.sachsen.de" + href,
                        "record_role": "supplemental_linked_parent",
                        "category": (
                            "Parent instrument linked by a REVOSAX component record "
                            "but absent from the current sitemap inventory."
                        ),
                        "discovered_from_ids": [root_item["id"]],
                    })
    return discovered


def merge_supplemental_items(existing, discovered, root_ids):
    """Merge supplementals by ID while preserving all discovery provenance."""
    merged = {}
    for item in list(existing) + list(discovered):
        doc_id = item.get("id")
        if not doc_id or doc_id in root_ids:
            continue
        if doc_id not in merged:
            merged[doc_id] = dict(item)
            merged[doc_id]["discovered_from_ids"] = sorted(set(
                item.get("discovered_from_ids", [])
            ))
        else:
            merged[doc_id]["discovered_from_ids"] = sorted(set(
                merged[doc_id].get("discovered_from_ids", [])
                + item.get("discovered_from_ids", [])
            ))
    return sorted(merged.values(), key=lambda item: item["id"])


def discover_supplementals_from_files(state, state_dir, root_items):
    found = []
    inspected = 0
    for item in root_items:
        path = os.path.join(state_dir, safe_filename(item["id"]))
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
            if validate_html_payload(state, text):
                inspected += 1
                found.extend(extract_supplemental_items(state, item, text))
        except OSError as exc:
            log.warning("[%s] could not inspect %s for linked texts: %s",
                        state, path, exc)
    return found, inspected


def persist_supplemental_index(
    state_dir, root_count, supplemental_items, discovery_complete
):
    index_path = os.path.join(state_dir, "index.json")
    index = load_json(index_path, {})
    index["supplemental_items"] = supplemental_items
    index["supplemental_count"] = len(supplemental_items)
    index["supplemental_discovery_complete"] = discovery_complete
    index["count_including_supplementals"] = root_count + len(supplemental_items)
    index["supplemental_inventory_note"] = (
        "Supplementals are discovered from downloaded root payloads and are "
        "separate from the authoritative primary inventory count."
    )
    write_json(index_path, index)


def title_from_html(text, fallback):
    soup = BeautifulSoup(text, "lxml")
    t = soup.find("title")
    if t:
        title = re.sub(r"\s+", " ", t.get_text(" ", strip=True))
        # strip portal boilerplate prefixes/suffixes
        title = re.sub(r"^REVOSax Landesrecht Sachsen\s*[-–]?\s*", "", title)
        title = re.sub(r"\s*[-–|]\s*(Bürgerservice|BRAVORS|REVOSax).*$", "", title).strip()
        if title:
            return title
    return fallback


def by_full_view_url(text):
    """Return Bayern's full-document link if the root page is multipart."""
    match = re.search(r'href="(/Content/Document/[A-Za-z0-9._-]+/true)"', text)
    return "https://www.gesetze-bayern.de" + match.group(1) if match else None


def validate_html_payload(state, text):
    """Reject portal error/shell responses before treating a file as complete."""
    head = text[:1000].lstrip().lower()
    if not (head.startswith("<!doctype") or "<html" in head):
        return False
    lower = text.lower()
    if "aktuell nicht oder nicht mehr im datenbestand" in lower:
        return False
    if state == "by":
        return ('id="content"' in lower and "document-all" in lower
                and "<title" in lower)
    if state == "bb":
        return 'id="content"' in lower and "<title" in lower
    if state == "sn":
        return 'class="law_show"' in lower and "<title" in lower
    return False


def validate_pdf_payload(payload):
    stripped = payload.lstrip()
    return stripped.startswith(b"%PDF-") and stripped.rstrip().endswith(b"%%EOF")


def crawl_state(state, args):
    session = make_session()
    if state == "sn":
        session.max_redirects = 12
    root_items = get_index(session, state, args.refresh_index)
    state_dir = os.path.join(DATA_ROOT, state)
    manifest_path = os.path.join(state_dir, "manifest.json")
    pre_manifest = load_json(manifest_path, [])
    index_data = load_json(os.path.join(state_dir, "index.json"), {})
    root_ids = {item["id"] for item in root_items}
    supplemental_items = []
    supplemental_discovery_complete = True
    if state in ("bb", "sn"):
        cached_supplementals = index_data.get("supplemental_items", [])
        discovered, inspected_roots = discover_supplementals_from_files(
            state, state_dir, root_items
        )
        explicit_unavailable_root_ids = {
            entry.get("id") for entry in pre_manifest
            if (entry.get("id") in root_ids
                and entry.get("status") == "source_redirect_chain_unresolved")
        }
        supplemental_discovery_complete = (
            inspected_roots == len(root_items) - len(explicit_unavailable_root_ids)
        )
        supplemental_items = merge_supplemental_items(
            [] if supplemental_discovery_complete else cached_supplementals,
            discovered,
            root_ids,
        )
        if not args.dry_run:
            persist_supplemental_index(
                state_dir, len(root_items), supplemental_items,
                supplemental_discovery_complete,
            )
    all_items = list(root_items) + list(supplemental_items)
    log.info("[%s] expected primary count: %d; linked supplementals: %d",
             state, len(root_items), len(supplemental_items))
    items = list(all_items)
    if args.limit is not None:
        items = items[:max(args.limit, 0)]
    if args.dry_run:
        for it in items[:10]:
            log.info("[%s] DRY %s | %s", state, it["id"], it["title"][:80])
        log.info("[%s] dry-run: would download %d documents", state, len(items))
        return

    manifest = pre_manifest
    current_ids = {item["id"] for item in all_items}
    stale = [
        entry for entry in manifest
        if (entry.get("id") not in current_ids
            and (supplemental_discovery_complete
                 or not str(entry.get("record_role", "")).startswith("supplemental_")))
    ]
    if stale:
        stale_path = os.path.join(state_dir, "manifest_stale.json")
        archived = load_json(stale_path, [])
        archived_by_id = {entry.get("id"): entry for entry in archived}
        for entry in stale:
            entry["status"] = "stale_not_in_current_index"
            archived_by_id[entry.get("id")] = entry
        write_json(stale_path, sorted(
            archived_by_id.values(), key=lambda entry: entry.get("id", "")
        ))
        stale_ids = {entry.get("id") for entry in stale}
        manifest = [entry for entry in manifest if entry.get("id") not in stale_ids]
    by_id = {e["id"]: e for e in manifest}
    for item in all_items:
        if item["id"] in by_id:
            by_id[item["id"]]["category"] = item.get(
                "category", SCOPE_NOTES[state]
            )

    done = skipped = failed = unavailable = 0
    for i, it in enumerate(items):
        fname = safe_filename(it["id"])
        prev = by_id.get(it["id"])
        if (prev and prev.get("status") == "ok" and prev.get("file")
                and os.path.basename(prev["file"]) == prev["file"]):
            fname = prev["file"]
        fpath = os.path.join(state_dir, fname)
        discovered_source_url = it["source_url"]
        recovery = (
            BB_SUPPLEMENTAL_RECOVERY.get(it["id"])
            if state == "bb" else None
        )
        fetch_url = recovery["url"] if recovery else discovered_source_url
        if (state == "bb" and prev
                and prev.get("status") == "source_link_invalid_template"
                and prev.get("source_url") == discovered_source_url
                and not args.refresh_index):
            prev.update({
                "title": it["title"],
                "file": None,
                "format": None,
                "category": it.get("category", SCOPE_NOTES[state]),
                "downloaded_at": None,
                "record_role": it.get("record_role"),
                "discovered_from_ids": sorted(set(
                    it.get("discovered_from_ids", [])
                )),
            })
            skipped += 1
            continue
        if (state == "sn" and prev
                and prev.get("status") == "source_redirect_chain_unresolved"
                and prev.get("source_url") == fetch_url
                and not args.refresh_index):
            prev["file"] = None
            prev["format"] = None
            prev["downloaded_at"] = None
            skipped += 1
            continue
        if os.path.exists(fpath) and prev and prev.get("status") == "ok":
            existing_text = None
            if prev.get("format") == "pdf":
                with open(fpath, "rb") as fh:
                    existing_valid = validate_pdf_payload(fh.read())
            else:
                with open(fpath, encoding="utf-8", errors="replace") as fh:
                    existing_text = fh.read()
                existing_valid = validate_html_payload(state, existing_text)
            if not existing_valid:
                log.warning("[%s] existing payload failed validation; refreshing %s",
                            state, fpath)
            elif prev.get("format") == "pdf" and prev.get("source_url") == fetch_url:
                skipped += 1
                continue
            elif state == "by" and prev.get("source_url", "").endswith("/true"):
                skipped += 1
                continue
            if existing_valid and state == "by":
                full_url = by_full_view_url(existing_text)
                if full_url:
                    # The saved root page contains only the title/preamble.
                    fetch_url = full_url
                elif "aktuell nicht oder nicht mehr im Datenbestand" not in existing_text:
                    skipped += 1
                    continue
            elif (existing_valid
                  and prev.get("source_url") == fetch_url):
                skipped += 1
                continue
        is_bb_supplemental = (
            state == "bb"
            and str(it.get("record_role", "")).startswith("supplemental_")
        )
        resp = fetch(
            session, fetch_url, state,
            expect_html=not (is_bb_supplemental and recovery),
        )
        payload_format = "html"
        source_status = None
        source_reason = None
        if resp is not None and state == "by":
            # A freshly fetched root page may expose a separate Gesamtansicht.
            full_url = by_full_view_url(resp.text)
            if full_url:
                fetch_url = full_url
                resp = fetch(session, fetch_url, state)
            if (resp is not None
                    and "aktuell nicht oder nicht mehr im Datenbestand" in resp.text):
                log.warning("[by] portal returned a not-in-dataset page: %s", fetch_url)
                resp = None
        if resp is not None:
            if is_bb_supplemental and validate_pdf_payload(resp.content):
                payload_format = "pdf"
                fname = os.path.splitext(safe_filename(it["id"]))[0] + ".pdf"
                fpath = os.path.join(state_dir, fname)
            elif not validate_html_payload(state, resp.text):
                if (is_bb_supplemental
                        and resp.content.strip().lower() == b"invalid template"):
                    source_status = "source_link_invalid_template"
                    source_reason = (
                        "Official linked endpoint returned HTTP 200 with the literal "
                        "16-byte body 'invalid template'; no document payload was "
                        "available on 2026-08-05."
                    )
                else:
                    log.warning("[%s] response failed portal payload validation: %s",
                                state, fetch_url)
                resp = None
        redirect_target = None
        if resp is None and state == "sn":
            # REVOSax occasionally leaves a stale sitemap URL that redirects
            # through a long chain of successor instruments.  Saving the
            # terminal successor under the historical ID would be incorrect,
            # so retain the inventory record and state the source condition.
            try:
                probe = polite_get(
                    session, fetch_url, state, timeout=60,
                    allow_redirects=False,
                )
                if 300 <= probe.status_code < 400 and probe.headers.get("Location"):
                    redirect_target = probe.headers["Location"]
                    source_status = "source_redirect_chain_unresolved"
            except requests.RequestException:
                pass
        entry = {
            "id": it["id"],
            "title": it["title"],
            "source_url": fetch_url,
            "file": fname,
            "format": payload_format,
            "category": it.get("category", SCOPE_NOTES[state]),
            "downloaded_at": (
                now_iso() if resp is not None
                else (prev.get("downloaded_at") if prev and os.path.exists(fpath) else None)
            ),
            "observed_at": now_iso(),
            "status": "ok" if resp is not None else (source_status or "error"),
        }
        if source_status in (
                "source_redirect_chain_unresolved",
                "source_link_invalid_template"):
            # The sitemap identity is retained, but no historical payload was
            # obtained, or the official linked endpoint has no usable body.
            # Do not advertise a filename that does not exist.
            entry["file"] = None
            entry["format"] = None
            entry["downloaded_at"] = None
        if recovery:
            entry["discovered_source_url"] = discovered_source_url
            entry["source_recovery_reason"] = recovery["reason"]
        if source_reason:
            entry["source_reason"] = source_reason
            entry["source_http_status"] = 200
            entry["source_content_type"] = resp.headers.get("Content-Type") if resp else (
                "text/html; charset=utf-8"
            )
            entry["source_payload_sha256"] = (
                "d9c938f968a3eaf9a8695dbc5b95138895d59798550d3df45516c391f3edf3b2"
            )
        if it.get("record_role"):
            entry["record_role"] = it["record_role"]
        if it.get("discovered_from_ids"):
            entry["discovered_from_ids"] = sorted(set(
                it["discovered_from_ids"]
            ))
        if redirect_target:
            entry["redirect_target"] = redirect_target
        if resp is not None:
            tmp_path = fpath + ".tmp"
            if payload_format == "pdf":
                with open(tmp_path, "wb") as fh:
                    fh.write(resp.content)
                    fh.flush()
                    os.fsync(fh.fileno())
                entry["bytes"] = len(resp.content)
                entry["sha256"] = hashlib.sha256(resp.content).hexdigest()
                entry["content_type"] = resp.headers.get("Content-Type")
            else:
                with open(tmp_path, "w", encoding="utf-8") as fh:
                    fh.write(resp.text)
            os.replace(tmp_path, fpath)
            if payload_format == "html":
                entry["title"] = title_from_html(resp.text, it["title"])
            done += 1
            if (
                state in ("bb", "sn")
                and not it.get("record_role", "").startswith("supplemental_")
            ):
                newly_discovered = extract_supplemental_items(
                    state, it, resp.text
                )
                updated_supplementals = merge_supplemental_items(
                    supplemental_items, newly_discovered, root_ids
                )
                known_items = {item["id"]: item for item in all_items}
                for supplemental in updated_supplementals:
                    if supplemental["id"] in known_items:
                        known_items[supplemental["id"]].update(supplemental)
                    else:
                        all_items.append(supplemental)
                        current_ids.add(supplemental["id"])
                        known_items[supplemental["id"]] = supplemental
                        if args.limit is None:
                            items.append(supplemental)
                supplemental_items = updated_supplementals
        else:
            if source_status:
                unavailable += 1
            else:
                failed += 1
        if prev:
            if not redirect_target:
                prev.pop("redirect_target", None)
            for optional_key in (
                    "record_role", "discovered_from_ids",
                    "discovered_source_url", "source_recovery_reason",
                    "source_reason", "source_http_status",
                    "source_content_type", "source_payload_sha256",
                    "bytes", "sha256", "content_type"):
                if optional_key not in entry:
                    prev.pop(optional_key, None)
            prev.update(entry)
        else:
            manifest.append(entry)
            by_id[it["id"]] = entry
        if (i + 1) % 25 == 0:
            write_json(manifest_path, manifest)
            log.info("[%s] progress %d/%d (ok %d, skipped %d, unavailable %d, failed %d)",
                     state, i + 1, len(items), done, skipped, unavailable, failed)
    write_json(manifest_path, manifest)
    if state in ("bb", "sn"):
        persist_supplemental_index(
            state_dir, len(root_items), supplemental_items,
            supplemental_discovery_complete,
        )
    log.info("[%s] finished: downloaded %d, skipped %d, unavailable %d, failed %d "
             "(manifest: %d entries)",
             state, done, skipped, unavailable, failed, len(manifest))
    return failed


def main():
    ap = argparse.ArgumentParser(description="Crawl state legislation for BY, BB, SN")
    ap.add_argument("--state", default="all", choices=["by", "bb", "sn", "all"])
    ap.add_argument("--dry-run", action="store_true", help="enumerate only, no downloads")
    ap.add_argument("--limit", type=int, default=None, help="max documents per state")
    ap.add_argument("--refresh-index", action="store_true", help="re-enumerate, ignore cached index.json")
    args = ap.parse_args()

    setup_logging()
    states = ["by", "bb", "sn"] if args.state == "all" else [args.state]
    failed_states = []
    for state in states:
        log.info("=== state %s ===", state)
        try:
            failed_documents = crawl_state(state, args)
            if failed_documents:
                failed_states.append(state)
        except Exception as exc:
            log.error("[%s] fatal: %s", state, exc, exc_info=True)
            failed_states.append(state)
    return 1 if failed_states else 0


if __name__ == "__main__":
    sys.exit(main())
