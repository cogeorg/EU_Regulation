#!/usr/bin/env python3
"""Download integral assets linked by the BY, SN and NW legislation corpora.

The primary crawlers store preferred HTML legal texts.  This companion pass
stores annexes, forms, maps and attachment bundles which are not embedded in
those HTML files.  Root full-document PDF representations are intentionally
excluded because they duplicate the preferred HTML; the index states this
boundary explicitly.

Usage:
  python3 crawl_laender_assets.py --state by
  python3 crawl_laender_assets.py --state all [--dry-run] [--limit N]
"""

import argparse
import email.utils
import hashlib
import io
import json
import mimetypes
import os
import random
import re
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from project_paths import DATA_ROOT as PROJECT_DATA_ROOT


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_DATA_ROOT / "laender" / "stammgesetze"
USER_AGENT = "EU-Regulation-Research/1.0 (academic research)"
MAX_RETRIES = 4
BACKOFF_BASE = 2.0
STREAM_CHUNK_BYTES = 1024 * 1024
DISK_SAFETY_RESERVE_BYTES = 256 * 1024 * 1024
DELAYS = {"by": (0.4, 0.6), "sn": (0.4, 0.6), "nw": (0.4, 0.6)}
BASE_URLS = {
    "by": "https://www.gesetze-bayern.de",
    "sn": "https://www.revosax.sachsen.de",
    "nw": "https://recht.nrw.de",
}
# A small number of references embedded in current REVOSAX legal texts point
# to URLs which their own government publishers have moved.  Keep the linked
# URL for provenance and fetch the verified government-hosted successor.
SOURCE_REPLACEMENTS = {
    (
        "https://www.bauen-wohnen.sachsen.de/download/"
        "Erlaeuterung_Indikatorenberechnung.pdf"
    ): {
        "url": (
            "https://www.bauen-wohnen.sachsen.de/download/"
            "Erlaeuterungen_zu_den_Datengrundlagen_der_Indikatoren.pdf"
        ),
        "reason": "official publisher moved the referenced PDF",
    },
    (
        "https://www.cio.bund.de/SharedDocs/downloads/Webs/CIO/DE/it-rat/"
        "beschluesse/beschluss_2015_03.pdf?__blob=publicationFile&v=2"
    ): {
        "url": (
            "https://www.finanzen.bremen.de/sixcms/media.php/13/"
            "Fachkonzept_WiBe50.24236.pdf"
        ),
        "reason": (
            "stale CIO Bund URL; identical WiBe 5.0 publication retained by "
            "the official Bremen transparency portal"
        ),
    },
    "http://www.bergbehoerde.sachsen.de/set/431/RL%20SpW.pdf": {
        "url": "https://www.oba.sachsen.de/download/RL_SpW.pdf",
        "reason": (
            "verified title/date-identical legal directive moved by the official "
            "publisher to OBA Sachsen"
        ),
    },
    "http://www.bergbehoerde.sachsen.de/set/431/RL_BesBergw.pdf": {
        "url": "https://www.oba.sachsen.de/download/RL_BesBergw.pdf",
        "reason": (
            "verified title/date-identical legal directive moved by the official "
            "publisher to OBA Sachsen"
        ),
    },
    (
        "http://www.bergbehoerde.sachsen.de/set/431/"
        "BetriebsplanrichtlinieTagebaue2011Aug.pdf"
    ): {
        "url": (
            "https://www.oba.sachsen.de/download/CMS/"
            "Merkblatt_Betriebsplanunterlagen_Tgb_2025_06.pdf"
        ),
        "reason": (
            "historical 2011 linked PDF is unavailable; this is the current "
            "topically corresponding June 2025 OBA Sachsen document, not a "
            "proven content-identical replacement"
        ),
    },
    "http://www.bergbehoerde.sachsen.de/set/431/RL_Sachverstaendige.pdf": {
        "terminal_status": "source_superseded_current_root",
        "replacement_root_id": "21182-Sachverstaendigenrichtlinie",
        "replacement_root_file": "21182-Sachverstaendigenrichtlinie.html",
        "url": (
            "https://www.revosax.sachsen.de/vorschrift/"
            "21182-Sachverstaendigenrichtlinie"
        ),
        "reason": (
            "historical 2009 linked directive was revoked in 2016 and its PDF "
            "is unavailable; the current 5 December 2024 successor is already "
            "retained as a root text and is not content-identical"
        ),
    },
    "https://www.oecd.org/corruption/anti-bribery/Germany-Phase-4-Report-GER.pdf": {
        "url": (
            "https://www.oecd.org/content/dam/oecd/en/publications/reports/2018/10/"
            "implementing-the-oecd-anti-bribery-convention-phase-4-report-germany_"
            "83014c17/f0f268d1-en.pdf"
        ),
        "reason": "official OECD publication moved to its current content repository",
    },
    (
        "https://www.bmwi.de/Redaktion/DE/Downloads/B/"
        "bund-laender-vereinbarung-invkg.pdf"
    ): {
        "url": (
            "https://www.bundeswirtschaftsministerium.de/Redaktion/DE/Downloads/B/"
            "bund-laender-vereinbarung-invkg.pdf?__blob=publicationFile&v=1"
        ),
        "reason": (
            "official ministry host changed; fetch the publication-file endpoint "
            "rather than its HTML landing page"
        ),
    },
    (
        "https://www.iqb.hu-berlin.de/abitur/abitur/dokumente/mathematik/"
        "M_Grundstock_von.pdf"
    ): {
        "url": "https://www.iqb.hu-berlin.de/media/documents/M_Grundstock_von_Operatoren.pdf",
        "reason": "official IQB companion document moved to the current media repository",
    },
    (
        "https://www.iqb.hu-berlin.de/abitur/abitur/dokumente/deutsch/"
        "D_Grundstock_von.pdf"
    ): {
        "url": (
            "https://www.iqb.hu-berlin.de/media/documents/"
            "D_Grundstock_von_Operatoren_uFqEtHs.pdf"
        ),
        "reason": "official IQB companion document moved to the current media repository",
    },
    (
        "https://www.iqb.hu-berlin.de/abitur/abitur/dokumente/naturwissenschaften/"
        "N_Grundstock_von.pdf"
    ): {
        "url": "https://www.iqb.hu-berlin.de/media/documents/N_Grundstock_von_Operatoren.pdf",
        "reason": "official IQB companion document moved to the current media repository",
    },
    (
        "https://www.iqb.hu-berlin.de/abitur/abitur/dokumente/naturwissenschaften/"
        "N_Grund-stock_von_Operatoren.pdf"
    ): {
        "url": "https://www.iqb.hu-berlin.de/media/documents/N_Grundstock_von_Operatoren.pdf",
        "reason": "official IQB companion document moved to the current media repository",
    },
    (
        "https://www.iqb.huberlin.de/abitur/abitur/dokumente/naturwissenschaften/"
        "N_Grundstock_von_Operatoren.pdf"
    ): {
        "url": "https://www.iqb.hu-berlin.de/media/documents/N_Grundstock_von_Operatoren.pdf",
        "reason": "obsolete IQB host; official document is in the current media repository",
    },
}
SCOPE_NOTES = {
    "by": (
        "All same-portal /Content/Resource links in current legal HTML, "
        "including annexes, forms and labels."
    ),
    "sn": (
        "All same-portal /attachments/<id> assets and "
        "/law_versions/<id>/pdf_attachments bundles. Whole-document "
        "vorschrift_gesamt_pdf links are excluded as duplicate representations. "
        "Direct external PDFs are retained and classified separately as legal "
        "assets or background references."
    ),
    "nw": (
        "All same-portal file links inside div#attachments, plus /system/files/BH "
        "full-text fallbacks when a root page has no inline legaldoc article. The "
        "root 'Dokument herunterladen' PDF is otherwise excluded as a duplicate."
    ),
}
DISCOVERY_FIELDS = (
    "id", "title", "source_url", "linked_source_url",
    "source_replacement_reason", "source_terminal_status",
    "replacement_root_id", "replacement_root_file", "record_role",
    "discovered_from_ids",
)
ROOT_COMPLETE_STATUSES = {
    "by": {"ok"},
    "sn": {"ok", "source_redirect_chain_unresolved"},
    "nw": {"ok", "wrapper_with_fulltext_fallback"},
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(value, fh, ensure_ascii=False, indent=1)
        fh.write("\n")
    tmp.replace(path)


def normalized_url(state, href):
    href = href.strip()
    if not href or href.startswith(("#", "javascript:", "mailto:")):
        return None
    url = urljoin(BASE_URLS[state], href)
    parsed = urlparse(url)
    if parsed.netloc != urlparse(BASE_URLS[state]).netloc:
        return None
    return parsed._replace(fragment="").geturl()


def extract_assets(state, parent, text):
    if state == "by" and "Content/Resource" not in text:
        return []
    if state == "sn" and not any(
        marker in text for marker in ("/attachments/", "/pdf_attachments")
    ) and not re.search(r'https?://[^\s"\']+\.pdf(?:[?#][^\s"\']*)?', text, re.I):
        return []
    if (
        state == "nw"
        and 'id="attachments"' not in text
        and not (
            "legaldoc-article" not in text
            and (
                "/system/files/BH/" in text
                or (
                    "Dokument herunterladen" in text
                    and "/system/files/pdf/" in text
                )
            )
        )
    ):
        return []

    soup = BeautifulSoup(text, "lxml")
    anchors = []
    if state == "by":
        anchors = soup.select('a[href*="Content/Resource"]')
    elif state == "sn":
        anchors = soup.select(
            'a[href^="/attachments/"], '
            'a[href^="/law_versions/"][href*="/pdf_attachments"]'
        )
        anchors += [
            anchor for anchor in soup.select('a[href^="http://"], a[href^="https://"]')
            if urlparse(anchor.get("href", "")).path.lower().endswith(".pdf")
        ]
    elif state == "nw":
        container = soup.select_one("div#attachments")
        anchors = container.select("a[href]") if container is not None else []
        if "legaldoc-article" not in text:
            anchors += soup.select(
                'iframe[src^="/system/files/BH/"], a[href^="/system/files/BH/"], '
                'a[download][href^="/system/files/pdf/"]'
            )

    assets = []
    for anchor in anchors:
        href = (anchor.get("href") or anchor.get("src") or "").strip()
        parsed_href = urlparse(href)
        is_external_sn_pdf = (
            state == "sn"
            and parsed_href.scheme in ("http", "https")
            and parsed_href.netloc != urlparse(BASE_URLS[state]).netloc
            and parsed_href.path.lower().endswith(".pdf")
        )
        url = (
            parsed_href._replace(fragment="").geturl()
            if is_external_sn_pdf else normalized_url(state, href)
        )
        if url is None:
            continue
        path = urlparse(url).path
        if state == "by" and path != "/Content/Resource":
            continue
        if state == "sn" and not is_external_sn_pdf and not (
            re.match(r"^/attachments/[^/?#]+", path)
            or re.match(r"^/law_versions/[^/]+/pdf_attachments/?$", path)
        ):
            continue
        if state == "nw" and not path.startswith("/system/files/"):
            continue
        linked_url = url
        replacement = SOURCE_REPLACEMENTS.get(linked_url)
        source_url = replacement["url"] if replacement else linked_url
        # The stable identifier follows the URL actually present in the law,
        # so a publisher-side relocation does not create a duplicate asset.
        asset_id = hashlib.sha256(linked_url.encode("utf-8")).hexdigest()[:24]
        record_role = "integral_linked_asset"
        if (
            state == "nw"
            and "legaldoc-article" not in text
            and path.lower().startswith(("/system/files/bh/", "/system/files/pdf/"))
        ):
            record_role = "root_fulltext_fallback"
        if is_external_sn_pdf:
            lower_url = url.lower()
            if (
                "bergbehoerde.sachsen.de/set/431/" in lower_url
                or lower_url.endswith("/bund-laender-vereinbarung-invkg.pdf")
            ):
                record_role = "integral_external_legal_asset"
            else:
                record_role = "external_reference_asset"
        record = {
            "id": asset_id,
            "title": anchor.get_text(" ", strip=True) or "Linked attachment",
            "source_url": source_url,
            "record_role": record_role,
            "discovered_from_ids": [parent["id"]],
        }
        if replacement:
            record.update({
                "linked_source_url": linked_url,
                "source_replacement_reason": replacement["reason"],
            })
            for key in (
                "terminal_status", "replacement_root_id", "replacement_root_file",
            ):
                if key in replacement:
                    record[
                        "source_terminal_status" if key == "terminal_status" else key
                    ] = replacement[key]
        assets.append(record)
    return assets


def merge_assets(assets):
    merged = {}
    for item in assets:
        asset_id = item["id"]
        if asset_id not in merged:
            merged[asset_id] = dict(item)
        else:
            merged[asset_id]["discovered_from_ids"] = sorted(set(
                merged[asset_id].get("discovered_from_ids", [])
                + item.get("discovered_from_ids", [])
            ))
            if merged[asset_id]["title"] == "Linked attachment":
                merged[asset_id]["title"] = item["title"]
    return sorted(merged.values(), key=lambda item: item["id"])


def discovery_projection(item):
    projected = {key: item[key] for key in DISCOVERY_FIELDS if key in item}
    if not projected.get("id") or not projected.get("source_url"):
        return None
    projected.setdefault("title", "Linked attachment")
    projected.setdefault("record_role", "integral_linked_asset")
    projected.setdefault("discovered_from_ids", [])
    return projected


def validate_root_html_payload(state, data):
    head = data[:1000].lstrip().lower()
    if not (head.startswith(b"<!doctype") or b"<html" in head):
        return False
    lower = data.lower()
    if any(marker in lower for marker in (
        b"<title>error", b"<title>fehler", b"access denied",
        b"seite nicht gefunden", b"page not found",
        b"aktuell nicht oder nicht mehr im datenbestand",
    )):
        return False
    if state == "by":
        return b'id="content"' in lower and b"document-all" in lower and b"<title" in lower
    if state == "sn":
        return b'class="law_show"' in lower and b"<title" in lower
    if state == "nw":
        base_valid = b"recht.nrw.de" in lower and b"<h1" in lower and b"<title" in lower
        inline = b"legaldoc-article" in lower
        fallback = (
            (b"/system/files/bh/" in lower and b"<iframe" in lower)
            or (
                b"dokument herunterladen" in lower
                and b"/system/files/pdf/" in lower
            )
        )
        return base_valid and (inline or fallback)
    return False


def root_completeness(state, state_dir, index, manifest):
    """Require exact root inventory agreement before treating links as deletions."""
    reasons = []
    index_items = list(index.get("items") or [])
    if state == "sn":
        index_items += list(index.get("supplemental_items") or [])
    index_ids = [item.get("id") for item in index_items if item.get("id")]
    manifest_ids = [item.get("id") for item in manifest if item.get("id")]

    if not index_items:
        reasons.append("root index has no items")
    if len(index_ids) != len(index_items) or len(set(index_ids)) != len(index_ids):
        reasons.append("root index IDs are missing or duplicated")
    if len(manifest_ids) != len(manifest) or len(set(manifest_ids)) != len(manifest_ids):
        reasons.append("root manifest IDs are missing or duplicated")

    if state == "sn":
        expected = index.get("count_including_supplementals")
        if index.get("enumeration_complete") is not True:
            reasons.append("root enumeration_complete is not true")
        if index.get("supplemental_discovery_complete") is not True:
            reasons.append("supplemental_discovery_complete is not true")
    else:
        expected = index.get("expected_total")
        if state == "by" and index.get("enumeration_complete") is not True:
            reasons.append("root enumeration_complete is not true")
        # NW's source index predates the explicit flag; exact expected-total,
        # set and payload checks below are the equivalent completeness proof.
        if state == "nw" and index.get("enumeration_complete") not in (None, True):
            reasons.append("root enumeration_complete is not true")
    if not isinstance(expected, int) or expected <= 0:
        reasons.append("root expected total is unavailable")
    else:
        if len(index_items) != expected:
            reasons.append(
                "root index count %d differs from expected %d" %
                (len(index_items), expected)
            )
        if len(manifest) != expected:
            reasons.append(
                "root manifest count %d differs from expected %d" %
                (len(manifest), expected)
            )
    if set(index_ids) != set(manifest_ids):
        reasons.append("root index/manifest ID sets differ")

    allowed_statuses = ROOT_COMPLETE_STATUSES[state]
    for item in manifest:
        item_id = item.get("id", "<unknown>")
        status = item.get("status")
        if status not in allowed_statuses:
            reasons.append("root %s has incomplete status %r" % (item_id, status))
            break
        if status == "source_redirect_chain_unresolved":
            if state != "sn" or item.get("file"):
                reasons.append("root %s has invalid unavailable-source record" % item_id)
                break
            continue
        rel = item.get("file")
        path = state_dir / rel if rel else None
        valid_payload = False
        if rel and item.get("format") == "html" and path.exists() and path.is_file():
            try:
                valid_payload = validate_root_html_payload(state, path.read_bytes())
            except OSError:
                valid_payload = False
        if not valid_payload:
            reasons.append("root %s lacks a validated local HTML payload" % item_id)
            break

    return not reasons, "; ".join(reasons) if reasons else "exact root inventory validated"


def cached_assets(state_dir):
    """Return last-known live discoveries; never resurrect the stale archive."""
    cached = []
    prior_index = load_json(state_dir / "assets_index.json", {})
    for item in prior_index.get("items", []):
        projected = discovery_projection(item)
        if projected:
            cached.append(projected)
    for item in load_json(state_dir / "assets_manifest.json", []):
        projected = discovery_projection(item)
        if projected:
            cached.append(projected)
    return merge_assets(cached)


def discover(state):
    state_dir = DATA_ROOT / state
    index = load_json(state_dir / "index.json", {})
    manifest = load_json(state_dir / "manifest.json", [])
    found = []
    inspected = 0
    for parent in manifest:
        if (
            parent.get("status") not in ("ok", "wrapper_with_fulltext_fallback")
            or parent.get("format") != "html"
        ):
            continue
        rel = parent.get("file")
        if not rel:
            continue
        path = state_dir / rel
        if not path.exists() or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        inspected += 1
        found.extend(extract_assets(state, parent, text))
    current_assets = merge_assets(found)
    complete, reason = root_completeness(
        state, state_dir, index, manifest
    )
    retained_cached = 0
    if not complete:
        cached = cached_assets(state_dir)
        current_ids = {item["id"] for item in current_assets}
        retained_cached = sum(item["id"] not in current_ids for item in cached)
        current_assets = merge_assets(current_assets + cached)
    return (
        current_assets, inspected, manifest, complete, reason,
        retained_cached, len(found),
    )


def retry_after_seconds(value):
    value = (value or "").strip()
    if value.isdigit():
        return float(value)
    if value:
        try:
            when = email.utils.parsedate_to_datetime(value)
            return max(0.0, when.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            pass
    return None


def fetch(session, url):
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=(30, 90), stream=True)
            if response.status_code == 200:
                if response.headers.get("Content-Length", "").strip() == "0":
                    # Let the caller distinguish a known empty attachment
                    # bundle from a malformed empty response elsewhere.
                    return response, "empty response"
                return response, None
            if response.status_code in (404, 410):
                response.close()
                return None, "HTTP %s (permanent)" % response.status_code
            last_error = "HTTP %s" % response.status_code
            wait = BACKOFF_BASE * (2 ** attempt)
            if response.status_code in (429, 503):
                retry_after = retry_after_seconds(response.headers.get("Retry-After"))
                if retry_after is not None:
                    wait = max(wait, retry_after)
            response.close()
        except requests.RequestException as exc:
            last_error = str(exc)
            wait = BACKOFF_BASE * (2 ** attempt)
        if attempt + 1 < MAX_RETRIES:
            time.sleep(wait)
    return None, last_error or "request failed"


def clean_filename(value):
    value = unquote(value).split("?")[0].strip().replace("\\", "_")
    value = os.path.basename(value)
    value = re.sub(r"[^A-Za-z0-9ÄÖÜäöüß._-]+", "_", value).strip("._")
    return value[:120] or "attachment"


def url_filename(url):
    parsed = urlparse(url)
    if parsed.path == "/Content/Resource":
        resource_path = parse_qs(parsed.query).get("path", [""])[0]
        if resource_path:
            return clean_filename(resource_path)
    return clean_filename(parsed.path)


def detected_content_type(data):
    if data.startswith(b"%PDF"):
        return "application/pdf"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if data.startswith(b"PK\x03\x04"):
        return "application/zip"
    head = data[:1000].lstrip().lower()
    if head.startswith(b"<!doctype") or b"<html" in head:
        return "text/html"
    return "application/octet-stream"


def response_filename(item, response, detected=None):
    candidate = url_filename(item["source_url"])
    disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r"filename\*?=(?:UTF-8''|\")?([^\";]+)", disposition, re.I)
    if match:
        candidate = clean_filename(match.group(1))
    suffix = Path(candidate).suffix
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
    if not suffix:
        extension = mimetypes.guess_extension(content_type) or {
            "application/pdf": ".pdf",
            "text/html": ".html",
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/tiff": ".tiff",
        }.get(content_type, ".bin")
        candidate += extension
    detected = detected or "application/octet-stream"
    detected_extension = {
        "application/pdf": ".pdf",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/tiff": ".tiff",
        "application/zip": ".zip",
        "text/html": ".html",
    }.get(detected)
    if detected_extension and Path(candidate).suffix.lower() != detected_extension:
        candidate = str(Path(candidate).with_suffix(detected_extension))
    return "%s_%s" % (item["id"], candidate)


def validate_payload(data, filename, content_type=""):
    detected = detected_content_type(data)
    if detected == "application/zip":
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                return bool(archive.namelist()) and archive.testzip() is None
        except (OSError, zipfile.BadZipFile):
            return False
    if detected in (
        "application/pdf", "image/jpeg", "image/png", "image/tiff",
    ):
        return True
    suffix = Path(filename).suffix.lower()
    content_type = (content_type or "").lower()
    if suffix == ".pdf" or "application/pdf" in content_type:
        return False
    head = data[:1000].lstrip().lower()
    if suffix in (".html", ".htm") or "text/html" in content_type:
        if not (head.startswith(b"<!doctype") or b"<html" in head):
            return False
        return not any(marker in head for marker in (
            b"<title>error", b"<title>fehler", b"access denied",
            b"page not found", b"seite nicht gefunden",
        ))
    return bool(data)


def inspect_file(path, filename=None, content_type=""):
    """Validate and hash a local payload without loading it into memory."""
    path = Path(path)
    try:
        size = path.stat().st_size
        if size <= 0:
            return None
        digest = hashlib.sha256()
        head = b""
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(STREAM_CHUNK_BYTES)
                if not chunk:
                    break
                if len(head) < 4096:
                    head += chunk[:4096 - len(head)]
                digest.update(chunk)
        detected = detected_content_type(head)
        suffix_name = filename or path.name
        if detected == "application/zip":
            try:
                with zipfile.ZipFile(str(path)) as archive:
                    valid = bool(archive.namelist()) and archive.testzip() is None
            except (OSError, zipfile.BadZipFile):
                valid = False
        elif detected == "application/pdf":
            with path.open("rb") as fh:
                fh.seek(max(0, size - 65536))
                valid = b"%%EOF" in fh.read()
        elif detected == "image/jpeg":
            with path.open("rb") as fh:
                fh.seek(max(0, size - 16))
                valid = fh.read().rstrip().endswith(b"\xff\xd9")
        elif detected == "image/png":
            with path.open("rb") as fh:
                fh.seek(max(0, size - 32))
                valid = b"IEND" in fh.read()
        elif detected == "image/tiff":
            valid = True
        elif detected == "text/html":
            valid = validate_payload(head, suffix_name, "text/html")
        else:
            valid = validate_payload(head, suffix_name, content_type)
        if not valid:
            return None
        return {
            "bytes": size,
            "sha256": digest.hexdigest(),
            "content_type": detected,
        }
    except OSError:
        return None


def adopt_existing_asset(
    item, assets_dir, prior_declared_content_type="", expected_sha256=None
):
    """Adopt one valid post-flush file by stable ID; reject ambiguity."""
    candidates = sorted(
        path for path in assets_dir.glob(item["id"] + "_*")
        if path.is_file() and not path.name.endswith(".tmp")
    ) if assets_dir.exists() else []
    valid = []
    for path in candidates:
        metadata = inspect_file(path, path.name, prior_declared_content_type)
        if metadata and (
            not expected_sha256 or metadata["sha256"] == expected_sha256
        ):
            valid.append((path, metadata))
    if len(valid) != 1:
        return None
    path, metadata = valid[0]
    return {
        "file": str(Path("assets") / path.name),
        "format": path.suffix.lstrip(".").lower() or "bin",
        "content_type": metadata["content_type"],
        "declared_content_type": (
            prior_declared_content_type or metadata["content_type"]
        ),
        "bytes": metadata["bytes"],
        "sha256": metadata["sha256"],
        "downloaded_at": datetime.fromtimestamp(
            path.stat().st_mtime, timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "ok",
        "adopted_existing_unmanifested": True,
    }


def stream_response_to_asset(item, response, assets_dir):
    """Stream one response to an atomic same-filesystem temp and validate it."""
    assets_dir.mkdir(parents=True, exist_ok=True)
    declared = response.headers.get("Content-Type", "").split(";", 1)[0]
    length_header = response.headers.get("Content-Length", "").strip()
    expected_bytes = int(length_header) if length_header.isdigit() else None
    if expected_bytes is not None:
        free = shutil.disk_usage(str(assets_dir)).free
        if free < expected_bytes + DISK_SAFETY_RESERVE_BYTES:
            response.close()
            return None, (
                "insufficient disk space for declared %d-byte payload plus reserve"
                % expected_bytes
            )

    tmp = assets_dir / (".%s.download.tmp" % item["id"])
    total = 0
    head = b""
    try:
        with tmp.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                if not chunk:
                    continue
                if len(head) < 4096:
                    head += chunk[:4096 - len(head)]
                fh.write(chunk)
                total += len(chunk)
            fh.flush()
            os.fsync(fh.fileno())
        if total <= 0:
            return None, "empty response"
        if (
            expected_bytes is not None
            and not response.headers.get("Content-Encoding")
            and total != expected_bytes
        ):
            return None, (
                "truncated payload: received %d of declared %d bytes"
                % (total, expected_bytes)
            )
        detected = detected_content_type(head)
        filename = response_filename(item, response, detected)
        metadata = inspect_file(tmp, filename, detected)
        if not metadata:
            return None, "response failed type/payload validation"
        destination = assets_dir / filename
        tmp.replace(destination)
        return {
            "file": str(Path("assets") / filename),
            "format": destination.suffix.lstrip(".").lower() or "bin",
            "content_type": metadata["content_type"],
            "declared_content_type": declared,
            "bytes": metadata["bytes"],
            "sha256": metadata["sha256"],
            "downloaded_at": now_iso(),
            "status": "ok",
        }, None
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    finally:
        response.close()
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def crawl_state(state, args):
    state_dir = DATA_ROOT / state
    assets_dir = state_dir / "assets"
    prior_asset_index = load_json(state_dir / "assets_index.json", {})
    (
        assets, inspected, root_manifest, discovery_complete,
        completeness_reason, retained_cached, raw_current_links,
    ) = discover(state)
    prior_expected = prior_asset_index.get("expected_total")
    if not isinstance(prior_expected, int):
        prior_expected = (
            prior_asset_index.get("count")
            if prior_asset_index.get("discovery_complete") is True else None
        )
    expected_total = (
        len(assets) if discovery_complete
        else max(len(assets), prior_expected or 0)
    )
    index = {
        "state": state,
        "generated_at": now_iso(),
        "root_manifest_entries": len(root_manifest),
        "root_html_files_inspected": inspected,
        "count": len(assets),
        "discovery_complete": discovery_complete,
        "root_discovery_complete": discovery_complete,
        "expected_total": expected_total,
        "root_completeness_reason": completeness_reason,
        "cached_assets_retained_due_to_incomplete_root": retained_cached,
        "raw_current_links_seen": raw_current_links,
        "inventory_scope": SCOPE_NOTES[state],
        "items": assets,
    }
    if not args.dry_run:
        write_json(state_dir / "assets_index.json", index)
    print(
        "%s: discovered %d unique assets from %d HTML files "
        "(discovery_complete=%s, cached_retained=%d)" %
        (state, len(assets), inspected, discovery_complete, retained_cached)
    )
    selected = assets[:max(args.limit, 0)] if args.limit is not None else assets
    if args.dry_run:
        for item in selected[:10]:
            print("DRY %s %s %s" % (state, item["id"], item["source_url"]))
        return 0

    old_manifest = load_json(state_dir / "assets_manifest.json", [])
    old_by_id = {item["id"]: item for item in old_manifest}
    current_ids = {item["id"] for item in assets}
    stale = (
        [item for item in old_manifest if item.get("id") not in current_ids]
        if discovery_complete else []
    )
    if stale:
        archived = load_json(state_dir / "assets_manifest_stale.json", [])
        archived_by_id = {item.get("id"): item for item in archived}
        for item in stale:
            item["status"] = "stale_not_currently_linked"
            archived_by_id[item.get("id")] = item
        write_json(state_dir / "assets_manifest_stale.json", sorted(
            archived_by_id.values(), key=lambda item: item.get("id", "")
        ))
    manifest = (
        [item for item in old_manifest if item.get("id") in current_ids]
        if discovery_complete else list(old_manifest)
    )
    by_id = {item["id"]: item for item in manifest}

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    downloaded = skipped = unavailable = failed = 0
    for position, item in enumerate(selected, 1):
        previous = by_id.get(item["id"])
        terminal_status = item.get("source_terminal_status")
        if terminal_status:
            entry = dict(item)
            entry.update({
                "file": None,
                "format": None,
                "downloaded_at": None,
                "observed_at": now_iso(),
                "status": terminal_status,
            })
            if previous and previous.get("status") == terminal_status:
                skipped += 1
            else:
                unavailable += 1
            if previous:
                previous.update(entry)
            else:
                manifest.append(entry)
                by_id[item["id"]] = entry
            continue
        if (
            previous
            and previous.get("status") == "source_attachment_bundle_empty"
            and state == "sn"
            and re.match(
                r"^/law_versions/[^/]+/pdf_attachments/?$",
                urlparse(item["source_url"]).path,
            )
        ):
            previous.update(item)
            skipped += 1
            continue
        previous_payload_valid = False
        if previous and previous.get("status") == "ok" and previous.get("file"):
            path = state_dir / previous["file"]
            if path.exists() and path.stat().st_size > 0:
                metadata = inspect_file(
                    path, path.name, previous.get("content_type", "")
                )
                previous_payload_valid = bool(metadata) and (
                    not previous.get("sha256")
                    or metadata["sha256"] == previous["sha256"]
                )
                if (
                    previous_payload_valid
                    and previous.get("source_url") == item["source_url"]
                ):
                    prior_content_type = previous.get("content_type")
                    previous.update(item)
                    previous.update({
                        "content_type": metadata["content_type"],
                        "declared_content_type": previous.get(
                            "declared_content_type",
                            prior_content_type or metadata["content_type"],
                        ),
                        "bytes": metadata["bytes"],
                        "sha256": metadata["sha256"],
                    })
                    skipped += 1
                    continue

        adopted = None
        if not previous or previous.get("source_url") == item["source_url"]:
            adopted = adopt_existing_asset(
                item,
                assets_dir,
                previous.get("declared_content_type", "") if previous else "",
                previous.get("sha256") if previous else None,
            )
        if adopted:
            entry = dict(item)
            entry.update(adopted)
            if previous:
                previous.update(entry)
            else:
                manifest.append(entry)
                by_id[item["id"]] = entry
            skipped += 1
            continue

        response, error = fetch(session, item["source_url"])
        entry = dict(item)
        handled_unavailable = False
        if (
            response is not None
            and error == "empty response"
            and state == "sn"
            and re.match(
                r"^/law_versions/[^/]+/pdf_attachments/?$",
                urlparse(item["source_url"]).path,
            )
        ):
            entry.update({
                "file": None,
                "format": "pdf",
                "declared_content_type": response.headers.get(
                    "Content-Type", ""
                ).split(";", 1)[0],
                "downloaded_at": None,
                "observed_at": now_iso(),
                "status": "source_attachment_bundle_empty",
            })
            unavailable += 1
            handled_unavailable = True
            response.close()
        if response is not None and not handled_unavailable:
            try:
                metadata, stream_error = stream_response_to_asset(
                    item, response, assets_dir
                )
            except Exception as exc:  # retries occur on the next resumable run
                metadata, stream_error = None, str(exc)
            if metadata:
                entry.update(metadata)
                if (
                    previous_payload_valid and previous
                    and previous.get("file") != metadata.get("file")
                ):
                    entry["source_landing_file"] = previous.get("file")
                downloaded += 1
            else:
                error = stream_error or "response failed type/payload validation"
        if not handled_unavailable and (response is None or error):
            entry.update({
                "file": (
                    previous.get("file")
                    if previous and previous_payload_valid else None
                ),
                "downloaded_at": previous.get("downloaded_at") if previous else None,
                "status": "error: %s" % (error or "invalid response"),
            })
            failed += 1
        if previous:
            previous.update(entry)
        else:
            manifest.append(entry)
            by_id[item["id"]] = entry
        if position % 25 == 0:
            write_json(state_dir / "assets_manifest.json", sorted(
                manifest, key=lambda value: value["id"]
            ))
            print(
                "%s: [%d/%d] downloaded=%d skipped=%d unavailable=%d failed=%d" %
                (
                    state, position, len(selected), downloaded, skipped,
                    unavailable, failed,
                )
            )
        lo, hi = DELAYS[state]
        time.sleep(random.uniform(lo, hi))

    write_json(state_dir / "assets_manifest.json", sorted(
        manifest, key=lambda value: value["id"]
    ))
    print(
        "%s done: %d assets, %d downloaded, %d skipped, %d unavailable, %d failed" %
        (state, len(selected), downloaded, skipped, unavailable, failed)
    )
    return failed


def main():
    parser = argparse.ArgumentParser(description="Download linked state-law assets")
    parser.add_argument("--state", choices=("by", "sn", "nw", "all"), default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    # Accepted for launcher symmetry; discovery is deliberately refreshed each run.
    parser.add_argument("--refresh-index", action="store_true")
    args = parser.parse_args()
    states = ("by", "sn", "nw") if args.state == "all" else (args.state,)
    failed = 0
    for state in states:
        try:
            failed += crawl_state(state, args)
        except Exception as exc:  # noqa: BLE001 - make per-state failure visible
            print("%s fatal: %s" % (state, exc), file=sys.stderr)
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
