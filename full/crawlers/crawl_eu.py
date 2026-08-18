#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Crawler for EU legal texts from EUR-Lex / CELLAR.

Covers two categories:

1. verfassungen  -- the EU "constitutional" layer: founding, amending and
   accession treaties plus the consolidated treaties in force (TEU, TFEU,
   Euratom, Charter of Fundamental Rights). Uses a curated list of CELEX
   numbers (more reliable than scraping the EUR-Lex overview pages).

2. stammgesetze  -- EU basic acts (regulations REG, directives DIR,
   decisions DEC). Enumerated via the CELLAR SPARQL endpoint; the index is
   cached in celex_index.json. By default only acts in force are
   downloaded (--scope all downloads everything ever enumerated).

Treaties use EUR-Lex with its robots.txt 10-second crawl delay.  Basic acts
use German (then English) HTML/XHTML content negotiation on the official CELLAR
CELEX resource; EUR-Lex is only a compliant fallback when CELLAR has no XHTML
manifestation.  HTTP 200 error/landing pages are rejected before saving.
An OS-level lock permits only one crawler CLI at a time, so a second process
cannot bypass the per-host crawl delay.

Usage:
    python3 crawl_eu.py [--category {verfassungen,stammgesetze,all}]
                        [--dry-run] [--limit N] [--refresh-index]
                        [--refresh-merger-languages]
                        [--scope {in-force,all}] [--workers N]
"""

import argparse
import concurrent.futures
import datetime
import email.utils
import fcntl
import hashlib
import html as html_module
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
from urllib.parse import parse_qsl, quote, unquote, urlsplit

import requests

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

from pathlib import Path

from project_paths import CRAWLER_LOG_ROOT, DATA_ROOT

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = DATA_ROOT / "eu"
VERF_DIR = DATA_DIR / "verfassungen"
STAMM_DIR = DATA_DIR / "stammgesetze"
SOURCE_CONTAINER_DIR = STAMM_DIR / "_source_containers"
DERIVED_PRINT_RESOLUTION = "cellar_print_parent_pdf_sibling_page_extract"
INDEX_FILE = STAMM_DIR / "celex_index.json"
INDEX_PARTIAL_FILE = STAMM_DIR / "celex_index.partial.jsonl"
MERGER_LANGUAGE_PARTIAL_FILE = (
    STAMM_DIR / "merger_language_index.partial.jsonl")
ERROR_LOG = CRAWLER_LOG_ROOT / "eu_errors.log"
TECHNICAL_RETRY_FILE = STAMM_DIR / "technical_retry_journal.json"
LOCK_FILE = DATA_DIR / ".crawl_eu.lock"

USER_AGENT = (
    "EU-Legal-Repository-Crawler/1.0 "
    "(research; contact: repository-maintainer)")
EURLEX_URL = "https://eur-lex.europa.eu/legal-content/{lang}/TXT/{fmt}/?uri=CELEX:{celex}"
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX_URL = "https://publications.europa.eu/resource/celex/{celex}"
EURLEX_IDENTITY_URL = (
    "https://eur-lex.europa.eu/legal-content/{lang}/TXT/HTML/?uri={identity}")

# Upstream CELEX resolution for 32007D0512 currently returns the neighboring
# 32007D0511 act.  The official OJ identity below is the authoritative record
# for 2007/512/EG (OJ L 188, pp. 17--23).
EURLEX_OJ_OVERRIDES = {
    "32007D0512": {
        "identity": "OJ:JOL_2007_188_R_0017_01",
        "expected_headings": {
            "DE": "2007/512/EG",
            "EN": "2007/512/EC",
        },
    },
    "32017D0529(01)": {
        "identity": "OJ:JOC_2017_168_R_0002",
        "expected_headings": {
            "DE": "2017/C 168/02",
            "EN": "2017/C 168/02",
        },
    },
    "32017D0529(02)": {
        "identity": "OJ:JOC_2017_168_R_0003",
        "expected_headings": {
            "DE": "2017/C 168/03",
            "EN": "2017/C 168/03",
        },
    },
}

# Audited upstream metadata defects where the operative body/title identity is
# conclusive.  Preserve the original index value alongside each correction.
MANIFEST_METADATA_CORRECTIONS = {
    "32004R1410": {
        "field": "title",
        "original_value": (
            "Verordnung (EG) Nr. 1409/2004 der Kommission vom 2. August "
            "2004 zur Änderung der Verordnung (EG) Nr. 1159/2003 mit "
            "Durchführungsbestimmungen zur Einfuhr von Rohrzucker im Rahmen "
            "bestimmter Zollkontingente und Präferenzabkommen für die "
            "Wirtschaftsjahre 2003/04, 2004/05 und 2005/06 sowie zur "
            "Änderung der Verordnungen (EG) Nr. 1464/95 und (EG) Nr. 779/96"),
        "value": (
            "Verordnung (EG) Nr. 1410/2004 der Kommission vom 2. August 2004 "
            "zur Änderung der Verordnung (EG) Nr. 1185/2004 zur Eröffnung "
            "einer Dauerausschreibung zur Ausfuhr von Roggen aus Beständen "
            "der deutschen Interventionsstelle"),
        "reason": (
            "official index title identifies 1409/2004, while the operative "
            "body heading is 1410/2004"),
    },
    "31991D0314": {
        "field": "date", "original_value": "1990-06-26",
        "value": "1991-06-26",
        "reason": "official index year 1990 conflicts with body/title year 1991",
    },
    "32006D0635": {
        "field": "date", "original_value": "2005-04-04",
        "value": "2006-04-04",
        "reason": "official index year 2005 conflicts with body/title year 2006",
    },
    "32009R0112": {
        "field": "date", "original_value": "2006-02-06",
        "value": "2009-02-06",
        "reason": "official index year 2006 conflicts with body/title year 2009",
    },
    "32013R0034": {
        "field": "date", "original_value": "2012-01-16",
        "value": "2013-01-16",
        "reason": "official index year 2012 conflicts with body/title year 2013",
    },
}

# These are official CELEX/index aliases whose numeric CELEX suffix differs
# from the act number in the official index title and operative body.  The
# payload is correct for the indexed work; retain the official CELEX key and
# record the discrepancy as metadata, never as a route/identity failure.
IDENTIFIER_METADATA_WARNINGS = {
    "32002D0681": "2002/654/EGKS",
    "32006R2038": "1891/2006",
    "32010L0044": "2010/42/EU",
    "32010L0053": "2010/45/EU",
    "32010R0053": "23/2010",
    "32012R0972": "927/2012",
}

# EUR-Lex robots.txt specifies Crawl-delay: 10.  Publications Office/CELLAR
# does not specify a delay; use a conservative aggregate 0.4 s spacing there.
EURLEX_THROTTLE_SECONDS = 20.0
CELLAR_THROTTLE_SECONDS = 0.4
SPARQL_PAGE_SIZE = 2000         # keyset pagination page size
MERGER_LANGUAGE_PAGE_SIZE = 2000
MAX_RETRIES = 4
# Print-only SPARQL resolution is rare and can be pathologically slow for
# records with no manifestation.  Keep its total wait bounded: at most two
# 90-second requests and one Retry-After/backoff sleep capped at 90 seconds.
PRINT_SPARQL_MAX_RETRIES = 2
PRINT_SPARQL_TIMEOUT_SECONDS = 90
PRINT_RETRY_AFTER_CAP_SECONDS = 90
MIN_VALID_SIZE = 500            # bytes; smaller downloads are treated as failed
MANIFEST_FLUSH_EVERY = 100      # rewrite manifest.json every N processed docs
PROGRESS_EVERY = 50
DEFAULT_WORKERS = 4

RESOURCE_TYPES = ("REG", "DIR", "DEC")

# CELLAR uses three-letter authority codes.  Keep DE/EN first, then choose
# deterministically by two-letter code when an act exists only in another EU
# language (for example, a language-specific corrigendum).
EU_LANGUAGES = {
    "BUL": ("BG", ("Официален вестник на Европейския съюз",)),
    "CES": ("CS", ("Úřední věstník Evropské unie",)),
    "DAN": ("DA", ("Den Europæiske Unions Tidende",
                    "De Europæiske Fællesskabers Tidende")),
    "DEU": ("DE", ("Amtsblatt der Europäischen Union",
                    "Amtsblatt der Europäischen Gemeinschaften")),
    "ELL": ("EL", ("Επίσημη Εφημερίδα της Ευρωπαϊκής Ένωσης",
                    "Επίσημη Εφημερίδα των Ευρωπαϊκών Κοινοτήτων")),
    "ENG": ("EN", ("Official Journal of the European Union",
                    "Official Journal of the European Communities")),
    "EST": ("ET", ("Euroopa Liidu Teataja",)),
    "FIN": ("FI", ("Euroopan unionin virallinen lehti",
                    "Euroopan yhteisöjen virallinen lehti")),
    "FRA": ("FR", ("Journal officiel de l’Union européenne",
                    "Journal officiel des Communautés européennes")),
    "GLE": ("GA", ("Iris Oifigiúil an Aontais Eorpaigh",)),
    "HRV": ("HR", ("Službeni list Europske unije",)),
    "HUN": ("HU", ("Az Európai Unió Hivatalos Lapja",)),
    "ITA": ("IT", ("Gazzetta ufficiale dell’Unione europea",
                    "Gazzetta ufficiale delle Comunità europee")),
    "LAV": ("LV", ("Eiropas Savienības Oficiālais Vēstnesis",)),
    "LIT": ("LT", ("Europos Sąjungos oficialusis leidinys",)),
    "MLT": ("MT", ("Il-Ġurnal Uffiċjali tal-Unjoni Ewropea",)),
    "NLD": ("NL", ("Publicatieblad van de Europese Unie",
                    "Publicatieblad van de Europese Gemeenschappen",
                    "Publikatieblad van de Europese Gemeenschappen")),
    "POL": ("PL", ("Dziennik Urzędowy Unii Europejskiej",)),
    "POR": ("PT", ("Jornal Oficial da União Europeia",
                    "Jornal Oficial das Comunidades Europeias")),
    "RON": ("RO", ("Jurnalul Oficial al Uniunii Europene",)),
    "SLK": ("SK", ("Úradný vestník Európskej únie",)),
    "SLV": ("SL", ("Uradni list Evropske unije",)),
    "SPA": ("ES", ("Diario Oficial de la Unión Europea",
                    "Diario Oficial de las Comunidades Europeas")),
    "SWE": ("SV", ("Europeiska unionens officiella tidning",
                    "Europeiska gemenskapernas officiella tidning")),
}
LANGUAGE_URI_PREFIX = (
    "http://publications.europa.eu/resource/authority/language/")

# Markers that only appear in the EUR-Lex site chrome (error page, landing
# page, search UI) -- never in the statically served full-text document HTML.
ERROR_PAGE_MARKERS = (
    "The requested document does not exist",
    "piwikPro",
    "ecl-site-header",
    "class=\"ecl-",
)

# Curated treaty list. "candidates" are CELEX numbers tried in order; for
# each candidate all language/format combinations are attempted before
# moving to the next candidate. Historic treaties are only available as
# scanned German PDFs under <celex>/TXT; the bare CELEX number often serves
# only a table of contents, hence /TXT comes first.
TREATIES = [
    # -- consolidated versions in force (2016) --
    ("12016M_TXT", "Vertrag über die Europäische Union (konsolidierte Fassung 2016, EUV)", ["12016M/TXT"]),
    ("12016E_TXT", "Vertrag über die Arbeitsweise der Europäischen Union (konsolidierte Fassung 2016, AEUV)", ["12016E/TXT"]),
    ("12016A_TXT", "Vertrag zur Gründung der Europäischen Atomgemeinschaft (konsolidierte Fassung 2016, Euratom)", ["12016A/TXT"]),
    ("12016P_TXT", "Charta der Grundrechte der Europäischen Union (2016)", ["12016P/TXT"]),
    # -- founding and amending treaties --
    ("11951K_TXT", "Vertrag über die Gründung der Europäischen Gemeinschaft für Kohle und Stahl (EGKS-Vertrag, Paris 1951)", ["11951K/TXT", "11951K"]),
    ("11956K_TXT", "Änderungen des EGKS-Vertrags (1956, Saarvertrag)", ["11956K/TXT", "11956K"]),
    ("11957E_TXT", "Vertrag zur Gründung der Europäischen Wirtschaftsgemeinschaft (EWG-Vertrag, Rom 1957)", ["11957E/TXT", "11957E"]),
    ("11957A_TXT", "Vertrag zur Gründung der Europäischen Atomgemeinschaft (Euratom-Vertrag, Rom 1957)", ["11957A/TXT", "11957A"]),
    ("11957K_TXT", "Abkommen über gemeinsame Organe für die Europäischen Gemeinschaften (Rom 1957)", ["11957K/TXT", "11957K"]),
    ("11962E_TXT", "Protokoll betreffend die Niederländischen Antillen (1962)", ["11962E/TXT", "11962E"]),
    ("11965F_TXT", "Vertrag zur Einsetzung eines gemeinsamen Rates und einer gemeinsamen Kommission (Fusionsvertrag, 1965)", ["11965F/TXT", "11965F"]),
    ("11970F_TXT", "Vertrag zur Änderung bestimmter Haushaltsvorschriften (1970)", ["11970F/TXT", "11970F"]),
    ("11975R_TXT", "Vertrag zur Änderung bestimmter Finanzvorschriften (1975)", ["11975R/TXT", "11975R"]),
    ("11975X_TXT", "Vertrag zur Änderung des Protokolls über die Satzung der Europäischen Investitionsbank (1975)", ["11975X/TXT", "11975X"]),
    ("11985G_TXT", "Grönland-Vertrag (1984)", ["11985G/TXT", "11985G"]),
    ("11986U_TXT", "Einheitliche Europäische Akte (1986)", ["11986U/TXT", "11986U"]),
    ("11992M_TXT", "Vertrag über die Europäische Union (Maastricht 1992)", ["11992M/TXT", "11992M"]),
    ("11997D_TXT", "Vertrag von Amsterdam (1997)", ["11997D/TXT", "11997D"]),
    ("12001C_TXT", "Vertrag von Nizza (2001)", ["12001C/TXT", "12001C"]),
    ("12007L_TXT", "Vertrag von Lissabon (2007)", ["12007L/TXT", "12007L"]),
    # -- accession treaties --
    ("11972B_TXT", "Beitrittsvertrag 1972 (Dänemark, Irland, Vereinigtes Königreich, Norwegen)", ["11972B/TXT", "11972B"]),
    ("11979H_TXT", "Beitrittsvertrag 1979 (Griechenland)", ["11979H/TXT", "11979H"]),
    ("11985I_TXT", "Beitrittsvertrag 1985 (Spanien, Portugal)", ["11985I/TXT", "11985I"]),
    ("11994N_TXT", "Beitrittsvertrag 1994 (Österreich, Finnland, Schweden, Norwegen)", ["11994N/TXT", "11994N"]),
    ("12003T_TXT", "Beitrittsvertrag 2003 (zehn Staaten, Osterweiterung)", ["12003T/TXT", "12003T"]),
    ("12005S_TXT", "Beitrittsvertrag 2005 (Bulgarien, Rumänien)", ["12005S/TXT", "12005S"]),
    ("12012J_TXT", "Beitrittsvertrag 2012 (Kroatien)", ["12012J/TXT", "12012J"]),
]

# EUR-Lex serves the 1965 Merger Treaty package page as a 3.8 kB table of
# contents.  Its operative treaty, final act and privileges protocol are
# separate official CELEX records, so retain those components alongside the
# package page rather than mistaking the contents page for the full text.
TREATY_COMPONENTS = {
    "11965F_TXT": (
        ["11965F%03d" % n for n in range(40)] +
        ["11965F/AFI/00", "11965F/AFI/N01", "11965F/AFI/N02"] +
        ["11965F/PRO/PRI/%02d" % n for n in range(23)]
    ),
}

log = logging.getLogger("crawl_eu")
request_throttle_locks = {
    "eurlex": threading.Lock(),
    "cellar": threading.Lock(),
}
next_request_at = {"eurlex": 0.0, "cellar": 0.0}
process_lock_handle = None
trusted_merger_language_cache = {}
technical_retry_lock = threading.Lock()
technical_retry_target_ids = set()
defer_on_waf_enabled = False


class FetchResult(tuple):
    """Four-field fetch tuple with optional manifest provenance metadata."""

    def __new__(cls, content, extension, language, url, metadata=None):
        obj = tuple.__new__(cls, (content, extension, language, url))
        obj.metadata = dict(metadata or {})
        return obj


class MissingResult:
    """Clean exhaustive miss with machine-readable attempted-language metadata."""

    def __init__(self, metadata=None):
        self.metadata = dict(metadata or {})


class TechnicalWAFDefer(Exception):
    """Typed, journal-backed deferral for one exact AWS WAF challenge."""

    def __init__(self, celex, url, reason):
        self.celex = celex
        self.url = url
        self.reason = reason
        super().__init__(reason)


class ResolutionAudit:
    """Accumulate retryable transport/server failures across fallback routes."""

    def __init__(self):
        self.technical_errors = []

    def record(self, route, url):
        self.technical_errors.append("%s: %s" % (route, url))

    def raise_if_incomplete(self, celex):
        if self.technical_errors:
            raise RuntimeError(
                "technical failures prevent exhaustive resolution of %s: %s" %
                (celex, "; ".join(self.technical_errors)))


CLEAN_UNAVAILABLE_STATUS = {404, 406, 410}


def record_unexpected_response(audit, route, response, reason):
    if audit is not None:
        audit.record("%s (%s; HTTP %s)" %
                     (route, reason, response.status_code), response.url)


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def make_session():
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def throttle_request(source, delay):
    """Globally space HTTP request starts, including across worker threads."""
    with request_throttle_locks[source]:
        now = time.monotonic()
        if next_request_at[source] > now:
            time.sleep(next_request_at[source] - now)
        next_request_at[source] = time.monotonic() + delay


def retry_after_seconds(response, cap=3600.0):
    """Parse Retry-After seconds or HTTP-date, capped at one hour."""
    value = response.headers.get("Retry-After")
    if not value:
        return 0.0
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            when = email.utils.parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=datetime.timezone.utc)
            delay = (when - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return 0.0
    return min(max(delay, 0.0), cap)


TECHNICAL_RETRY_SCHEMA = 1
TECHNICAL_LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) "
    r"(?:WARNING|ERROR) (?P<reason>(?:HTTP (?:202|429|5\d\d)|request error) "
    r"for (?P<url>\S+).*)$")


def celex_from_official_technical_url(url):
    """Extract one exact CELEX only from an official CELEX-addressed URL."""
    if not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.hostname not in ("eur-lex.europa.eu", "publications.europa.eu"):
        return None
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() == "uri" and value.upper().startswith("CELEX:"):
            celex = value.split(":", 1)[1]
            return celex if celex else None
    if parts.hostname == "publications.europa.eu":
        match = re.search(r"/resource/celex/([^/?#]+)", parts.path)
        if match:
            celex = unquote(match.group(1))
            return celex if celex else None
    return None


def parse_technical_log_line(line):
    """Return an exact official-URL technical event or None."""
    match = TECHNICAL_LOG_PATTERN.match(str(line).rstrip("\r\n"))
    if match is None:
        return None
    celex = celex_from_official_technical_url(match.group("url"))
    if celex is None:
        return None
    return {
        "celex": celex,
        "log_timestamp": match.group("timestamp"),
        "reason": match.group("reason"),
    }


def load_technical_retry_journal(valid_ids=None):
    """Load valid journal entries, ignoring malformed/out-of-scope records."""
    if not TECHNICAL_RETRY_FILE.exists():
        return {}
    try:
        with open(str(TECHNICAL_RETRY_FILE), "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as exc:
        log.warning("ignoring unreadable technical retry journal: %s", exc)
        return {}
    if (not isinstance(payload, dict) or
            payload.get("schema_version") != TECHNICAL_RETRY_SCHEMA or
            not isinstance(payload.get("entries"), list)):
        log.warning("ignoring malformed technical retry journal root")
        return {}
    valid = set(valid_ids) if valid_ids is not None else None
    entries = {}
    for raw in payload["entries"]:
        if not isinstance(raw, dict):
            continue
        celex = raw.get("celex")
        first_seen = raw.get("first_seen_at")
        last_seen = raw.get("last_seen_at")
        last_reason = raw.get("last_reason")
        event_count = raw.get("event_count")
        if (not isinstance(celex, str) or not celex or
                (valid is not None and celex not in valid) or
                not isinstance(first_seen, str) or not first_seen or
                not isinstance(last_seen, str) or not last_seen or
                not isinstance(last_reason, str) or not last_reason or
                not isinstance(event_count, int) or event_count < 1 or
                celex in entries):
            continue
        entry = {
            "celex": celex,
            "first_seen_at": first_seen,
            "last_seen_at": last_seen,
            "last_reason": last_reason,
            "event_count": event_count,
        }
        if isinstance(raw.get("last_source_log_timestamp"), str):
            entry["last_source_log_timestamp"] = raw[
                "last_source_log_timestamp"]
        entries[celex] = entry
    return entries


def write_technical_retry_journal(entries):
    """Atomically persist a stable exact-ID retry set."""
    now = now_iso()
    atomic_write_json(TECHNICAL_RETRY_FILE, {
        "schema_version": TECHNICAL_RETRY_SCHEMA,
        "updated_at": now,
        "entries": [entries[celex] for celex in sorted(entries)],
    })


def record_technical_retry(celex, reason, source_log_timestamp=None):
    """Thread-safely add/update a selected target after a technical anomaly."""
    if (not isinstance(celex, str) or
            celex not in technical_retry_target_ids or
            not isinstance(reason, str) or not reason):
        return False
    with technical_retry_lock:
        entries = load_technical_retry_journal(technical_retry_target_ids)
        now = now_iso()
        entry = entries.get(celex)
        if entry is None:
            entry = {
                "celex": celex,
                "first_seen_at": now,
                "event_count": 0,
            }
        entry["last_seen_at"] = now
        entry["last_reason"] = reason[:4000]
        entry["event_count"] += 1
        if source_log_timestamp:
            entry["last_source_log_timestamp"] = source_log_timestamp
        entries[celex] = entry
        write_technical_retry_journal(entries)
    return True


def remove_technical_retries(celex_ids):
    """Main-thread atomic removal after successful file/manifest handling."""
    remove_ids = {celex for celex in celex_ids if isinstance(celex, str)}
    if not remove_ids:
        return
    with technical_retry_lock:
        entries = load_technical_retry_journal(technical_retry_target_ids)
        changed = False
        for celex in remove_ids:
            if celex in entries:
                del entries[celex]
                changed = True
        if changed:
            write_technical_retry_journal(entries)


def technical_retry_is_journaled(celex):
    """Read the current atomic journal after a worker records an event."""
    if not isinstance(celex, str):
        return False
    with technical_retry_lock:
        return celex in load_technical_retry_journal(
            technical_retry_target_ids)


def initialize_technical_retry_journal(targets, manifest):
    """Load the journal or seed it once from exact current technical log URLs."""
    global technical_retry_target_ids
    technical_retry_target_ids = {entry["celex"] for entry in targets}
    if TECHNICAL_RETRY_FILE.exists():
        return load_technical_retry_journal(technical_retry_target_ids)
    entries = {}
    if ERROR_LOG.exists():
        try:
            with open(str(ERROR_LOG), "r", encoding="utf-8") as fh:
                lines = list(fh)
        except OSError as exc:
            log.warning("cannot seed technical retry journal: %s", exc)
            lines = []
        for line in lines:
            event = parse_technical_log_line(line)
            if event is None:
                continue
            celex = event["celex"]
            prior = manifest.get(celex)
            if (celex not in technical_retry_target_ids or
                    (prior or {}).get("status") == "ok" or
                    existing_file(STAMM_DIR, sanitize_celex(celex)) is not None):
                continue
            now = now_iso()
            entry = entries.get(celex)
            if entry is None:
                entry = {
                    "celex": celex,
                    "first_seen_at": now,
                    "event_count": 0,
                }
            entry.update({
                "last_seen_at": now,
                "last_reason": "seeded from crawl_errors.log: " +
                event["reason"][:3950],
                "last_source_log_timestamp": event["log_timestamp"],
                "event_count": entry["event_count"] + 1,
            })
            entries[celex] = entry
    with technical_retry_lock:
        write_technical_retry_journal(entries)
    if entries:
        log.warning("seeded technical retry journal with %d unresolved targets",
                    len(entries))
    return entries


def get_with_retry(session, url, timeout=120, source="eurlex", headers=None,
                   audit=None, route=None, technical_celex=None):
    """GET with throttling and exponential backoff on 429/5xx/network errors.

    Returns the Response, or None if all retries failed.
    """
    delay = (EURLEX_THROTTLE_SECONDS if source == "eurlex"
             else CELLAR_THROTTLE_SECONDS)
    for attempt in range(MAX_RETRIES):
        throttle_request(source, delay)
        try:
            resp = session.get(url, timeout=timeout, headers=headers)
        except requests.RequestException as exc:
            log.warning("request error for %s (attempt %d): %s", url, attempt + 1, exc)
            record_technical_retry(
                technical_celex,
                "request error via %s %s: %s" % (source, url, exc))
            if attempt + 1 < MAX_RETRIES:
                time.sleep(2 ** (attempt + 1))
            continue
        waf_challenge = (
            resp.status_code == 202 and
            resp.headers.get("x-amzn-waf-action", "").lower() == "challenge")
        if resp.status_code == 429 or resp.status_code >= 500 or waf_challenge:
            log.warning("HTTP %d for %s (attempt %d)", resp.status_code, url, attempt + 1)
            reason = ("HTTP %d%s via %s %s" % (
                resp.status_code, " WAF challenge" if waf_challenge else "",
                source, url))
            journaled = record_technical_retry(technical_celex, reason)
            if (waf_challenge and journaled and defer_on_waf_enabled and
                    technical_retry_is_journaled(technical_celex)):
                raise TechnicalWAFDefer(technical_celex, url, reason)
            if attempt + 1 < MAX_RETRIES:
                backoff = 2 ** (attempt + 1)
                time.sleep(max(backoff, retry_after_seconds(resp)))
            continue
        return resp
    log.error("giving up on %s after %d attempts", url, MAX_RETRIES)
    if audit is not None:
        audit.record(route or source, url)
    return None


def looks_like_error_page(html_text):
    return any(marker in html_text for marker in ERROR_PAGE_MARKERS)


def valid_pdf_content(content):
    """Require PDF magic and a terminal EOF marker, allowing trailing whitespace/NUL."""
    if not content.startswith(b"%PDF"):
        return False
    return content.rstrip(b"\x00\x09\x0a\x0c\x0d\x20").endswith(b"%%EOF")


def sha256_file(path):
    digest = hashlib.sha256()
    with open(str(path), "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def language_code_from_uri(uri):
    if not isinstance(uri, str) or not uri.startswith(LANGUAGE_URI_PREFIX):
        return None
    return uri.rsplit("/", 1)[-1].upper()


def ordered_language_codes(codes):
    unique = {str(code).upper() for code in codes if code}
    preferred = [code for code in ("DEU", "ENG") if code in unique]
    remaining = sorted(
        unique - set(preferred),
        key=lambda code: (EU_LANGUAGES.get(code, (code,))[0], code))
    return preferred + remaining


def input_order_language_codes(codes):
    """Deduplicate authority codes without changing the caller's priority."""
    ordered = []
    seen = set()
    for code in codes:
        code = str(code or "").upper()
        if code and code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def output_language(code):
    return EU_LANGUAGES.get(code, (code, None))[0]


def normalize_output_language(marker):
    marker = str(marker or "").upper()
    return output_language(marker) if marker in EU_LANGUAGES else marker


def normalize_oj_header(value):
    value = unicodedata.normalize("NFKC", str(value))
    value = value.replace("’", "'").replace("‘", "'").replace("`", "'")
    return re.sub(r"\s+", " ", value).strip().casefold()


def infer_language_from_text(text):
    patterns = (
        r'<html\b[^>]*\blang=["\']\s*([A-Za-z]{2,3})(?:[-_][A-Za-z]+)?\s*["\']',
        r'<meta\b[^>]*\bname=["\']DC\.language["\'][^>]*\bcontent=["\']\s*([A-Za-z]{2,3})\s*["\']',
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).upper()
    for match in re.finditer(
            r'<p\b[^>]*\bclass=["\']([^"\']*)["\'][^>]*>\s*([A-Za-z]{2,3})\s*</p>',
            text, flags=re.IGNORECASE):
        class_tokens = {token.lower() for token in match.group(1).split()}
        if class_tokens & {"hd-lg", "oj-hd-lg"}:
            return match.group(2).upper()
    return None


def valid_legal_markup(text):
    """Reject metadata/landing shells while accepting legacy and modern XHTML."""
    prefix = text[:2000].lower()
    if (("<html" not in prefix and "<!doctype" not in prefix and
         "<?xml" not in prefix) or looks_like_error_page(text)):
        return False
    visible = visible_body_text(text)
    return len(visible) >= 100


def visible_body_text(text):
    body_match = re.search(r"<body\b[^>]*>(.*)</body\s*>", text,
                           flags=re.IGNORECASE | re.DOTALL)
    if body_match is None:
        return ""
    visible = html_module.unescape(
        re.sub(r"<[^>]+>", " ", body_match.group(1)))
    return re.sub(r"\s+", " ", visible).strip()


def merger_case_number(celex):
    match = re.fullmatch(r"3\d{4}M(\d+)", str(celex))
    return match.group(1) if match else None


def merger_text_identity_match(text, celex):
    """Return the target only when it is the first full M-case token."""
    number = merger_case_number(celex)
    if number is None:
        return None
    match = re.search(
        r"(?<![A-Z0-9])M\s*[./-]\s*(\d+)(?!\d)",
        text, flags=re.IGNORECASE)
    if match is None or match.group(1) != number:
        return None
    return match.group(0)


def merger_text_identity_matches(text, celex):
    if merger_case_number(celex) is None:
        return True
    return merger_text_identity_match(text, celex) is not None


def merger_pdf_identity_match(content, celex):
    """Return an M reference from text extracted from the first five PDF pages."""
    if merger_case_number(celex) is None:
        return None
    if not valid_pdf_content(content):
        return None
    try:
        completed = subprocess.run(
            ["gs", "-q", "-dSAFER", "-dBATCH", "-dNOPAUSE",
             "-sDEVICE=txtwrite", "-dFirstPage=1", "-dLastPage=5",
             "-sOutputFile=-", "-"],
            input=content, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=60, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    text = completed.stdout.decode("utf-8", errors="replace")
    return merger_text_identity_match(text, celex)


def merger_pdf_identity_matches(content, celex):
    if merger_case_number(celex) is None:
        return True
    return merger_pdf_identity_match(content, celex) is not None


def merger_payload_identity_metadata(content, extension, celex,
                                     source_resolution, source_url):
    """Build durable exact-file and visible-case evidence for an M payload."""
    number = merger_case_number(celex)
    if number is None:
        return {}
    extension = str(extension).lower()
    if extension == "html":
        text = content.decode("utf-8", errors="replace")
        matched = merger_text_identity_match(visible_body_text(text), celex)
        method = "visible_html_body_exact_M_token"
    elif extension == "pdf":
        matched = merger_pdf_identity_match(content, celex)
        method = "ghostscript_txtwrite_pages_1_5_exact_M_token"
    else:
        return None
    if matched is None:
        return None
    return {
        "file_sha256": hashlib.sha256(content).hexdigest(),
        "file_bytes": len(content),
        "merger_expected_case_reference": "M." + number,
        "merger_matched_case_reference": matched,
        "merger_identity_validation_method": method,
        "merger_source_resolution": source_resolution,
        "merger_source_url": source_url,
    }


def response_uri_identity_matches(url, identity):
    """Require exactly one final URI query parameter with an exact identity."""
    # parse_qsl handles percent-encoded colons, slashes, and corrigendum
    # parentheses without accepting a neighboring CELEX record.
    try:
        uri_values = [value for key, value in parse_qsl(
            urlsplit(url).query, keep_blank_values=True)
            if key.casefold() == "uri"]
    except (TypeError, ValueError):
        return False
    return uri_values == [identity]


def eurlex_response_identity_matches(url, celex):
    """Require the final EUR-Lex response URL to retain the exact CELEX key."""
    return response_uri_identity_matches(url, "CELEX:" + celex)


def fetch_document_languages(session, celex, language_codes, audit=None,
                             formats=("HTML", "PDF"),
                             preserve_language_order=False):
    """Try official EUR-Lex HTML/PDF in each requested authority language."""
    codes = (input_order_language_codes(language_codes)
             if preserve_language_order else
             ordered_language_codes(language_codes))
    formats = tuple(str(fmt).upper() for fmt in formats)
    if not formats or any(fmt not in ("HTML", "PDF") for fmt in formats):
        raise ValueError("formats must contain only HTML and/or PDF")
    # Preserve the established preferred order: DE/EN HTML before DE/EN PDF.
    attempts = [(code, fmt) for fmt in formats for code in codes]
    for code, fmt in attempts:
        lang = output_language(code)
        url = EURLEX_URL.format(lang=lang, fmt=fmt, celex=celex)
        resp = get_with_retry(
            session, url, audit=audit,
            route="EUR-Lex %s %s" % (lang, fmt),
            technical_celex=celex)
        route = "EUR-Lex %s %s" % (lang, fmt)
        if resp is None:
            continue
        if resp.status_code != 200:
            if resp.status_code not in CLEAN_UNAVAILABLE_STATUS:
                record_unexpected_response(
                    audit, route, resp, "unexpected non-success")
            continue
        content = resp.content
        if len(content) < MIN_VALID_SIZE:
            text = content.decode(resp.encoding or "utf-8", errors="replace")
            if not looks_like_error_page(text):
                record_unexpected_response(
                    audit, route, resp, "undersized HTTP 200 payload")
            continue
        if not content.startswith(b"%PDF"):
            error_text = content.decode(
                resp.encoding or "utf-8", errors="replace")
            if looks_like_error_page(error_text):
                # A recognized EUR-Lex no-document/site shell is conclusive
                # route absence, even when its chrome declares EN and the
                # requested legal-content route was another language.
                continue
        final_route_matches = (
            "/legal-content/%s/" % lang.upper() in resp.url and
            eurlex_response_identity_matches(resp.url, celex))
        response_language = resp.headers.get("Content-Language", "")
        response_language = response_language.split(",", 1)[0].strip()
        response_language = normalize_output_language(
            response_language.split("-", 1)[0])
        if (not final_route_matches or
                (response_language and response_language != lang)):
            record_unexpected_response(
                audit, route, resp, "language route/header mismatch")
            continue
        if fmt == "PDF":
            if not valid_pdf_content(content):
                record_unexpected_response(
                    audit, route, resp, "invalid PDF payload")
                continue
            merger_metadata = merger_payload_identity_metadata(
                content, "pdf", celex, "eurlex_direct", resp.url)
            if merger_metadata is None:
                record_unexpected_response(
                    audit, route, resp, "wrong/unprovable M case identity")
                continue
            result_metadata = {
                "source_resolution": "eurlex_direct",
                "source_identity": "CELEX:" + celex,
                "source_language_authority_code": code,
            }
            result_metadata.update(merger_metadata)
            return FetchResult(
                content, "pdf", lang, resp.url,
                result_metadata)
        text = content.decode(resp.encoding or "utf-8", errors="replace")
        explicit_language = normalize_output_language(
            infer_language_from_text(text))
        if looks_like_error_page(text):
            continue
        if (not valid_legal_markup(text) or
                (explicit_language and explicit_language != lang) or
                not (explicit_language == lang or
                     response_language == lang or final_route_matches)):
            record_unexpected_response(
                audit, route, resp, "invalid/shell/mismatched HTML payload")
            continue
        merger_metadata = merger_payload_identity_metadata(
            content, "html", celex, "eurlex_direct", resp.url)
        if merger_metadata is None:
            record_unexpected_response(
                audit, route, resp, "wrong/unprovable M case identity")
            continue
        result_metadata = {
            "source_resolution": "eurlex_direct",
            "source_identity": "CELEX:" + celex,
            "source_language_authority_code": code,
        }
        result_metadata.update(merger_metadata)
        return FetchResult(
            content, "html", lang, resp.url,
            result_metadata)
    return None


def fetch_document(session, celex, audit=None):
    """Try preferred German/English EUR-Lex HTML and PDF routes."""
    return fetch_document_languages(
        session, celex, ("DEU", "ENG"), audit=audit)


def override_text_matches(celex, text, language=None):
    override = EURLEX_OJ_OVERRIDES.get(celex)
    if override is None or not valid_legal_markup(text):
        return None
    expected = override["expected_headings"]
    language = normalize_output_language(language)
    headings = ([expected[language]] if language in expected else
                [expected[key] for key in sorted(expected)])
    # The decision number must occur in the heading portion, not merely in a
    # later cross-reference to a neighboring act.
    visible = visible_body_text(text)
    heading_end = min(
        [position for marker in (" DER RAT ", " THE COUNCIL ")
         for position in [visible.upper().find(marker)] if position >= 0] or
        [2000])
    heading_text = visible[:min(heading_end, 2000)]
    return next((heading for heading in headings if heading in heading_text), None)


def fetch_eurlex_oj_override(session, celex, audit=None):
    override = EURLEX_OJ_OVERRIDES[celex]
    identity = quote(override["identity"], safe="")
    for code in ("DEU", "ENG"):
        language = output_language(code)
        route = "EUR-Lex OJ override %s" % language
        url = EURLEX_IDENTITY_URL.format(lang=language, identity=identity)
        resp = get_with_retry(
            session, url, audit=audit, route=route,
            technical_celex=celex)
        if resp is None:
            continue
        if resp.status_code != 200:
            if resp.status_code not in CLEAN_UNAVAILABLE_STATUS:
                record_unexpected_response(
                    audit, route, resp, "unexpected non-success")
            continue
        text = resp.content.decode(resp.encoding or "utf-8", errors="replace")
        explicit_language = normalize_output_language(
            infer_language_from_text(text))
        final_route_matches = (
            "/legal-content/%s/" % language in resp.url and
            response_uri_identity_matches(resp.url, override["identity"]))
        matched_heading = override_text_matches(celex, text, language=language)
        if (len(resp.content) < MIN_VALID_SIZE or
                not final_route_matches or
                (explicit_language and explicit_language != language) or
                not matched_heading):
            record_unexpected_response(
                audit, route, resp, "wrong act/language/markup from OJ identity")
            continue
        return FetchResult(
            resp.content, "html", language, resp.url,
            {
                "source_resolution": "eurlex_official_oj_identity_override",
                "source_identity": override["identity"],
                "source_expected_headings": override["expected_headings"],
                "source_matched_heading": matched_heading,
                "source_language_authority_code": code,
                "file_sha256": hashlib.sha256(resp.content).hexdigest(),
                "file_bytes": len(resp.content),
            })
    raise RuntimeError("official OJ override did not resolve correct %s" % celex)


def fetch_cellar_document_languages(session, celex, language_codes, audit=None):
    """Fetch HTML via CELLAR in deterministic requested-language order.

    This is the robots-compliant bulk route.  Content negotiation on the
    official CELEX resource resolves the work to a language-specific CELLAR
    manifestation without crawling EUR-Lex's 10-second-delay web frontend.
    """
    url = CELLAR_CELEX_URL.format(celex=quote(celex, safe=""))
    for code in ordered_language_codes(language_codes):
        language = output_language(code)
        # Recent acts are exposed as XHTML; many older acts have only the
        # legacy text/html manifestation.  Both routes resolve to an exact
        # official CELLAR DOC_1 resource.
        for accept in ("application/xhtml+xml", "text/html"):
            route = "CELLAR %s %s" % (code, accept)
            resp = get_with_retry(
                session, url, source="cellar",
                headers={
                    "Accept": accept,
                    "Accept-Language": code.lower(),
                }, audit=audit, route=route, technical_celex=celex)
            if resp is None:
                continue
            if resp.status_code != 200:
                if resp.status_code not in CLEAN_UNAVAILABLE_STATUS:
                    record_unexpected_response(
                        audit, route, resp, "unexpected non-success")
                continue
            content = resp.content
            if len(content) < MIN_VALID_SIZE:
                text = content.decode(
                    resp.encoding or "utf-8", errors="replace")
                if not looks_like_error_page(text):
                    record_unexpected_response(
                        audit, route, resp, "undersized HTTP 200 payload")
                continue
            text = content.decode(resp.encoding or "utf-8", errors="replace")
            if looks_like_error_page(text):
                continue
            explicit_language = normalize_output_language(
                infer_language_from_text(text))
            response_language = resp.headers.get("Content-Language", "")
            response_language = response_language.split(",", 1)[0].strip()
            response_language = normalize_output_language(
                response_language.split("-", 1)[0])
            if (not valid_legal_markup(text) or
                    "/resource/cellar/" not in resp.url or
                    not resp.url.rstrip("/").endswith("/DOC_1") or
                    (explicit_language and explicit_language != language) or
                    (response_language and response_language != language) or
                    not (explicit_language == language or
                         response_language == language)):
                record_unexpected_response(
                    audit, route, resp, "invalid/shell/mismatched HTML payload")
                continue
            merger_metadata = merger_payload_identity_metadata(
                content, "html", celex, "cellar_content_negotiation",
                resp.url)
            if merger_metadata is None:
                record_unexpected_response(
                    audit, route, resp, "wrong/unprovable M case identity")
                continue
            result_metadata = {
                "source_resolution": "cellar_content_negotiation",
                "source_language_authority_code": code,
            }
            result_metadata.update(merger_metadata)
            return FetchResult(
                content, "html", language, resp.url,
                result_metadata)
    return None


def fetch_cellar_document(session, celex, audit=None):
    """Fetch preferred German (then English) HTML via CELLAR."""
    return fetch_cellar_document_languages(
        session, celex, ("DEU", "ENG"), audit=audit)


def fetch_cellar_merger_pdf_languages(session, celex, language_codes,
                                      audit=None):
    """Fetch an exact M-case PDF by official CELLAR content negotiation."""
    if merger_case_number(celex) is None:
        return None
    url = CELLAR_CELEX_URL.format(celex=quote(celex, safe=""))
    for code in input_order_language_codes(language_codes):
        language = output_language(code)
        route = "CELLAR merger PDF %s" % code
        resp = get_with_retry(
            session, url, source="cellar",
            headers={"Accept": "application/pdf",
                     "Accept-Language": code.lower()},
            audit=audit, route=route, technical_celex=celex)
        if resp is None:
            continue
        if resp.status_code != 200:
            if resp.status_code not in CLEAN_UNAVAILABLE_STATUS:
                record_unexpected_response(
                    audit, route, resp, "unexpected non-success")
            continue
        content_type = resp.headers.get("Content-Type", "")
        content_type = content_type.split(";", 1)[0].strip().lower()
        content = resp.content
        response_language = resp.headers.get("Content-Language", "")
        response_language = response_language.split(",", 1)[0].strip()
        response_language = normalize_output_language(
            response_language.split("-", 1)[0])
        official_manifestation = (
            "/resource/cellar/" in resp.url and
            resp.url.rstrip("/").endswith("/DOC_1"))
        if (content_type != "application/pdf" or
                len(content) < MIN_VALID_SIZE or
                not valid_pdf_content(content) or
                not official_manifestation or
                (response_language and response_language != language)):
            text = content.decode(resp.encoding or "utf-8", errors="replace")
            if not looks_like_error_page(text):
                record_unexpected_response(
                    audit, route, resp,
                    "invalid PDF/MIME/manifestation/language response")
            continue
        merger_metadata = merger_payload_identity_metadata(
            content, "pdf", celex, "cellar_pdf_content_negotiation",
            resp.url)
        if merger_metadata is None:
            record_unexpected_response(
                audit, route, resp, "wrong/unprovable first M case identity")
            continue
        result_metadata = {
            "source_resolution": "cellar_pdf_content_negotiation",
            "source_identity": "CELEX:" + celex,
            "source_language_authority_code": code,
        }
        result_metadata.update(merger_metadata)
        return FetchResult(
            content, "pdf", language, resp.url, result_metadata)
    return None


def discover_cellar_languages(session, celex):
    """Enumerate every language expression and deterministic title.

    Failure to complete this authoritative discovery is a crawler error, not
    evidence that the law is absent.
    """
    cached = trusted_merger_language_cache.get(celex)
    if cached is not None:
        return {
            "codes": list(cached["codes"]),
            "titles": dict(cached["titles"]),
            "source_resolution": "trusted_merger_language_cache",
        }
    escaped = celex.replace("\\", "\\\\").replace('"', '\\"')
    query = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?lang (MIN(STR(?ti)) AS ?title) WHERE {
  ?work cdm:resource_legal_id_celex ?id .
  FILTER(STR(?id) = "%s")
  ?exp cdm:expression_belongs_to_work ?work ;
       cdm:expression_uses_language ?lang .
  OPTIONAL { ?exp cdm:expression_title ?ti }
}
GROUP BY ?lang
ORDER BY ?lang
""" % escaped
    rows = None
    last_error = "no response"
    for attempt in range(PRINT_SPARQL_MAX_RETRIES):
        throttle_request("cellar", CELLAR_THROTTLE_SECONDS)
        try:
            resp = session.post(
                SPARQL_ENDPOINT,
                data={"query": query,
                      "format": "application/sparql-results+json"},
                timeout=PRINT_SPARQL_TIMEOUT_SECONDS)
            if resp.status_code == 200:
                rows = resp.json()["results"]["bindings"]
                break
            last_error = "HTTP %d" % resp.status_code
            log.warning("language-discovery SPARQL HTTP %d for %s (attempt %d)",
                        resp.status_code, celex, attempt + 1)
            if (resp.status_code in (202, 429) or
                    resp.status_code >= 500):
                record_technical_retry(
                    celex, "language-discovery SPARQL HTTP %d via %s" %
                    (resp.status_code, SPARQL_ENDPOINT))
        except (requests.RequestException, ValueError, KeyError) as exc:
            last_error = str(exc)
            log.warning("language-discovery SPARQL error for %s (attempt %d): %s",
                        celex, attempt + 1, exc)
            record_technical_retry(
                celex, "language-discovery SPARQL error via %s: %s" %
                (SPARQL_ENDPOINT, exc))
            resp = None
        if attempt + 1 < PRINT_SPARQL_MAX_RETRIES:
            backoff = 2 ** (attempt + 1)
            retry_after = (retry_after_seconds(
                resp, cap=PRINT_RETRY_AFTER_CAP_SECONDS + 1)
                if resp is not None else 0)
            if retry_after > PRINT_RETRY_AFTER_CAP_SECONDS:
                raise RuntimeError(
                    "language discovery Retry-After exceeds bounded wait")
            time.sleep(max(backoff, retry_after))
    if rows is None:
        raise RuntimeError("CELLAR language discovery failed for %s: %s" %
                           (celex, last_error))
    codes = ordered_language_codes(
        language_code_from_uri(binding_value(row, "lang")) for row in rows)
    titles = {}
    for row in rows:
        code = language_code_from_uri(binding_value(row, "lang"))
        title = binding_value(row, "title")
        if code and isinstance(title, str) and title.strip():
            titles[code] = title.strip()
    return {"codes": codes, "titles": titles}


def enrich_result_title(result, language_titles):
    if result is None:
        return None
    code = result.metadata.get("source_language_authority_code")
    title = language_titles.get(code)
    if title:
        result.metadata["source_expression_title"] = title
        result.metadata["source_expression_title_language"] = (
            output_language(code))
    return result


PRINT_IDENTITY_FIELDS = (
    "work", "exp", "print", "parent_print", "parent_exp", "pdf",
    "page_first", "page_last", "pages_total",
)


def binding_value(binding, field):
    return binding.get(field, {}).get("value")


def select_print_pdf_bindings(rows, celex):
    """Return every complete unambiguous language binding in preference order."""
    selected = []
    language_codes = ordered_language_codes(
        language_code_from_uri(binding_value(row, "lang")) for row in rows)
    for language_code in language_codes:
        language_uri = LANGUAGE_URI_PREFIX + language_code
        language = output_language(language_code)
        candidates = [row for row in rows
                      if binding_value(row, "lang") == language_uri]
        if not candidates:
            continue
        identities = {
            tuple(binding_value(row, field) for field in PRINT_IDENTITY_FIELDS)
            for row in candidates
        }
        if (len(identities) != 1 or
                any(value is None for value in next(iter(identities)))):
            log.warning("ambiguous CELLAR print relationship for %s (%s): "
                        "%d rows, %d identities",
                        celex, language, len(candidates), len(identities))
            continue
        binding = candidates[0]
        try:
            page_first = int(binding_value(binding, "page_first"))
            page_last = int(binding_value(binding, "page_last"))
            pages_total = int(binding_value(binding, "pages_total"))
        except (TypeError, ValueError):
            log.warning("invalid CELLAR page metadata for %s (%s)",
                        celex, language)
            continue
        if (page_first < 1 or page_last < page_first or pages_total < 1 or
                page_last - page_first + 1 != pages_total):
            log.warning("inconsistent CELLAR page span for %s (%s): %s-%s/%s",
                        celex, language, page_first, page_last, pages_total)
            continue
        selected.append(
            (binding, language_code, page_first, page_last, pages_total))
    return selected


def select_print_pdf_binding(rows, celex):
    """Backward-compatible first exact binding helper used by diagnostics."""
    selected = select_print_pdf_bindings(rows, celex)
    return selected[0] if selected else None


def _run_ghostscript(arguments, timeout=300):
    executable = shutil.which("gs")
    if executable is None:
        log.warning("Ghostscript unavailable; cannot extract OJ page span")
        return None
    try:
        return subprocess.run(
            [executable] + list(arguments), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("Ghostscript failed: %s", exc)
        return None


def extract_oj_page_span(container, page_first, page_last, pages_total,
                         language_code):
    """Extract an OJ span only after proving every printed page label.

    CELLAR's parent PDF is a complete Official Journal issue.  CDM page
    numbers are printed OJ labels, not physical PDF page numbers.  Render each
    page to text, locate every requested header exactly once, and require the
    corresponding physical pages to be consecutive before extracting.
    """
    if not valid_pdf_content(container):
        return None
    with tempfile.TemporaryDirectory(prefix="eu-oj-extract-") as tmp_name:
        tmp_dir = Path(tmp_name)
        container_path = tmp_dir / "container.pdf"
        container_path.write_bytes(container)
        page_pattern = str(tmp_dir / "page-%06d.txt")
        rendered = _run_ghostscript([
            "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=txtwrite",
            "-sOutputFile=" + page_pattern, str(container_path),
        ])
        if rendered is None or rendered.returncode != 0:
            return None
        page_files = sorted(tmp_dir.glob("page-*.txt"))
        if not page_files:
            return None

        language_info = EU_LANGUAGES.get(language_code)
        if language_info is None:
            log.warning("no audited OJ header mapping for language %s",
                        language_code)
            return None
        official_headers = tuple(normalize_oj_header(value)
                                 for value in language_info[1])
        label_hits = {page: [] for page in range(page_first, page_last + 1)}
        label_text = {}
        for physical_page, text_path in enumerate(page_files, 1):
            text = text_path.read_text(encoding="utf-8", errors="replace")[:1200]
            normalized_text = normalize_oj_header(text)
            if not any(header in normalized_text for header in official_headers):
                continue
            for printed_page in label_hits:
                pattern = (r"\b([LSC]\s*\d+\s*/\s*0*%d)\b" % printed_page)
                match = re.search(pattern, text, flags=re.IGNORECASE)
                if match:
                    label_hits[printed_page].append(physical_page)
                    label_text[printed_page] = re.sub(
                        r"\s+", " ", match.group(1).upper())

        if any(len(hits) != 1 for hits in label_hits.values()):
            log.warning("could not uniquely map OJ labels %s-%s in %s PDF",
                        page_first, page_last, output_language(language_code))
            return None
        physical_pages = [label_hits[page][0]
                          for page in range(page_first, page_last + 1)]
        if (len(physical_pages) != pages_total or
                physical_pages != list(range(physical_pages[0],
                                             physical_pages[0] + pages_total))):
            log.warning("OJ labels %s-%s map to nonconsecutive physical pages %s",
                        page_first, page_last, physical_pages)
            return None

        output_path = tmp_dir / "excerpt.pdf"
        extracted = _run_ghostscript([
            "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=pdfwrite",
            "-dFirstPage=%d" % physical_pages[0],
            "-dLastPage=%d" % physical_pages[-1],
            "-sOutputFile=" + str(output_path), str(container_path),
        ])
        if (extracted is None or extracted.returncode != 0 or
                not output_path.exists()):
            return None
        excerpt = output_path.read_bytes()
        if not valid_pdf_content(excerpt):
            return None

        verify_pattern = str(tmp_dir / "verify-%06d.txt")
        verified = _run_ghostscript([
            "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=txtwrite",
            "-sOutputFile=" + verify_pattern, str(output_path),
        ])
        if (verified is None or verified.returncode != 0 or
                len(list(tmp_dir.glob("verify-*.txt"))) != pages_total):
            return None
        version = _run_ghostscript(["--version"], timeout=30)
        version_text = (version.stdout.decode("ascii", errors="replace").strip()
                        if version is not None and version.returncode == 0
                        else "unknown")
        metadata = {
            "source_page_labels": [label_text[page]
                                   for page in range(page_first, page_last + 1)],
            "source_physical_pages": physical_pages,
            "source_page_first": page_first,
            "source_page_last": page_last,
            "source_pages_total": pages_total,
            "extraction_tool": "GPL Ghostscript " + version_text,
            "extraction_method": "pdfwrite after unique OJ-header label mapping",
        }
        return excerpt, metadata


def fetch_cellar_print_pdf(session, celex, audit=None):
    """Resolve and safely extract a print-only act from its parent OJ PDF."""
    escaped = celex.replace("\\", "\\\\").replace('"', '\\"')
    query = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?work ?exp ?print ?parent_print ?parent_exp ?pdf ?lang
                ?page_first ?page_last ?pages_total WHERE {
  ?work cdm:resource_legal_id_celex ?id .
  FILTER(STR(?id) = "%s")
  ?exp cdm:expression_belongs_to_work ?work ;
       cdm:expression_uses_language ?lang .
  ?print cdm:manifestation_manifests_expression ?exp ;
         cdm:manifestation_type ?print_type ;
         cdm:manifestation_part_of_manifestation ?parent_print ;
         cdm:manifestation_official-journal_part_page_first ?page_first ;
         cdm:manifestation_official-journal_part_page_last ?page_last ;
         cdm:manifestation_official-journal_part_pages_total ?pages_total .
  FILTER(STR(?print_type) = "print")
  ?parent_print cdm:manifestation_manifests_expression ?parent_exp .
  ?parent_exp cdm:expression_uses_language ?lang .
  ?pdf cdm:manifestation_manifests_expression ?parent_exp ;
       cdm:manifestation_type ?pdf_type .
  FILTER(STR(?pdf_type) = "pdf")
}
ORDER BY ?lang ?work ?exp ?print ?parent_print ?parent_exp ?pdf
""" % escaped

    rows = None
    for attempt in range(PRINT_SPARQL_MAX_RETRIES):
        throttle_request("cellar", CELLAR_THROTTLE_SECONDS)
        try:
            resp = session.post(
                SPARQL_ENDPOINT,
                data={"query": query,
                      "format": "application/sparql-results+json"},
                timeout=PRINT_SPARQL_TIMEOUT_SECONDS)
            if resp.status_code == 200:
                rows = resp.json()["results"]["bindings"]
                break
            log.warning("print-fallback SPARQL HTTP %d for %s (attempt %d)",
                        resp.status_code, celex, attempt + 1)
            if (resp.status_code in (202, 429) or
                    resp.status_code >= 500):
                record_technical_retry(
                    celex, "print-fallback SPARQL HTTP %d via %s" %
                    (resp.status_code, SPARQL_ENDPOINT))
        except (requests.RequestException, ValueError, KeyError) as exc:
            log.warning("print-fallback SPARQL error for %s (attempt %d): %s",
                        celex, attempt + 1, exc)
            record_technical_retry(
                celex, "print-fallback SPARQL error via %s: %s" %
                (SPARQL_ENDPOINT, exc))
            resp = None
        if attempt + 1 < PRINT_SPARQL_MAX_RETRIES:
            backoff = 2 ** (attempt + 1)
            retry_after = (retry_after_seconds(
                resp, cap=PRINT_RETRY_AFTER_CAP_SECONDS + 1)
                if resp is not None else 0)
            if retry_after > PRINT_RETRY_AFTER_CAP_SECONDS:
                log.warning("print-fallback Retry-After for %s exceeds %ss; "
                            "not retrying early", celex,
                            PRINT_RETRY_AFTER_CAP_SECONDS)
                break
            time.sleep(max(backoff, retry_after))
    if rows is None:
        raise RuntimeError("CELLAR print-parent SPARQL failed for %s" % celex)
    if not rows:
        return None

    selected_bindings = select_print_pdf_bindings(rows, celex)
    if not selected_bindings:
        raise RuntimeError(
            "CELLAR print-parent identity is present but not uniquely resolvable "
            "for %s" % celex)
    failures = []
    for binding, language_code, page_first, page_last, pages_total in selected_bindings:
        language = output_language(language_code)
        manifestation = binding_value(binding, "pdf")
        url = re.sub(r"^http://", "https://", manifestation)
        resp = get_with_retry(
            session, url, source="cellar",
            headers={"Accept": "application/pdf"}, audit=audit,
            route="CELLAR print-parent %s" % language_code,
            technical_celex=celex)
        if (resp is None or resp.status_code != 200 or
                len(resp.content) < MIN_VALID_SIZE or
                not valid_pdf_content(resp.content)):
            failures.append("%s parent PDF unavailable/invalid" % language_code)
            continue
        extracted = extract_oj_page_span(
            resp.content, page_first, page_last, pages_total, language_code)
        if extracted is None:
            failures.append("%s OJ page identity not proven" % language_code)
            continue
        content, extraction_metadata = extracted
        merger_metadata = merger_payload_identity_metadata(
            content, "pdf", celex, DERIVED_PRINT_RESOLUTION, resp.url)
        if merger_metadata is None:
            failures.append("%s extracted PDF has wrong/unprovable M case "
                            "identity" % language_code)
            continue
        container_sha256 = hashlib.sha256(resp.content).hexdigest()
        try:
            container_path = store_source_container(
                resp.content, container_sha256)
        except OSError as exc:
            log.error("cannot persist verified source container for %s/%s: %s",
                      celex, language_code, exc)
            failures.append("%s container persistence failed" % language_code)
            continue
        metadata = {
            "source_resolution": DERIVED_PRINT_RESOLUTION,
            "source_language_authority_code": language_code,
            "cellar_work_uri": binding_value(binding, "work"),
            "cellar_source_expression_uri": binding_value(binding, "exp"),
            "cellar_print_part_uri": binding_value(binding, "print"),
            "cellar_parent_print_uri": binding_value(binding, "parent_print"),
            "cellar_parent_expression_uri": binding_value(binding, "parent_exp"),
            "cellar_pdf_manifestation_uri": manifestation,
            "source_container_url": resp.url,
            "source_container_file": str(container_path.relative_to(STAMM_DIR)),
            "source_container_sha256": container_sha256,
            "source_container_bytes": len(resp.content),
            "file_sha256": hashlib.sha256(content).hexdigest(),
            "file_bytes": len(content),
        }
        metadata.update(extraction_metadata)
        metadata.update(merger_metadata)
        log.info("CELLAR OJ-page fallback resolved %s (%s, labels %s-%s)",
                 celex, language, page_first, page_last)
        return FetchResult(content, "pdf", language, resp.url, metadata)
    raise RuntimeError("all CELLAR print candidates failed for %s: %s" %
                       (celex, "; ".join(failures)))


def fetch_basic_act(session, celex):
    """Resolve all official expression languages before declaring a miss."""
    audit = ResolutionAudit()
    is_merger_decision = merger_case_number(celex) is not None
    if celex in EURLEX_OJ_OVERRIDES:
        return fetch_eurlex_oj_override(session, celex, audit=audit)
    result = fetch_cellar_document(session, celex, audit=audit)
    if result is not None:
        return result
    if is_merger_decision:
        result = fetch_cellar_merger_pdf_languages(
            session, celex, ("ENG", "DEU"), audit=audit)
        if result is not None:
            return result
        # An exact EUR-Lex PDF is the common bulk representation for merger
        # decisions whose CELEX work has neither CELLAR HTML nor a directly
        # negotiated CELLAR PDF.  Try the preferred EN/DE routes before the
        # slower SPARQL language-discovery and print-parent resolvers.  The
        # shared EUR-Lex limiter still serializes these requests at the
        # robots.txt crawl delay, and fetch_document_languages retains the
        # exact final-URI, language, PDF-integrity, and first-M-token guards.
        result = fetch_document_languages(
            session, celex, ("ENG", "DEU"), audit=audit,
            formats=("PDF",), preserve_language_order=True)
        if result is not None:
            return result
        # A minority of merger works expose a directly negotiated CELLAR PDF
        # only in a nonpreferred official expression language.  Exhaust the
        # finite audited authority-code set before invoking per-work SPARQL.
        # Every request remains language-specific: an unspecified negotiation
        # cannot establish authoritative language provenance when CELLAR omits
        # Content-Language, and is therefore deliberately not used.
        other_cellar_pdf_codes = [
            code for code in ordered_language_codes(EU_LANGUAGES)
            if code not in ("DEU", "ENG")]
        result = fetch_cellar_merger_pdf_languages(
            session, celex, other_cellar_pdf_codes, audit=audit)
        if result is not None:
            return result
    try:
        discovery = discover_cellar_languages(session, celex)
    except RuntimeError:
        # Language discovery is authoritative for an exhaustive miss, but a
        # transient SPARQL outage must not suppress independently addressable
        # preferred EUR-Lex payloads.  Merger EN/DE PDFs were already tried by
        # the fast path, so its degraded-discovery branch tries HTML only and
        # never repeats the throttled preferred PDF routes.
        if is_merger_decision:
            result = fetch_document_languages(
                session, celex, ("DEU", "ENG"), audit=audit,
                formats=("HTML",), preserve_language_order=True)
        else:
            result = fetch_document(session, celex, audit=audit)
        if result is not None:
            return result
        raise
    language_codes = discovery["codes"]
    language_titles = discovery["titles"]
    other_codes = [code for code in language_codes
                   if code not in ("DEU", "ENG")]
    result = fetch_cellar_document_languages(
        session, celex, other_codes, audit=audit)
    if result is not None:
        return enrich_result_title(result, language_titles)
    if is_merger_decision:
        # EN/DE were already tried by the fast path above.  Retain exhaustive
        # support for merger decisions published only in another official
        # expression language, but never repeat the preferred routes.
        merger_pdf_codes = input_order_language_codes(
            code for code in language_codes if code not in ("ENG", "DEU"))
        if merger_pdf_codes:
            result = fetch_document_languages(
                session, celex, merger_pdf_codes, audit=audit,
                formats=("PDF",), preserve_language_order=True)
            if result is not None:
                return enrich_result_title(result, language_titles)
    print_error = None
    try:
        result = fetch_cellar_print_pdf(session, celex, audit=audit)
        if result is not None:
            return enrich_result_title(result, language_titles)
    except RuntimeError as exc:
        print_error = exc
    log.warning("CELLAR HTML/XHTML unavailable for %s; trying EUR-Lex fallback",
                celex)
    result = fetch_document_languages(
        session, celex, list(language_codes) + ["DEU", "ENG"], audit=audit,
        formats=(("HTML",) if is_merger_decision else ("HTML", "PDF")))
    if result is not None:
        return enrich_result_title(result, language_titles)
    if print_error is not None:
        raise print_error
    audit.raise_if_incomplete(celex)
    missing_metadata = {
        "available_expression_language_codes": language_codes,
        "available_expression_languages": [
            output_language(code) for code in language_codes],
        "available_language_titles": {
            output_language(code): language_titles[code]
            for code in language_codes if code in language_titles},
    }
    for code in language_codes:
        if code in language_titles:
            missing_metadata.update({
                "source_expression_title": language_titles[code],
                "source_expression_title_language": output_language(code),
            })
            break
    return MissingResult(missing_metadata)


def sanitize_celex(celex):
    """Make a CELEX number safe as a filename: 12016M/TXT -> 12016M_TXT,
    32019R0001(01) -> 32019R0001_01_."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", celex)


# --------------------------------------------------------------------------
# Manifest handling
# --------------------------------------------------------------------------

def load_manifest(folder):
    path = folder / "manifest.json"
    if path.exists():
        try:
            with open(str(path), "r", encoding="utf-8") as fh:
                entries = json.load(fh)
            if not isinstance(entries, list):
                raise ValueError("manifest root is not a list")
            return {entry["id"]: entry for entry in entries}
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("could not parse %s (%s); starting fresh", path, exc)
    return {}


def save_manifest(folder, manifest):
    path = folder / "manifest.json"
    tmp = folder / "manifest.json.tmp"
    entries = [manifest[key] for key in sorted(manifest)]
    with open(str(tmp), "w", encoding="utf-8") as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=1)
    tmp.replace(path)


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def valid_document_file(path):
    """Reject truncated files and EUR-Lex HTML error/landing pages."""
    if not path.exists() or path.stat().st_size < MIN_VALID_SIZE:
        return False
    try:
        content = path.read_bytes()
    except OSError:
        return False
    if path.suffix == ".pdf":
        return valid_pdf_content(content)
    if path.suffix == ".html":
        text = content.decode("utf-8", errors="replace")
        prefix = text[:2000].lower()
        return (("<html" in prefix or "<!doctype" in prefix or
                 "<?xml" in prefix) and not looks_like_error_page(text))
    return False


def exact_file_identity(path, expected_bytes, expected_sha256):
    """Validate structure, exact byte count, and SHA-256 for a saved payload."""
    try:
        expected_bytes = int(expected_bytes)
        return (valid_document_file(path) and
                path.stat().st_size == expected_bytes and
                sha256_file(path) == expected_sha256)
    except (OSError, TypeError, ValueError):
        return False


MERGER_IDENTITY_FIELDS = (
    "file_sha256", "file_bytes", "merger_expected_case_reference",
    "merger_matched_case_reference", "merger_identity_validation_method",
    "merger_source_resolution", "merger_source_url",
)


def official_merger_source_route_matches(celex, entry, path):
    """Allow only audited official route/resolution pairs for M payloads."""
    if not entry:
        return False
    resolution = entry.get("source_resolution")
    source_url = str(entry.get("source_url", ""))
    if resolution == "eurlex_direct":
        expected_route = "/TXT/%s/" % path.suffix.lstrip(".").upper()
        return (expected_route in source_url and
                eurlex_response_identity_matches(source_url, celex))
    if resolution == "cellar_content_negotiation":
        return (path.suffix.lower() == ".html" and
                "/resource/cellar/" in source_url and
                source_url.rstrip("/").endswith("/DOC_1"))
    if resolution == "cellar_pdf_content_negotiation":
        return (path.suffix.lower() == ".pdf" and
                "/resource/cellar/" in source_url and
                source_url.rstrip("/").endswith("/DOC_1") and
                entry.get("source_identity") == "CELEX:" + celex and
                entry.get("source_language_authority_code") in EU_LANGUAGES and
                output_language(entry["source_language_authority_code"]) ==
                entry.get("language"))
    if resolution == DERIVED_PRINT_RESOLUTION:
        return (path.suffix.lower() == ".pdf" and
                valid_derived_metadata(entry) and
                derived_record_files_match(entry, path))
    return False


def merger_record_matches(celex, entry, path):
    """Verify durable M-case evidence without re-running PDF extraction."""
    number = merger_case_number(celex)
    if number is None:
        return True
    expected_method = {
        ".html": "visible_html_body_exact_M_token",
        ".pdf": "ghostscript_txtwrite_pages_1_5_exact_M_token",
    }.get(path.suffix.lower())
    if (entry is None or entry.get("status") != "ok" or
            expected_method is None or
            any(entry.get(field) in (None, "")
                for field in MERGER_IDENTITY_FIELDS) or
            not exact_file_identity(
                path, entry.get("file_bytes"), entry.get("file_sha256")) or
            entry.get("merger_expected_case_reference") != "M." + number or
            entry.get("merger_identity_validation_method") != expected_method or
            entry.get("merger_source_resolution") !=
            entry.get("source_resolution") or
            entry.get("merger_source_url") != entry.get("source_url") or
            not official_merger_source_route_matches(
                celex, entry, path)):
        return False
    matched = entry.get("merger_matched_case_reference")
    return merger_text_identity_match(matched, celex) == matched


def override_record_matches(celex, entry, path):
    override = EURLEX_OJ_OVERRIDES.get(celex)
    language = normalize_output_language((entry or {}).get("language"))
    expected_heading = override and override["expected_headings"].get(language)
    if (override is None or entry is None or path.suffix != ".html" or
            entry.get("status") != "ok" or
            entry.get("source_resolution") !=
            "eurlex_official_oj_identity_override" or
            entry.get("source_identity") != override["identity"] or
            not response_uri_identity_matches(
                entry.get("source_url", ""), override["identity"]) or
            entry.get("source_expected_headings") !=
            override["expected_headings"] or
            not expected_heading or
            entry.get("source_matched_heading") != expected_heading or
            not exact_file_identity(
                path, entry.get("file_bytes"), entry.get("file_sha256"))):
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return override_text_matches(celex, text, language=language) == expected_heading


def apply_manifest_metadata_correction(celex, manifest_entry, index_entry):
    """Apply an audited upstream metadata correction with full provenance."""
    correction = MANIFEST_METADATA_CORRECTIONS.get(celex)
    if correction is None:
        return manifest_entry
    field = correction["field"]
    original = index_entry.get(field)
    original_key = "original_index_%s" % field
    if original != correction["original_value"]:
        # Do not perpetuate a local override after the authoritative index is
        # corrected, or silently apply it to a different upstream anomaly.
        if original == correction["value"]:
            manifest_entry.pop(original_key, None)
            manifest_entry.pop("body_%s" % field, None)
            manifest_entry.pop("metadata_anomaly", None)
        else:
            log.warning("metadata correction for %s no longer matches the "
                        "audited index value; leaving upstream value intact", celex)
        return manifest_entry
    if original is not None:
        manifest_entry.setdefault(original_key, original)
    manifest_entry[field] = correction["value"]
    manifest_entry["body_%s" % field] = correction["value"]
    manifest_entry["metadata_anomaly"] = {
        "field": field,
        "original_index_value": original,
        "corrected_value": correction["value"],
        "reason": correction["reason"],
        "evidence": "operative body heading/title",
    }
    return manifest_entry


def apply_identifier_metadata_warning(celex, manifest_entry):
    """Annotate an audited official CELEX/body-number discrepancy."""
    body_identifier = IDENTIFIER_METADATA_WARNINGS.get(celex)
    if body_identifier is None:
        return manifest_entry
    manifest_entry["identifier_metadata_warning"] = {
        "classification": "official_celex_body_number_discrepancy",
        "celex": celex,
        "body_identifier": body_identifier,
        "evidence": "official index title and operative body heading",
        "disposition": (
            "retain official CELEX key; payload is the indexed legal act"),
    }
    return manifest_entry


DERIVED_CELLAR_URI_FIELDS = (
    "cellar_work_uri", "cellar_source_expression_uri",
    "cellar_print_part_uri", "cellar_parent_print_uri",
    "cellar_parent_expression_uri", "cellar_pdf_manifestation_uri",
)

DERIVED_ALTERNATE_FIELDS = (
    "file", "format", "language", "source_url", "downloaded_at",
    "source_resolution", "source_language_authority_code",
    "source_container_url", "source_container_file",
    "source_container_sha256", "source_container_bytes",
    "file_sha256", "file_bytes", "source_page_labels",
    "source_physical_pages", "source_page_first", "source_page_last",
    "source_pages_total", "extraction_tool", "extraction_method",
) + DERIVED_CELLAR_URI_FIELDS

PROVENANCE_INCOMPLETE_FILES = {
    "32006D1004": {
        "file": "32006D1004.pdf",
        "format": "pdf",
        "language": "DE",
        "source_language_authority_code": "DEU",
        "role": (
            "exact_official_oj_page_derivative_upstream_uri_metadata_lost"),
        "provenance_status": "incomplete_upstream_uri_metadata",
        "provenance_warning": (
            "CELLAR URI bindings lost during interrupted manifest transition; "
            "not reconstructed"),
        "file_sha256": (
            "af7f29ed7931a324297f5161fc10dd25e06ae966548cd9b84436c04bdc280c08"),
        "file_bytes": 54747,
        "source_container_file": (
            "_source_containers/"
            "bdc6ce09044a760d11349eab05d3bee8d60826f9727f2ec6c42be843bd617c1b.pdf"),
        "source_container_sha256": (
            "bdc6ce09044a760d11349eab05d3bee8d60826f9727f2ec6c42be843bd617c1b"),
        "source_container_bytes": 14581191,
        "source_page_labels": ["L 410/178", "L 410/179", "L 410/180"],
        "source_physical_pages": [179, 180, 181],
        "source_page_first": 178,
        "source_page_last": 180,
        "source_pages_total": 3,
        "extraction_tool": "GPL Ghostscript 10.06.0",
        "extraction_method": (
            "pdfwrite after unique OJ-header label mapping"),
        "identity_validation_method": (
            "GS pages 1-3 exact OJ labels and 2006/1004/EG heading"),
    },
}


def derived_alternate_from_entry(entry, path):
    """Return a self-contained tracked alternate for a verified derivative."""
    candidate = dict(entry or {})
    if (candidate.get("source_resolution") == DERIVED_PRINT_RESOLUTION and
            not candidate.get("source_language_authority_code")):
        matching_codes = [
            code for code in EU_LANGUAGES
            if output_language(code) == candidate.get("language")]
        if len(matching_codes) == 1:
            candidate["source_language_authority_code"] = matching_codes[0]
    if (not candidate or candidate.get("source_resolution") !=
            DERIVED_PRINT_RESOLUTION or
            not derived_record_files_match(candidate, path)):
        return None
    alternate = {field: candidate[field] for field in DERIVED_ALTERNATE_FIELDS
                 if field in candidate}
    alternate["role"] = "exact_official_print_derivative_alternate"
    return alternate


def merge_alternate_files(manifest_entry, alternates):
    """Merge only currently verified derivatives, dropping stale records."""
    merged = {}
    prior_alternates = list(manifest_entry.pop("alternate_files", []))
    for alternate in prior_alternates + list(
            alternates or []):
        if not isinstance(alternate, dict):
            continue
        file_name = str(alternate.get("file") or "")
        if not file_name or Path(file_name).name != file_name:
            log.warning("dropping unsafe alternate file path %r", file_name)
            continue
        verified = derived_alternate_from_entry(
            alternate, STAMM_DIR / file_name)
        if verified is None:
            log.warning("dropping invalid/stale derivative alternate %s",
                        file_name)
            continue
        key = (verified.get("file"), verified.get("file_sha256"))
        if all(key):
            merged[key] = verified
    if merged:
        manifest_entry["alternate_files"] = [
            merged[key] for key in sorted(merged)]
    return manifest_entry


def incomplete_provenance_file_matches(record):
    """Fail closed on the one exact derivative whose CELLAR URIs were lost."""
    expected = PROVENANCE_INCOMPLETE_FILES.get("32006D1004")
    if record != expected:
        return False
    file_path = STAMM_DIR / record["file"]
    container_path = STAMM_DIR / record["source_container_file"]
    if (not exact_file_identity(
            file_path, record["file_bytes"], record["file_sha256"]) or
            not exact_file_identity(
                container_path, record["source_container_bytes"],
                record["source_container_sha256"])):
        return False
    try:
        completed = subprocess.run(
            ["gs", "-q", "-dSAFER", "-dBATCH", "-dNOPAUSE",
             "-sDEVICE=txtwrite", "-dFirstPage=1", "-dLastPage=3",
             "-sOutputFile=-", "-"],
            input=file_path.read_bytes(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=60, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    if completed.returncode != 0:
        return False
    text = unicodedata.normalize(
        "NFKC", completed.stdout.decode("utf-8", errors="replace"))
    text = re.sub(r"\s+", " ", text).upper()
    return all(marker in text for marker in (
        "L 410/178", "L 410/179", "L 410/180",
        "BESCHLUSS DES RATES", "VOM 21. DEZEMBER 2006",
        "(2006/1004/EG)", "ARTIKEL 1", "ARTIKEL 2"))


def apply_provenance_incomplete_files(celex, manifest_entry):
    expected = PROVENANCE_INCOMPLETE_FILES.get(celex)
    manifest_entry.pop("provenance_incomplete_files", None)
    if expected is None:
        return manifest_entry
    record = dict(expected)
    if incomplete_provenance_file_matches(record):
        manifest_entry["provenance_incomplete_files"] = [record]
    else:
        log.warning("incomplete-provenance derivative failed exact validation "
                    "for %s; not tracking it", celex)
    return manifest_entry


def valid_derived_metadata(entry):
    """Require the complete identity/page/extraction record for a derivative."""
    if (entry.get("source_resolution") != DERIVED_PRINT_RESOLUTION or
            entry.get("format") != "pdf" or
            not str(entry.get("file", "")).endswith(".pdf") or
            not isinstance(entry.get("language"), str)):
        return False
    language_code = entry.get("source_language_authority_code")
    if (language_code not in EU_LANGUAGES or
            output_language(language_code) != entry.get("language")):
        return False
    file_sha = entry.get("file_sha256")
    container_sha = entry.get("source_container_sha256")
    if (not isinstance(file_sha, str) or
            not re.fullmatch(r"[0-9a-f]{64}", file_sha) or
            not isinstance(container_sha, str) or
            not re.fullmatch(r"[0-9a-f]{64}", container_sha)):
        return False
    try:
        if (int(entry.get("file_bytes")) < MIN_VALID_SIZE or
                int(entry.get("source_container_bytes")) < MIN_VALID_SIZE):
            return False
        page_first = int(entry.get("source_page_first"))
        page_last = int(entry.get("source_page_last"))
        pages_total = int(entry.get("source_pages_total"))
    except (TypeError, ValueError):
        return False
    labels = entry.get("source_page_labels")
    physical = entry.get("source_physical_pages")
    if (page_first < 1 or page_last < page_first or pages_total < 1 or
            page_last - page_first + 1 != pages_total or
            not isinstance(labels, list) or len(labels) != pages_total or
            not all(isinstance(label, str) and label for label in labels) or
            not isinstance(physical, list) or len(physical) != pages_total or
            not all(isinstance(page, int) and page >= 1 for page in physical) or
            physical != list(range(physical[0], physical[0] + pages_total))):
        return False
    label_prefix = None
    for offset, label in enumerate(labels):
        match = re.fullmatch(
            r"\s*([LSC])\s*(\d+)\s*/\s*0*(\d+)\s*", label,
            flags=re.IGNORECASE)
        if match is None or int(match.group(3)) != page_first + offset:
            return False
        current_prefix = (match.group(1).upper(), int(match.group(2)))
        if label_prefix is None:
            label_prefix = current_prefix
        elif current_prefix != label_prefix:
            return False
    expected_container = (Path("_source_containers") /
                          (container_sha + ".pdf")).as_posix()
    if entry.get("source_container_file") != expected_container:
        return False
    cellar_prefix = "http://publications.europa.eu/resource/cellar/"
    secure_cellar_prefix = "https://publications.europa.eu/resource/cellar/"
    for field in DERIVED_CELLAR_URI_FIELDS:
        value = entry.get(field)
        if (not isinstance(value, str) or
                not value.startswith((cellar_prefix, secure_cellar_prefix))):
            return False
    for field in ("extraction_tool", "extraction_method"):
        if not isinstance(entry.get(field), str) or not entry[field]:
            return False
    container_url = entry.get("source_container_url")
    pdf_uri = entry.get("cellar_pdf_manifestation_uri")
    if (not isinstance(container_url, str) or
            re.sub(r"^https?://", "", container_url).rstrip("/") !=
            re.sub(r"^https?://", "", pdf_uri).rstrip("/") + "/DOC_1"):
        return False
    return True


def derived_record_files_match(entry, path):
    """Prove complete metadata, derivative bytes, and parent-container bytes."""
    if (not valid_derived_metadata(entry) or
            entry.get("file") != path.name or
            not exact_file_identity(
                path, entry.get("file_bytes"), entry.get("file_sha256"))):
        return False
    container_path = STAMM_DIR / entry["source_container_file"]
    return exact_file_identity(
        container_path, entry.get("source_container_bytes"),
        entry.get("source_container_sha256"))


def pending_derived_write_matches(entry, path):
    return (entry.get("status") == "pending_write" and
            derived_record_files_match(entry, path))


def infer_document_language(path):
    """Infer a language only from explicit markup in a saved HTML file."""
    if path.suffix != ".html":
        return None
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    return infer_language_from_text(text)


def existing_file(folder, base_name):
    """Return an existing validated document file, if any."""
    for ext in ("html", "pdf"):
        candidate = folder / (base_name + "." + ext)
        if valid_document_file(candidate):
            return candidate
    return None


def write_document(path, content):
    """Write a document atomically so an interrupted run remains resumable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".part", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, str(path))
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def store_source_container(content, expected_sha256):
    """Store a SHA-named source PDF, replacing any mismatched existing file."""
    SOURCE_CONTAINER_DIR.mkdir(parents=True, exist_ok=True)
    path = SOURCE_CONTAINER_DIR / (expected_sha256 + ".pdf")
    matches = False
    if valid_document_file(path):
        try:
            matches = (path.stat().st_size == len(content) and
                       sha256_file(path) == expected_sha256)
        except OSError:
            matches = False
    if not matches:
        write_document(path, content)
    if (path.stat().st_size != len(content) or
            sha256_file(path) != expected_sha256):
        raise OSError("source-container integrity check failed for %s" % path)
    return path


# --------------------------------------------------------------------------
# Category 1: Verfassungen (treaties)
# --------------------------------------------------------------------------

def crawl_treaty_components(session, doc_id, component_ids, previous=None,
                            dry_run=False):
    """Download constituent CELEX records for a treaty package.

    Component paths are recorded relative to VERF_DIR in the parent treaty's
    manifest entry.  Existing files are content-validated just like ordinary
    treaty documents.
    """
    component_dir = VERF_DIR / (doc_id + "_components")
    component_dir.mkdir(parents=True, exist_ok=True)
    old_entries = {
        entry.get("id"): entry for entry in (previous or [])
        if isinstance(entry, dict) and entry.get("id")
    }
    result_entries = []
    done = skipped = failed = 0
    for celex in component_ids:
        base = sanitize_celex(celex)
        existing = existing_file(component_dir, base)
        if existing is not None:
            entry = dict(old_entries.get(celex, {}))
            entry.update({
                "id": celex,
                "file": str(existing.relative_to(VERF_DIR)),
                "format": existing.suffix.lstrip("."),
                "status": "ok",
            })
            entry.setdefault(
                "source_url",
                EURLEX_URL.format(lang="DE", fmt="HTML", celex=celex))
            result_entries.append(entry)
            skipped += 1
            continue
        if dry_run:
            result_entries.append({
                "id": celex,
                "source_url": EURLEX_URL.format(
                    lang="DE", fmt="HTML", celex=celex),
                "file": None, "format": None, "downloaded_at": None,
                "status": "pending",
            })
            continue
        fetched = fetch_document(session, celex)
        if fetched is None:
            result_entries.append({
                "id": celex,
                "source_url": EURLEX_URL.format(
                    lang="DE", fmt="HTML", celex=celex),
                "file": None, "format": None, "downloaded_at": now_iso(),
                "status": "missing",
            })
            failed += 1
            continue
        content, ext, lang, url = fetched
        path = component_dir / (base + "." + ext)
        write_document(path, content)
        component_entry = {
            "id": celex, "source_url": url,
            "file": str(path.relative_to(VERF_DIR)), "format": ext,
            "language": lang, "downloaded_at": now_iso(), "status": "ok",
        }
        component_entry.update(getattr(fetched, "metadata", {}))
        result_entries.append(component_entry)
        done += 1
    log.info("verfassungen components %s: %d downloaded, %d already present, "
             "%d failed", doc_id, done, skipped, failed)
    return result_entries

def crawl_verfassungen(session, dry_run=False, limit=None):
    VERF_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(VERF_DIR)
    todo = TREATIES if limit is None else TREATIES[:limit]
    log.info("verfassungen: %d treaties in curated list, processing %d",
             len(TREATIES), len(todo))

    done = skipped = failed = 0
    for doc_id, title, candidates in todo:
        existing = existing_file(VERF_DIR, doc_id)
        if existing is not None:
            skipped += 1
            entry = dict(manifest.get(doc_id, {}))
            entry.update({
                    "id": doc_id, "title": title,
                    "file": existing.name, "format": existing.suffix.lstrip("."),
                    "status": "ok",
            })
            language = entry.get("language") or infer_document_language(existing)
            if language:
                entry["language"] = language
            source_url = entry.get("source_url")
            expected_lang = entry.get("language", "DE")
            expected_fmt = existing.suffix.lstrip(".").upper()
            if (not source_url or
                    "/legal-content/%s/" % expected_lang not in source_url or
                    "/TXT/%s/" % expected_fmt not in source_url):
                entry["source_url"] = EURLEX_URL.format(
                    lang=expected_lang, fmt=expected_fmt,
                    celex=candidates[0])
                entry["source_resolution"] = "reconstructed"
            entry.setdefault("downloaded_at", now_iso())
            if doc_id in TREATY_COMPONENTS:
                components = crawl_treaty_components(
                    session, doc_id, TREATY_COMPONENTS[doc_id],
                    previous=entry.get("components"), dry_run=dry_run)
                entry["components"] = components
                states = {item["status"] for item in components}
                entry["components_status"] = (
                    "ok" if states == {"ok"} else
                    "pending" if states <= {"ok", "pending"} else "partial")
                if entry["components_status"] == "partial":
                    entry["status"] = "partial"
            manifest[doc_id] = entry
            continue
        if dry_run:
            manifest.setdefault(doc_id, {
                "id": doc_id, "title": title,
                "source_url": EURLEX_URL.format(lang="DE", fmt="HTML", celex=candidates[0]),
                "file": None, "format": None,
                "downloaded_at": None, "status": "pending",
            })
            continue

        result = None
        for celex in candidates:
            result = fetch_document(session, celex)
            if result is not None:
                break
        if result is None:
            failed += 1
            log.error("verfassungen: no version found for %s (%s)", doc_id, title)
            manifest[doc_id] = {
                "id": doc_id, "title": title,
                "source_url": EURLEX_URL.format(lang="DE", fmt="HTML", celex=candidates[0]),
                "file": None, "format": None,
                "downloaded_at": now_iso(), "status": "missing",
            }
            continue

        content, ext, lang, url = result
        file_name = doc_id + "." + ext
        write_document(VERF_DIR / file_name, content)
        entry = {
            "id": doc_id, "title": title, "source_url": url,
            "file": file_name, "format": ext, "language": lang,
            "downloaded_at": now_iso(), "status": "ok",
        }
        entry.update(getattr(result, "metadata", {}))
        if doc_id in TREATY_COMPONENTS:
            components = crawl_treaty_components(
                session, doc_id, TREATY_COMPONENTS[doc_id], dry_run=dry_run)
            entry["components"] = components
            states = {item["status"] for item in components}
            entry["components_status"] = (
                "ok" if states == {"ok"} else
                "pending" if states <= {"ok", "pending"} else "partial")
            if entry["components_status"] == "partial":
                entry["status"] = "partial"
        manifest[doc_id] = entry
        done += 1
        log.info("verfassungen: downloaded %s (%s, %s, %d bytes)", doc_id, ext, lang, len(content))

    save_manifest(VERF_DIR, manifest)
    log.info("verfassungen finished: %d downloaded, %d already present, %d failed",
             done, skipped, failed)


# --------------------------------------------------------------------------
# Category 2: Stammgesetze (basic acts) -- SPARQL enumeration
# --------------------------------------------------------------------------

SPARQL_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex (SAMPLE(?t) AS ?type) (SAMPLE(?d) AS ?date)
       (SAMPLE(?f) AS ?force) (SAMPLE(?ti) AS ?title)
WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  ?work cdm:work_has_resource-type ?t .
  FILTER(?t IN ({types}))
  {cursor_filter}
  OPTIONAL {{ ?work cdm:work_date_document ?d }}
  OPTIONAL {{ ?work cdm:resource_legal_in-force ?f }}
  OPTIONAL {{ ?exp cdm:expression_belongs_to_work ?work ;
                   cdm:expression_uses_language
                     <http://publications.europa.eu/resource/authority/language/DEU> ;
                   cdm:expression_title ?ti }}
}}
GROUP BY ?celex
ORDER BY ?celex
LIMIT {limit}
"""


MERGER_LANGUAGE_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex ?langKey ?lang (MIN(STR(?ti)) AS ?title)
WHERE {{
  ?work cdm:resource_legal_id_celex ?celex ;
        cdm:work_has_resource-type
          <http://publications.europa.eu/resource/authority/resource-type/DEC> ;
        cdm:resource_legal_in-force ?force .
  FILTER(STR(?force) = "1" || LCASE(STR(?force)) = "true")
  FILTER(REGEX(STR(?celex), "^3[0-9]{{4}}M[0-9]+$"))
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language ?lang .
    OPTIONAL {{ ?exp cdm:expression_title ?ti }}
  }}
  BIND(IF(BOUND(?lang), STR(?lang), "") AS ?langKey)
  {cursor_filter}
}}
GROUP BY ?celex ?langKey ?lang
ORDER BY ?celex ?langKey
LIMIT {limit}
"""

BASE_INDEX_ENTRY_FIELDS = ("celex", "type", "date", "in_force", "title")
MERGER_LANGUAGE_CACHE_SCHEMA = 1


def canonical_json_sha256(value):
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def base_index_projection(index):
    """Return the immutable source-index fields used by cache fingerprints."""
    if not isinstance(index, dict) or not isinstance(index.get("entries"), list):
        raise ValueError("CELEX source index is not a valid object")
    entries = []
    seen = set()
    for raw in index["entries"]:
        if not isinstance(raw, dict):
            raise ValueError("CELEX source index contains a non-object entry")
        entry = {field: raw.get(field) for field in BASE_INDEX_ENTRY_FIELDS}
        celex = entry["celex"]
        if not isinstance(celex, str) or not celex or celex in seen:
            raise ValueError("CELEX source index has a missing/duplicate ID")
        seen.add(celex)
        entries.append(entry)
    if index.get("total") != len(entries):
        raise ValueError("CELEX source index total does not match entries")
    in_force = sum(1 for entry in entries if entry.get("in_force") is True)
    if index.get("in_force") != in_force:
        raise ValueError("CELEX source index in-force count is inconsistent")
    return {
        "generated_at": index.get("generated_at"),
        "source": index.get("source"),
        "resource_types": index.get("resource_types"),
        "total": index.get("total"),
        "in_force": index.get("in_force"),
        "entries": entries,
    }


def merger_ids_from_base_index(index):
    projection = base_index_projection(index)
    ids = [entry["celex"] for entry in projection["entries"]
           if entry.get("type") == "DEC" and
           entry.get("in_force") is True and
           merger_case_number(entry.get("celex")) is not None]
    return sorted(ids)


def merger_id_set_sha256(ids):
    return hashlib.sha256(("\n".join(ids) + "\n").encode("utf-8")).hexdigest()


def merger_language_query(cursor=None):
    if cursor is None:
        cursor_filter = ""
    else:
        celex, lang_key = cursor
        if (merger_case_number(celex) is None or
                not isinstance(lang_key, str) or '"' in lang_key or
                "\\" in lang_key):
            raise ValueError("unsafe merger-language keyset cursor")
        cursor_filter = (
            'FILTER(STR(?celex) > "{celex}" || '
            '(STR(?celex) = "{celex}" && ?langKey > "{lang}"))'
        ).format(celex=celex, lang=lang_key)
    return MERGER_LANGUAGE_QUERY.format(
        cursor_filter=cursor_filter, limit=MERGER_LANGUAGE_PAGE_SIZE)


def merger_language_sparql_page(session, cursor):
    """Fetch one exact (CELEX, language-key) keyset page."""
    query = merger_language_query(cursor)
    for attempt in range(MAX_RETRIES):
        throttle_request("cellar", CELLAR_THROTTLE_SECONDS)
        try:
            resp = session.post(
                SPARQL_ENDPOINT,
                data={"query": query,
                      "format": "application/sparql-results+json"},
                timeout=300)
            if resp.status_code == 200:
                return resp.json()["results"]["bindings"]
            log.warning("merger-language SPARQL HTTP %d (attempt %d)",
                        resp.status_code, attempt + 1)
        except (requests.RequestException, ValueError, KeyError) as exc:
            log.warning("merger-language SPARQL error (attempt %d): %s",
                        attempt + 1, exc)
        if attempt + 1 < MAX_RETRIES:
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(
        "merger-language SPARQL failed at cursor %r" % (cursor,))


def normalize_merger_language_binding(row, expected_ids):
    """Validate one SPARQL row, retaining an explicit unbound-language row."""
    try:
        celex = row["celex"]["value"]
        lang_key = row["langKey"]["value"]
    except (KeyError, TypeError):
        raise ValueError("merger-language row lacks CELEX/langKey")
    if celex not in expected_ids:
        raise ValueError("SPARQL returned out-of-index merger ID %r" % celex)
    if not isinstance(lang_key, str):
        raise ValueError("merger-language key is not text")
    language_uri = row.get("lang", {}).get("value")
    title = row.get("title", {}).get("value")
    if title is not None and not isinstance(title, str):
        raise ValueError("merger-language title is not text")
    if lang_key == "":
        if language_uri not in (None, ""):
            raise ValueError("unbound merger-language row has a language URI")
        language_uri = None
        language_code = None
    else:
        if language_uri != lang_key:
            raise ValueError("merger-language key/URI mismatch")
        language_code = language_code_from_uri(language_uri)
        if language_code not in EU_LANGUAGES:
            raise ValueError(
                "unknown merger expression language URI %r" % language_uri)
    return {
        "kind": "row",
        "celex": celex,
        "lang_key": lang_key,
        "language_uri": language_uri,
        "language_code": language_code,
        "title": title.strip() if isinstance(title, str) and title.strip()
        else None,
    }


def validate_partial_merger_row(record, expected_ids):
    if not isinstance(record, dict) or record.get("kind") != "row":
        raise ValueError("invalid merger-language partial row")
    celex = record.get("celex")
    lang_key = record.get("lang_key")
    language_uri = record.get("language_uri")
    language_code = record.get("language_code")
    title = record.get("title")
    if celex not in expected_ids or not isinstance(lang_key, str):
        raise ValueError("partial row has invalid CELEX/language key")
    if title is not None and not isinstance(title, str):
        raise ValueError("partial row title is not text")
    if lang_key == "":
        if language_uri is not None or language_code is not None:
            raise ValueError("partial unbound row contains language metadata")
    else:
        if (language_uri != lang_key or
                language_code_from_uri(language_uri) != language_code or
                language_code not in EU_LANGUAGES):
            raise ValueError("partial row language metadata is invalid")
    return (celex, lang_key)


def merger_language_partial_header(index, merger_ids):
    projection = base_index_projection(index)
    return {
        "kind": "header",
        "schema_version": MERGER_LANGUAGE_CACHE_SCHEMA,
        "source_endpoint": SPARQL_ENDPOINT,
        "source_index_fingerprint": canonical_json_sha256(projection),
        "source_index_generated_at": projection.get("generated_at"),
        "source_index_total": projection["total"],
        "source_index_in_force": projection["in_force"],
        "merger_count": len(merger_ids),
        "merger_ids_sha256": merger_id_set_sha256(merger_ids),
        "query_sha256": hashlib.sha256(
            MERGER_LANGUAGE_QUERY.encode("utf-8")).hexdigest(),
        "page_size": MERGER_LANGUAGE_PAGE_SIZE,
    }


def load_or_initialize_merger_language_partial(index, merger_ids):
    """Load a strictly matching JSONL checkpoint or create/fsync its header."""
    expected_ids = set(merger_ids)
    expected_header = merger_language_partial_header(index, merger_ids)
    rows = []
    if MERGER_LANGUAGE_PARTIAL_FILE.exists():
        with open(str(MERGER_LANGUAGE_PARTIAL_FILE), "r", encoding="utf-8") as fh:
            lines = list(fh)
        if not lines:
            raise ValueError("empty merger-language partial checkpoint")
        try:
            header = json.loads(lines[0])
        except ValueError:
            raise ValueError("malformed merger-language partial header")
        if header != expected_header:
            raise ValueError(
                "merger-language partial does not match the source index")
        prior_pair = None
        seen = set()
        for line_no, line in enumerate(lines[1:], 2):
            try:
                record = json.loads(line)
                pair = validate_partial_merger_row(record, expected_ids)
            except (ValueError, TypeError) as exc:
                raise ValueError("invalid merger-language partial line %d: %s" %
                                 (line_no, exc))
            if pair in seen or (prior_pair is not None and pair <= prior_pair):
                raise ValueError(
                    "duplicate/out-of-order merger-language partial row")
            seen.add(pair)
            prior_pair = pair
            rows.append(record)
        return expected_header, rows
    MERGER_LANGUAGE_PARTIAL_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_merger_language_partial(expected_header, [])
    return expected_header, rows


def write_merger_language_partial(header, rows):
    """Atomically replace the fsynced JSONL checkpoint after a full page."""
    tmp = MERGER_LANGUAGE_PARTIAL_FILE.with_name(
        MERGER_LANGUAGE_PARTIAL_FILE.name + ".page.tmp")
    with open(str(tmp), "w", encoding="utf-8") as fh:
        fh.write(json.dumps(
            header, ensure_ascii=False, sort_keys=True) + "\n")
        for row in rows:
            fh.write(json.dumps(
                row, ensure_ascii=False, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(str(tmp), str(MERGER_LANGUAGE_PARTIAL_FILE))
    fsync_parent_directory(MERGER_LANGUAGE_PARTIAL_FILE)


def fsync_parent_directory(path):
    try:
        directory_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        pass


def build_merger_language_cache(index, header, rows, merger_ids):
    expected_ids = set(merger_ids)
    covered_ids = {row["celex"] for row in rows}
    if covered_ids != expected_ids:
        missing = sorted(expected_ids - covered_ids)
        extra = sorted(covered_ids - expected_ids)
        raise RuntimeError(
            "merger-language coverage mismatch: missing=%r extra=%r" %
            (missing[:10], extra[:10]))
    grouped = {celex: {"codes": [], "titles": {}, "unbound": False,
                       "row_count": 0}
               for celex in merger_ids}
    seen_pairs = set()
    prior_pair = None
    for row in rows:
        pair = validate_partial_merger_row(row, expected_ids)
        if pair in seen_pairs or (prior_pair is not None and pair <= prior_pair):
            raise RuntimeError("duplicate/out-of-order final merger-language row")
        seen_pairs.add(pair)
        prior_pair = pair
        group = grouped[row["celex"]]
        group["row_count"] += 1
        code = row["language_code"]
        if code is None:
            group["unbound"] = True
        else:
            group["codes"].append(code)
            if row.get("title"):
                group["titles"][code] = row["title"]
    entries = []
    for celex in merger_ids:
        group = grouped[celex]
        codes = input_order_language_codes(group["codes"])
        if len(codes) != len(group["codes"]):
            raise RuntimeError("duplicate merger language for %s" % celex)
        if ((group["unbound"] and
             (codes or group["row_count"] != 1)) or
                (not group["unbound"] and
                 (not codes or group["row_count"] != len(codes)))):
            raise RuntimeError(
                "invalid bound/unbound merger-language coverage for %s" %
                celex)
        entries.append({
            "celex": celex,
            "language_codes": codes,
            "titles_by_authority_code": {
                code: group["titles"][code] for code in codes
                if code in group["titles"]},
            "unbound_language_row": bool(group["unbound"]),
            "row_count": group["row_count"],
        })
    return {
        "schema_version": MERGER_LANGUAGE_CACHE_SCHEMA,
        "complete": True,
        "generated_at": now_iso(),
        "source_endpoint": header["source_endpoint"],
        "source_query_sha256": header["query_sha256"],
        "source_index_fingerprint": header["source_index_fingerprint"],
        "source_index_generated_at": header["source_index_generated_at"],
        "source_index_total": header["source_index_total"],
        "source_index_in_force": header["source_index_in_force"],
        "merger_count": len(merger_ids),
        "merger_ids_sha256": header["merger_ids_sha256"],
        "row_count": len(rows),
        "pagination": {
            "method": "strict_pair_keyset",
            "keys": ["celex", "langKey"],
            "page_size": header["page_size"],
            "explicit_unbound_rows": True,
        },
        "language_authority_uri_prefix": LANGUAGE_URI_PREFIX,
        "entries": entries,
    }


def atomic_write_json(path, payload):
    """Write and fsync a JSON object before one same-directory replace."""
    tmp = path.with_name(path.name + ".atomic.tmp")
    with open(str(tmp), "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(str(tmp), str(path))
    fsync_parent_directory(path)


def validate_trusted_merger_language_cache(index):
    """Return immutable worker lookup only for a complete exact cache."""
    try:
        cache = index.get("merger_language_cache")
        expected_query_sha = hashlib.sha256(
            MERGER_LANGUAGE_QUERY.encode("utf-8")).hexdigest()
        expected_pagination = {
            "method": "strict_pair_keyset",
            "keys": ["celex", "langKey"],
            "page_size": MERGER_LANGUAGE_PAGE_SIZE,
            "explicit_unbound_rows": True,
        }
        generated_at = cache.get("generated_at") if isinstance(cache, dict) else None
        try:
            datetime.datetime.strptime(generated_at, "%Y-%m-%dT%H:%M:%SZ")
        except (TypeError, ValueError):
            return None
        if (not isinstance(cache, dict) or cache.get("complete") is not True or
                cache.get("schema_version") != MERGER_LANGUAGE_CACHE_SCHEMA):
            return None
        projection = base_index_projection(index)
        merger_ids = merger_ids_from_base_index(index)
        if (cache.get("source_endpoint") != SPARQL_ENDPOINT or
                cache.get("source_query_sha256") != expected_query_sha or
                cache.get("pagination") != expected_pagination or
                cache.get("language_authority_uri_prefix") !=
                LANGUAGE_URI_PREFIX or
                cache.get("source_index_fingerprint") !=
                canonical_json_sha256(projection) or
                cache.get("source_index_generated_at") !=
                projection.get("generated_at") or
                cache.get("source_index_total") != projection["total"] or
                cache.get("source_index_in_force") != projection["in_force"] or
                cache.get("merger_count") != len(merger_ids) or
                cache.get("merger_ids_sha256") !=
                merger_id_set_sha256(merger_ids) or
                not isinstance(cache.get("entries"), list) or
                len(cache["entries"]) != len(merger_ids)):
            return None
        lookup = {}
        row_total = 0
        for expected_celex, entry in zip(merger_ids, cache["entries"]):
            if not isinstance(entry, dict) or entry.get("celex") != expected_celex:
                return None
            codes = entry.get("language_codes")
            titles = entry.get("titles_by_authority_code")
            unbound = entry.get("unbound_language_row")
            row_count = entry.get("row_count")
            if (not isinstance(codes, list) or
                    codes != input_order_language_codes(codes) or
                    any(code not in EU_LANGUAGES for code in codes) or
                    not isinstance(titles, dict) or
                    any(code not in codes or not isinstance(title, str)
                        for code, title in titles.items()) or
                    not isinstance(unbound, bool) or
                    (unbound and (bool(codes) or row_count != 1)) or
                    (not unbound and
                     (not codes or row_count != len(codes)))):
                return None
            row_total += row_count
            lookup[expected_celex] = {
                "codes": tuple(codes), "titles": dict(titles),
                "unbound_language_row": unbound,
            }
        if cache.get("row_count") != row_total:
            return None
        return lookup
    except (KeyError, TypeError, ValueError):
        return None


def refresh_merger_language_cache(session):
    """Opt-in resumable M-language enumeration and atomic index enrichment."""
    if not INDEX_FILE.exists():
        raise RuntimeError(
            "CELEX index is missing; run --refresh-index before this opt-in step")
    with open(str(INDEX_FILE), "r", encoding="utf-8") as fh:
        index = json.load(fh)
    projection = base_index_projection(index)
    merger_ids = merger_ids_from_base_index(index)
    if not merger_ids:
        raise RuntimeError("source index contains no in-force DEC M records")
    header, rows = load_or_initialize_merger_language_partial(
        index, merger_ids)
    expected_ids = set(merger_ids)
    seen_pairs = {(row["celex"], row["lang_key"]) for row in rows}
    cursor = (rows[-1]["celex"], rows[-1]["lang_key"]) if rows else None
    page_no = len(rows) // MERGER_LANGUAGE_PAGE_SIZE
    while True:
        raw_rows = merger_language_sparql_page(session, cursor)
        page_no += 1
        normalized = []
        page_cursor = cursor
        for raw in raw_rows:
            row = normalize_merger_language_binding(raw, expected_ids)
            pair = (row["celex"], row["lang_key"])
            if pair in seen_pairs or (page_cursor is not None and
                                      pair <= page_cursor):
                raise RuntimeError(
                    "duplicate/non-advancing merger-language pair %r" %
                    (pair,))
            seen_pairs.add(pair)
            page_cursor = pair
            normalized.append(row)
        candidate_rows = rows + normalized
        if normalized:
            write_merger_language_partial(header, candidate_rows)
        rows = candidate_rows
        log.info("merger-language page %d: +%d rows (total %d, cursor %r)",
                 page_no, len(raw_rows), len(rows), cursor)
        if len(raw_rows) < MERGER_LANGUAGE_PAGE_SIZE:
            break
        if not normalized:
            raise RuntimeError("full merger-language page did not advance")
        cursor = page_cursor
    cache = build_merger_language_cache(index, header, rows, merger_ids)
    enriched = dict(index)
    enriched["merger_language_cache"] = cache
    if (base_index_projection(enriched) != projection or
            enriched.get("total") != index.get("total") or
            enriched.get("in_force") != index.get("in_force")):
        raise RuntimeError("merger enrichment changed base-index invariants")
    atomic_write_json(INDEX_FILE, enriched)
    if MERGER_LANGUAGE_PARTIAL_FILE.exists():
        MERGER_LANGUAGE_PARTIAL_FILE.unlink()
        fsync_parent_directory(MERGER_LANGUAGE_PARTIAL_FILE)
    log.info("atomic merger-language cache written: %d works, %d rows -> %s",
             len(merger_ids), len(rows), INDEX_FILE)
    return cache


def sparql_page(session, cursor):
    """Fetch one keyset-paginated page of CELEX numbers from CELLAR.

    Virtuoso rejects ORDER BY with OFFSET+LIMIT > 10000, so we paginate by
    CELEX value instead: each page asks for celex > <last seen value>.
    """
    types = ", ".join(
        "<http://publications.europa.eu/resource/authority/resource-type/%s>" % t
        for t in RESOURCE_TYPES)
    cursor_filter = 'FILTER(STR(?celex) > "%s")' % cursor if cursor else ""
    query = SPARQL_QUERY.format(types=types, cursor_filter=cursor_filter,
                                limit=SPARQL_PAGE_SIZE)
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(
                SPARQL_ENDPOINT,
                data={"query": query, "format": "application/sparql-results+json"},
                timeout=300)
            if resp.status_code == 200:
                return resp.json()["results"]["bindings"]
            log.warning("SPARQL HTTP %d (attempt %d): %s",
                        resp.status_code, attempt + 1, resp.text[:200])
        except (requests.RequestException, ValueError) as exc:
            log.warning("SPARQL error (attempt %d): %s", attempt + 1, exc)
        time.sleep(5 * (attempt + 1))
    raise RuntimeError("SPARQL enumeration failed at cursor %r" % cursor)


def enumerate_celex(session, refresh=False):
    """Enumerate all REG/DIR/DEC works (in force and historic) via CELLAR.

    Result is cached in celex_index.json. Returns a list of dicts:
    {celex, type, date, in_force, title}.
    """
    global trusted_merger_language_cache
    if INDEX_FILE.exists() and not refresh:
        with open(str(INDEX_FILE), "r", encoding="utf-8") as fh:
            index = json.load(fh)
        trusted = validate_trusted_merger_language_cache(index)
        trusted_merger_language_cache = trusted or {}
        if trusted is not None:
            log.info("trusted complete merger-language cache: %d works",
                     len(trusted))
        elif index.get("merger_language_cache") is not None:
            log.warning("ignoring incomplete/stale merger-language cache; "
                        "workers will use per-case discovery")
        log.info("loaded cached CELEX index: %d entries (created %s); "
                 "use --refresh-index to re-enumerate",
                 len(index["entries"]), index.get("generated_at"))
        return index["entries"]

    trusted_merger_language_cache = {}
    STAMM_DIR.mkdir(parents=True, exist_ok=True)
    log.info("enumerating REG/DIR/DEC works via CELLAR SPARQL "
             "(page size %d) ...", SPARQL_PAGE_SIZE)
    entries = {}
    if INDEX_PARTIAL_FILE.exists():
        with open(str(INDEX_PARTIAL_FILE), "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                try:
                    entry = json.loads(line)
                    entries[entry["celex"]] = entry
                except (ValueError, KeyError, TypeError):
                    log.warning("ignoring malformed partial-index line %d in %s",
                                line_no, INDEX_PARTIAL_FILE)
        log.info("resuming partial CELEX enumeration with %d entries", len(entries))
    cursor = max(entries) if entries else ""
    page_no = len(entries) // SPARQL_PAGE_SIZE
    while True:
        rows = sparql_page(session, cursor)
        page_no += 1
        page_entries = []
        for row in rows:
            celex = row["celex"]["value"]
            force_raw = row.get("force", {}).get("value", "")
            entry = {
                "celex": celex,
                "type": row["type"]["value"].rsplit("/", 1)[-1],
                "date": row.get("date", {}).get("value"),
                "in_force": force_raw in ("1", "true"),
                "title": row.get("title", {}).get("value"),
            }
            entries[celex] = entry
            page_entries.append(entry)
        if page_entries:
            with open(str(INDEX_PARTIAL_FILE), "a", encoding="utf-8") as fh:
                for entry in page_entries:
                    fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        log.info("SPARQL page %d: +%d rows (total %d, cursor %r)",
                 page_no, len(rows), len(entries), cursor)
        if len(rows) < SPARQL_PAGE_SIZE:
            break
        cursor = rows[-1]["celex"]["value"]

    entry_list = [entries[key] for key in sorted(entries)]
    n_force = sum(1 for entry in entry_list if entry["in_force"])
    index = {
        "generated_at": now_iso(),
        "source": SPARQL_ENDPOINT,
        "resource_types": list(RESOURCE_TYPES),
        "total": len(entry_list),
        "in_force": n_force,
        "entries": entry_list,
    }
    tmp = INDEX_FILE.with_suffix(".json.tmp")
    with open(str(tmp), "w", encoding="utf-8") as fh:
        json.dump(index, fh, ensure_ascii=False)
    tmp.replace(INDEX_FILE)
    if INDEX_PARTIAL_FILE.exists():
        INDEX_PARTIAL_FILE.unlink()
    log.info("CELEX index written: %d works total, %d in force -> %s",
             len(entry_list), n_force, INDEX_FILE)
    return entry_list


def iter_bounded_futures(executor, entries, fetch_function, max_in_flight):
    """Yield every target future once, with at most max_in_flight submitted.

    A completed slow/failed target cannot block replenishment by other workers.
    The caller owns the executor and remains responsible for interpreting each
    future's result and checkpointing it.
    """
    iterator = iter(entries)
    in_flight = {}

    def submit_one():
        try:
            entry = next(iterator)
        except StopIteration:
            return False
        future = executor.submit(fetch_function, entry)
        in_flight[future] = entry
        return True

    try:
        for _ in range(max_in_flight):
            if not submit_one():
                break
        while in_flight:
            completed, _ = concurrent.futures.wait(
                tuple(in_flight),
                return_when=concurrent.futures.FIRST_COMPLETED)
            for future in completed:
                entry = in_flight.pop(future)
                yield entry, future
                submit_one()
    finally:
        for future in in_flight:
            future.cancel()


def order_pending_entries(pending_fresh_regular, pending_fresh_merger,
                          pending_technical_merger,
                          pending_retry_regular, pending_retry_merger,
                          limit=None):
    """Return one deterministic, exact-once queue without mutating inputs.

    Fresh ordinary acts retain authoritative source-index order.  Fresh merger
    decisions use descending (date, numeric M-case, CELEX) order so accessible
    recent decisions are not starved behind a persistent block of hard older
    cases.  Fresh merger IDs with durable technical events follow untouched
    fresh merger work in the same stable newest order.  Historical retries
    retain source-index order and remain last.
    """
    source_partitions = (
        list(pending_fresh_regular), list(pending_fresh_merger),
        list(pending_technical_merger), list(pending_retry_regular),
        list(pending_retry_merger))
    source_pending = [entry for partition in source_partitions
                      for entry in partition]
    ids = [entry.get("celex") if isinstance(entry, dict) else None
           for entry in source_pending]
    if any(not isinstance(celex, str) or not celex for celex in ids):
        raise ValueError("pending queue contains an invalid CELEX entry")
    if len(ids) != len(set(ids)):
        raise ValueError("pending queue contains duplicate CELEX entries")
    for entry in source_partitions[1] + source_partitions[2]:
        date = entry.get("date") or ""
        if (merger_case_number(entry["celex"]) is None or
                not isinstance(date, str) or
                (date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date) is None)):
            raise ValueError("fresh merger queue contains an invalid identity")
    def merger_newest_key(entry):
        return (
            entry.get("date") or "",
            int(merger_case_number(entry["celex"])),
            entry["celex"])

    fresh_merger_newest_first = sorted(
        source_partitions[1], key=merger_newest_key, reverse=True)
    technical_merger_newest_first = sorted(
        source_partitions[2], key=merger_newest_key, reverse=True)
    partitions = (
        source_partitions[0], fresh_merger_newest_first,
        technical_merger_newest_first,
        source_partitions[3], source_partitions[4])
    pending = [entry for partition in partitions for entry in partition]
    if limit is not None:
        pending = pending[:limit]
    return pending


def durable_waf_deferred_entry_matches(celex, entry):
    """Strictly validate a durable typed WAF deferral for opt-in skipping."""
    if not isinstance(entry, dict):
        return False
    url = entry.get("technical_defer_url")
    reason = entry.get("technical_defer_reason")
    if (entry.get("status") != "worker_error" or
            entry.get("source_resolution") != "technical_waf_deferred" or
            entry.get("file") is not None or entry.get("format") is not None or
            entry.get("technical_defer_http_status") != 202 or
            entry.get("technical_defer_waf_action") != "challenge" or
            celex_from_official_technical_url(url) != celex or
            not isinstance(reason, str) or
            entry.get("error") != "TechnicalWAFDefer: " + reason):
        return False
    return reason in (
        "HTTP 202 WAF challenge via eurlex " + url,
        "HTTP 202 WAF challenge via cellar " + url,
    )


def crawl_stammgesetze(session, dry_run=False, limit=None,
                       refresh_index=False, scope="in-force",
                       workers=DEFAULT_WORKERS, defer_on_waf=False):
    STAMM_DIR.mkdir(parents=True, exist_ok=True)
    entries = enumerate_celex(session, refresh=refresh_index)
    if scope == "in-force":
        targets = [entry for entry in entries if entry["in_force"]]
    else:
        targets = list(entries)
    log.info("stammgesetze: %d works enumerated, %d selected for download "
             "(scope=%s)", len(entries), len(targets), scope)
    if dry_run:
        log.info("dry run: index written, skipping downloads")
        return

    manifest = load_manifest(STAMM_DIR)
    technical_entries = initialize_technical_retry_journal(targets, manifest)
    technical_ids = set(technical_entries)
    global defer_on_waf_enabled
    defer_on_waf_enabled = bool(defer_on_waf)
    done = skipped = failed = deferred_skipped = 0
    processed_since_flush = 0
    pending_fresh_regular = []
    pending_fresh_merger = []
    pending_technical_merger = []
    pending_retry_regular = []
    pending_retry_merger = []
    resolved_technical_ids = set()
    successful_technical_ids_pending_checkpoint = set()
    preserved_alternates_by_celex = {}
    retained_derived_primary_by_celex = {}
    for entry in targets:
        celex = entry["celex"]
        base = sanitize_celex(celex)
        prior_entry = manifest.get(celex)
        existing = existing_file(STAMM_DIR, base)
        if (defer_on_waf and celex in technical_ids and
                durable_waf_deferred_entry_matches(celex, prior_entry) and
                existing is None):
            deferred_skipped += 1
            continue
        preserved_alternates = list(
            (prior_entry or {}).get("alternate_files", []))
        retained_derived_primary = None
        prior_file_name = str((prior_entry or {}).get("file") or "")
        if prior_file_name and Path(prior_file_name).name == prior_file_name:
            prior_primary_path = STAMM_DIR / prior_file_name
            prior_primary_alternate = derived_alternate_from_entry(
                prior_entry, prior_primary_path)
            if prior_primary_alternate is not None:
                preserved_alternates.append(prior_primary_alternate)
                retained_derived_primary = dict(prior_entry)
                retained_derived_primary.update({
                    key: value for key, value in prior_primary_alternate.items()
                    if key != "role"})
                retained_derived_primary["status"] = "ok"
        merger_enrichment = None
        if (existing is not None and prior_entry and
                prior_entry.get("preferred_repair_pending")):
            log.warning("retrying preferred-format repair for %s while "
                        "retaining verified derivative", celex)
            existing = None
        if (celex in EURLEX_OJ_OVERRIDES and existing is not None and
                not override_record_matches(celex, prior_entry, existing)):
            log.warning("official OJ override identity mismatch for %s; "
                        "refetching", celex)
            existing = None
        if (existing is not None and existing.suffix == ".html" and
                prior_entry and prior_entry.get("language")):
            explicit_existing_language = infer_document_language(existing)
            if (explicit_existing_language and
                    explicit_existing_language != prior_entry.get("language")):
                log.warning("explicit payload/manifest language mismatch for %s "
                            "(%s != %s); refetching", celex,
                            explicit_existing_language,
                            prior_entry.get("language"))
                existing = None
        if (existing is not None and merger_case_number(celex) is not None and
                not merger_record_matches(celex, prior_entry, existing)):
            source_url = str((prior_entry or {}).get("source_url", ""))
            source_resolution = (prior_entry or {}).get("source_resolution")
            if (not prior_entry or prior_entry.get("status") != "ok" or
                    not official_merger_source_route_matches(
                        celex, prior_entry, existing)):
                merger_enrichment = None
            else:
                try:
                    merger_enrichment = merger_payload_identity_metadata(
                        existing.read_bytes(), existing.suffix.lstrip("."),
                        celex, source_resolution, source_url)
                except OSError:
                    merger_enrichment = None
            if merger_enrichment is None:
                log.warning("M-case identity/provenance failed for %s; "
                            "refetching", celex)
                existing = None
            else:
                log.info("validated legacy M-case payload for provenance "
                         "enrichment: %s", celex)
        promote_pending = False
        if (prior_entry and prior_entry.get("source_resolution") ==
                DERIVED_PRINT_RESOLUTION):
            if (prior_entry.get("status") in ("ok", "pending_write") and
                    existing is not None and
                    derived_record_files_match(prior_entry, existing)):
                promote_pending = prior_entry.get("status") == "pending_write"
            else:
                log.warning("derived file for %s failed rich metadata or exact "
                            "file/container identity; refetching", celex)
                existing = None
        elif prior_entry and prior_entry.get("status") == "pending_write":
            log.warning("incomplete pending-write provenance for %s; refetching",
                        celex)
            existing = None
        elif (existing is not None and existing.suffix == ".pdf" and
              prior_entry and prior_entry.get("status") == "ok" and
              not prior_entry.get("source_resolution") and
              "/TXT/PDF/" not in str(prior_entry.get("source_url", ""))):
            log.warning("anomalous PDF for %s has no PDF-source provenance; "
                        "refetching", celex)
            existing = None
        elif (existing is not None and existing.suffix == ".pdf" and
              (prior_entry is None or prior_entry.get("status") != "ok")):
            log.warning("uncommitted PDF for %s lacks pending provenance; "
                        "refetching", celex)
            existing = None
        if existing is not None:
            skipped += 1
            manifest_entry = dict(prior_entry or {})
            manifest_entry.update({
                "id": celex, "title": entry.get("title"),
                "type": entry.get("type"), "date": entry.get("date"),
                "in_force": entry.get("in_force"),
                "file": existing.name,
                "format": existing.suffix.lstrip("."), "status": "ok",
            })
            manifest_entry.setdefault(
                "source_url",
                CELLAR_CELEX_URL.format(celex=quote(celex, safe="")))
            if not manifest_entry.get("language"):
                language = infer_document_language(existing)
                if language:
                    manifest_entry["language"] = language
            stable_celex_url = CELLAR_CELEX_URL.format(
                celex=quote(celex, safe=""))
            source_url = str(manifest_entry.get("source_url", ""))
            expected_route = "/TXT/%s/" % existing.suffix.lstrip(".").upper()
            if (not manifest_entry.get("source_resolution") and
                    expected_route in source_url):
                manifest_entry["source_resolution"] = "eurlex_direct"
            elif (not manifest_entry.get("source_resolution") and
                  "/resource/cellar/" in source_url and
                  source_url.endswith("/DOC_1")):
                manifest_entry["source_resolution"] = (
                    "cellar_content_negotiation")
            if (prior_entry is None or
                    (manifest_entry.get("source_url") == stable_celex_url and
                     not manifest_entry.get("source_resolution"))):
                manifest_entry["source_resolution"] = "recovered"
            if not manifest_entry.get("downloaded_at"):
                manifest_entry["downloaded_at"] = now_iso()
            if merger_enrichment is not None:
                merger_enrichment["merger_source_resolution"] = (
                    manifest_entry.get("source_resolution"))
                merger_enrichment["merger_source_url"] = (
                    manifest_entry.get("source_url"))
                manifest_entry.update(merger_enrichment)
            apply_manifest_metadata_correction(celex, manifest_entry, entry)
            apply_identifier_metadata_warning(celex, manifest_entry)
            merge_alternate_files(manifest_entry, preserved_alternates)
            apply_provenance_incomplete_files(celex, manifest_entry)
            manifest[celex] = manifest_entry
            if celex in technical_ids:
                resolved_technical_ids.add(celex)
            processed_since_flush += 1
            if promote_pending:
                save_manifest(STAMM_DIR, manifest)
                processed_since_flush = 0
                log.info("promoted verified pending derived file %s", celex)
            continue
        preserved_alternates_by_celex[celex] = preserved_alternates
        if retained_derived_primary is not None:
            retained_derived_primary_by_celex[celex] = (
                retained_derived_primary)
        is_retry = ((prior_entry or {}).get("status") in
                    ("missing", "worker_error"))
        is_merger = merger_case_number(celex) is not None
        if is_retry and is_merger:
            pending_retry_merger.append(entry)
        elif is_retry:
            pending_retry_regular.append(entry)
        elif is_merger and celex in technical_ids:
            pending_technical_merger.append(entry)
        elif is_merger:
            pending_fresh_merger.append(entry)
        else:
            pending_fresh_regular.append(entry)

    # A repeatedly slow historical miss or hard old merger record must not
    # occupy the first bounded worker slots on every restart.  The pure helper
    # preserves five disjoint priority partitions and applies --limit only
    # after constructing the complete stable ordering.
    pending = order_pending_entries(
        pending_fresh_regular, pending_fresh_merger, pending_technical_merger,
        pending_retry_regular, pending_retry_merger, limit=limit)

    if processed_since_flush:
        save_manifest(STAMM_DIR, manifest)
        processed_since_flush = 0
    remove_technical_retries(resolved_technical_ids)

    worker_sessions = threading.local()

    def fetch_entry(entry):
        if not hasattr(worker_sessions, "session"):
            worker_sessions.session = make_session()
        return entry, fetch_basic_act(worker_sessions.session, entry["celex"])

    if defer_on_waf:
        log.info("stammgesetze: %d already present, %d durable WAF deferrals, "
                 "%d queued with %d workers", skipped, deferred_skipped,
                 len(pending), workers)
    else:
        log.info("stammgesetze: %d already present, %d queued with %d workers",
                 skipped, len(pending), workers)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    interrupted = False
    try:
        for scheduled_entry, future in iter_bounded_futures(
                executor, pending, fetch_entry, workers):
            worker_error = None
            technical_defer = None
            try:
                returned_entry, result = future.result()
                if returned_entry["celex"] != scheduled_entry["celex"]:
                    raise ValueError("worker returned a different CELEX target")
                entry = scheduled_entry
            except Exception as exc:
                entry = scheduled_entry
                result = None
                if isinstance(exc, TechnicalWAFDefer):
                    technical_defer = exc
                worker_error = "%s: %s" % (type(exc).__name__, exc)
                log.error("worker failed for %s: %s",
                          entry["celex"], worker_error)
            celex = entry["celex"]
            base = sanitize_celex(celex)
            preserved_alternates = preserved_alternates_by_celex.get(
                celex, [])
            retained_derived_primary = retained_derived_primary_by_celex.get(
                celex)
            missing_metadata = {}
            if isinstance(result, MissingResult):
                missing_metadata = result.metadata
                result = None

            if result is None:
                failed += 1
                status = "worker_error" if worker_error else "missing"
                if worker_error:
                    log.error("stammgesetze: worker error for %s", celex)
                else:
                    log.error("stammgesetze: no version found for %s", celex)
                manifest_entry = {
                    "id": celex,
                    "title": (entry.get("title") or
                              missing_metadata.get("source_expression_title")),
                    "type": entry.get("type"), "date": entry.get("date"),
                    "in_force": entry.get("in_force"),
                    "source_url": EURLEX_URL.format(
                        lang="DE", fmt="HTML", celex=celex),
                    "file": None, "format": None,
                    "downloaded_at": now_iso(), "status": status,
                }
                if worker_error:
                    manifest_entry.update({
                        "source_resolution": (
                            "technical_waf_deferred" if technical_defer
                            else "worker_exception"),
                        "error": worker_error,
                    })
                    if technical_defer is not None:
                        manifest_entry.update({
                            "technical_defer_reason": technical_defer.reason,
                            "technical_defer_url": technical_defer.url,
                            "technical_defer_http_status": 202,
                            "technical_defer_waf_action": "challenge",
                        })
                else:
                    manifest_entry.update({
                        "source_resolution": "exhausted_official_fallbacks",
                        "attempted_sources": [
                            {
                                "source": "CELLAR CELEX content negotiation",
                                "url": CELLAR_CELEX_URL.format(
                                    celex=quote(celex, safe="")),
                                "language_scope": (
                                    "all CELLAR expression languages; DE/EN preferred"),
                                "media_types": [
                                    "application/xhtml+xml", "text/html"],
                            },
                            {
                                "source": "CELLAR print-parent SPARQL",
                                "url": SPARQL_ENDPOINT,
                                "result_required": (
                                    "exact language/identity/page mapping"),
                            },
                            {
                                "source": "EUR-Lex direct fallback",
                                "url_template": EURLEX_URL,
                                "language_scope": (
                                    "all CELLAR expression languages plus DE/EN"),
                                "formats": ["HTML", "PDF"],
                            },
                        ],
                    })
                    if merger_case_number(celex) is not None:
                        manifest_entry["attempted_sources"].insert(1, {
                            "source": (
                                "CELLAR exact merger PDF content negotiation"),
                            "url": CELLAR_CELEX_URL.format(
                                celex=quote(celex, safe="")),
                            "language_scope": "ENG then DEU",
                            "media_types": ["application/pdf"],
                            "identity_required": (
                                "official DOC_1 and exact first M-case token"),
                        })
                    manifest_entry.update(missing_metadata)
                    if (not entry.get("title") and
                            missing_metadata.get(
                                "source_expression_title_language")):
                        manifest_entry["title_language"] = missing_metadata[
                            "source_expression_title_language"]
                if retained_derived_primary is not None:
                    repair_failure = {
                        "attempted_at": now_iso(),
                        "outcome": status,
                        "error": worker_error,
                        "disposition": (
                            "retained verified exact print derivative; retry "
                            "preferred-format repair on next run"),
                    }
                    manifest_entry = dict(retained_derived_primary)
                    manifest_entry.update({
                        "id": celex, "title": entry.get("title"),
                        "type": entry.get("type"), "date": entry.get("date"),
                        "in_force": entry.get("in_force"), "status": "ok",
                        "preferred_repair_pending": True,
                        "preferred_repair_warning": repair_failure,
                    })
                    apply_manifest_metadata_correction(
                        celex, manifest_entry, entry)
                    apply_identifier_metadata_warning(celex, manifest_entry)
                    preserved_alternates = [
                        alternate for alternate in preserved_alternates
                        if alternate.get("file") != manifest_entry.get("file")]
                    log.warning("preferred-format repair failed for %s; "
                                "retained verified derivative as primary", celex)
                merge_alternate_files(manifest_entry, preserved_alternates)
                apply_provenance_incomplete_files(celex, manifest_entry)
                manifest[celex] = manifest_entry
            else:
                content, ext, lang, url = result
                file_name = base + "." + ext
                result_metadata = dict(getattr(result, "metadata", {}))
                derived_write = (result_metadata.get("source_resolution") ==
                                 DERIVED_PRINT_RESOLUTION)
                if derived_write and (
                        result_metadata.get("file_bytes") != len(content) or
                        result_metadata.get("file_sha256") !=
                        hashlib.sha256(content).hexdigest()):
                    raise ValueError("derived payload metadata mismatch for %s" %
                                     celex)
                manifest_entry = {
                    "id": celex,
                    "title": (entry.get("title") or
                              result_metadata.get("source_expression_title")),
                    "type": entry.get("type"), "date": entry.get("date"),
                    "in_force": entry.get("in_force"),
                    "source_url": url, "file": file_name, "format": ext,
                    "language": lang, "downloaded_at": now_iso(),
                    "status": "pending_write" if derived_write else "ok",
                }
                manifest_entry.update(result_metadata)
                if (not entry.get("title") and
                        result_metadata.get(
                            "source_expression_title_language")):
                    manifest_entry["title_language"] = result_metadata[
                        "source_expression_title_language"]
                apply_manifest_metadata_correction(celex, manifest_entry, entry)
                apply_identifier_metadata_warning(celex, manifest_entry)
                merge_alternate_files(
                    manifest_entry,
                    [alternate for alternate in preserved_alternates
                     if alternate.get("file") != file_name])
                apply_provenance_incomplete_files(celex, manifest_entry)
                if derived_write and not valid_derived_metadata(manifest_entry):
                    raise ValueError("incomplete derived provenance for %s" % celex)
                if derived_write:
                    manifest[celex] = manifest_entry
                    save_manifest(STAMM_DIR, manifest)
                write_document(STAMM_DIR / file_name, content)
                if (derived_write and not derived_record_files_match(
                        manifest_entry, STAMM_DIR / file_name)):
                    raise OSError("derived disk identity check failed for %s" %
                                  celex)
                manifest_entry["status"] = "ok"
                manifest[celex] = manifest_entry
                if derived_write:
                    save_manifest(STAMM_DIR, manifest)
                    remove_technical_retries((celex,))
                    processed_since_flush = 0
                elif celex in technical_retry_target_ids:
                    successful_technical_ids_pending_checkpoint.add(celex)
                done += 1
            processed_since_flush += 1

            attempted = done + failed
            if attempted % PROGRESS_EVERY == 0:
                log.info("stammgesetze progress: %d downloaded, %d skipped, "
                         "%d failed (of %d selected)", done, skipped,
                         failed, len(targets))
            if processed_since_flush >= MANIFEST_FLUSH_EVERY:
                save_manifest(STAMM_DIR, manifest)
                remove_technical_retries(
                    successful_technical_ids_pending_checkpoint)
                successful_technical_ids_pending_checkpoint.clear()
                processed_since_flush = 0
    except KeyboardInterrupt:
        interrupted = True
        save_manifest(STAMM_DIR, manifest)
        remove_technical_retries(
            successful_technical_ids_pending_checkpoint)
        successful_technical_ids_pending_checkpoint.clear()
        log.warning("interrupted: checkpointed %d manifest entries; "
                    "cancelling queued requests", len(manifest))
        raise
    except Exception:
        try:
            save_manifest(STAMM_DIR, manifest)
            remove_technical_retries(
                successful_technical_ids_pending_checkpoint)
            successful_technical_ids_pending_checkpoint.clear()
        except Exception as checkpoint_error:
            log.error("could not checkpoint manifest after fatal error: %s",
                      checkpoint_error)
        log.exception("fatal crawler error; completed results were checkpointed")
        raise
    finally:
        executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

    save_manifest(STAMM_DIR, manifest)
    remove_technical_retries(successful_technical_ids_pending_checkpoint)
    successful_technical_ids_pending_checkpoint.clear()
    log.info("stammgesetze finished: %d downloaded, %d already present, %d failed",
             done, skipped, failed)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def setup_logging():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    stream.setLevel(logging.INFO)
    log.addHandler(stream)
    errfile = logging.FileHandler(str(ERROR_LOG), encoding="utf-8")
    errfile.setFormatter(fmt)
    errfile.setLevel(logging.WARNING)
    log.addHandler(errfile)


def acquire_process_lock():
    """Hold an OS-level lock so host crawl delays cannot be bypassed by two CLIs."""
    global process_lock_handle
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    handle = open(str(LOCK_FILE), "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.seek(0)
        holder = handle.read().strip() or "unknown process"
        handle.close()
        raise RuntimeError("another crawl_eu process holds %s (%s)" %
                           (LOCK_FILE, holder))
    handle.seek(0)
    handle.truncate()
    handle.write("pid=%d started=%s\n" % (os.getpid(), now_iso()))
    handle.flush()
    process_lock_handle = handle


def main():
    parser = argparse.ArgumentParser(
        description="Download EU treaties and basic acts from EUR-Lex/CELLAR.")
    parser.add_argument("--category", choices=["verfassungen", "stammgesetze", "all"],
                        default="all", help="which category to crawl (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="enumerate and write index/manifest only, no downloads")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="cap number of document downloads per category")
    parser.add_argument("--refresh-index", action="store_true",
                        help="force re-enumeration via SPARQL even if cached")
    parser.add_argument(
        "--refresh-merger-languages", action="store_true",
        help=("opt-in resumable SPARQL enrichment of the cached CELEX index "
              "with complete M-case language coverage, then exit; run only "
              "during a coordinated stop"))
    parser.add_argument("--scope", choices=["in-force", "all"], default="in-force",
                        help="which basic acts to download (default: in-force)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, metavar="N",
                        help="parallel download workers; requests remain globally "
                             "throttled (default: %(default)s)")
    parser.add_argument(
        "--defer-on-waf", action="store_true",
        help=("opt in for journaled technical targets: atomically defer on "
              "the first exact HTTP 202 AWS WAF challenge instead of "
              "traversing all fallback languages (default: disabled)"))
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be non-negative")
    if args.refresh_merger_languages and args.refresh_index:
        parser.error("--refresh-merger-languages requires an existing stable "
                     "index and cannot be combined with --refresh-index")
    if args.refresh_merger_languages and (args.dry_run or args.limit is not None):
        parser.error("--refresh-merger-languages cannot be combined with "
                     "--dry-run or --limit")

    setup_logging()
    try:
        acquire_process_lock()
    except RuntimeError as exc:
        log.error("%s", exc)
        return 2
    session = make_session()
    started = time.time()
    if args.refresh_merger_languages:
        refresh_merger_language_cache(session)
        log.info("merger-language refresh done in %.1f s", time.time() - started)
        return 0
    if args.category in ("verfassungen", "all"):
        crawl_verfassungen(session, dry_run=args.dry_run, limit=args.limit)
    if args.category in ("stammgesetze", "all"):
        crawl_stammgesetze(session, dry_run=args.dry_run, limit=args.limit,
                           refresh_index=args.refresh_index, scope=args.scope,
                           workers=args.workers,
                           defer_on_waf=args.defer_on_waf)
    log.info("all done in %.1f s", time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
