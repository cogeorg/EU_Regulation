#!/usr/bin/env python3
"""Crawler for the 16 German state constitutions (Landesverfassungen).

Downloads each constitution into
``$EU_REGULATION_DATA_DIR/laender/verfassungen/<state>/`` and maintains a
manifest.json next to the data. Idempotent: existing non-empty files are not
re-downloaded (but are re-validated and kept in the manifest).

Usage:
    python3 full/crawlers/crawl_laender_verfassungen.py [--state XX] [--dry-run]
"""

import argparse
import html
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from project_paths import DATA_ROOT

USER_AGENT = "EU-Regulation-Research/1.0 (academic research)"
SLEEP_SECONDS = 0.5
MAX_RETRIES = 3
TIMEOUT = 60

DATA_DIR = DATA_ROOT / "laender" / "verfassungen"
MANIFEST_PATH = DATA_DIR / "manifest.json"

# Declarative target list. Each state has one or more downloads (be inclusive:
# where both an HTML and a PDF version exist, we keep both). "format" is one
# of "html" / "pdf" and decides both the file extension and the validation.
TARGETS = [
    {
        "state": "bw",
        "title": "Verfassung des Landes Baden-Württemberg",
        "downloads": [
            # Standalone LVerf PDF from the Landtag BW (cleaner than the LpB
            # bundle below, which combines GG + LVerf in one file).
            {
                "url": "https://www.landtag-bw.de/resource/blob/17338/4e675df2c0fd1e0938c9d6922b35b134/Landesverfassung.pdf",
                "format": "pdf",
                "filename": "verfassung_bw.pdf",
            },
            # Original source link (LpB brochure bundling GG + LVerf BW):
            {
                "id": "bw_lpb_gg_bundle",
                "url": "https://www.lpb-bw.de/fileadmin/lpb_hauptportal/pdf/publikationen/gg_landesverfassung_bf.pdf",
                "format": "pdf",
                "filename": "verfassung_bw_lpb_gg_bundle.pdf",
            },
            # No usable HTML: https://www.landesrecht-bw.de/bsbw/document/jlr-VerfBWrahmen
            # is a JavaScript-only juris shell and serves no static text.
        ],
        "notes": "landesrecht-bw.de is JS-only; using Landtag BW standalone PDF plus original LpB GG+LVerf bundle.",
    },
    {
        "state": "by",
        "title": "Verfassung des Freistaates Bayern",
        "downloads": [
            # Official complete PDF export (Art. 1-188) from gesetze-bayern.de.
            # The original HTML link
            # https://www.gesetze-bayern.de/Content/Document/BayVerf only serves
            # a table of contents; the article texts load via per-article pages.
            {
                "url": "https://www.gesetze-bayern.de/Content/Pdf/BayVerf?all=True",
                "format": "pdf",
                "filename": "verfassung_by.pdf",
            },
        ],
        "notes": "gesetze-bayern.de HTML page is TOC-only; using its official full-document PDF export.",
    },
    {
        "state": "be",
        "title": "Verfassung von Berlin",
        "downloads": [
            # Consolidated full text (juris Gesamtausgabe) published by the
            # Abgeordnetenhaus von Berlin, incl. changes through July 2025.
            # Original link https://www.berlin.de/rbmskzl/politik/senat/verfassung/
            # only offers the text split across nine section subpages, and
            # https://gesetze.berlin.de/bsbe/document/jlr-VerfBE1995rahmen is a
            # JavaScript-only juris shell.
            {
                "url": "https://www.parlament-berlin.de/media/download/5549",
                "format": "pdf",
                "filename": "verfassung_be.pdf",
                "status_if_valid": "stale_incomplete",
                "document_role": "stale_consolidated_text",
            },
            {
                "id": "be_amendment_2026",
                "url": "https://www.parlament-berlin.de/ados/19/IIIPlen/vorgang/d19-2956.pdf",
                "format": "pdf",
                "filename": "verfassung_be_aenderung_2026_adopted_bill.pdf",
                "document_role": "adopted_legislative_artifact",
                "notes": (
                    "Official Abgeordnetenhaus Drs. 19/2956, adopted 23 Apr "
                    "2026. The resulting Nineteenth Act amending Art. 84 is "
                    "dated 29 Apr 2026 and promulgated at GVBl. Berlin p. 186."
                ),
            },
            {
                "id": "be_promulgation_2026",
                "url": "https://www.berlin.de/sen/justiz/service/gesetze-und-verordnungen/2026/ausgabe-nr-14-vom-1252026-s-185-204.pdf?ts=1785148923",
                "format": "pdf",
                "filename": "verfassung_be_gvbl_2026_heft_14.pdf",
                "document_role": "official_promulgation_issue",
                "notes": (
                    "Official GVBl. Berlin 2026 No. 14, pp. 185-204; the "
                    "Nineteenth Act amending Art. 84 is promulgated at p. 186."
                ),
            },
        ],
        "notes": (
            "The consolidated PDF is amended only through 10 Jul 2025 and is "
            "stale: it omits the 29 Apr 2026 amendment to Art. 84. The official "
            "adopted bill and authoritative promulgation issue are stored alongside it."
        ),
    },
    {
        "state": "bb",
        "title": "Verfassung des Landes Brandenburg",
        "downloads": [
            # The BRAVORS landing page itself contains the complete text.
            {
                "url": "https://bravors.brandenburg.de/de/gesetze-212792",
                "format": "html",
                "filename": "verfassung_bb.html",
            },
        ],
        "notes": "Original BRAVORS URL contains the full text directly.",
    },
    {
        "state": "hb",
        "title": "Landesverfassung der Freien Hansestadt Bremen",
        "downloads": [
            # The transparency-portal page contains the complete text inline.
            {
                "url": "https://www.transparenz.bremen.de/metainformationen/landesverfassung-der-freien-hansestadt-bremen-in-der-fassung-der-bekanntmachung-vom-12-august-2019-232507?asl=bremen203_tpgesetz.c.55340.de&template=20_gp_ifg_meta_detail_d",
                "format": "html",
                "filename": "verfassung_hb.html",
            },
            # PDF rendering offered by the same portal:
            {
                "id": "hb_pdf",
                "url": "https://www.transparenz.bremen.de/sixcms/detail.php?gsid=bremen203.c.232507.de&template=00_html_to_pdf_d",
                "format": "pdf",
                "filename": "verfassung_hb.pdf",
            },
        ],
        "notes": "Original metadata page carries the full text; PDF export taken as well.",
    },
    {
        "state": "hh",
        "title": "Verfassung der Freien und Hansestadt Hamburg",
        "downloads": [
            # Stand Juli 2025 (incl. the 23rd/24th amendments of 5 March 2025
            # and the change of 23 July 2025). The previously used blob
            # (...stand-04-2023...) missed the 2025 amendments.
            {
                "url": "https://www.hamburg.de/resource/blob/1089682/02cbe1908499e3d97576c7c201200c79/verfassung-hh-stand-07-2025-final-online-data.pdf",
                "format": "pdf",
                "filename": "verfassung_hh.pdf",
                # Extracted text was independently checked to say that the
                # constitution was last amended on 23 July 2025.  Pin the
                # immutable official blob so a stale/corrupt replacement is
                # never accepted merely because it starts with %PDF.
                "sha256": "71e37d49a090fee67dd2fdb4dde2a3934211b088373248e9cd1b7f92bcf12787",
            },
        ],
        "notes": "Direct PDF (Stand 07/2025).",
    },
    {
        "state": "he",
        "title": "Verfassung des Landes Hessen",
        "downloads": [
            {
                "url": "https://hessischer-landtag.de/sites/default/files/dateien/2026-05/HL_Verfassung_Apr2026_final_web_2.pdf",
                "format": "pdf",
                "filename": "verfassung_he.pdf",
            },
        ],
        "notes": "Direct PDF.",
    },
    {
        "state": "mv",
        "title": "Verfassung des Landes Mecklenburg-Vorpommern",
        "downloads": [
            {
                "url": "https://www.landtag-mv.de/fileadmin/Publikationen/Publikationen_Aktuell/Verfassung_Neuauflage_April25.pdf",
                "format": "pdf",
                "filename": "verfassung_mv.pdf",
            },
        ],
        "notes": "Direct PDF.",
    },
    {
        "state": "ni",
        "title": "Niedersächsische Verfassung",
        "downloads": [
            # Continuously updated official brochure PDF from the Landtag.
            # Original link
            # https://voris.wolterskluwer-online.de/browse/document/8e776f11-d3cf-3af3-8a48-5ca082829340
            # is a JavaScript-only Wolters Kluwer shell with no static text.
            {
                "url": "https://www.landtag-niedersachsen.de/fileadmin/user_upload/redaktion/hauptseite/downloads/publikationen/Broschuere_Niedersaechsische_Verfassung.pdf",
                "format": "pdf",
                "filename": "verfassung_ni.pdf",
            },
        ],
        "notes": "VORIS page is JS-only; using the Landtag Niedersachsen online edition PDF.",
    },
    {
        "state": "nw",
        "title": "Verfassung für das Land Nordrhein-Westfalen",
        "downloads": [
            {
                "url": "https://recht.nrw.de/lrgv/gesetz/16012026-verfassung-fuer-das-land-nordrhein-westfalen/",
                "format": "html",
                "filename": "verfassung_nw.html",
            },
        ],
        "notes": "HTML full text on recht.nrw.de.",
    },
    {
        "state": "rp",
        "title": "Verfassung für Rheinland-Pfalz",
        "downloads": [
            {
                "url": "https://www.rlp.de/fileadmin/02/Unser_Land/Landesverfassung/Verfassung_fuer_Rheinland-Pfalz_Stand_2015.pdf",
                "format": "pdf",
                "filename": "verfassung_rp.pdf",
                "status_if_valid": "stale_incomplete",
                "document_role": "stale_consolidated_text",
            },
            {
                "id": "rp_amendment_2022",
                "url": "https://dokumente.landtag.rlp.de/landtag/drucksachen/2684-18.pdf",
                "format": "pdf",
                "filename": "verfassung_rp_aenderung_2022_final_recommendation.pdf",
                "document_role": "adopted_final_legislative_artifact",
                "notes": (
                    "Final recommendation Drs. 18/2684 for the amendment of "
                    "Arts. 117 and 143e, adopted 1 Apr 2022; resulting law "
                    "dated 8 Apr 2022."
                ),
            },
            {
                "id": "rp_amendment_2024",
                "url": "https://dokumente.landtag.rlp.de/landtag/drucksachen/9732-18.pdf",
                "format": "pdf",
                "filename": "verfassung_rp_aenderung_2024_final_recommendation.pdf",
                "document_role": "adopted_final_legislative_artifact",
                "notes": (
                    "Final recommendation Drs. 18/9732 for the amendment of "
                    "Art. 113; resulting law dated 19 Jun 2024."
                ),
            },
            {
                "id": "rp_amendment_2026",
                "url": "https://dokumente.landtag.rlp.de/landtag/drucksachen/14464-18.pdf",
                "format": "pdf",
                "filename": "verfassung_rp_aenderung_2026_adopted_text.pdf",
                "document_role": "adopted_legislative_text",
                "notes": (
                    "Adopted text Drs. 18/14464 of the amendment to Art. 91; "
                    "resulting law dated 6 May 2026."
                ),
            },
        ],
        "notes": (
            "Direct PDF, but Stand 2015: misses the amendments of 8 Apr 2022 "
            "(Art. 117/143e), 19 Jun 2024 (Art. 113) and 6 May 2026 (Art. 91). "
            "No newer official consolidated full text is statically "
            "downloadable (landesrecht.rlp.de is a JS-only juris shell)."
        ),
    },
    {
        "state": "sl",
        "title": "Verfassung des Saarlandes",
        "downloads": [
            {
                "url": "https://www.landtag-saar.de/media/32el1zo3/190410_verfassung-des-saarlandes_din-a4.pdf",
                "format": "pdf",
                "filename": "verfassung_sl.pdf",
                "status_if_valid": "stale_incomplete",
                "document_role": "stale_consolidated_text",
            },
            {
                "id": "sl_amendments_2024",
                "url": "https://www.amtsblatt.saarland.de/jportal/?quelle=jlink&docid=VB-SL-ABlI2024145-G&psml=bsverkslprod.psml&max=true",
                "format": "pdf",
                "filename": "verfassung_sl_amtsblatt_11_2024.pdf",
                "resolve_embedded_pdf": True,
                "document_role": "official_promulgation_issue",
                "notes": (
                    "Official Amtsblatt Teil I 2024 No. 11, pp. 145-194. "
                    "Acts 2128-2130 amending the constitution are at pp. 146-147."
                ),
            },
            {
                "id": "sl_amendments_2026",
                "url": "https://www.amtsblatt.saarland.de/jportal/?quelle=jlink&docid=VB-SL-ABlI2026381-G&psml=bsverkslprod.psml&max=true",
                "format": "pdf",
                "filename": "verfassung_sl_amtsblatt_23_2026.pdf",
                "resolve_embedded_pdf": True,
                "document_role": "official_promulgation_issue",
                "notes": (
                    "Official Amtsblatt Teil I 2026 No. 23, pp. 381-402. "
                    "Acts 2198-2200 amending the constitution are at pp. 382-384."
                ),
            },
        ],
        "notes": (
            "The Landtag PDF is consolidated only through 10 Apr 2019. "
            "Official promulgation issues containing the three amendments of "
            "7 Feb 2024 and the three amendments of 29 Apr 2026 are stored alongside it."
        ),
    },
    {
        "state": "sn",
        "title": "Verfassung des Freistaates Sachsen",
        "downloads": [
            # REVOSAX hosts the full text as HTML. The original landing page
            # https://www.landtag.sachsen.de/de/parlament/zusammensetzung-und-rechtsgrundlagen/verfassung-9196.cshtml
            # does not link the full text directly.
            {
                "url": "https://www.revosax.sachsen.de/vorschrift/3975-Verfassung-des-Freistaates-Sachsen",
                "format": "html",
                "filename": "verfassung_sn.html",
            },
        ],
        "notes": "Full text taken from REVOSAX instead of the Landtag landing page.",
    },
    {
        "state": "st",
        "title": "Verfassung des Landes Sachsen-Anhalt",
        "downloads": [
            {
                "url": "https://www.landtag.sachsen-anhalt.de/fileadmin/Downloads/Verzeichnisse_Sitzordnung_Plenarsaal/Verfassung_des_Landes_Sachsen-Anhalt.pdf",
                "format": "pdf",
                "filename": "verfassung_st.pdf",
                "status_if_valid": "stale_incomplete",
                "document_role": "stale_consolidated_text",
            },
            {
                "id": "st_parlamentsreform_2026",
                # Official final recommendation adopted with the required
                # two-thirds majority on 23 April 2026.  The resulting law is
                # dated 4 May 2026 and promulgated at GVBl. LSA 2026, p. 178.
                # This file is a legislative artifact, not the promulgated
                # GVBl PDF and not a consolidated constitution.
                "url": "https://padoka.landtag.sachsen-anhalt.de/files/drs/wp8/drs/d6871vbe.pdf",
                "format": "pdf",
                "filename": "verfassung_st_parlamentsreform_2026_final_recommendation.pdf",
                "document_role": "adopted_final_legislative_artifact",
                "notes": (
                    "Beschlussempfehlung Drs. 8/6871, adopted 23 Apr 2026; "
                    "resulting Gesetz zur Parlamentsreform 2026 dated 4 May "
                    "2026, promulgated GVBl. LSA No. 9 of 11 May 2026, p. 178."
                ),
            },
        ],
        "notes": (
            "The consolidated PDF says last amended 20 Mar 2020 and is stale. "
            "It omits the enacted 2026 Parlamentsreform; the official adopted "
            "final recommendation is stored alongside it."
        ),
    },
    {
        "state": "sh",
        "title": "Verfassung des Landes Schleswig-Holstein",
        "downloads": [
            {
                "url": "https://www.landtag.ltsh.de/export/sites/ltsh/service/downloadgallery/kurzinfos/06_Landesverfassung.pdf",
                "format": "pdf",
                "filename": "verfassung_sh.pdf",
                "status_if_valid": "stale_incomplete",
                "document_role": "stale_consolidated_text",
            },
            {
                "id": "sh_amendment_2024",
                # The former official GVOBl PDF URL now returns HTTP 404.
                # Preserve the official Landtag's final recommendation, whose
                # recommended wording was then adopted unanimously.
                "url": "https://www.landtag.ltsh.de/infothek/wahl20/drucks/02500/drucksache-20-02561.pdf",
                "format": "pdf",
                "filename": "verfassung_sh_aenderung_2024_final_recommendation.pdf",
                "document_role": "adopted_final_legislative_artifact",
                "notes": (
                    "Final recommendation Drs. 20/2561, adopted unanimously "
                    "on 16 Oct 2024. The resulting Gesetz zur Änderung der "
                    "Verfassung des Landes Schleswig-Holstein is dated 22 Oct "
                    "2024 and promulgated at GVOBl. 2024, p. 749; the former "
                    "official GVOBl PDF URL currently returns HTTP 404."
                ),
            },
            {
                "id": "sh_gvobl_2024",
                "url": "https://verkuendungsportal.schleswig-holstein.de/mm/gvobl_jahrgang_2024/II_GVOBl_Jahrgang_2024",
                "format": "pdf",
                "filename": "verfassung_sh_gvobl_jahrgang_2024.pdf",
                "document_role": "official_promulgation_archive",
                "notes": (
                    "Official complete GVOBl 2024 archive; the constitutional "
                    "amendment of 22 Oct 2024 appears in issue 13 at p. 749."
                ),
            },
        ],
        "notes": (
            "The consolidated Landtag PDF says last amended 20 Apr 2021 and "
            "omits the 22 Oct 2024 amendment to Art. 46; the official GVOBl "
            "issue containing that amendment is stored alongside it."
        ),
    },
    {
        "state": "th",
        "title": "Verfassung des Freistaats Thüringen",
        "downloads": [
            {
                "url": "https://www.thueringer-landtag.de/fileadmin/user_upload/Verfassung_DIN_A_6_Internet.pdf",
                "format": "pdf",
                "filename": "verfassung_th.pdf",
            },
        ],
        "notes": "Direct PDF.",
    },
]

STATE_CODES = [t["state"] for t in TARGETS]


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate(content, fmt):
    """Return None if content looks like a valid constitution document,
    otherwise a short error description."""
    if not content:
        return "empty download"
    if fmt == "pdf":
        if not content.startswith(b"%PDF"):
            return "not a PDF (missing %PDF header)"
        return None
    # HTML: must mention the constitution and its articles.
    text = content.decode("utf-8", errors="replace")
    if "Verfassung" not in text:
        return "HTML lacks keyword 'Verfassung'"
    if not re.search(r"Art(ikel|\.)\s*\d", text):
        return "HTML lacks article references ('Artikel'/'Art.')"
    return None


def validate_download(content, download):
    error = validate(content, download["format"])
    expected_hash = download.get("sha256")
    if error is None and expected_hash:
        actual_hash = hashlib.sha256(content).hexdigest()
        if actual_hash != expected_hash:
            return "SHA-256 mismatch (expected %s, got %s)" % (
                expected_hash, actual_hash
            )
    return error


def fetch(session, url):
    """Download url with retries. Returns (content, error)."""
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.content, None
        except requests.RequestException as exc:
            last_err = str(exc)
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_SECONDS * (2 ** attempt))
    return None, last_err


def fetch_download(session, download):
    """Fetch a configured payload, resolving session-bound embedded PDFs when needed."""
    if not download.get("resolve_embedded_pdf"):
        return fetch(session, download["url"])

    page, err = fetch(session, download["url"])
    if err is not None:
        return None, err
    page_text = page.decode("utf-8", errors="replace")
    match = re.search(
        r'href="([^\"]+\.pdf(?:;jsessionid=[^\"?#]+)?)"', page_text, re.I
    )
    if match is None:
        return None, "official record contains no embedded PDF link"
    pdf_url = requests.compat.urljoin(download["url"], html.unescape(match.group(1)))
    return fetch(session, pdf_url)


def load_manifest():
    if MANIFEST_PATH.exists():
        try:
            with open(MANIFEST_PATH, encoding="utf-8") as fh:
                return {e["file"]: e for e in json.load(fh)}
        except (json.JSONDecodeError, KeyError, TypeError):
            print("WARNING: existing manifest unreadable, rebuilding", file=sys.stderr)
    return {}


def save_manifest(entries):
    ordered = sorted(entries.values(), key=lambda e: (e["id"], e["file"]))
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = MANIFEST_PATH.with_suffix(MANIFEST_PATH.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(ordered, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    tmp_path.replace(MANIFEST_PATH)


def main():
    parser = argparse.ArgumentParser(
        description="Download the 16 German state constitutions (Landesverfassungen)."
    )
    parser.add_argument("--state", choices=STATE_CODES, help="only crawl this state")
    parser.add_argument(
        "--dry-run", action="store_true", help="show what would be done, download nothing"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="limit the number of state targets (launcher smoke tests)",
    )
    parser.add_argument(
        "--refresh-existing", action="store_true",
        help=(
            "re-fetch valid same-source files; use periodically because some "
            "official static URLs are updated in place"
        ),
    )
    args = parser.parse_args()

    targets = [t for t in TARGETS if args.state is None or t["state"] == args.state]
    if args.limit is not None:
        targets = targets[: max(args.limit, 0)]

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    manifest = load_manifest()
    configured_files = {
        "%s/%s" % (target["state"], download["filename"])
        for target in TARGETS for download in target["downloads"]
    }
    manifest = {
        rel: entry for rel, entry in manifest.items() if rel in configured_files
    }
    n_ok = n_skip = n_err = 0

    for target in targets:
        state = target["state"]
        state_dir = DATA_DIR / state
        for dl in target["downloads"]:
            path = state_dir / dl["filename"]
            rel = str(path.relative_to(DATA_DIR))

            prev = manifest.get(rel, {})
            source_unchanged = prev.get("source_url") == dl["url"]
            if (
                path.exists()
                and path.stat().st_size > 0
                and source_unchanged
                and not args.refresh_existing
            ):
                content = path.read_bytes()
                err = validate_download(content, dl)
                if err is None:
                    print("SKIP  %s (%s exists, %d bytes)" %
                          (state, rel, path.stat().st_size))
                    n_skip += 1
                    # Keep/refresh metadata; preserve original timestamp.
                    manifest[rel] = {
                        "id": dl.get("id", state),
                        "title": target["title"],
                        "source_url": dl["url"],
                        "file": rel,
                        "format": dl["format"],
                        "downloaded_at": prev.get("downloaded_at") or now_iso(),
                        "status": dl.get("status_if_valid", "ok"),
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "document_role": dl.get("document_role", "consolidated_text"),
                        "notes": dl.get("notes", target.get("notes")),
                    }
                    continue
                print("REFRESH %s (%s existing payload failed validation: %s)" %
                      (state, rel, err))
            if (
                path.exists()
                and path.stat().st_size > 0
                and source_unchanged
                and args.refresh_existing
            ):
                print("REFRESH %s (%s --refresh-existing)" % (state, rel))
            if path.exists() and path.stat().st_size > 0 and not source_unchanged:
                print("REFRESH %s (%s source URL changed)" % (state, rel))

            if args.dry_run:
                print("DRY   %s would download %s -> %s" % (state, dl["url"], rel))
                continue

            content, err = fetch_download(session, dl)
            if err is None:
                err = validate_download(content, dl)
            if err is None:
                state_dir.mkdir(parents=True, exist_ok=True)
                tmp_path = path.with_suffix(path.suffix + ".tmp")
                tmp_path.write_bytes(content)
                tmp_path.replace(path)
                print("OK    %s %s (%d bytes)" % (state, rel, len(content)))
                n_ok += 1
                status = dl.get("status_if_valid", "ok")
            else:
                print("ERROR %s %s: %s" % (state, dl["url"], err), file=sys.stderr)
                n_err += 1
                if path.exists() and path.stat().st_size > 0:
                    # Never relabel retained old bytes as the new intended
                    # source when a refresh fails.
                    old_content = path.read_bytes()
                    manifest[rel] = {
                        "id": dl.get("id", state),
                        "title": target["title"],
                        "source_url": prev.get("source_url"),
                        "intended_source_url": dl["url"],
                        "file": rel,
                        "format": dl["format"],
                        "downloaded_at": prev.get("downloaded_at"),
                        "status": "error_refresh_failed_old_payload_retained",
                        "sha256": hashlib.sha256(old_content).hexdigest(),
                        "document_role": prev.get(
                            "document_role", dl.get("document_role", "consolidated_text")
                        ),
                        "notes": "Refresh failed: %s" % err,
                    }
                    time.sleep(SLEEP_SECONDS)
                    continue
                status = "error"

            manifest[rel] = {
                "id": dl.get("id", state),
                "title": target["title"],
                "source_url": dl["url"],
                "file": rel,
                "format": dl["format"],
                "downloaded_at": now_iso(),
                "status": status,
                "sha256": hashlib.sha256(content).hexdigest() if content else None,
                "document_role": dl.get("document_role", "consolidated_text"),
                "notes": dl.get("notes", target.get("notes")),
            }
            time.sleep(SLEEP_SECONDS)

    if not args.dry_run:
        save_manifest(manifest)
        print(
            "Done: %d downloaded, %d skipped (already present), %d errors. Manifest: %s"
            % (n_ok, n_skip, n_err, MANIFEST_PATH)
        )
    return 1 if n_err else 0


if __name__ == "__main__":
    sys.exit(main())
