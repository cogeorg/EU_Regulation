#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create a CSV with CELEX identifiers of EU acts currently in force.

Sources:
  - dump    : scan a local EUR‑Lex "legislation in force" metadata dump (RDF/XML, Turtle, N-Triples)
  - sparql  : query the Publications Office SPARQL endpoint (seek pagination)
  - formex  : scan a Formex OJ package for CELEX and (optionally) verify in-force online
  - both    : dump + sparql
  - all     : dump + sparql + formex (union)

Example (your MTD dump):
  unzip LEG_EN_MTD_20251012_01_00.zip -d INFORCE_RDF
  python celex_inforce_list.py --source dump \
    --dump_dir ./INFORCE_RDF \
    --dump_glob '**/tree_non_inferred.rdf' \
    --sectors 3 \
    --output_csv ./celex_inforce.csv
"""

import argparse, re, time, random
from pathlib import Path
from typing import Iterable
import pandas as pd
from tqdm import tqdm
import requests

# ---------------- CLI ----------------

def parse_args():
    ap = argparse.ArgumentParser(description="List CELEX numbers for all EU acts currently in force.")
    ap.add_argument("--source", choices=["dump","sparql","formex","both","all"], default="dump",
                    help="Where to pull from.")
    ap.add_argument("--output_csv", default="./celex_inforce.csv",
                    help="Output CSV path (one CELEX per row).")
    ap.add_argument("--sectors", default="3",
                    help="Comma-separated CELEX sectors to include (e.g., 3 or 2,3,4).")

    # Dump mode (RDF)
    ap.add_argument("--dump_dir", default="",
                    help="Root folder of the EUR‑Lex 'legislation in force' MTD dump (unzipped).")
    ap.add_argument("--dump_glob", default="**/*.rdf",
                    help="Glob pattern inside dump_dir (e.g., **/*.rdf, **/*.ttl, **/*.nt).")
    ap.add_argument("--dump_max_files", type=int, default=0,
                    help="Limit number of files to scan in dump (0 = all).")

    # SPARQL mode
    ap.add_argument("--endpoint", default="https://publications.europa.eu/webapi/rdf/sparql",
                    help="SPARQL endpoint URL.")
    ap.add_argument("--limit", type=int, default=3000,
                    help="Page size for SPARQL (seek pagination, no OFFSET).")
    ap.add_argument("--sleep_min", type=float, default=0.5,
                    help="Min sleep between HTTP requests.")
    ap.add_argument("--sleep_max", type=float, default=1.5,
                    help="Max sleep between HTTP requests.")
    ap.add_argument("--retries", type=int, default=5,
                    help="HTTP retries per page or per notice.")
    ap.add_argument("--timeout", type=int, default=60,
                    help="HTTP timeout (seconds) for SPARQL/notice calls.")
    ap.add_argument("--year_from", type=int, default=1958,
                    help="Earliest year to query (SPARQL mode).")
    ap.add_argument("--year_to", type=int, default=2025,
                    help="Latest year to query (SPARQL mode).")

    # Formex mode
    ap.add_argument("--formex_dir", default="",
                    help="Root folder of a Formex OJ package (e.g., LEG_EN_FMX_YYYYMMDD_01_00).")
    ap.add_argument("--formex_glob", default="**/*.xml",
                    help="Glob for XML files under formex_dir.")
    ap.add_argument("--verify_inforce_online", action="store_true",
                    help="Verify in-force by fetching notice RDF per CELEX (slower; requires internet).")
    ap.add_argument("--max_verify", type=int, default=0,
                    help="Verify at most N CELEX (0 = verify all).")

    return ap.parse_args()

# -------------- Common helpers --------------

def sectors_set(s: str) -> set[str]:
    return set(x.strip() for x in s.split(",") if x.strip())

def in_sectors(celex: str, sectors: set[str]) -> bool:
    return bool(celex) and celex[0] in sectors

def polite_sleep(a, b):
    time.sleep(random.uniform(max(0.0, a), max(0.0, b)))

# -------------- DUMP MODE (RDF) --------------

# TTL/N-Triples quick path: ... resource_legal_id_celex "32014L0065"
RE_TTL = re.compile(r'resource_legal_id_celex[^"\n]*"([^"\n]+)"')

# RDF/XML quick path (FIXED):
# - allow namespace prefixes with dots/hyphens (e.g., j.0:)
# - allow attributes on the element (e.g., rdf:datatype="...")
RE_XML = re.compile(
    r'<(?:[\w\.\-]+:)?resource_legal_id_celex\b[^>]*>\s*([^<\s]+)\s*</(?:[\w\.\-]+:)?resource_legal_id_celex\s*>',
    re.IGNORECASE
)

def scan_dump_celex(dump_dir: Path, glob_pat: str, max_files: int, sectors: set[str]) -> list[str]:
    """
    Scan the MTD (metadata) dump for CELEX values.
    Fast path: regex over RDF/XML and TTL/NT.
    Slow path fallback: rdflib graph parsing to extract cdm:resource_legal_id_celex.
    """
    if not dump_dir or not dump_dir.exists():
        print("[error] --dump_dir missing or does not exist.")
        return []
    files = sorted(dump_dir.glob(glob_pat))
    if max_files and max_files > 0:
        files = files[:max_files]
    if not files:
        print("[warn] no files under dump_dir with that glob.")
        return []

    # Import rdflib lazily (only used if regex hits nothing in a given file)
    try:
        from rdflib import Graph, Namespace
        CDM = Namespace("http://publications.europa.eu/ontology/cdm#")
        have_rdflib = True
    except Exception:
        Graph = None
        CDM = None
        have_rdflib = False

    celexes: set[str] = set()
    for fp in tqdm(files, desc="scan dump", unit="file"):
        try:
            text = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            text = ""

        hits = set()

        # Fast regexes
        for m in RE_XML.finditer(text):
            hits.add(m.group(1).strip())
        for m in RE_TTL.finditer(text):
            hits.add(m.group(1).strip())

        # Fallback: parse RDF properly if nothing matched
        if not hits and have_rdflib:
            try:
                g = Graph()
                g.parse(fp.as_posix())  # auto-detect format
                for _, _, o in g.triples((None, CDM.resource_legal_id_celex, None)):
                    hits.add(str(o).strip())
            except Exception:
                pass  # ignore this file if parsing fails

        # Keep sector-filtered CELEX
        for val in hits:
            if in_sectors(val, sectors):
                celexes.add(val)

    out = sorted(celexes)
    print(f"[ok] dump scan -> {len(out):,} CELEX (sectors: {','.join(sorted(sectors))})")
    if not out:
        print("[hint] Try a narrower glob, e.g., --dump_glob '**/tree_non_inferred.rdf', "
              "or ensure you downloaded the MTD (metadata) dump, not FMX.")
    if not have_rdflib:
        print("[hint] rdflib not installed; installed fallback would improve resilience: pip install rdflib")
    return out

# -------------- SPARQL MODE --------------

SPARQL_TPL = """\
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?celex WHERE {
  ?w cdm:resource_legal_id_celex ?celex ;
     cdm:resource_legal_in-force ?inforce .
  FILTER ( ?inforce = true || lcase(str(?inforce)) = "true" || str(?inforce) = "1" )
  FILTER ( STRSTARTS(STR(?celex), "{sector}") )
  FILTER ( SUBSTR(STR(?celex), 2, 4) = "{year}" )
  FILTER ( STR(?celex) > "{after}" )
}
ORDER BY STR(?celex)
LIMIT {limit}
"""

def sparql_post(session: requests.Session, endpoint: str, query: str, timeout: int) -> dict:
    r = session.post(endpoint, data={"query": query},
                     headers={"Accept": "application/sparql-results+json"},
                     timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    return r.json()

def iter_sparql_celex(endpoint: str, sectors: set[str], year_from: int, year_to: int,
                      page_limit: int, sleep_min: float, sleep_max: float,
                      retries: int, timeout: int):
    s = requests.Session()
    s.headers.update({"User-Agent": "celex-inforce-list/1.2"})
    for year in range(year_from, year_to + 1):
        for sector in sorted(sectors):
            after = ""
            while True:
                q = SPARQL_TPL.format(sector=sector, year=str(year),
                                      after=after.replace('"','\\"'), limit=page_limit)
                err = None
                for _ in range(retries):
                    try:
                        js = sparql_post(s, endpoint, q, timeout)
                        err = None; break
                    except Exception as e:
                        err = e; polite_sleep(0.4, 1.2)
                if err:
                    print(f"[warn] skipping sector={sector} year={year} after='{after}': {err}")
                    break
                bindings = js.get("results", {}).get("bindings", [])
                if not bindings: break
                rows = sorted({b["celex"]["value"] for b in bindings if "celex" in b})
                for ce in rows: yield ce
                after = rows[-1]
                polite_sleep(sleep_min, sleep_max)

def fetch_sparql_celex(endpoint: str, sectors: set[str], y_from: int, y_to: int,
                        page_limit: int, sleep_min: float, sleep_max: float,
                        retries: int, timeout: int) -> list[str]:
    seen: set[str] = set()
    for ce in tqdm(iter_sparql_celex(endpoint, sectors, y_from, y_to,
                                     page_limit, sleep_min, sleep_max, retries, timeout),
                   desc="sparql fetch", unit="id"):
        if in_sectors(ce, sectors): seen.add(ce)
    out = sorted(seen)
    print(f"[ok] SPARQL fetch -> {len(out):,} CELEX (sectors: {','.join(sorted(sectors))})")
    return out

# -------------- FORMEX MODE --------------

CELEX_TEXT_RE  = re.compile(r"CELEX:([0-9A-Z()/:.\-_;]+)")
CELEX_URI_RE   = re.compile(r"/resource/celex/([0-9A-Z()[\]/.\-]+)", re.IGNORECASE)
CELEX_PARAM_RE = re.compile(r"uri=CELEX:([0-9A-Z()[\]/.\-]+)", re.IGNORECASE)
ELI_RE         = re.compile(r"/eli/(reg|dir|dec|reg_impl|reg_del|dir_impl|dir_del|dec_impl|dec_del)/(\d{4})/([0-9]{1,5})\b", re.IGNORECASE)

def eli_to_celex(m: re.Match) -> str:
    typ, year, num = m.group(1).lower(), m.group(2), m.group(3)
    letter = {"reg":"R","dir":"L","dec":"D",
              "reg_impl":"R","reg_del":"R",
              "dir_impl":"L","dir_del":"L",
              "dec_impl":"D","dec_del":"D"}[typ]
    return f"3{year}{int(num):04d}{letter}" if False else f"3{year}{letter}{int(num):04d}"

def normalize_celex(token: str) -> str | None:
    if not token: return None
    t = token.strip().strip('";,.)]}\'>').strip()
    if ":" in t: t = t.split(":", 1)[0]
    return t if re.match(r"^[0-9]\d{4}[A-Z]", t) else None

def scan_formex_celex(formex_dir: Path, xml_glob: str, sectors: set[str]) -> list[str]:
    if not formex_dir or not formex_dir.exists():
        print("[error] --formex_dir missing or does not exist.")
        return []
    xmls = sorted(formex_dir.glob(xml_glob))
    if not xmls:
        print("[warn] no XML files under formex_dir.")
        return []
    found: set[str] = set()
    for fp in tqdm(xmls, desc="scan formex", unit="file"):
        try:
            text = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        cands = []
        for m in CELEX_URI_RE.finditer(text):   c = normalize_celex(m.group(1));  c and cands.append(c)
        for m in CELEX_PARAM_RE.finditer(text): c = normalize_celex(m.group(1));  c and cands.append(c)
        for m in CELEX_TEXT_RE.finditer(text):  c = normalize_celex(m.group(1));  c and cands.append(c)
        for m in ELI_RE.finditer(text):         cands.append(eli_to_celex(m))
        for c in set(cands):
            if in_sectors(c, sectors): found.add(c)
    out = sorted(found)
    print(f"[ok] formex scan -> {len(out):,} CELEX (sectors: {','.join(sorted(sectors))})")
    return out

# Optional online verification of in-force (notice RDF)
def verify_inforce_online(celex_iter: Iterable[str], timeout: int, retries: int,
                          sleep_min: float, sleep_max: float, max_verify: int = 0) -> list[str]:
    from rdflib import Graph, Namespace
    CDM = Namespace("http://publications.europa.eu/ontology/cdm#")
    s = requests.Session()
    s.headers.update({"User-Agent": "celex-inforce-verify/1.0"})
    out = []
    n = 0
    for ce in tqdm(list(celex_iter), desc="verify in-force", unit="id"):
        if max_verify and n >= max_verify: break
        n += 1
        url = f"http://publications.europa.eu/resource/celex/{ce}?language=eng"
        headers = {"Accept":"application/rdf+xml;notice=tree", "Accept-Language":"eng", "Connection":"close"}
        last_err = None
        for _ in range(retries):
            try:
                r = s.get(url, headers=headers, timeout=timeout)
                if r.status_code >= 400:
                    last_err = RuntimeError(f"HTTP {r.status_code}")
                    polite_sleep(sleep_min, sleep_max); continue
                g = Graph(); g.parse(data=r.text)
                in_force = False
                for _, _, o in g.triples((None, CDM.resource_legal_in_force, None)):
                    val = str(o).strip().lower()
                    if val in ("true","1","yes"): in_force = True; break
                if in_force: out.append(ce)
                last_err = None
                break
            except Exception as e:
                last_err = e
            finally:
                polite_sleep(sleep_min, sleep_max)
        # conservative: skip on persistent error
    return sorted(set(out))

# -------------- MAIN ----------------

def main():
    args = parse_args()
    sectors = sectors_set(args.sectors)
    all_celex: set[str] = set()

    # DUMP
    if args.source in ("dump","both","all"):
        if args.dump_dir:
            ce_dump = scan_dump_celex(Path(args.dump_dir), args.dump_glob, args.dump_max_files, sectors)
            all_celex.update(ce_dump)
        else:
            print("[warn] --dump_dir not provided; skipping dump mode.")

    # SPARQL
    if args.source in ("sparql","both","all"):
        ce_sp = fetch_sparql_celex(args.endpoint, sectors, args.year_from, args.year_to,
                                   args.limit, args.sleep_min, args.sleep_max,
                                   args.retries, args.timeout)
        all_celex.update(ce_sp)

    # FORMEX
    if args.source in ("formex","all"):
        if args.formex_dir:
            ce_fx = scan_formex_celex(Path(args.formex_dir), args.formex_glob, sectors)
            if args.verify_inforce_online and ce_fx:
                ce_fx = verify_inforce_online(ce_fx, args.timeout, args.retries,
                                              args.sleep_min, args.sleep_max, args.max_verify)
                print(f"[ok] after verification -> {len(ce_fx):,} CELEX in force")
            all_celex.update(ce_fx)
        else:
            print("[warn] --formex_dir not provided; skipping formex mode.")

    if not all_celex:
        print("[error] No CELEX found. Check your --source and input paths.")
        return

    df = pd.DataFrame(sorted(all_celex), columns=["celex"])
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output_csv, index=False)
    print(f"[ok] wrote {args.output_csv}  ({len(df):,} CELEX)")

# ----------------

if __name__ == "__main__":
    main()
