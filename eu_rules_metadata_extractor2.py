#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parallel EURLEX/CELLAR metadata extractor with tqdm progress.

- Parallelizes per-CELEX metadata retrieval (SPARQL + optional EUR-Lex title scrape).
- Shows a single tqdm progress bar that advances as futures complete.
- Adds --workers to control concurrency (default 16; set 1 for single-threaded).
"""

import csv
import argparse
import sys
import os
import time
import random
from os.path import exists
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

from tqdm.auto import tqdm
import requests
from bs4 import BeautifulSoup

from SPARQLWrapper import SPARQLWrapper, TURTLE, JSON
from rdflib import Graph, Literal

# ----------------------------
# CLI
# ----------------------------
argParser = argparse.ArgumentParser(
    description='EURLEX PDF and HTML legislative documents metadata downloader (parallel + tqdm)'
)
required = argParser.add_argument_group('required arguments')
required.add_argument(
    "-in", "--input", required=True,
    help="Path to input CSV file (single column, no header, list of CELEX identifiers)."
)
required.add_argument(
    "-out", "--output", required=True,
    help="Path to a CSV file to store the metadata, e.g. 'path/to/metadata.csv'."
)
argParser.add_argument(
    "--workers", type=int, default=16,
    help="Number of parallel workers (I/O-bound, threads). Use 1 for single-threaded. Default: 16"
)
argParser.add_argument(
    "--timeout", type=int, default=60,
    help="HTTP/SPARQL timeout in seconds (default: 60)"
)
args = argParser.parse_args()

if args.input is None:
    sys.exit('No input file specified. Type "python eu_rules_metadata_extractor_parallel_tqdm.py -h" for help.')

if args.output is None:
    sys.exit('No output file specified. Type "python eu_rules_metadata_extractor_parallel_tqdm.py -h" for help.')

IN_CELEX_FILE = str(args.input)
OUT_METADATA_FILE = str(args.output)
MAX_WORKERS = max(1, int(args.workers))
TIMEOUT = int(args.timeout)

# ----------------------------
# Constants & mappings
# ----------------------------
SPARQL_ENDPOINT_URL = "http://publications.europa.eu/webapi/rdf/sparql"
CDM_PREFIX = "http://publications.europa.eu/ontology/cdm#"
XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#"

property_mapping = {
    "celex"            : f"{CDM_PREFIX}resource_legal_id_celex",
    "author"           : f"{CDM_PREFIX}work_created_by_agent",
    "responsible_body" : f"{CDM_PREFIX}regulation_service_responsible",
    "form"             : f"{CDM_PREFIX}resource_legal_type",
    "title"            : f"{CDM_PREFIX}work_title",
    "addressee"        : f"{CDM_PREFIX}resource_legal_addresses_country",
    "date_adoption"    : f"{CDM_PREFIX}work_date_document",
    "date_in_force"    : f"{CDM_PREFIX}resource_legal_date_entry-into-force",
    "date_end_validity": f"{CDM_PREFIX}resource_legal_date_end-of-validity",
    "directory_code"   : f"{CDM_PREFIX}resource_legal_is_about_concept_directory-code",
    "procedure_code"   : f"{CDM_PREFIX}procedure_code_interinstitutional_basis_legal",
    "eurovoc"          : f"{CDM_PREFIX}work_is_about_concept_eurovoc",
    "subject_matters"  : f"{CDM_PREFIX}resource_legal_is_about_subject-matter"
}

metadata_header_row = [
    'celex', 'author', 'responsible_body', 'form', 'title', 'addressee',
    'date_adoption', 'date_in_force', 'date_end_validity',
    'directory_code', 'procedure_code', 'eurovoc', 'subject_matters'
]

# ----------------------------
# Helpers
# ----------------------------
def read_celex_list(path: str) -> List[str]:
    celex_nums: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.reader(f, delimiter=","):
            if not row:
                continue
            cell = row[0].strip()
            if not cell or cell.lower() == "celex":
                continue
            celex_nums.append(cell)
    return celex_nums

def backoff_sleep(attempt: int, base: float = 0.8, cap: float = 8.0) -> None:
    # Exponential backoff with jitter
    delay = min(cap, base * (2 ** attempt)) + random.uniform(0, 0.3)
    time.sleep(delay)

def get_title_fallback(celex: str, timeout: int = TIMEOUT) -> str:
    """Scrape the EN title from EUR-Lex as a fallback."""
    url = f"https://eur-lex.europa.eu/legal-content/EN/ALL/?uri=CELEX:{celex}"
    for attempt in range(4):
        try:
            with requests.Session() as s:
                s.headers.update({"User-Agent": "EuroMetaBot/1.0 (+https://example.com)"})
                r = s.get(url, timeout=timeout)
                if r.status_code >= 500:
                    backoff_sleep(attempt)
                    continue
                soup = BeautifulSoup(r.content, 'lxml-xml')
                res = soup.find('meta', attrs={"property": "eli:title", "lang": "en"})
                return res.get('content') if res else ''
        except Exception:
            backoff_sleep(attempt)
    return ''

def execute_sparql_turtle(sparql: SPARQLWrapper, query: str) -> str:
    """Run a SPARQL CONSTRUCT query, return data as TURTLE str; -1 sentinel on failure."""
    sparql.setReturnFormat(TURTLE)
    sparql.setQuery(query)
    for attempt in range(4):
        try:
            res = sparql.query().convert()
            return res.decode("utf-8")
        except Exception:
            backoff_sleep(attempt)
    return -1  # sentinel

def get_string_label(sparql: SPARQLWrapper, uri: str, pred: str) -> str:
    """Dereference a URI to its English skos:prefLabel; specialized handling for directory_code."""
    u = uri
    if pred == 'directory_code':
        # keep the most general segment (first two digits)
        parts = uri.split('/')
        if parts:
            dc = parts[-1][:2]
            u = '/'.join(parts[:-1] + [dc])

    q = f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    SELECT (STR(?o) AS ?label)
    WHERE {{
        <{u}> skos:prefLabel ?o .
        FILTER (lang(?o) = "en")
    }}
    """
    sparql.setReturnFormat(JSON)
    sparql.setQuery(q)
    for attempt in range(4):
        try:
            res = sparql.query().convert()
            bindings = res.get("results", {}).get("bindings", [])
            if not bindings:
                return ''
            return bindings[0]["label"]["value"]
        except Exception:
            backoff_sleep(attempt)
    return ''

# ----------------------------
# Core per-document worker
# ----------------------------
def process_celex(celex_num: str, endpoint_url: str, timeout: int) -> List[str]:
    """Fetch and assemble one row of metadata for a given CELEX."""
    # One SPARQLWrapper per thread to avoid thread-safety issues
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setTimeout(timeout)
    sparql.setReturnFormat(TURTLE)

    metadata_query = f"""
    PREFIX cdm: <{CDM_PREFIX}>
    PREFIX xsd: <{XSD_PREFIX}>

    CONSTRUCT {{ ?s ?p ?o }}
    WHERE {{
      SELECT DISTINCT ?s ?p ?o WHERE {{
         ?s cdm:resource_legal_id_celex "{celex_num}"^^xsd:string .
         ?s ?p ?o .
      }}
    }}
    """

    current_row: Dict[str, Any] = {
        "celex": [], "author": [], "responsible_body": [], "form": [],
        "title": [], "addressee": [], "date_adoption": [], "date_in_force": [],
        "date_end_validity": [], "directory_code": [], "procedure_code": [],
        "eurovoc": [], "subject_matters": []
    }

    turtle = execute_sparql_turtle(sparql, metadata_query)
    if turtle == -1:
        # Return a minimal row with the celex, leave others blank
        return [celex_num] + [''] * (len(metadata_header_row) - 1)

    # Parse the RDF, collect values
    g = Graph().parse(data=str(turtle), format='turtle')
    pm_values = set(property_mapping.values())
    for s, p, o in g.triples((None, None, None)):
        p_str = str(p)
        if p_str in pm_values:
            # Which key is this predicate?
            key = next(k for k, v in property_mapping.items() if v == p_str)
            if isinstance(o, Literal):
                current_row[key].append(str(o))
            else:
                lbl = get_string_label(sparql, str(o), key)
                current_row[key].append(lbl)

    # Normalize field lists to strings
    for k, v in current_row.items():
        if not v:
            current_row[k] = ''
        elif len(v) == 1:
            current_row[k] = v[0]
        else:
            # de-duplicate and join with ' | ' like the original
            current_row[k] = ' | '.join(sorted(set(v)))

    # Fallback for missing title
    if not current_row['title']:
        current_row['title'] = get_title_fallback(celex_num, timeout=timeout)

    # Return in header order
    return [current_row[f] if f != 'celex' else celex_num for f in metadata_header_row]

# ----------------------------
# Orchestrators (parallel + sequential)
# ----------------------------
def get_metadata_parallel(celex_nums: List[str], endpoint_url: str, timeout: int, max_workers: int) -> List[List[str]]:
    """Parallel path with a single tqdm bar that ticks as futures complete."""
    rows: List[List[str]] = [metadata_header_row]  # header first
    if not celex_nums:
        return rows

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_celex, c, endpoint_url, timeout): c for c in celex_nums}
        with tqdm(total=len(futures), desc="Fetching", unit="doc", dynamic_ncols=True) as pbar:
            for fut in as_completed(futures):
                celex_id = futures[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    tqdm.write(f"[WARN] {celex_id}: {e}")
                    row = [celex_id] + [''] * (len(metadata_header_row) - 1)
                rows.append(row)
                pbar.update(1)
    return rows

def get_metadata_sequential(celex_nums: List[str], endpoint_url: str, timeout: int) -> List[List[str]]:
    """Single-thread path with per-item tqdm bar (classic)."""
    rows: List[List[str]] = [metadata_header_row]
    if not celex_nums:
        return rows

    with tqdm(total=len(celex_nums), desc="CELEX", unit="doc", dynamic_ncols=True) as pbar:
        for c in celex_nums:
            try:
                rows.append(process_celex(c, endpoint_url, timeout))
            except Exception as e:
                tqdm.write(f"[WARN] {c}: {e}")
                rows.append([c] + [''] * (len(metadata_header_row) - 1))
            pbar.update(1)
    return rows

# ----------------------------
# Main
# ----------------------------
def main():
    celex_nums = read_celex_list(IN_CELEX_FILE)

    st = time.time()
    if MAX_WORKERS == 1:
        metadata = get_metadata_sequential(celex_nums, SPARQL_ENDPOINT_URL, TIMEOUT)
    else:
        metadata = get_metadata_parallel(celex_nums, SPARQL_ENDPOINT_URL, TIMEOUT, MAX_WORKERS)
    et = time.time()

    # Save CSV
    os.makedirs(os.path.dirname(OUT_METADATA_FILE) or ".", exist_ok=True)
    with open(OUT_METADATA_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerows(metadata)

    print(f"Processed {len(celex_nums)} documents in {et - st:.1f}s "
          f"({(et - st)/max(1,len(celex_nums)):.2f} s/doc; workers={MAX_WORKERS}).")

if __name__ == "__main__":
    main()
