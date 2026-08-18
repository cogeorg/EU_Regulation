# AGENTS.md — Working Agreements for this Repository

## Project
Build the most complete repository of EU, German federal (Bund), and German state (Länder)
legal texts possible. Source overview lives in
`~/Dropbox/Papers/00_Ideas/EU_Regulation/administration/260728_Datenquellen aus Gesetzgebung und Rechtsprechung.docx`.

## User preferences (keep up to date)
1. **Logging (mandatory):** Every query from the user, and how it is addressed by any agent
   (including subagents), MUST be recorded in
   `~/Dropbox/Papers/00_Ideas/EU_Regulation/administration/LOGBOOK.md` with a date-time
   timestamp (format: `YYYY-MM-DD HH:MM TZ`). Record: the query (verbatim or faithful
   summary), the actions taken, and the outcome.
2. **Preferred formats:** `.html`, `.json`, or `.txt` preferred; `.pdf`, `.docx`, XML or
   anything else is acceptable if nothing better is available. Be inclusive: when in doubt,
   mark/download too much rather than miss documents.
3. **Dashboard:** light-themed, simple, static HTML
   (`~/Dropbox/Papers/00_Ideas/EU_Regulation/dashboard/index.html`), gives an overview of
   all sources and download status. Run `python3 full/update_dashboard_summary.py` after
   changing data so its compact external-data description stays current. Do not copy raw
   data or full manifests into the shared dashboard folder.
4. **Reproducibility:** crawlers are launched via a shell script; usage documented in `README.md`.
5. **Agentic workflow:** one crawler agent per source, run in parallel; each crawler is
   adversarially audited by a separate agent before being trusted.

## Layout
- `~/Dropbox/Projects/EU_Regulation/Data/{eu,bund,laender}/{verfassungen,stammgesetze}/`
  — downloaded legal texts and their authoritative manifests.
  Länder folders are further split per state code (bw, by, be, bb, hb, hh, he, mv, ni, nw,
  rp, sl, sn, st, sh, th).
- `full/crawlers/` — one crawler per source, plus shared helpers.
- `~/Dropbox/Papers/00_Ideas/EU_Regulation/{dashboard,administration,logs}/` —
  minimal co-author-facing dashboard, project administration, and runtime logs. Research
  data and data-like analytical outputs belong under the Projects data tree, not here.
- `~/Dropbox/Projects/EU_Regulation/Data/legacy/regulation_measurement/` — preserved
  raw, processed, input, and derived data from the earlier sample workflow.
- `sample/` — legacy sample-based pipeline (kept as is).

The defaults can be overridden with `EU_REGULATION_DATA_DIR`,
`EU_REGULATION_SHARED_DIR`, and `EU_REGULATION_LOG_DIR`; never assume collaborators use
the same absolute Dropbox path.

## Crawler conventions
- Python 3, standard library + `requests` (+ `lxml`/`beautifulsoup4` if needed).
- Idempotent and resumable: skip files that already exist; safe to re-run.
- Each crawler writes a `manifest.json` next to its data (list of downloaded docs with
  source URL, title, format, timestamp).
- Runtime and error logs belong under the configured shared log directory, never beside
  the raw data or inside the Git checkout.
- Polite crawling: identify with a User-Agent, throttle requests, obey robots.txt where
  it applies to bulk access.
