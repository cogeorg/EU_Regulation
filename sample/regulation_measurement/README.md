# Legacy regulation-measurement workflow

This directory contains the earlier EU regulation-measurement scripts that previously
lived beside their generated data in Dropbox. The data now live outside Git at:

```text
~/Dropbox/Projects/EU_Regulation/Data/legacy/regulation_measurement/
```

That directory preserves the original `eurlex_downloads/`, `eurlex_processed/`, `htmls/`,
`german_laws/`, `input/`, and `output/` trees plus the top-level analytical CSV files.

Run the word-count workflow from any directory with:

```bash
./sample/regulation_measurement/01_do_analysis.sh
```

Override the data location with `EU_REGULATION_LEGACY_DATA_DIR`. Python dependencies are
covered by [`../requirements.txt`](../requirements.txt); the R downloader additionally
requires the `eurlex` package. The legacy RegData dashboard is a small, data-free HTML
output in the shared Dropbox dashboard folder and asks the viewer to select a CSV locally.
