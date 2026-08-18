#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LEGACY_DATA_DIR="${EU_REGULATION_LEGACY_DATA_DIR:-$HOME/Dropbox/Projects/EU_Regulation/Data/legacy/regulation_measurement}"
PYTHON="${PYTHON:-python3}"

# STEP 0 - PREPROCESSING

# mkdir -p "$LEGACY_DATA_DIR/eurlex_processed/"
# for pdf in "$LEGACY_DATA_DIR"/eurlex_downloads/*.pdf; do
#     [ -f "$pdf" ] || continue
#     pdftotext "$pdf" "$LEGACY_DATA_DIR/eurlex_processed/$(basename "$pdf" .pdf).txt"
# done

# STEP 1 - Count RegData restriction words
"$PYTHON" "$SCRIPT_DIR/20_word_counter_regdata.py" \
    --input_folder "$LEGACY_DATA_DIR/eurlex_processed/" \
    --output_file "$LEGACY_DATA_DIR/regdata_analysis.csv" \
    --legal_info "$LEGACY_DATA_DIR/list_of_legal_acts.csv"

# STEP 2 - GENERATE THE DASHBOARD
# "$PYTHON" "$SCRIPT_DIR/30_generate_dashboard.py" --output /path/to/regdata_dashboard.html
