#!/bin/bash

# --- Configuration ---
# Set the main directory where your files are located.
# Using "$HOME" is a reliable way to refer to your home directory.
WORK_DIR="$HOME/Dropbox/Papers/00_Ideas/EU_Regulation"

# The name of your Python script
PYTHON_SCRIPT="eu_rules_metadata_extractor.py"

# --- Script Logic ---
# This loop iterates through numbers from 0 to 8.
# Using printf ensures that the numbers are zero-padded (00, 01, 02, etc.),
# which is more portable than `seq -w` on some systems like macOS.
for i in {0..7}; do
  # Format the number with a leading zero (e.g., 0 -> 00, 1 -> 01)
  i_padded=$(printf "%02d" $i)

  # Define the input and output filenames for the current iteration
  INPUT_FILE="${WORK_DIR}/output/celex_list-${i_padded}"
  OUTPUT_FILE="${WORK_DIR}/output/metadata-${i_padded}.csv"

  # Check if the input file actually exists before trying to process it
  if [ -f "$INPUT_FILE" ]; then
    echo "Processing ${INPUT_FILE} -> ${OUTPUT_FILE}"
    # Run the python script in the background using '&'
    # This allows the loop to continue and start the next process immediately.
    python "$PYTHON_SCRIPT" --input "$INPUT_FILE" --output "$OUTPUT_FILE"
  else
    echo "Warning: Input file ${INPUT_FILE} not found. Skipping."
  fi
done

cd ${WORK_DIR}/output/ ; rm metadata.csv 2>/dev/null ; cat metadata-*.csv >> metadata.csv ; cd -