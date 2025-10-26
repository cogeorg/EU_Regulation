#!/usr/bin/env python3

"""
Analyzes HTML files corresponding to a given CELEX list to count the
occurrence of specific regulatory words ("shall", "must", "may not",
"required", "prohibited").

This script requires the 'beautifulsoup4' library.
Install it using: pip install beautifulsoup4
"""

import argparse
import csv
import os
import re
import sys
from bs4 import BeautifulSoup
from tqdm import tqdm

def load_celex_list(celex_file):
    """
    Loads the CELEX list and filters for rows where 'is_finance'
    or 'is_agriculture' is '1'.
    
    Returns a list of dictionaries, each containing celex, 
    year_passed, year_enacted, is_finance, and is_agriculture 
    for processing.
    """
    celex_to_process = []
    print(f"Loading CELEX list from {celex_file}...")
    try:
        with open(celex_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                is_finance = row.get('is_finance', '0')
                is_agriculture = row.get('is_agriculture', '0')
                
                if is_finance == '1' or is_agriculture == '1':
                    celex = row.get('celex')
                    if celex:
                        celex_info = {
                            'celex': celex,
                            'year_passed': row.get('year_passed'),
                            'year_enacted': row.get('year_enacted'),
                            'is_finance': is_finance,
                            'is_agriculture': is_agriculture
                        }
                        celex_to_process.append(celex_info)
                        
    except FileNotFoundError:
        print(f"Error: CELEX file not found at {celex_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CELEX file: {e}", file=sys.stderr)
        sys.exit(1)
        
    print(f"Loaded {len(celex_to_process)} CELEX identifiers marked for processing.")
    return celex_to_process

def analyze_html_file(file_path, regex_patterns):
    """
    Analyzes a single HTML file.
    
    Returns a tuple:
    (total_word_count, counts_dict, total_obligation_count, error_message)
    """
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            content = f.read()
            
        # Use BeautifulSoup to strip HTML tags
        soup = BeautifulSoup(content, 'html.parser')
        clean_text = soup.get_text()
        
        # 1. Get total word count
        total_word_count = len(clean_text.split())
        
        # 2. Count obligation words (case-insensitive)
        counts_dict = {}
        total_obligation_count = 0
        
        for key, pattern in regex_patterns.items():
            count = len(pattern.findall(clean_text))
            counts_dict[key] = count
            total_obligation_count += count
            
        return total_word_count, counts_dict, total_obligation_count, None

    except FileNotFoundError:
        return 0, {}, 0, "File not found"
    except Exception as e:
        return 0, {}, 0, f"Error processing file: {e}"

def main():
    """
    Main function to parse arguments and coordinate processing.
    """
    parser = argparse.ArgumentParser(
        description="Measure word counts in prepared regulatory data."
    )
    parser.add_argument(
        '--celex_list',
        required=True,
        help="Input CSV file with celex, year_passed, year_enacted, is_finance, is_agriculture."
    )
    parser.add_argument(
        '--html_directory',
        required=True,
        help="Directory containing HTML files named [celex].html."
    )
    parser.add_argument(
        '--output_file',
        required=True,
        help="Path for the output CSV file."
    )
    
    args = parser.parse_args()

    # 1. Define word patterns and compile regex
    # We use re.IGNORECASE for case-insensitive matching
    word_patterns = {
        'shall': r'\bshall\b',
        'must': r'\bmust\b',
        'may_not': r'\bmay\s+not\b', # Use_key_compatible_with_header
        'required': r'\brequired\b',
        'prohibited': r'\bprohibited\b'
    }
    
    regex_patterns = {
        key: re.compile(pattern, re.IGNORECASE) 
        for key, pattern in word_patterns.items()
    }
    
    # 2. Load the list of CELEX numbers to process
    celex_list = load_celex_list(args.celex_list)
    
    if not celex_list:
        print("No CELEX identifiers to process. Exiting.")
        return

    # 3. Process each HTML file
    output_data = []
    files_not_found = 0
    files_processed = 0
    
    print(f"\nProcessing {len(celex_list)} HTML files from {args.html_directory}...")
    
    for celex_info in tqdm(celex_list, desc="Analyzing HTML", unit="file"):
        celex = celex_info['celex']
        file_path = os.path.join(args.html_directory, f"{celex}.html")
        
        total_words, counts, total_obligation, error = analyze_html_file(
            file_path, regex_patterns
        )
        
        if error == "File not found":
            files_not_found += 1
        elif error:
            print(f"Warning: Could not process {file_path}. Error: {error}", file=sys.stderr)
        else:
            files_processed += 1
            
        output_row = {
            'celex': celex,
            'year_passed': celex_info['year_passed'],
            'year_enacted': celex_info['year_enacted'],
            'is_finance': celex_info['is_finance'],
            'is_agriculture': celex_info['is_agriculture'],
            'total_word_count': total_words,
            'count_shall': counts.get('shall', 0),
            'count_must': counts.get('must', 0),
            'count_may_not': counts.get('may_not', 0),
            'count_required': counts.get('required', 0),
            'count_prohibited': counts.get('prohibited', 0),
            'total_obligation_word_count': total_obligation
        }
        output_data.append(output_row)
        
    print(f"Processing complete. {files_processed} files analyzed.")
    if files_not_found > 0:
        print(f"Warning: {files_not_found} files listed in celex_list were not found in the HTML directory.", file=sys.stderr)

    # 4. Write output file
    print(f"\nWriting results to {args.output_file}...")
    try:
        fieldnames = [
            'celex', 
            'year_passed',
            'year_enacted',
            'is_finance',
            'is_agriculture',
            'total_word_count', 
            'count_shall', 
            'count_must', 
            'count_may_not', 
            'count_required', 
            'count_prohibited', # <-- This was the typo, changed from 'prohibited'
            'total_obligation_word_count'
        ]
        
        with open(args.output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_data)
            
        print("Successfully wrote output file.")
        
    except IOError as e:
        print(f"Error writing output file: {e}", file=sys.stderr)
        
if __name__ == "__main__":
    # Check for bs4 import
    try:
        import bs4
    except ImportError:
        print("Error: The 'beautifulsoup4' library is required but not installed.", file=sys.stderr)
        print("Please install it using: pip install beautifulsoup4", file=sys.stderr)
        sys.exit(1)
        
    main()

