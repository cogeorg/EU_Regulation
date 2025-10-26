#!/usr/bin/env python3

"""
Processes a metadata CSV file and a Eurovoc mapping file to generate
two reports:
1. A CELEX-level file flagging finance and agriculture domains,
   including year_passed and year_enacted.
2. A file counting total Eurovoc category occurrences across all years.
"""

import argparse
import csv
import sys
from collections import Counter, defaultdict
from tqdm import tqdm

def load_eurovoc_mapping(mapping_file):
    """
    Loads the Eurovoc mapping file and returns sets of n-grams
    for finance (domain 24) and agriculture (domain 56).
    """
    finance_ngrams = set()
    agriculture_ngrams = set()
    
    print(f"Loading Eurovoc mapping from {mapping_file}...")
    try:
        with open(mapping_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = row.get('eurovoc_domain')
                ngram = row.get('ngram')
                
                if not ngram:
                    continue
                    
                if domain == '24':
                    finance_ngrams.add(ngram)
                elif domain == '56':
                    agriculture_ngrams.add(ngram)
                    
    except FileNotFoundError:
        print(f"Error: Mapping file not found at {mapping_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading mapping file: {e}", file=sys.stderr)
        sys.exit(1)
        
    print(f"Loaded {len(finance_ngrams)} finance n-grams (domain 24).")
    print(f"Loaded {len(agriculture_ngrams)} agriculture n-grams (domain 56).")
    
    return finance_ngrams, agriculture_ngrams

def get_year_from_date(date_data):
    """
    Extracts the YYYY year from a date string.
    Date string might be empty, YYYY-MM-DD, or YYYY-MM-DD | ...
    """
    if not date_data:
        return None
    
    # Get the first date if multiple are listed
    first_date_str = date_data.split('|')[0].strip()
    
    # Check if it looks like a date (YYYY-MM-DD)
    if len(first_date_str) >= 4:
        year = first_date_str[:4]
        if year.isdigit():
            return year
            
    return None

def process_metadata(input_file, finance_ngrams, agriculture_ngrams, output_identifier):
    """
    Processes the main metadata file to generate celex and category reports.
    """
    
    # --- Collectors ---
    # For $output_identifier-celex.csv
    celex_output_data = [] 
    # For $output_identifier-categories.csv
    total_category_counts = Counter()
    
    # --- Stats Counters (for celex file) ---
    total_rows = 0
    domain_found_count = 0
    finance_count = 0
    agriculture_count = 0
    both_count = 0

    print(f"\nProcessing metadata from {input_file}...")
    try:
        with open(input_file, mode='r', encoding='utf-8') as f:
            # Read all rows into memory to use tqdm with a total
            # For very large files, this could be memory-intensive
            # An alternative is to not show the total in tqdm
            try:
                reader = csv.DictReader(f)
                rows = list(reader)
                total_rows = len(rows)
            except Exception as e:
                print(f"Error reading input CSV: {e}. Check file format and encoding.", file=sys.stderr)
                sys.exit(1)

            if total_rows == 0:
                print("Input file is empty. No output will be generated.", file=sys.stderr)
                return

            for row in tqdm(rows, desc="Processing metadata", unit="rows"):
                celex = row.get('celex')
                eurovoc_data = row.get('eurovoc', '')
                date_adoption_data = row.get('date_adoption', '')
                date_in_force_data = row.get('date_in_force', '')

                # Get years
                year_passed = get_year_from_date(date_adoption_data)
                year_enacted = get_year_from_date(date_in_force_data)

                # Get all unique, stripped categories from the 'eurovoc' column
                row_categories = {cat.strip() for cat in eurovoc_data.split('|') if cat.strip()}

                # --- Task 1: CELEX Domain Flagging ---
                
                # Check for overlap between row categories and domain n-gram sets
                is_finance = 1 if not finance_ngrams.isdisjoint(row_categories) else 0
                is_agriculture = 1 if not agriculture_ngrams.isdisjoint(row_categories) else 0
                
                celex_output_data.append({
                    'celex': celex,
                    'year_passed': year_passed,
                    'year_enacted': year_enacted,
                    'is_finance': is_finance, 
                    'is_agriculture': is_agriculture
                })

                # Update stats
                if is_finance:
                    finance_count += 1
                if is_agriculture:
                    agriculture_count += 1
                if is_finance and is_agriculture:
                    both_count += 1
                if is_finance or is_agriculture:
                    domain_found_count += 1

                # --- Task 2: Category Count (Total) ---
                
                if row_categories:
                    total_category_counts.update(row_categories)

    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}", file=sys.stderr)
        sys.exit(1)

    # --- After Loop: Write File 1 and Print Stats ---
    
    celex_file = f"{output_identifier}-celex.csv"
    print(f"\nWriting CELEX domain data to {celex_file}...")
    try:
        with open(celex_file, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['celex', 'year_passed', 'year_enacted', 'is_finance', 'is_agriculture']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(celex_output_data)
        print(f"Successfully wrote {celex_file}")
    except IOError as e:
        print(f"Error writing to file {celex_file}: {e}", file=sys.stderr)

    # Print debug stats for Task 1
    print("\n--- CELEX Domain Statistics ---")
    print(f"Total rows processed: {total_rows}")
    print(f"Rows with finance or agriculture domain: {domain_found_count}")
    print(f"Rows with NO finance or agriculture domain: {total_rows - domain_found_count}")
    print("---------------------------------")
    print(f"Total 'is_finance' == 1: {finance_count}")
    print(f"Total 'is_agriculture' == 1: {agriculture_count}")
    print(f"Total with BOTH == 1: {both_count}")
    print("---------------------------------")


    # --- Write File 2 ---
    
    categories_file = f"{output_identifier}-categories.csv"
    print(f"\nWriting total category counts to {categories_file}...")
    try:
        with open(categories_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['eurovoc_category', 'count'])
            writer.writeheader()
            
            # Sort by category name for consistent output
            for category in sorted(total_category_counts.keys()):
                count = total_category_counts[category]
                writer.writerow({
                    'eurovoc_category': category, 
                    'count': count
                })
        print(f"Successfully wrote {categories_file}")
    except IOError as e:
        print(f"Error writing to file {categories_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred while writing {categories_file}: {e}", file=sys.stderr)


def main():
    """
    Parses command-line arguments and runs the processing functions.
    """
    parser = argparse.ArgumentParser(
        description="Prepare metadata by mapping Eurovoc domains and counting categories."
    )
    
    parser.add_argument(
        '--input_file',
        required=True,
        help="Path to the input metadata CSV file."
    )
    
    parser.add_argument(
        '--eurovoc_mapping',
        required=True,
        help="Path to the Eurovoc mapping CSV file."
    )
    
    parser.add_argument(
        '--output_identifier',
        required=True,
        help="A prefix for the output files (e.g., 'prepared_data')."
    )
    
    args = parser.parse_args()
    
    # Load mappings
    finance_ngrams, agriculture_ngrams = load_eurovoc_mapping(args.eurovoc_mapping)
    
    # Process data and write files
    process_metadata(
        args.input_file, 
        finance_ngrams, 
        agriculture_ngrams, 
        args.output_identifier
    )
    
    print("\nProcessing complete.")

if __name__ == "__main__":
    main()

