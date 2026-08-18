#!/usr/bin/env python3

import argparse
import csv
import re
from pathlib import Path
import sys
from tqdm import tqdm

def count_regdata_words(text):
    """
    Count occurrences of RegData restriction words and total words in text.

    Args:
        text: Input text string

    Returns:
        dict: Counts for each restriction word and total word count
    """
    # Convert to lowercase for case-insensitive matching
    text_lower = text.lower()

    # Define the five RegData words
    words = {
        'shall': r'\bshall\b',
        'must': r'\bmust\b',
        'may not': r'\bmay\s+not\b',
        'required': r'\brequired\b',
        'prohibited': r'\bprohibited\b'
    }

    # Count occurrences using regex for word boundaries
    counts = {}
    for word, pattern in words.items():
        matches = re.findall(pattern, text_lower)
        counts[word] = len(matches)

    # Count total words
    # Using regex to match word boundaries for consistency
    total_words = len(re.findall(r'\b\w+\b', text))
    counts['total_words'] = total_words

    return counts

def load_legal_info(legal_info_file):
    """
    Load legal information from CSV file.

    Args:
        legal_info_file: Path to CSV file

    Returns:
        list: List of dictionaries with legal info
        set: Set of CELEX identifiers
    """
    legal_info = []
    celex_set = set()

    try:
        with open(legal_info_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                legal_info.append(row)
                if 'celex' in row:
                    celex_set.add(row['celex'])
    except Exception as e:
        print(f"Error reading legal info file: {str(e)}", file=sys.stderr)
        sys.exit(1)

    return legal_info, celex_set

def process_files(input_folder, output_file, legal_info_file):
    """
    Process all text files and merge with legal information.

    Args:
        input_folder: Path to folder containing text files
        output_file: Path to output CSV file
        legal_info_file: Path to legal information CSV file
    """
    input_path = Path(input_folder)

    # Validate input folder
    if not input_path.exists():
        print(f"Error: Input folder '{input_path}' does not exist", file=sys.stderr)
        sys.exit(1)

    # Load legal information
    print("Loading legal information...")
    legal_info, celex_from_legal = load_legal_info(legal_info_file)
    print(f"Loaded {len(legal_info)} legal acts from CSV")

    # Get all text files
    txt_files = list(input_path.glob('*.txt'))
    print(f"Found {len(txt_files)} text files in {input_path}")

    # Create mapping of CELEX to text file paths
    celex_to_file = {}
    celex_from_files = set()
    for txt_file in txt_files:
        celex = txt_file.stem  # filename without extension
        celex_to_file[celex] = txt_file
        celex_from_files.add(celex)

    # Calculate coverage statistics
    matched_celex = celex_from_legal & celex_from_files
    only_in_legal = celex_from_legal - celex_from_files
    only_in_files = celex_from_files - celex_from_legal

    print("\n=== Coverage Statistics ===")
    print(f"Total unique CELEX in legal info: {len(celex_from_legal)}")
    print(f"Total unique CELEX in text files: {len(celex_from_files)}")
    print(f"Matched CELEX (in both): {len(matched_celex)}")
    print(f"CELEX only in legal info (no text file): {len(only_in_legal)}")
    print(f"CELEX only in text files (no legal info): {len(only_in_files)}")
    print("==========================\n")

    # Process text files and build counts dictionary
    print("Processing text files...")
    celex_to_counts = {}

    for txt_file in tqdm(txt_files, desc="Counting RegData words", unit="file"):
        celex = txt_file.stem
        try:
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
            counts = count_regdata_words(content)
            celex_to_counts[celex] = counts
        except Exception as e:
            print(f"\nError processing {txt_file.name}: {str(e)}", file=sys.stderr)
            celex_to_counts[celex] = {
                'shall': 0,
                'must': 0,
                'may not': 0,
                'required': 0,
                'prohibited': 0,
                'total_words': 0
            }

    # Prepare output data
    print("\nMerging data...")
    output_data = []

    # First, add all rows from legal_info
    for row in tqdm(legal_info, desc="Processing legal info rows"):
        output_row = row.copy()
        celex = row.get('celex', '')

        if celex and celex in celex_to_counts:
            # Add counts if we have them
            output_row.update(celex_to_counts[celex])
        else:
            # Add zero counts if no text file
            output_row.update({
                'shall': 0,
                'must': 0,
                'may not': 0,
                'required': 0,
                'prohibited': 0,
                'total_words': 0
            })
        output_data.append(output_row)

    # Then, add rows for text files without legal info
    for celex in tqdm(only_in_files, desc="Processing unmatched text files"):
        output_row = {
            'work': '',
            'type': '',
            'celex': celex,
            'date': '',
            'force': '',
            'downloaded': ''
        }
        output_row.update(celex_to_counts[celex])
        output_data.append(output_row)

    # Determine fieldnames (preserve order from legal_info, add RegData words and total_words)
    if legal_info and len(legal_info) > 0:
        fieldnames = list(legal_info[0].keys())
    else:
        fieldnames = ['work', 'type', 'celex', 'date', 'force', 'downloaded']

    # Add RegData words and total_words if not already present
    regdata_words = ['shall', 'must', 'may not', 'required', 'prohibited']
    for word in regdata_words:
        if word not in fieldnames:
            fieldnames.append(word)

    # Add total_words column
    if 'total_words' not in fieldnames:
        fieldnames.append('total_words')

    # Write results to CSV
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(output_data)

    print(f"\nResults saved to: {output_path}")

    # Print summary statistics
    total_counts = {word: 0 for word in regdata_words}
    total_word_count = 0
    for row in output_data:
        for word in regdata_words:
            total_counts[word] += int(row.get(word, 0))
        total_word_count += int(row.get('total_words', 0))

    print("\n=== Total Word Counts ===")
    for word, count in total_counts.items():
        print(f"  {word}: {count:,}")
    print(f"  total words: {total_word_count:,}")
    print("========================")

def main():
    parser = argparse.ArgumentParser(
        description='Count RegData restriction words and merge with legal information'
    )
    parser.add_argument(
        '--input_folder',
        required=True,
        help='Input folder containing text files'
    )
    parser.add_argument(
        '--output_file',
        required=True,
        help='Output CSV file path'
    )
    parser.add_argument(
        '--legal_info',
        required=True,
        help='Legal information CSV file path'
    )

    args = parser.parse_args()

    process_files(args.input_folder, args.output_file, args.legal_info)

if __name__ == "__main__":
    main()