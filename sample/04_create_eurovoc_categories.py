#!/usr/bin/env python3

"""
Processes a metadata CSV file to extract, count, and sort n-grams
from the 'eurovoc' column.

The 'eurovoc' column is expected to contain one or more phrases
delimited by '|'.

The script outputs a new CSV file with the following columns:
- ngram: The unique n-gram (phrase).
- word_count: The number of words in the n-gram.
- char_length: The number of characters in the n-gram.
- count: The frequency of the n-gram in the metadata file.

The output is sorted by word_count (descending), then char_length (descending),
and finally alphabetically by the n-gram itself (ascending).
"""

import argparse
import csv
from collections import Counter

def process_eurovoc_metadata(metadata_file, output_file):
    """
    Reads the metadata file, processes the 'eurovoc' column,
    and writes the sorted n-gram counts to the output file.
    """
    # Use a Counter to store the frequency of each n-gram
    ngram_counts = Counter()
    
    print(f"Reading metadata from {metadata_file}...")

    try:
        with open(metadata_file, mode='r', encoding='utf-8') as f:
            # Use DictReader to easily access the 'eurovoc' column
            reader = csv.DictReader(f)
            
            for row in reader:
                # Check if the 'eurovoc' column exists and is not empty
                eurovoc_data = row.get('eurovoc')
                if eurovoc_data:
                    # Split the column content by the '|' delimiter
                    phrases = eurovoc_data.split('|')
                    
                    for phrase in phrases:
                        # Clean up leading/trailing whitespace
                        cleaned_phrase = phrase.strip()
                        
                        # Only count non-empty phrases
                        if cleaned_phrase:
                            ngram_counts[cleaned_phrase] += 1

        print(f"Found {len(ngram_counts)} unique n-grams.")

        # --- Data Preparation for Sorting ---
        
        output_data = []
        for ngram, count in ngram_counts.items():
            # Calculate word count (splitting by space)
            word_count = len(ngram.split())
            # Calculate character length
            char_length = len(ngram)
            
            output_data.append({
                'ngram': ngram,
                'word_count': word_count,
                'char_length': char_length,
                'count': count
            })

        # --- Sorting ---
        # Sort by: 1. word_count (desc), 2. char_length (desc), 3. ngram (asc)
        sorted_data = sorted(
            output_data,
            key=lambda item: (-item['word_count'], -item['char_length'], item['ngram'])
        )

        # --- Writing Output ---
        
        print(f"Writing sorted data to {output_file}...")
        
        with open(output_file, mode='w', encoding='utf-8', newline='') as f:
            # Define the fieldnames for the output CSV
            fieldnames = ['ngram', 'word_count', 'char_length', 'count']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # Write the header row
            writer.writeheader()
            # Write all the sorted data
            writer.writerows(sorted_data)
            
        print("Processing complete.")

    except FileNotFoundError:
        print(f"Error: The file {metadata_file} was not found.")
    except KeyError:
        print("Error: A 'eurovoc' column was not found in the metadata file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def main():
    """
    Parses command-line arguments and runs the processing function.
    """
    parser = argparse.ArgumentParser(
        description="Extract and count n-grams from the 'eurovoc' column of a CSV file."
    )
    
    parser.add_argument(
        '--metadata',
        required=True,
        help="Path to the input metadata CSV file."
    )
    
    parser.add_argument(
        '--output',
        required=True,
        help="Path for the output CSV file."
    )
    
    args = parser.parse_args()
    
    process_eurovoc_metadata(args.metadata, args.output)

if __name__ == "__main__":
    main()

