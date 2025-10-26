#!/usr/bin/env python3

"""
Performs an aggregation on regulation metrics.

This script reads a word count CSV and a readability CSV, merges them,
and aggregates data by year, calculating totals, averages, and 
standard deviations for finance and agriculture categories.
"""

import argparse
import csv
import sys
import statistics
from collections import defaultdict

def calculate_stats(values):
    """
    Calculates count, total, mean, and standard deviation for a list of values.
    Handles empty lists and lists with a single value.
    """
    count = len(values)
    
    if count == 0:
        return 0, 0.0, 0.0, 0.0
        
    total = sum(values)
    mean = total / count
    
    # Standard deviation requires at least 2 data points
    if count < 2:
        stdev = 0.0
    else:
        stdev = statistics.stdev(values)
        
    return count, total, mean, stdev

def aggregate_data(word_count_file, readability_file, output_file):
    """
    Reads the input CSVs, merges data, aggregates by year, 
    and writes the results to the output CSV.
    """
    
    # 1. Load readability scores into a lookup dictionary
    print(f"Loading readability data from {readability_file}...")
    readability_scores = {}
    try:
        with open(readability_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                celex = row.get('celex')
                if not celex:
                    continue
                try:
                    readability_scores[celex] = float(row.get('flesch_reading_ease', 0.0))
                except (ValueError, TypeError):
                    readability_scores[celex] = 0.0 # Default if conversion fails
        print(f"Loaded readability scores for {len(readability_scores)} CELEX identifiers.")
    except FileNotFoundError:
        print(f"Error: Readability file not found at {readability_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading readability file: {e}", file=sys.stderr)
        sys.exit(1)


    # 2. Use defaultdict to store lists of values for std dev calculation
    year_stats = defaultdict(lambda: {
        'obl_finance_values': [],
        'obl_agri_values': [],
        'read_finance_values': [],
        'read_agri_values': []
    })

    print(f"Reading and aggregating word count data from {word_count_file}...")
    try:
        with open(word_count_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            processed_rows = 0
            missing_readability = 0
            
            for row in reader:
                # 1. Determine the year (prioritize 'year_passed')
                year_str = row.get('year_passed') or row.get('year_enacted')
                if not year_str:
                    continue # Skip if no year is found
                
                try:
                    year = int(year_str)
                except ValueError:
                    continue # Skip if year is not a valid integer

                celex = row.get('celex')
                if not celex:
                    continue

                # 2. Get obligation count
                try:
                    obligation_count = int(row.get('total_obligation_word_count', 0))
                except ValueError:
                    obligation_count = 0
                
                # 3. Get readability score from our lookup
                readability_score = readability_scores.get(celex)
                if readability_score is None:
                    missing_readability += 1
                
                # 4. Check finance/agriculture flags
                is_finance = row.get('is_finance') == '1'
                is_agriculture = row.get('is_agriculture') == '1'

                # 5. Aggregate lists of values
                if is_finance:
                    year_stats[year]['obl_finance_values'].append(obligation_count)
                    if readability_score is not None:
                        year_stats[year]['read_finance_values'].append(readability_score)
                
                if is_agriculture:
                    year_stats[year]['obl_agri_values'].append(obligation_count)
                    if readability_score is not None:
                        year_stats[year]['read_agri_values'].append(readability_score)
                
                processed_rows += 1
        
        print(f"Successfully processed {processed_rows} rows from word count file.")
        if missing_readability > 0:
            print(f"Warning: {missing_readability} rows had no matching readability score.")

    except FileNotFoundError:
        print(f"Error: Word count file not found at {word_count_file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading word count file: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 3. Prepare data for output ---
    output_data = []
    
    print("Calculating final aggregates, averages, and standard deviations...")
    
    # Sort by year for a clean output file
    for year in sorted(year_stats.keys()):
        stats = year_stats[year]
        
        # Calculate stats for all four lists
        (obl_fin_count, obl_fin_total, obl_fin_avg, obl_fin_std) = calculate_stats(stats['obl_finance_values'])
        (obl_agri_count, obl_agri_total, obl_agri_avg, obl_agri_std) = calculate_stats(stats['obl_agri_values'])
        (read_fin_count, _, read_fin_avg, read_fin_std) = calculate_stats(stats['read_finance_values'])
        (read_agri_count, _, read_agri_avg, read_agri_std) = calculate_stats(stats['read_agri_values'])

        
        output_data.append({
            'year': year,
            # Obligation - Finance
            'total_celex_finance': obl_fin_count,
            'total_obligation_finance': obl_fin_total,
            'avg_obligation_finance': obl_fin_avg,
            'stdev_obligation_finance': obl_fin_std,
            # Obligation - Agriculture
            'total_celex_agriculture': obl_agri_count,
            'total_obligation_agriculture': obl_agri_total,
            'avg_obligation_agriculture': obl_agri_avg,
            'stdev_obligation_agriculture': obl_agri_std,
            # Readability - Finance
            'total_celex_readability_finance': read_fin_count,
            'avg_readability_finance': read_fin_avg,
            'stdev_readability_finance': read_fin_std,
            # Readability - Agriculture
            'total_celex_readability_agriculture': read_agri_count,
            'avg_readability_agriculture': read_agri_avg,
            'stdev_readability_agriculture': read_agri_std
        })

    # --- 4. Write output file ---
    if not output_data:
        print("No data was aggregated. Output file will be empty.")
        return

    print(f"Writing aggregated data to {output_file}...")
    try:
        fieldnames = [
            'year', 
            'total_celex_finance', 'total_obligation_finance', 'avg_obligation_finance', 'stdev_obligation_finance',
            'total_celex_agriculture', 'total_obligation_agriculture', 'avg_obligation_agriculture', 'stdev_obligation_agriculture',
            'total_celex_readability_finance', 'avg_readability_finance', 'stdev_readability_finance',
            'total_celex_readability_agriculture', 'avg_readability_agriculture', 'stdev_readability_agriculture'
        ]
        
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_data)
            
        print(f"Successfully wrote {len(output_data)} years of data to {output_file}.")

    except IOError as e:
        print(f"Error writing output file: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred while writing output: {e}", file=sys.stderr)


def main():
    """
    Parses arguments and runs the aggregation.
    """
    parser = argparse.ArgumentParser(
        description="Aggregate regulatory metrics (word counts and readability) by year."
    )
    parser.add_argument(
        '--word_count_file',
        required=True,
        help="Input CSV file (e.g., regulation_word_counts.csv)."
    )
    parser.add_argument(
        '--readability_file',
        required=True,
        help="Input CSV file (e.g., regulation_readability_scores.csv)."
    )
    parser.add_argument(
        '--output_file',
        required=True,
        help="Path for the output CSV aggregates file."
    )
    
    args = parser.parse_args()
    
    aggregate_data(args.word_count_file, args.readability_file, args.output_file)

if __name__ == "__main__":
    main()

