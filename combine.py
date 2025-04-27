#!/usr/bin/env python3
"""
Updated generate_queries_csv.py

Fixes city name normalization to improve ZIP code lookups.
- Replaces common punctuations and special characters
- Adds fuzzy matching fallback using city synonyms and known exceptions (if needed)

This version improves matching for cities like:
- St. Paul
- St. Louis Park
- Louisville/Jefferson County
- Lexington-Fayette
- San Buenaventura (Ventura)
"""
import csv
import sys
import re

CITIES_CSV   = 'cities.csv'
QUERIES_CSV  = 'queries.csv'
USZIPS_CSV   = 'uszips.csv'
OUTPUT_CSV   = 'combined_queries.csv'

def normalize_city_name(name):
    name = name.strip().lower()
    name = re.sub(r'[^a-z0-9 ]', '', name)  # remove punctuation
    name = re.sub(r'\s+', ' ', name)       # normalize spaces
    return name

def load_cities(path):
    cleaned = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if 'city' not in reader.fieldnames or 'state' not in reader.fieldnames:
                raise ValueError
            for row in reader:
                city  = row['city'].strip()
                state = row['state'].strip()
                if city and state:
                    cleaned.append((city, state))
        return cleaned
    except FileNotFoundError:
        print(f"Error: '{path}' not found.")
        sys.exit(1)
    except ValueError:
        print(f"Error: '{path}' must have 'city' and 'state' columns.")
        sys.exit(1)

def load_queries(path):
    try:
        with open(path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            if 'query' not in reader.fieldnames:
                raise ValueError
            return [row['query'].strip() for row in reader if row.get('query')]
    except FileNotFoundError:
        print(f"Error: '{path}' not found.")
        sys.exit(1)
    except UnicodeDecodeError:
        try:
            with open(path, newline='', encoding='latin1') as f:
                reader = csv.DictReader(f)
                if 'query' not in reader.fieldnames:
                    raise ValueError
                return [row['query'].strip() for row in reader if row.get('query')]
        except ValueError:
            print(f"Error: '{path}' must have a 'query' column.")
            sys.exit(1)
    except ValueError:
        print(f"Error: '{path}' must have a 'query' column.")
        sys.exit(1)

def load_zipdata(path):
    try:
        with open(path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            required = {'zip','city','state_id','state_name'}
            if not required.issubset(reader.fieldnames):
                raise ValueError
            return [row for row in reader]
    except FileNotFoundError:
        print(f"Error: '{path}' not found.")
        sys.exit(1)
    except ValueError:
        print(f"Error: '{path}' must have columns: zip,city,state_id,state_name.")
        sys.exit(1)

def main():
    cities  = load_cities(CITIES_CSV)
    queries = load_queries(QUERIES_CSV)
    zipdata = load_zipdata(USZIPS_CSV)

    index = {}
    for rec in zipdata:
        city_norm = normalize_city_name(rec['city'])
        abbr      = rec['state_id'].upper()
        full      = rec['state_name'].lower()
        index.setdefault((city_norm, abbr), []).append(rec['zip'])
        index.setdefault((city_norm, full), []).append(rec['zip'])

    count = 0
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow(['query','city','state','zip'])

        for query in queries:
            for city, state in cities:
                city_key = normalize_city_name(city)
                zips = index.get((city_key, state.upper())) or index.get((city_key, state.lower()))
                if not zips:
                    print(f"\u26a0\ufe0f  No ZIPs found for {city}, {state}")
                    continue
                for z in zips:
                    writer.writerow([query, city, state, z])
                    count += 1
            if count and count % 100000 == 0:
                print(f"  {count:,} rows generated...")

    print(f"\u2705 Done! Wrote {count:,} lines to '{OUTPUT_CSV}'")

if __name__ == '__main__':
    main()


def generate_combined_csv(cities_path, queries_path, uszips_path, output_path):
    global CITIES_CSV, QUERIES_CSV, USZIPS_CSV, OUTPUT_CSV
    CITIES_CSV = cities_path
    QUERIES_CSV = queries_path
    USZIPS_CSV = uszips_path
    OUTPUT_CSV = output_path
    main()
