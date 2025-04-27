import csv
import json
import requests
import unicodedata
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Constants ===
ENDPOINT = "https://google.serper.dev/places"
MAX_WORKERS = 200
MIN_REVIEWS = 10
OUTPUT_PATH = 'uploads/output.csv'

# === Globals ===
api_call_count = 0

# === Create Session with Retry ===
def create_session(api_key):
    session = requests.Session()
    session.headers.update({
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    })
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

# === Normalize Text (remove accents) ===
def normalize_text(text):
    if not isinstance(text, str):
        return text
    normalized = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return normalized.strip()

# === Load Queries ===
def load_queries(filepath):
    try:
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not {"query", "city", "zip"}.issubset(reader.fieldnames):
                raise ValueError(f"Input file must contain 'query', 'city', and 'zip' columns")
            return list(reader)
    except Exception as e:
        print(f"Error loading '{filepath}': {e}")
        exit(1)

# === Fetch Places from Serper ===
def fetch_places(session, row):
    global api_call_count
    search_term = f"{normalize_text(row['query'])} in {normalize_text(row['city'])} {row.get('zip', '').strip()}"
    page = 1
    collected = []

    while True:
        payload = {"q": search_term, "location": row.get("zip", "").strip()}
        if page > 1:
            payload["page"] = page

        print(f"Fetching '{search_term}', page {page}...")
        try:
            resp = session.post(ENDPOINT, json=payload, timeout=10)
            api_call_count += 1
            resp.raise_for_status()
            data = json.loads(resp.text, parse_int=str, parse_float=str)  # Force CIDs as strings
            places = data.get("places", [])
        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            break

        if not places:
            break

        for place in places:
            entry = {
                "query": normalize_text(row['query']),
                "city": normalize_text(row['city']),
                "zip": row.get('zip', '').strip(),
                "search_term": search_term,
                "page": page,
            }
            # Normalize place fields as well
            for key, value in place.items():
                entry[key] = normalize_text(value)

            collected.append(entry)

        print(f"→ Page {page} returned {len(places)} places")
        page += 1

    return collected

# === Clean Ratings ===
def clean_rating_count(val):
    try:
        if isinstance(val, (int, float)):
            return int(val)
        return int(str(val).replace(",", ""))
    except Exception:
        return 0

# === Validate Entry ===
def is_valid(entry):
    website = entry.get("website", "").strip()
    rating = clean_rating_count(entry.get("ratingCount", 0))
    return bool(website) and rating >= MIN_REVIEWS

# === Main Runner ===
def run_serper(queries_path, api_key):
    global api_call_count
    session = create_session(api_key)
    queries = load_queries(queries_path)

    print(f"🚀 Starting scrape of {len(queries)} queries with up to {MAX_WORKERS} workers...")

    all_results = []
    all_keys = set(["query", "city", "zip", "search_term", "page", "cid", "is_valid", "maps_url"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_places, session, row): row for row in queries}
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            places = future.result()
            for place in places:
                place["is_valid"] = "TRUE" if is_valid(place) else "FALSE"
                all_results.append(place)
                all_keys.update(place.keys())

            if i % 100 == 0 or i == len(queries):
                print(f"✅ Completed {i}/{len(queries)} queries. Total results: {len(all_results)}")

    print(f"\n🧹 Deduplicating {len(all_results)} rows by 'cid'...")
    seen_cids = set()
    deduped_results = []
    for entry in all_results:
        cid = entry.get("cid")
        if cid and cid not in seen_cids:
            seen_cids.add(cid)
            deduped_results.append(entry)

    print(f"📦 Final deduplicated results: {len(deduped_results)}")

    headers = ["query", "city", "zip", "search_term", "page", "is_valid", "maps_url"] + sorted(
        key for key in all_keys if key not in {"query", "city", "zip", "search_term", "page", "is_valid", "maps_url"}
    )

    try:
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for entry in deduped_results:
                cid = entry.get('cid')
                if cid:
                    # Add clean Google Maps URL
                    entry["maps_url"] = f"https://www.google.com/maps?cid={cid}"
                    # Protect CID in Excel
                    entry["cid"] = f"'{cid}"

                writer.writerow(entry)

        print(f"\n✅ Done! Wrote {len(deduped_results)} rows to '{OUTPUT_PATH}'")
        print(f"📊 Total API calls made to Serper: {api_call_count}")

    except Exception as e:
        print(f"❌ Error writing output file: {e}")

    return OUTPUT_PATH
