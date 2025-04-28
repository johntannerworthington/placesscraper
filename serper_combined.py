import csv
import json
import requests
import unicodedata
import concurrent.futures
import os
import uuid
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Constants ===
ENDPOINT = "https://google.serper.dev/places"
MAX_WORKERS = 200
MIN_REVIEWS = 10
UPLOADS_DIR = 'uploads'

# === Globals ===
api_call_count = 0
seen_cids = set()

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
            # Normalize place fields
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
    global api_call_count, seen_cids
    session = create_session(api_key)
    queries = load_queries(queries_path)

    print(f"🚀 Starting scrape of {len(queries)} queries with up to {MAX_WORKERS} workers...")

    all_keys = set(["query", "city", "zip", "search_term", "page", "cid", "is_valid", "maps_url"])

    session_id = str(uuid.uuid4())
    session_dir = os.path.join(UPLOADS_DIR, session_id)
    os.makedirs(session_dir, exist_ok=True)
    output_path = os.path.join(session_dir, "output.csv")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_places, session, row): row for row in queries}
            for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                places = future.result()
                rows_to_write = []
                for place in places:
                    cid = place.get("cid")
                    if cid and cid not in seen_cids:
                        seen_cids.add(cid)
                        place["is_valid"] = "TRUE" if is_valid(place) else "FALSE"
                        place["maps_url"] = f"https://www.google.com/maps?cid={cid}"
                        place["cid"] = f"'{cid}"  # Excel safe CID
                        all_keys.update(place.keys())
                        rows_to_write.append(place)

                if rows_to_write:
                    if writer is None:
                        headers = [
                            "query", "city", "zip", "search_term", "page", "is_valid", "maps_url"
                        ] + sorted(k for k in all_keys if k not in {"query", "city", "zip", "search_term", "page", "is_valid", "maps_url"})
                        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
                        writer.writeheader()

                    writer.writerows(rows_to_write)

                if i % 100 == 0 or i == len(queries):
                    print(f"✅ Completed {i}/{len(queries)} queries. Current unique businesses: {len(seen_cids)}")

    print(f"\n✅ Done! Wrote {len(seen_cids)} deduplicated rows to '{output_path}'")
    print(f"📊 Total API calls made to Serper: {api_call_count}")

    return output_path
