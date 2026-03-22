import os
import json
import time
import requests
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────
APIFY_API_TOKEN      = os.environ["APIFY_API_TOKEN"]
APIFY_ACTOR_ID       = "compass~crawler-google-places"
JUSTCALL_API_KEY     = os.environ["JUSTCALL_API_KEY"]
JUSTCALL_API_SECRET  = os.environ["JUSTCALL_API_SECRET"]
JUSTCALL_CAMPAIGN_ID = os.environ["JUSTCALL_CAMPAIGN_ID"]

SEEN_FILE        = "seen_phones.json"
CITY_INDEX_FILE  = "city_index.json"
DAILY_LEAD_CAP   = 150
CITIES_PER_DAY   = 3
MIN_RATING       = 4.0
MIN_REVIEWS      = 3

FLORIDA_CITIES = [
    "Miami", "Orlando", "Tampa", "Jacksonville", "Fort Lauderdale",
    "St Petersburg", "Hialeah", "Tallahassee", "Cape Coral", "Fort Myers",
    "Pembroke Pines", "Hollywood", "Gainesville", "Miramar", "Coral Springs",
    "Miami Gardens", "Clearwater", "Palm Bay", "Pompano Beach", "West Palm Beach",
    "Lakeland", "Davie", "Miami Beach", "Boca Raton", "Deltona",
    "Plantation", "Sunrise", "Palm Coast", "Deerfield Beach", "Melbourne",
    "Boynton Beach", "Lauderhill", "Weston", "Kissimmee", "Homestead",
    "Daytona Beach", "Delray Beach", "Tamarac", "Port St Lucie", "Pensacola",
    "Ocala", "Sarasota", "Naples", "Fort Pierce", "Bradenton",
    "Doral", "Sanford", "Margate", "Coral Gables", "Coconut Creek",
]

SEARCH_KEYWORDS = [
    "car wrap",
    "vehicle wrap",
    "auto wrap",
    "vinyl wrap",
]

# ── City rotation ──────────────────────────────────────────────────────────────

def get_todays_cities():
    if os.path.exists(CITY_INDEX_FILE):
        with open(CITY_INDEX_FILE) as f:
            data = json.load(f)
        current_index = data.get("next_index", 0)
    else:
        current_index = 0

    total = len(FLORIDA_CITIES)
    cities = [FLORIDA_CITIES[(current_index + i) % total] for i in range(CITIES_PER_DAY)]
    next_index = (current_index + CITIES_PER_DAY) % total

    with open(CITY_INDEX_FILE, "w") as f:
        json.dump({"next_index": next_index}, f)

    return cities

# ── Seen phones ────────────────────────────────────────────────────────────────

def load_seen_phones():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()

def save_seen_phones(phones):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(phones), f)

def clean_phone(phone):
    if not phone:
        return None
    digits = "".join(c for c in str(phone) if c.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None

# ── Step 1: Apify scrape ───────────────────────────────────────────────────────

def run_apify_scrape(queries):
    print(f"🔍 Scraping {len(queries)} queries via Apify...")
    url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs?token={APIFY_API_TOKEN}"
    payload = {
        "searchStringsArray": queries,
        "language": "en",
        "countryCode": "us",
        "maxCrawledPlacesPerSearch": 100,
        "exportPlaceUrls": False,
        "includeHistogram": False,
        "includeOpeningHours": False,
        "includePeopleAlsoSearch": False,
        "additionalInfo": False,
    }
    resp = requests.post(url, json=payload)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    print(f"✅ Apify run started: {run_id}")
    return run_id

def wait_for_apify(run_id):
    print("⏳ Waiting for Apify to finish...")
    url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_API_TOKEN}"
    for _ in range(80):
        time.sleep(30)
        resp = requests.get(url)
        status = resp.json()["data"]["status"]
        print(f"   Status: {status}")
        if status == "SUCCEEDED":
            dataset_id = resp.json()["data"]["defaultDatasetId"]
            print(f"✅ Scrape complete. Dataset: {dataset_id}")
            return dataset_id
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise Exception(f"Apify run failed: {status}")
    raise Exception("Apify timed out after 40 minutes")

def fetch_apify_results(dataset_id):
    print("📥 Fetching results...")
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_API_TOKEN}&format=json&limit=5000"
    resp = requests.get(url)
    resp.raise_for_status()
    results = resp.json()
    print(f"✅ {len(results)} raw results fetched")
    return results

# ── Step 2: Filter & clean ─────────────────────────────────────────────────────

def process_leads(raw_results, seen_phones):
    print("🧹 Filtering and deduplicating...")
    new_leads = []
    session_phones = set()

    for item in raw_results:
        if len(new_leads) >= DAILY_LEAD_CAP:
            break

        phone = clean_phone(item.get("phone") or item.get("phoneUnformatted"))
        if not phone:
            continue
        if phone in seen_phones or phone in session_phones:
            continue

        try:
            rating       = float(item.get("totalScore") or item.get("rating") or 0)
            review_count = int(item.get("reviewsCount") or item.get("reviews") or 0)
            if rating < MIN_RATING or review_count < MIN_REVIEWS:
                continue
        except (ValueError, TypeError):
            continue

        state       = (item.get("state") or "").upper()
        address     = (item.get("address") or item.get("street") or "").upper()
        city_field  = (item.get("city") or "").upper()
        combined    = f"{address} {city_field} {state}"
        if state not in ("FL", "FLORIDA") and ", FL" not in combined and "FLORIDA" not in combined:
            continue

        name_parts = (item.get("title") or "").strip().split(" ", 1)
        lead = {
            "first_name": name_parts[0] if name_parts else "",
            "last_name":  name_parts[1] if len(name_parts) > 1 else "",
            "phone":      phone,
            "company":    item.get("title") or "",
            "email":      item.get("email") or "",
            "address":    f"{item.get('address','')}, {item.get('city','')}, {state}".strip(", "),
            "website":    item.get("website") or "",
            "rating":     rating,
            "reviews":    review_count,
        }
        new_leads.append(lead)
        session_phones.add(phone)

    print(f"✅ {len(new_leads)} clean leads ready (cap: {DAILY_LEAD_CAP})")
    return new_leads

# ── Step 3: Upload to JustCall ─────────────────────────────────────────────────

def upload_to_justcall(leads):
    if not leads:
        print("ℹ️  No new leads to upload.")
        return 0

    print(f"📤 Uploading {len(leads)} leads to JustCall...")
    url = "https://api.justcall.io/v1/autodialer/campaign/addcontacts"
    headers = {
        "Accept":        "application/json",
        "Content-Type":  "application/json",
        "Authorization": f"{JUSTCALL_API_KEY}:{JUSTCALL_API_SECRET}",
    }

    success = 0
    failed  = 0

    for i, lead in enumerate(leads):
        payload = {
            "campaign_id": JUSTCALL_CAMPAIGN_ID,
            "name":        f"{lead['first_name']} {lead['last_name']}".strip() or lead["company"],
            "phone":       lead["phone"],
            "email":       lead["email"],
            "company":     lead["company"],
            "notes":       f"⭐ {lead['rating']} ({lead['reviews']} reviews) | {lead['website']} | {lead['address']}",
        }
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            success += 1
        else:
            failed += 1
            print(f"   ⚠️  Failed {lead['phone']}: {resp.text}")

        if (i + 1) % 50 == 0:
            time.sleep(1)

    print(f"✅ Upload complete: {success} succeeded, {failed} failed")
    return success

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"🚀 Florida Car Wrap Pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    cities = get_todays_cities()
    print(f"📍 Today's cities: {', '.join(cities)}")

    queries = [
        f"{keyword} {city} Florida"
        for city in cities
        for keyword in SEARCH_KEYWORDS
    ]
    print(f"🔎 Running {len(queries)} searches: {queries}\n")

    seen_phones = load_seen_phones()
    print(f"📋 {len(seen_phones)} phones already seen from previous runs\n")

    run_id     = run_apify_scrape(queries)
    dataset_id = wait_for_apify(run_id)
    raw        = fetch_apify_results(dataset_id)
    leads      = process_leads(raw, seen_phones)
    uploaded   = upload_to_justcall(leads)

    seen_phones.update(lead["phone"] for lead in leads)
    save_seen_phones(seen_phones)

    print(f"\n{'='*55}")
    print(f"✅ Done! {uploaded} fresh leads added to JustCall.")
    print(f"   Cities searched today: {', '.join(cities)}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
