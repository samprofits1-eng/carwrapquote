import os
import json
import time
import requests
import csv
from datetime import datetime

# ── Config from environment variables (set as GitHub Secrets) ──────────────────
APIFY_API_TOKEN      = os.environ["APIFY_API_TOKEN"]
APIFY_ACTOR_ID       = "compass/crawler-google-places"
JUSTCALL_API_KEY     = os.environ["JUSTCALL_API_KEY"]
JUSTCALL_API_SECRET  = os.environ["JUSTCALL_API_SECRET"]
JUSTCALL_CAMPAIGN_ID = os.environ["JUSTCALL_CAMPAIGN_ID"]

SEEN_FILE = "seen_phones.json"

SEARCH_QUERIES = [
    "car wrap Florida",
    "vehicle wrap Florida",
    "auto wrap Florida",
    "car vinyl wrap Florida",
    "fleet wrap Florida",
    "car wrap shop Florida",
]

# ── Helpers ────────────────────────────────────────────────────────────────────

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

# ── Step 1: Run Apify scrape ───────────────────────────────────────────────────

def run_apify_scrape():
    print("🔍 Starting Apify scrape...")
    url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs?token={APIFY_API_TOKEN}"
    payload = {
        "searchStringsArray": SEARCH_QUERIES,
        "language": "en",
        "countryCode": "us",
        "maxCrawledPlacesPerSearch": 200,
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
    for _ in range(60):  # wait up to 30 minutes
        time.sleep(30)
        resp = requests.get(url)
        status = resp.json()["data"]["status"]
        print(f"   Status: {status}")
        if status == "SUCCEEDED":
            dataset_id = resp.json()["data"]["defaultDatasetId"]
            print(f"✅ Scrape complete. Dataset: {dataset_id}")
            return dataset_id
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise Exception(f"Apify run failed with status: {status}")
    raise Exception("Apify run timed out after 30 minutes")

def fetch_apify_results(dataset_id):
    print("📥 Fetching results from Apify...")
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_API_TOKEN}&format=json&limit=10000"
    resp = requests.get(url)
    resp.raise_for_status()
    results = resp.json()
    print(f"✅ Fetched {len(results)} raw results")
    return results

# ── Step 2: Clean & deduplicate ────────────────────────────────────────────────

def process_leads(raw_results, seen_phones):
    print("🧹 Processing and deduplicating leads...")
    new_leads = []
    session_phones = set()

    for item in raw_results:
        phone = clean_phone(item.get("phone") or item.get("phoneUnformatted"))
        if not phone:
            continue
        if phone in seen_phones or phone in session_phones:
            continue

        # Basic Florida filter
        address = item.get("address") or item.get("street") or ""
        city    = item.get("city") or ""
        state   = item.get("state") or ""
        full_address = f"{address} {city} {state}".upper()
        if "FLORIDA" not in full_address and ", FL" not in full_address and " FL " not in full_address:
            # Still include if state matches
            if state.upper() not in ("FL", "FLORIDA"):
                continue

        name_parts = (item.get("title") or "").strip().split(" ", 1)
        first_name = name_parts[0] if name_parts else ""
        last_name  = name_parts[1] if len(name_parts) > 1 else ""

        lead = {
            "first_name": first_name,
            "last_name":  last_name,
            "phone":      phone,
            "company":    item.get("title") or "",
            "email":      item.get("email") or "",
            "address":    f"{address}, {city}, {state}".strip(", "),
            "website":    item.get("website") or "",
        }
        new_leads.append(lead)
        session_phones.add(phone)

    print(f"✅ {len(new_leads)} fresh leads after dedup")
    return new_leads

# ── Step 3: Upload to JustCall ─────────────────────────────────────────────────

def upload_to_justcall(leads):
    if not leads:
        print("ℹ️  No new leads to upload.")
        return

    print(f"📤 Uploading {len(leads)} leads to JustCall campaign {JUSTCALL_CAMPAIGN_ID}...")
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
            "notes":       f"Website: {lead['website']} | Address: {lead['address']}",
        }
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            success += 1
        else:
            failed += 1
            print(f"   ⚠️  Failed lead {lead['phone']}: {resp.text}")

        # Avoid rate limiting
        if (i + 1) % 50 == 0:
            time.sleep(1)

    print(f"✅ Upload complete: {success} succeeded, {failed} failed")
    return success

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*50}")
    print(f"🚀 Florida Car Wrap Lead Pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    seen_phones = load_seen_phones()
    print(f"📋 {len(seen_phones)} phones already seen from previous runs\n")

    run_id     = run_apify_scrape()
    dataset_id = wait_for_apify(run_id)
    raw        = fetch_apify_results(dataset_id)
    leads      = process_leads(raw, seen_phones)

    uploaded = upload_to_justcall(leads)

    # Save newly uploaded phones so we never re-upload them
    new_phones = {lead["phone"] for lead in leads}
    seen_phones.update(new_phones)
    save_seen_phones(seen_phones)

    print(f"\n✅ Done! {uploaded} new leads added to JustCall.")

if __name__ == "__main__":
    main()
