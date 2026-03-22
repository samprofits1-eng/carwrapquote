import os
import json
import time
import requests
from datetime import datetime

# ── Config from GitHub Secrets ─────────────────────────────────────────────────
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
    actor_id_url = APIFY_ACTOR_ID.replace("/", "~")
    url = f"https://api.apify.com/v2/acts/{actor_id_url}/runs?token={APIFY_API_TOKEN}"
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
    raise Exception("Apify timed out after 30 minutes")

def fetch_apify_results(dataset_id):
    print("📥 Fetching results from Apify...")
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_API_TOKEN}&limit=2000"
    resp = requests.get(url)
    resp.raise_for_status()
    items = resp.json()
    print(f"   Got {len(items)} raw results")
    return items

# ── Step 2: Clean & filter leads ──────────────────────────────────────────────

def process_leads(items, seen_phones):
    leads = []
    skipped = 0
    for item in items:
        phone = clean_phone(item.get("phone") or item.get("phoneUnformatted"))
        if not phone:
            skipped += 1
            continue
        if phone in seen_phones:
            skipped += 1
            continue
        # State check - make sure it's Florida
        address = item.get("address", "") or ""
        if address and ", FL" not in address and "Florida" not in address:
            skipped += 1
            continue

        name = item.get("title") or item.get("name") or "Unknown"
        website = item.get("website") or ""

        leads.append({
            "phone": phone,
            "name": name,
            "website": website,
            "address": address,
        })
        seen_phones.add(phone)

    print(f"✅ {len(leads)} new leads | {skipped} skipped (no phone / duplicate / out of state)")
    return leads

# ── Step 3: Upload to JustCall Sales Dialer Campaign ──────────────────────────
# Uses the correct v2 autodialer endpoint for Power/Sales Dialer campaigns

def upload_to_justcall(leads):
    if not leads:
        print("ℹ️  No new leads to upload.")
        return 0

    print(f"📤 Uploading {len(leads)} leads to JustCall campaign {JUSTCALL_CAMPAIGN_ID}...")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"{JUSTCALL_API_KEY}:{JUSTCALL_API_SECRET}",
    }

    # JustCall Sales Dialer: add contacts to campaign
    # API docs: POST https://api.justcall.io/v2.1/sales_dialer/campaigns/contact
    url = "https://api.justcall.io/v2.1/sales_dialer/campaigns/contact"

    uploaded = 0
    failed = 0

    for lead in leads:
        # Split name into first/last
        parts = lead["name"].split(" ", 1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else "."

        payload = {
            "campaign_id": JUSTCALL_CAMPAIGN_ID,
            "firstname": first_name,
            "lastname": last_name,
            "phone": lead["phone"],
            "company": lead["name"],
            "notes": lead["address"],
        }

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            if resp.status_code in (200, 201):
                uploaded += 1
            else:
                print(f"   ⚠️  Failed for {lead['name']}: {resp.status_code} - {resp.text[:200]}")
                failed += 1
        except Exception as e:
            print(f"   ⚠️  Error for {lead['name']}: {e}")
            failed += 1

        # Small delay to avoid rate limiting
        time.sleep(0.3)

    print(f"✅ Uploaded: {uploaded} | Failed: {failed}")
    return uploaded

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*50}")
    print(f"Florida Car Wrap Lead Pipeline")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    seen_phones = load_seen_phones()
    print(f"📋 Previously seen phones: {len(seen_phones)}")

    run_id = run_apify_scrape()
    dataset_id = wait_for_apify(run_id)
    items = fetch_apify_results(dataset_id)

    leads = process_leads(items, seen_phones)
    uploaded = upload_to_justcall(leads)

    save_seen_phones(seen_phones)

    print(f"\n{'='*50}")
    print(f"✅ Done! {uploaded} new leads added to JustCall.")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
