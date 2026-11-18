"""Scrape lat/lng coordinates from Google Maps list exports (google_lists/*.csv).

For each CSV, visits every place URL with headless Chromium (Playwright),
extracts the coordinates from the resolved Google Maps URL, and writes
processed/<list name>.json. Also emits a combined processed/all.json with a
"categories" field per place (deduplicated across lists).

Results are cached in places_cache.json keyed by Google Place ID, so re-runs
only scrape new places.

Usage:
    python convert.py                # scrape all lists (cached), then merge
    python convert.py --merge-only   # rebuild processed/all.json from existing JSON
"""

import os
import sys
import glob
import csv
import re
import json
import random
import asyncio
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright

MAX_CONCURRENT = 8   # be polite; Google Maps will throttle aggressive scraping
MAX_RETRIES = 3
CACHE_FILE = "places_cache.json"
PROCESSED_DIR = "processed"
FAILED_FILE = os.path.join(PROCESSED_DIR, "failed.csv")

PLACE_ID_RE = re.compile(r'1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)')
COORD_RE = re.compile(r'@([-0-9.]+),([-0-9.]+)')

# Google lists often split into parts: "... 2", "... 3", "(1)". These suffixes
# are stripped to recover the base list name.
SUFFIX_RE = re.compile(r'\s*(?:\(\d+\)|\d+)$')

# Spelling variants of the same list map to one canonical category.
# "Favourite …" lists stay their own category, separate from the base list.
CATEGORY_ALIASES = {
    'old_hindu_temples': 'old hindu temples',
    'favorite hindu temples': 'favourite hindu temples',
    'favorite forts in india': 'favourite forts of india',
}


def canonical_category(list_name):
    """Map a raw list file name to its canonical category (e.g. both
    'Hindu temple ruins 3' and 'hindu temple ruins' -> 'hindu temple ruins').
    Canonical categories are always lowercase so differently-cased exports
    of the same list still merge."""
    base = SUFFIX_RE.sub('', list_name).strip().lower()
    return CATEGORY_ALIASES.get(base, base)


def load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default


def atomic_write_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def extract_place_id(url):
    m = PLACE_ID_RE.search(url or "")
    return m.group(1) if m else None


def cache_key_for(url):
    pid = extract_place_id(url)
    return pid if pid else url  # fall back to the raw URL for non-place links


async def fetch_place(browser, row, cache, semaphore):
    title = (row.get('Title') or '').strip()
    note = (row.get('Note') or '').strip()
    tags = (row.get('Tags') or '').strip()
    url = (row.get('URL') or '').strip()
    if not url:
        return None, ('no-url', title, url)

    key = cache_key_for(url)
    if key in cache:
        return cache[key], None

    async with semaphore:
        for attempt in range(MAX_RETRIES):
            page = None
            try:
                page = await browser.new_page()
                # small random delay to look less robotic
                await page.wait_for_timeout(random.randint(300, 1200))
                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(4000)  # wait for JS redirect
                final_url = page.url
                await page.close()
                page = None

                match = COORD_RE.search(final_url)
                if match:
                    lat, lng = match.groups()
                    record = {
                        "name": title,
                        "lat": float(lat),
                        "lng": float(lng),
                        "url": final_url,
                        "place_id": extract_place_id(final_url) or extract_place_id(url),
                        "note": note,
                        "tags": tags,
                    }
                    cache[key] = record
                    return record, None
            except Exception as e:
                print(f"⚠️  Attempt {attempt + 1} failed for {title}: {e}")
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:
                        pass
            # exponential backoff with jitter before retrying
            await asyncio.sleep(2 ** attempt + random.random())

        return None, (title, url)


def write_failed(rows):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    with open(FAILED_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["list", "title", "url"])
        w.writerows(rows)


async def scrape_all(cache):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    failed = []

    for input_file in sorted(glob.glob("google_lists/*.csv")):
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_file = os.path.join(PROCESSED_DIR, f"{base_name}.json")

        with open(input_file, 'r', encoding='utf-8') as f:
            rows = [row for row in csv.DictReader(f) if row.get('URL')]

        places = []
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        tasks = []

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                tasks = [asyncio.create_task(fetch_place(browser, row, cache, semaphore))
                         for row in rows]

                for coro in tqdm.as_completed(tasks, total=len(tasks),
                                              desc=f"Processing {base_name}"):
                    record, failure = await coro
                    if record:
                        places.append(record)
                    elif failure:
                        failed.append([base_name] + list(failure))

                await browser.close()
        except KeyboardInterrupt:
            # cancel pending scrapes, persist what we have, then stop
            for t in tasks:
                t.cancel()
            print("\n⏹  Interrupted — writing partial results...")
            atomic_write_json(output_file, places)
            atomic_write_json(CACHE_FILE, cache)
            write_failed(failed)
            sys.exit(130)

        atomic_write_json(output_file, places)
        atomic_write_json(CACHE_FILE, cache)
        print(f"✅ Extracted {len(places)} places (out of {len(rows)}) to {output_file}")

    write_failed(failed)
    if failed:
        print(f"⚠️  {len(failed)} places failed after {MAX_RETRIES} retries — see {FAILED_FILE}")


def merge_processed():
    """Build processed/all.json from per-list JSONs, deduplicating across lists.

    A place in both 'forts of india' and 'Favorite Forts in India' becomes one
    record with categories: ['Favorite Forts in India', 'forts of india'].
    """
    merged = {}
    for path in sorted(glob.glob(os.path.join(PROCESSED_DIR, "*.json"))):
        if path.endswith(os.path.join(PROCESSED_DIR, "all.json")):
            continue
        category = canonical_category(os.path.splitext(os.path.basename(path))[0])
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for rec in data:
            lat, lng = rec.get('lat'), rec.get('lng')
            key = rec.get('place_id') or (f"{lat:.4f},{lng:.4f}" if lat and lng else None)
            if key is None:
                continue
            if key in merged:
                if category not in merged[key]["categories"]:
                    merged[key]["categories"].append(category)
            else:
                merged[key] = {
                    "name": rec.get("name"),
                    "lat": lat,
                    "lng": lng,
                    "url": rec.get("url"),
                    "note": rec.get("note", ""),
                    "tags": rec.get("tags", ""),
                    "categories": [category],
                }

    all_places = sorted(merged.values(), key=lambda r: (r["categories"][0], r["name"] or ""))
    out = os.path.join(PROCESSED_DIR, "all.json")
    atomic_write_json(out, all_places)
    print(f"✅ Merged {len(all_places)} unique places into {out}")


def main():
    if "--merge-only" in sys.argv:
        merge_processed()
        return

    cache = load_json(CACHE_FILE, {})
    try:
        asyncio.run(scrape_all(cache))
    except KeyboardInterrupt:
        sys.exit(130)
    merge_processed()


if __name__ == "__main__":
    main()
