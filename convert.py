API_KEY = "AIzaSyDZQeULXlE3c1e0VTqr-2PAy5Mi0uG0TNU"

import csv
import re
import json
import asyncio
from tqdm.asyncio import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.async_api import async_playwright

INPUT_FILE = "old_hindu_temples.csv"
OUTPUT_FILE = "old_hindu_temples.json"

coord_pattern = re.compile(r'@([-0-9.]+),([-0-9.]+)')

async def fetch_place(browser, row):
    title = row['Title'].strip()
    url = row['URL'].strip()
    if not url:
        return None

    try:
        page = await browser.new_page()
        await page.goto(url, timeout=60000)
        #await page.wait_for_timeout(5000)  # crude wait
        page.wait_for_selector("canvas", timeout=15000)
        final_url = page.url
        await page.close()

        match = coord_pattern.search(final_url)
        if match:
            lat, lng = match.groups()
            return {
                "name": title,
                "lat": float(lat),
                "lng": float(lng),
                "url": final_url
            }
    except Exception as e:
        print(f"❌ Error for {title}: {e}")
    return None

async def main():
    # load CSV
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        rows = [row for row in csv.DictReader(f) if row['URL'].strip()]

    places = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = [fetch_place(browser, row) for row in rows]

        for coro in tqdm.as_completed(tasks, total=len(tasks), desc="Processing places"):
            result = await coro
            if result:
                places.append(result)

        await browser.close()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(places, f, indent=2)

    print(f"✅ Extracted {len(places)} places (out of {len(rows)}) to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())