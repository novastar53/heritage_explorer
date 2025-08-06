API_KEY = "AIzaSyDZQeULXlE3c1e0VTqr-2PAy5Mi0uG0TNU"

import os
import glob
import csv
import re
import json
import asyncio
from tqdm.asyncio import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.async_api import async_playwright
MAX_CONCURRENT = 24   # tune this (e.g., 3-8 is usually safe)

coord_pattern = re.compile(r'@([-0-9.]+),([-0-9.]+)')

async def fetch_place(browser, row, semaphore):
    title = row['Title'].strip()
    url = row['URL'].strip()
    if not url:
        return None

    async with semaphore:
        try:
            page = await browser.new_page()
            await page.goto(url, timeout=60000)
            await page.wait_for_timeout(5000)  # wait for JS to load
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
    input_files = glob.glob("google_lists/*.csv")
    for input_file in input_files:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_file = os.path.join("processed", f"{base_name}.json")

        with open(input_file, 'r', encoding='utf-8') as f:
            rows = [row for row in csv.DictReader(f) if 'URL' in row]

        places = []
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            tasks = [fetch_place(browser, row, semaphore) for row in rows]

            for coro in tqdm.as_completed(tasks, total=len(tasks), desc=f"Processing {base_name}"):
                result = await coro
                if result:
                    places.append(result)

            await browser.close()

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(places, f, indent=2)

        print(f"✅ Extracted {len(places)} places (out of {len(rows)}) to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())