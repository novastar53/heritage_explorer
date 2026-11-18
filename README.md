# Heritage Explorer

A personal explorer for heritage sites collected as Google Maps lists: temples,
forts, stepwells, Buddhist sites, palaces, Harappan sites, and more.

## How it works

```
google_lists/*.csv   (Google Maps list exports)
        │  convert.py  (Playwright scrapes lat/lng from each place URL)
        ▼
processed/<list>.json   (one per list, with name / lat / lng / url / note / tags)
processed/all.json      (combined, deduplicated across lists, "categories" per place)
        │  explorer.html  (Leaflet map: heatmap, clustered markers, search, filters)
        ▼
   http://localhost:8000/explorer.html
```

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Updating the data

```bash
python convert.py                # scrape any new places (cached), rebuild all.json
python convert.py --merge-only   # just rebuild processed/all.json from existing JSON
```

Scraped results are cached in `places_cache.json` keyed by Google Place ID, so
re-runs only visit new places. Failures are listed in `processed/failed.csv`.

During the merge, split lists are combined into canonical categories: numeric
suffixes (`Hindu temple ruins 3`, `Harappan Sites(1)`) are stripped, and
spelling variants are aliased (`old_hindu_temples`, American/British
"Favorite/Favourite"). "Favourite …" lists stay as their own categories,
separate from the base list (e.g. `favourite forts of india` vs `forts of
india`). Raw per-list JSONs keep the original names; the combining happens in
`processed/all.json` only. To adjust this, edit `SUFFIX_RE` /
`CATEGORY_ALIASES` in `convert.py`.

## Viewing the site

The page fetches JSON, so it needs HTTP (opening the file directly won't work):

```bash
python3 -m http.server 8000    # or: python serve.py
# then open http://localhost:8000/explorer.html
```

### Google Maps photos (optional)

Place details show Wikipedia/Wikimedia Commons content by default. For Google
Maps photos instead:

1. Google Cloud Console → enable the **Places API** → create an **API key**
   (personal use stays well within the monthly free tier — photos are only
   fetched for places you actually click).
2. `echo YOUR_KEY > api_key.txt` (gitignored) or `export GOOGLE_API_KEY=...`
3. Restart `serve.py` — it proxies the Places API so the key never reaches the
   browser, and shows Google photos (with the required attributions) in the
   details panel, falling back to Wikipedia/Commons when Google has none.

Note: headless scraping of Google Maps photos no longer works reliably — the
signed-out page is served without photo content — so the Places API is the
supported route.

### Pages

- **`index.html`** — landing page.
- **`explorer.html`** — the explorer: category filter, heatmap or clustered
  markers, cross-list heat weighting, name search, "near me" radius, dark
  basemap. Clicking a marker opens a details panel with a Wikipedia summary and
  photos from Wikimedia Commons (fetched live in the browser, no API key;
  cached in localStorage).
