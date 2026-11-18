"""Serve the heritage explorer locally, with a small proxy for Google Places photos.

Why a proxy: the Places API has no CORS support, so the browser can't call it
directly. serve.py calls it server-side and never exposes the API key to the
browser. If no key is configured, the explorer silently falls back to
Wikipedia/Wikimedia Commons photos.

Usage:
    python serve.py            # serve on http://localhost:8000 and open the browser
    python serve.py 8080       # custom port

Setup (optional, for Google photos):
    1. Google Cloud Console → enable "Places API" → create an API key
    2. Put the key in api_key.txt (gitignored) or set GOOGLE_API_KEY
    3. Restart serve.py
"""

import json
import os
import sys
import urllib.parse
import urllib.request
import webbrowser
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
KEY_FILE = "api_key.txt"
PHOTO_CACHE_MAX = 2000

API_KEY = os.environ.get("GOOGLE_API_KEY", "").strip()
if not API_KEY and os.path.exists(KEY_FILE):
    with open(KEY_FILE) as f:
        API_KEY = f.read().strip()

_details_cache = {}
_photo_cache = {}


def _places_api(path, params):
    params = {**params, "key": API_KEY}
    url = f"https://maps.googleapis.com/maps/api/place/{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.load(r)


class Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # Data files must never be served from browser cache without revalidation
        # (heuristic caching on Last-Modified otherwise serves stale all.json).
        if self.path.endswith(".json"):
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def do_GET(self):
        if self.path.startswith("/api/place-photos"):
            self._place_photos()
        elif self.path.startswith("/api/photo"):
            self._photo()
        else:
            super().do_GET()

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # GET /api/place-photos?pid=<cid> -> {photos: [{ref,w,h}], attributions: [...]}
    def _place_photos(self):
        if not API_KEY:
            return self._json({"error": "no-key"}, 501)
        pid = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("pid", [""])[0]
        if not pid:
            return self._json({"error": "missing pid"}, 400)
        if pid in _details_cache:
            return self._json(_details_cache[pid])
        try:
            data = _places_api("details/json", {"place_id": pid, "fields": "photos"})
        except Exception as e:
            return self._json({"error": str(e)}, 502)
        if data.get("status") not in ("OK",):
            return self._json({"error": data.get("status", "UNKNOWN"), "photos": [], "attributions": []},
                              404 if data.get("status") == "NOT_FOUND" else 502)
        photos = [{"ref": p["photo_reference"], "w": p.get("width"), "h": p.get("height")}
                  for p in data.get("result", {}).get("photos", [])]
        attributions = [a for p in data.get("result", {}).get("photos", [])
                        for a in p.get("html_attributions", [])][:5]
        out = {"photos": photos, "attributions": attributions}
        if len(_details_cache) > PHOTO_CACHE_MAX:
            _details_cache.clear()
        _details_cache[pid] = out
        self._json(out)

    # GET /api/photo?ref=<photo_reference>&w=800 -> the image bytes
    # (proxied so the API key never reaches the browser)
    def _photo(self):
        if not API_KEY:
            return self._json({"error": "no-key"}, 501)
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        ref = qs.get("ref", [""])[0]
        w = qs.get("w", ["800"])[0]
        if not ref:
            return self._json({"error": "missing ref"}, 400)
        try:
            data = _places_api("photo", {"photo_reference": ref, "maxwidth": w})
        except Exception as e:
            return self._json({"error": str(e)}, 502)
        self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    url = f"http://localhost:{PORT}/index.html"
    if API_KEY:
        print(f"Google Places photos enabled (key from {KEY_FILE if os.path.exists(KEY_FILE) else 'GOOGLE_API_KEY'})")
    else:
        print("No Google API key found — photos will come from Wikipedia/Wikimedia Commons.")
        print(f"To enable Google photos, put a key in {KEY_FILE} (see header of serve.py).")
    print(f"Serving on {url}  (Ctrl-C to stop)")
    webbrowser.open(url)
    ThreadingHTTPServer(("localhost", PORT), Handler).serve_forever()
