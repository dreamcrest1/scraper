import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import random
import argparse
import re
from urllib.parse import urlparse

# --- CONFIG ---
CONCURRENCY_LIMIT = 5
MAX_RETRIES = 5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
]

# ---------------- ARGUMENTS ----------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()

# ---------------- HELPERS ----------------
def get_headers(referer=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        h["Referer"] = referer
    return h

def force_save_html(filename, html):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html if html else "")
    print(f"🐞 Debug HTML forced save: {filename}, length={len(html) if html else 0}")

async def fetch_html(session, url, sem, referer=None):
    async with sem:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(1)
                async with session.get(url, headers=get_headers(referer), timeout=45) as r:
                    text = await r.text()
                    print(f"📡 Fetched [{r.status}] {url} (len={len(text)})")
                    return text
            except Exception as e:
                print(f"⚠️ Fetch error {e}, retry {attempt}")
                await asyncio.sleep(attempt * 2)
        return ""

# ---------------- PARSER ----------------
def parse_product(html):
    soup = BeautifulSoup(html, "lxml")
    return {
        "Name": soup.find("h1").get_text(strip=True) if soup.find("h1") else ""
    }

# ---------------- CSV ----------------
def save_csv(data, filename):
    print(f"💾 Saving CSV to: {filename}")
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"✅ CSV saved ({len(df)} rows)")

# ---------------- MAIN ----------------
async def main():
    args = parse_args()

    print(f"🟢 Starting scrape: {args.url} pages {args.start}–{args.end}")
    parsed = urlparse(args.url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"

    clean = re.sub(r'[\?&]p=\d+', "", args.url)
    template = clean + ("&p={}" if "?" in clean else "?p={}")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    product_links = []
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:

        # ---------- ALWAYS SAVE FIRST PAGE HTML ----------
        first_page_url = template.format(args.start)
        print(f"🔎 Fetching first page for debug: {first_page_url}")
        html = await fetch_html(session, first_page_url, sem, base_domain)

        # FORCE SAVE
        force_save_html("debug_listing_page.html", html)

        # Now continue as normal
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            product_links.append(a["href"])

        print(f"🔗 Found {len(product_links)} raw links")

        for url in product_links:
            full = url if url.startswith("http") else base_domain + url
            print(f"➡️ Fetching product page: {full}")
            p_html = await fetch_html(session, full, sem, base_domain)
            results.append(parse_product(p_html))

    save_csv(results, args.output)

if __name__ == "__main__":
    asyncio.run(main())
