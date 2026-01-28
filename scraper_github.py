import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import random
import re
import argparse
from urllib.parse import urlparse

# ---------------- CONFIG ----------------
CONCURRENCY_LIMIT = 5
MAX_RETRIES = 5
DEFAULT_STOCK_QUANTITY = "100"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
]

TEMPLATE_COLUMNS = [
    'ID','Type','SKU','GTIN, UPC, EAN, or ISBN','Name','Published',
    'Is featured?','Visibility in catalog','Short description','Description',
    'Date sale price starts','Date sale price ends','Tax status','Tax class',
    'In stock?','Stock','Low stock amount','Backorders allowed?',
    'Sold individually?','Weight (kg)','Length (cm)','Width (cm)',
    'Height (cm)','Allow customer reviews?','Purchase note','Sale price',
    'Regular price','Categories','Tags','Shipping class','Images',
    'Download limit','Download expiry days','Parent','Grouped products',
    'Upsells','Cross-sells','External URL','Button text','Position','Brands'
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
def headers(referer=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache"
    }
    if referer:
        h["Referer"] = referer
    return h

def save_debug_html(filename, html):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html or "")
    print(f"🐞 Debug HTML saved: {filename} (len={len(html) if html else 0})")

async def fetch_html(session, url, sem, referer=None):
    async with sem:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(1, 2))
                async with session.get(url, headers=headers(referer), timeout=45) as r:
                    text = await r.text()
                    print(f"📡 [{r.status}] {url} (len={len(text)})")
                    return text
            except Exception as e:
                print(f"⚠️ Fetch error {e} retry {attempt}")
                await asyncio.sleep(attempt * 2)
        return ""

# ---------------- PARSE PRODUCT ----------------
def parse_product(html):
    soup = BeautifulSoup(html, "lxml")
    data = {c: "" for c in TEMPLATE_COLUMNS}

    data["Type"] = "simple"
    data["Published"] = 1
    data["In stock?"] = 1
    data["Stock"] = DEFAULT_STOCK_QUANTITY

    h1 = soup.find("h1")
    data["Name"] = h1.get_text(strip=True) if h1 else "Unknown"
    data["SKU"] = f"SKU-{random.randint(100000,999999)}"

    return data

# ---------------- CSV ----------------
def save_csv(rows, filename):
    df = pd.DataFrame(rows)
    for c in TEMPLATE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[TEMPLATE_COLUMNS]
    df.to_csv(filename, index=False)
    print(f"✅ CSV saved: {filename} ({len(df)} rows)")

# ---------------- MAIN ----------------
async def main():
    args = parse_args()

    print("🚀 SCRAPER STARTED")
    print(f"URL: {args.url}")
    print(f"Pages: {args.start} to {args.end}")
    print(f"CSV: {args.output}")

    parsed = urlparse(args.url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"

    clean_url = re.sub(r'[\?&]p=\d+', '', args.url)
    page_url = clean_url + ("&p={}" if "?" in clean_url else "?p={}")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)

    product_links = set()
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        # -------- LISTING PAGES --------
        for page in range(args.start, args.end + 1):
            url = page_url.format(page)
            print(f"🔎 Listing page: {url}")
            html = await fetch_html(session, url, sem, base_domain)

            if page == args.start:
                save_debug_html("debug_listing_page.html", html)

            if not html:
                continue

            soup = BeautifulSoup(html, "lxml")

            # ✅ CORRECT ABF PRODUCT SELECTOR
            product_cards = soup.find_all(
                "a",
                class_=lambda x: x and "product-labeled" in x
            )

            print(f"🧾 Products found on page {page}: {len(product_cards)}")

            for a in product_cards:
                href = a.get("href")
                if not href:
                    continue
                if href.startswith("/"):
                    href = base_domain + href
                product_links.add(href)

        print(f"🔗 Total unique product URLs: {len(product_links)}")

        # -------- PRODUCT PAGES --------
        for url in product_links:
            html = await fetch_html(session, url, sem, base_domain)
            if html:
                results.append(parse_product(html))

    save_csv(results, args.output)

if __name__ == "__main__":
    asyncio.run(main())
