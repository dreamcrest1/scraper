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
        f.write(html)
    print(f"🐞 Debug HTML saved: {filename}")

async def fetch_html(session, url, sem, referer=None):
    async with sem:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(1, 2))
                async with session.get(url, headers=headers(referer), timeout=40) as r:
                    if r.status == 200:
                        text = await r.text()
                        if len(text) > 800:
                            print(f"✅ Fetched: {url}")
                            return text
                    elif r.status == 429:
                        print("⚠️ 429 Rate limit, sleeping")
                        await asyncio.sleep(30)
            except Exception as e:
                print(f"❌ Fetch error {e}, retry {attempt}")
                await asyncio.sleep(attempt * 2)
        return None

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
    print(f"💾 Saving CSV: {filename}")
    df = pd.DataFrame(rows)

    for c in TEMPLATE_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    df = df[TEMPLATE_COLUMNS]
    df.to_csv(filename, index=False)
    print(f"✅ CSV saved ({len(df)} rows)")

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
            if not html:
                print("❌ No HTML for listing page")
                continue

            # SAVE DEBUG HTML (ONLY FIRST PAGE)
            if page == args.start:
                save_debug_html("debug_listing_page.html", html)

            print(f"📄 Listing HTML length: {len(html)}")

            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "product" in href:
                    if href.startswith("/"):
                        href = base_domain + href
                    product_links.add(href)

        print(f"🔗 Found {len(product_links)} product URLs")

        # -------- PRODUCT PAGES --------
        for url in product_links:
            html = await fetch_html(session, url, sem, base_domain)
            if html:
                results.append(parse_product(html))

    save_csv(results, args.output)

if __name__ == "__main__":
    asyncio.run(main())
