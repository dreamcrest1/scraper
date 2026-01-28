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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
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

# ---------------- ARGUMENTS (GITHUB INPUTS) ----------------
def parse_args():
    parser = argparse.ArgumentParser(description="GitHub Actions Web Scraper")
    parser.add_argument("--url", required=True, help="Category URL")
    parser.add_argument("--start", type=int, required=True, help="Start page")
    parser.add_argument("--end", type=int, required=True, help="End page")
    parser.add_argument("--output", required=True, help="CSV filename (e.g. products.csv)")
    return parser.parse_args()

# ---------------- NETWORK ----------------
def headers(referer=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,*/*",
    }
    if referer:
        h["Referer"] = referer
    return h

async def fetch_html(session, url, sem, referer=None):
    async with sem:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(1, 2))
                async with session.get(url, headers=headers(referer), timeout=40) as r:
                    if r.status == 200:
                        html = await r.text()
                        if len(html) > 1000:
                            return html
                    elif r.status == 429:
                        await asyncio.sleep(30)
            except:
                await asyncio.sleep(attempt * 2)
        return None

# ---------------- PARSER ----------------
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
    if not rows:
        print("❌ No data scraped. CSV not created.")
        return

    df = pd.DataFrame(rows)
    for col in TEMPLATE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[TEMPLATE_COLUMNS]
    df.to_csv(filename, index=False)

    print(f"✅ CSV created: {filename}")

# ---------------- MAIN ----------------
async def main():
    args = parse_args()

    parsed = urlparse(args.url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"
    clean_url = re.sub(r'[\?&]p=\d+', '', args.url)
    page_url = clean_url + ("&p={}" if "?" in clean_url else "?p={}")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)

    product_links = set()
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        # Collect product links
        for page in range(args.start, args.end + 1):
            html = await fetch_html(session, page_url.format(page), sem, base_domain)
            if not html:
                continue

            soup = BeautifulSoup(html, "lxml")
            for a in soup.find_all("a", href=True):
                href = a.get("href")
                if href and "product" in href:
                    link = href if not href.startswith("/") else base_domain + href
                    product_links.add(link)

        print(f"🔗 Found {len(product_links)} product URLs")

        # Scrape products
        for url in product_links:
            html = await fetch_html(session, url, sem, base_domain)
            if html:
                results.append(parse_product(html))

    save_csv(results, args.output)

if __name__ == "__main__":
    asyncio.run(main())
