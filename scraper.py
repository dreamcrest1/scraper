import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import os
import random
import re
import time
import argparse
from urllib.parse import urlparse
from datetime import timedelta

# --- CONFIGURATION ---
CONCURRENCY_LIMIT = 5
MAX_RETRIES = 5
BASE_DELAY = 1.0
BACKUP_FREQUENCY = 20
DEFAULT_STOCK_QUANTITY = "100"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
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

# ---------------- NETWORK ----------------
def headers(referer=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,*/*"
    }
    if referer:
        h["Referer"] = referer
    return h

async def fetch_html_safe(session, url, semaphore, referer=None):
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(1, 2))
                async with session.get(url, headers=headers(referer), timeout=45) as r:
                    if r.status == 200:
                        text = await r.text()
                        if len(text) > 1000:
                            return text
                    elif r.status == 429:
                        await asyncio.sleep(60)
            except:
                await asyncio.sleep(attempt * 2)
        return None

# ---------------- PARSER ----------------
def parse_product_html(html):
    soup = BeautifulSoup(html, "lxml")
    data = {c: "" for c in TEMPLATE_COLUMNS}
    data["Type"] = "simple"
    data["Published"] = 1
    data["In stock?"] = 1

    h1 = soup.find("h1")
    data["Name"] = h1.get_text(strip=True) if h1 else "Unknown"
    data["SKU"] = f"SKU-{random.randint(100000,999999)}"
    data["Stock"] = DEFAULT_STOCK_QUANTITY
    return data

# ---------------- CSV ----------------
def save_csv(data, filename):
    if not data:
        print("⚠️ No data to save")
        return
    df = pd.DataFrame(data)
    for c in TEMPLATE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[TEMPLATE_COLUMNS]
    df.to_csv(filename, index=False)
    print(f"✅ Saved CSV: {filename}")

# ---------------- MAIN ----------------
async def main():
    args = parse_args()
    parsed = urlparse(args.url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"
    clean_url = re.sub(r'[\?&]p=\d+', '', args.url)
    url_template = clean_url + ("&p={}" if "?" in clean_url else "?p={}")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    results = []

    async with aiohttp.ClientSession(connector=connector) as session:
        urls = []
        for p in range(args.start, args.end + 1):
            html = await fetch_html_safe(session, url_template.format(p), semaphore, base_domain)
            if html:
                soup = BeautifulSoup(html, "lxml")
                for a in soup.find_all("a", href=True):
                    if "product" in a["href"]:
                        link = a["href"]
                        if link.startswith("/"):
                            link = base_domain + link
                        urls.append(link)

        urls = list(set(urls))
        print(f"🔗 Found {len(urls)} product URLs")

        for url in urls:
            html = await fetch_html_safe(session, url, semaphore, base_domain)
            if html:
                results.append(parse_product_html(html))

    save_csv(results, args.output)

if __name__ == "__main__":
    asyncio.run(main())
