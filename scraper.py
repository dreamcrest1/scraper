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

# ---------------- CONFIG ----------------
CONCURRENCY_LIMIT = 3
MAX_RETRIES = 5
BASE_DELAY = 1.0
BACKUP_FREQUENCY = 20
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

# ---------------- ARGUMENTS ----------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()

# ---------------- NETWORK ----------------
def headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,*/*"
    }

async def fetch(session, url, sem):
    async with sem:
        for i in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(random.uniform(1, 2))
                async with session.get(url, headers=headers(), timeout=40) as r:
                    if r.status == 200:
                        return await r.text()
                    elif r.status == 429:
                        await asyncio.sleep(30)
            except:
                await asyncio.sleep(i * 2)
        return None

# ---------------- PARSER ----------------
def parse_product(html):
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

# ---------------- LISTING ----------------
async def collect_links(session, url_template, page, base, sem):
    html = await fetch(session, url_template.format(page), sem)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    links = []

    for a in soup.find_all("a", href=True):
        if "product" in a["href"]:
            link = a["href"]
            if link.startswith("/"):
                link = base + link
            links.append(link)

    return list(set(links))

# ---------------- CSV ----------------
def save_csv(rows, file):
    if not rows:
        return
    df = pd.DataFrame(rows)
    for c in TEMPLATE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[TEMPLATE_COLUMNS]
    df.to_csv(file, index=False)

# ---------------- MAIN ----------------
async def main():
    args = parse_args()
    parsed = urlparse(args.url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    clean = re.sub(r'[\?&]p=\d+', '', args.url)
    template = clean + ("&p={}" if "?" in clean else "?p={}")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)

    all_products = []
    async with aiohttp.ClientSession(connector=connector) as session:
        urls = []
        for p in range(args.start, args.end + 1):
            urls.extend(await collect_links(session, template, p, base, sem))

        for url in urls:
            html = await fetch(session, url, sem)
            if html:
                all_products.append(parse_product(html))

    save_csv(all_products, args.output)

if __name__ == "__main__":
    asyncio.run(main())
