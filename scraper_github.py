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

# WooCommerce base columns
BASE_COLUMNS = [
    "Type","SKU","Name","Published","Visibility in catalog",
    "Short description","Description",
    "In stock?","Stock",
    "Regular price","Categories","Tags","Weight (kg)"
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
        "Accept": "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        h["Referer"] = referer
    return h

async def fetch_html(session, url, sem, referer=None):
    async with sem:
        for _ in range(MAX_RETRIES):
            try:
                async with session.get(url, headers=headers(referer), timeout=40) as r:
                    return await r.text()
            except:
                await asyncio.sleep(2)
        return None

# ---------------- PRODUCT PARSER ----------------
def parse_product(html, base_domain):
    soup = BeautifulSoup(html, "lxml")
    data = {}

    # ---- Defaults ----
    data["Type"] = "simple"
    data["Published"] = 1
    data["Visibility in catalog"] = "visible"
    data["In stock?"] = 1
    data["Stock"] = DEFAULT_STOCK_QUANTITY
    data["Regular price"] = ""
    data["Tags"] = ""

    # ---- Name ----
    h1 = soup.find("h1")
    data["Name"] = h1.get_text(strip=True) if h1 else ""

    # ---- Description ----
    desc_div = soup.select_one("#descriptiontext")
    if desc_div:
        data["Description"] = str(desc_div)
        text = desc_div.get_text(strip=True)
        data["Short description"] = text[:160]
    else:
        data["Description"] = ""
        data["Short description"] = ""

    # ---- Specifications / Attributes ----
    attributes = []
    attr_index = 1

    spec_sections = soup.select("section h3")
    for h in spec_sections:
        ul = h.find_next_sibling("ul")
        if not ul:
            continue

        for li in ul.find_all("li"):
            spans = li.find_all("span")
            if len(spans) < 2:
                continue

            key = spans[0].get_text(strip=True)
            val = spans[1].get_text(" ", strip=True)

            key_lower = key.lower()

            # SKU
            if "item number" in key_lower:
                data["SKU"] = val
                continue

            # Stock
            if "availability" in key_lower:
                qty = re.sub(r"[^0-9]", "", val)
                if qty:
                    data["Stock"] = qty
                continue

            # Brand
            if "brand" in key_lower:
                data["Tags"] = val
                continue

            # Category
            if key_lower == "category":
                data["Categories"] = val
                continue

            # Weight
            if "weight" in key_lower and "kg" in key_lower:
                data["Weight (kg)"] = re.sub(r"[^0-9.]", "", val)
                continue

            # Woo attribute
            data[f"Attribute {attr_index} name"] = key
            data[f"Attribute {attr_index} value(s)"] = val
            data[f"Attribute {attr_index} visible"] = 1
            data[f"Attribute {attr_index} global"] = 1
            attr_index += 1

    if "SKU" not in data:
        data["SKU"] = f"SKU-{random.randint(100000,999999)}"

    return data

# ---------------- MAIN ----------------
async def main():
    args = parse_args()

    parsed = urlparse(args.url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"

    clean = re.sub(r'[\?&]p=\d+', '', args.url)
    page_url = clean + ("&p={}" if "?" in clean else "?p={}")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)

    product_links = set()
    products = []

    async with aiohttp.ClientSession(connector=connector) as session:
        # ---- Collect product links ----
        for page in range(args.start, args.end + 1):
            html = await fetch_html(session, page_url.format(page), sem, base_domain)
            if not html:
                continue

            soup = BeautifulSoup(html, "lxml")
            cards = soup.find_all("a", class_=lambda x: x and "product-labeled" in x)

            for a in cards:
                href = a.get("href")
                if href:
                    product_links.add(base_domain + href if href.startswith("/") else href)

        # ---- Scrape products ----
        for url in product_links:
            html = await fetch_html(session, url, sem, base_domain)
            if html:
                products.append(parse_product(html, base_domain))

    # ---- Export CSV ----
    df = pd.DataFrame(products)

    # Ensure Woo base columns
    for col in BASE_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Order columns
    attr_cols = sorted([c for c in df.columns if c.startswith("Attribute")])
    df = df[BASE_COLUMNS + attr_cols]

    df.to_csv(args.output, index=False)
    print(f"✅ WooCommerce CSV created: {args.output} ({len(df)} products)")

if __name__ == "__main__":
    asyncio.run(main())
