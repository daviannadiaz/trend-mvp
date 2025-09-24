# scripts/farf_new_scraper.py
import argparse
import os
import random
import re
import time
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that", "are", "new",
    "all", "one", "our", "in", "on", "by", "of", "to", "a", "an", "at",
    "left", "just", "only", "preorder"
}

BASE = "https://www.farfetch.com"
START = "https://www.farfetch.com/en/sets/new-in-this-week-eu-women.aspx"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TrendScraper/1.0; +youremail@example.com)"}


def fetch(url, timeout=15):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def clean_tokens(text):
    words = re.findall(r"[A-Za-z']+", (text or "").lower())
    return [w for w in words if w not in STOPWORDS and len(w) > 2]


def run(max_products=None, sleep_min=1.0, sleep_max=2.2, verbose=True):
    html = fetch(START)
    soup = BeautifulSoup(html, "html.parser")

    product_cards = soup.find_all("a", attrs={"data-component": "ProductCardLink"})
    if verbose:
        print("Found product cards:", len(product_cards))

    items = []
    for a in product_cards[:max_products]:
        # Product URL
        href = a.get("href")
        url = urljoin(BASE, href) if href else ""

        # Brand / Designer
        designer_tag = a.select_one("[data-component='ProductCardBrandName']")
        designer = designer_tag.get_text(strip=True) if designer_tag else ""

        # Title / Description
        title_tag = a.select_one("[data-component='ProductCardDescription']")
        title = title_tag.get_text(strip=True) if title_tag else ""

        items.append({"url": url, "designer": designer, "title": title})

    # Analysis
    total = len(items)
    all_words = []
    for it in items:
        all_words.extend(clean_tokens(it["title"]))

    word_counts = Counter(all_words)
    top_words = word_counts.most_common(20)

    bigram_counts = Counter()
    for it in items:
        toks = clean_tokens(it["title"])
        for i in range(len(toks) - 1):
            bigram_counts[(toks[i], toks[i + 1])] += 1
    top_bigrams = [(" ".join(bg), c) for bg, c in bigram_counts.most_common(20)]

    designers = [it["designer"] for it in items if it["designer"]]
    designer_counts = Counter(designers)
    top_designers = designer_counts.most_common(10)

    # Save CSV in Moda Operandi style
    rows = []
    rows.append({"section": "summary", "metric": "total_items", "value": total})
    for word, cnt in top_words:
        rows.append({"section": "top_words", "metric": word, "value": cnt})
    for bg, cnt in top_bigrams:
        rows.append({"section": "top_bigrams", "metric": bg, "value": cnt})
    for ds, cnt in top_designers:
        rows.append({"section": "top_designers", "metric": ds, "value": cnt})
    for it in items:
        rows.append({"section": "items", "metric": f"{it['designer']} | {it['title']}", "value": it["url"]})

    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = f"data/farfetch_new_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    print("Saved", csv_path)

    # Quick summary
    print("TOTAL ITEMS:", total)
    print("Top 10 designers:", top_designers)
    print("Top 10 bigrams:", top_bigrams[:10])
    print("Top 20 words:", top_words[:20])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=None)
    ap.add_argument("--sleep-min", type=float, default=1.0)
    ap.add_argument("--sleep-max", type=float, default=2.2)
    args = ap.parse_args()
    run(max_products=args.max_products, sleep_min=args.sleep_min, sleep_max=args.sleep_max)


