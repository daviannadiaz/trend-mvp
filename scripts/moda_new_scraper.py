# scripts/moda_new_scraper.py
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

# Optional: NLTK stopwords (uncomment if you prefer using NLTK)
# import nltk
# nltk.download("stopwords")
# from nltk.corpus import stopwords
# STOPWORDS = set(stopwords.words("english"))

# Lightweight stopwords to avoid requiring nltk for quick runs
STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that", "are", "new",
    "all", "one", "our", "in", "on", "by", "of", "to", "a", "an", "at",
    "all", "left", "just", "only", "preorder"
}

BASE = "https://www.modaoperandi.com"
START = "https://www.modaoperandi.com/new"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TrendScraper/1.0; +youremail@example.com)"}


def fetch(url, timeout=12):
    """Simple GET with error handling."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def find_product_links_from_category(html):
    """Return absolute product URLs found in a category page by matching /women/p/ hrefs."""
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # target product detail pattern: /women/p/...
        if re.search(r"^/women/p/", href):
            links.add(urljoin(BASE, href))
    return links


def get_next_page(html):
    """Try a few common ways to find the 'next' page link."""
    soup = BeautifulSoup(html, "html.parser")
    # rel=next link
    link = soup.find("link", rel="next")
    if link and link.get("href"):
        return urljoin(BASE, link["href"])
    # aria-label / exact text
    a = soup.find("a", attrs={"aria-label": re.compile(r"next", re.I)})
    if a and a.get("href"):
        return urljoin(BASE, a["href"])
    a2 = soup.find("a", string=re.compile(r"^\s*Next\s*$", re.I))
    if a2 and a2.get("href"):
        return urljoin(BASE, a2["href"])
    # fallback: pagination anchors with page=
    for a in soup.find_all("a", href=True):
        if "page=" in a["href"] and re.search(r"page=\d+", a["href"]):
            # pick the next numeric page after current by inspecting visible text (best-effort)
            if a.get_text(strip=True).isdigit():
                continue
            if re.search(r"next", a.get_text(strip=True), re.I):
                return urljoin(BASE, a["href"])
    return None


def extract_title_from_card(a_tag):
    """Try to get product title from an anchor card: title attribute or image alt or visible text."""
    # title attribute (sometimes anchors carry it)
    title = a_tag.get("title")
    if title:
        return title.strip()
    img = a_tag.find("img")
    if img and img.get("alt"):
        return img["alt"].strip()
    txt = a_tag.get_text(" ", strip=True)
    return txt.strip() if txt else None


def extract_designer_from_card(a_tag):
    """Heuristic: look for patterns inside the card text like 'preorder {Designer}' or 'only X left {Designer}'."""
    txt = a_tag.get_text(" ", strip=True)
    # Look for patterns like 'preorder Erdem' or 'only 1 left Adam Lippes'
    m = re.search(r"(?:preorder|only\s+\d+\s+left)\s+([A-Z][A-Za-z &'().-]{1,60})", txt)
    if m:
        return m.group(1).strip()
    # sometimes designer appears as the first word(s) before title in card text e.g. "Erdem Embellished Midi Dress"
    # try splitting and checking capitalized tokens
    pieces = txt.split()
    if pieces and pieces[0].istitle() and len(pieces[0]) > 1:
        # return 1-3 token candidate
        cand = " ".join(pieces[:3])
        # trim if obviously long noise
        return cand.strip()
    return None


def parse_product_page(url):
    """Fetch product page and extract title + designer using a few heuristics (h1, og:title, 'All <Designer>' pattern)."""
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    # title: try og:title then h1
    title = None
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        title = og["content"].strip()
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

    # designer: try link to designers, or "All {Designer}" text, or first nearby anchor
    designer = None
    # 1) link to /designers/ or /designer/
    designer_link = soup.find("a", href=re.compile(r"/designers?/|/brand/|/designer/"), string=True)
    if designer_link:
        designer = designer_link.get_text(strip=True)
    # 2) "All {Designer}" pattern (observed on pages)
    if not designer:
        m = re.search(r"All\s+([A-Z][A-Za-z &'().-]{1,60})", html)
        if m:
            designer = m.group(1).strip()
    # 3) fallback: look for capitalized short anchor texts
    if not designer:
        for a in soup.find_all("a", href=True):
            txt = a.get_text(" ", strip=True)
            if not txt:
                continue
            if txt.lower() in {"women", "clothing", "dresses", "shoes", "bags", "accessories", "sale"}:
                continue
            # reasonable name heuristic: 2-4 words, Titlecase-ish
            if 1 <= len(txt.split()) <= 4 and txt[0].isalpha() and txt[0].isupper():
                designer = txt
                break

    return {"url": url, "title": title or "", "designer": designer or ""}


def clean_tokens(text):
    # simple tokenization: letters only, lowercase; drop stopwords and length <= 2
    words = re.findall(r"[A-Za-z']+", (text or "").lower())
    return [w for w in words if w not in STOPWORDS and len(w) > 2]


def run(max_products=None, sleep_min=1.0, sleep_max=2.2, verbose=True):
    # 1) crawl category pages, gather product URLs
    next_url = START
    product_urls = []
    seen = set()
    while next_url:
        if verbose:
            print("Fetching category:", next_url)
        html = fetch(next_url)
        found = find_product_links_from_category(html)
        if verbose:
            print("  found", len(found), "product links on page")
        for u in found:
            if u not in seen:
                seen.add(u)
                product_urls.append(u)
                if max_products and len(product_urls) >= max_products:
                    break
        if max_products and len(product_urls) >= max_products:
            break
        next_url = get_next_page(html)
        if next_url and verbose:
            print("  next page:", next_url)
        time.sleep(sleep_min + random.random() * (sleep_max - sleep_min))

    if verbose:
        print("Total unique product URLs collected:", len(product_urls))

    # 2) try to extract title + designer from category anchors first by re-fetching the category pages (fast path)
    # But to keep code simpler/robust we fetch each product page (guarantees data consistency).
    items = []
    for i, purl in enumerate(product_urls, 1):
        if verbose and i % 50 == 0:
            print(f"Processing {i}/{len(product_urls)}: {purl}")
        try:
            item = parse_product_page(purl)
            items.append(item)
        except Exception as e:
            if verbose:
                print("  failed parsing:", purl, e)
        time.sleep(sleep_min + random.random() * (sleep_max - sleep_min))
        if max_products and len(items) >= max_products:
            break

    # analysis
    total = len(items)
    all_words = []
    for it in items:
        all_words.extend(clean_tokens(it["title"]))

    word_counts = Counter(all_words)
    top_words = word_counts.most_common(20)

    bigram_counts = Counter()
    # build bigrams
    for it in items:
        toks = clean_tokens(it["title"])
        for i in range(len(toks) - 1):
            bigram_counts[(toks[i], toks[i + 1])] += 1
    top_bigrams = [(" ".join(bg), c) for bg, c in bigram_counts.most_common(20)]

    designers = [it["designer"] for it in items if it["designer"]]
    designer_counts = Counter(designers)
    top_designers = designer_counts.most_common(10)

    # save tidy CSV similar to your ingest_rss layout
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
    csv_path = f"data/moda_new_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    print("Saved", csv_path)

    # Also print quick summary to console
    print("TOTAL ITEMS:", total)
    print("Top 10 designers:", top_designers)
    print("Top 10 bigrams:", top_bigrams[:10])
    print("Top 20 words:", top_words[:20])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=None, help="Limit total products for fast testing")
    ap.add_argument("--sleep-min", type=float, default=1.0)
    ap.add_argument("--sleep-max", type=float, default=2.2)
    args = ap.parse_args()
    run(max_products=args.max_products, sleep_min=args.sleep_min, sleep_max=args.sleep_max)



