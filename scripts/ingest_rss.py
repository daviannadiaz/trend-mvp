# scripts/ingest_rss.py
import requests, feedparser, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone  # <-- updated
import hashlib
import os

rss_feeds = [
    "https://www.businessoffashion.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://www.vanityfair.com/feed/rss",
    "https://1granary.substack.com/feed",
    "https://www.highsnobiety.com/feeds/rss",
    "https://allthingsfashiontech.substack.com/feed",
    "https://www.wired.com/feed",
    "https://i-d.co/feed/",
    "https://www.dezeen.com/feed/",
    "https://www.dazeddigital.com/rss",
    "https://hypebeast.com/feed",
    "https://www.fastcompany.com/latest/rss",
    "https://www.mckinsey.com/insights/rss",
    "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/FashionandStyle.xml",
    "https://feeds.content.dowjones.io/public/rss/RSSWorldNews",
    "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain"
]

keywords_df = pd.read_csv("data/seed_keywords.csv")  # columns: category,keyword
keywords = list(keywords_df['keyword'])

all_entries = []
seen_hashes = set()

headers = {'User-Agent': 'Mozilla/5.0'}

for feed_url in rss_feeds:
    try:
        resp = requests.get(feed_url, headers=headers, timeout=10)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        print("Failed to fetch:", feed_url, e)
        continue

    for entry in feed.entries:
        title = entry.get("title", "")
        summary_raw = entry.get("summary") or entry.get("description") or ""
        summary = BeautifulSoup(summary_raw, "html.parser").get_text(separator=" ").strip()
        link = entry.get("link", "")
        ingested_at = datetime.now(timezone.utc).isoformat()  # <-- timezone-aware UTC

        # dedupe key
        if link:
            key = link
        else:
            key = hashlib.sha1((title + summary).encode("utf-8")).hexdigest()

        if key in seen_hashes:
            continue
        seen_hashes.add(key)

        # basic keyword matching
        matched = []
        text_lower = f"{title} {summary}".lower()
        for kw in keywords:
            if kw.lower() in text_lower:
                matched.append(kw)

        # try to get image url if present
        image_url = ""
        if entry.get("media_content"):
            mc = entry.get("media_content")
            if isinstance(mc, list) and mc:
                image_url = mc[0].get("url", "")
        if not image_url:
            soup = BeautifulSoup(entry.get("summary", ""), "html.parser")
            img = soup.find("img")
            if img and img.get("src"):
                image_url = img.get("src")

        all_entries.append({
            "title": title,
            "summary": summary,
            "link": link,
            "matched_keywords": ", ".join(matched),
            "ingested_at": ingested_at,
            "image_url": image_url
        })

# save timestamped CSV
df = pd.DataFrame(all_entries)
os.makedirs("data", exist_ok=True)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")  # <-- timezone-aware
csv_path = f"data/rss_results_{timestamp}.csv"
df.to_csv(csv_path, index=False)
print("Saved", csv_path)

