import requests
import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime  # <-- add this

# Step 1: list of RSS feeds to collect from
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

# Step 2: load your keywords
keywords_df = pd.read_csv("data/seed_keywords.csv")

# ✅ Debug line: show what keywords we’re actually checking
print("Checking keywords:", list(keywords_df['keyword']))

# Step 3: create an empty list to store results
all_entries = []

# Step 4: go through each RSS feed with requests + feedparser
headers = {'User-Agent': 'Mozilla/5.0'}

for feed_url in rss_feeds:
    print(f"\nParsing feed: {feed_url}")
    
    try:
        response = requests.get(feed_url, headers=headers, timeout=10)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
    except Exception as e:
        print(f"❌ Failed to fetch feed: {e}")
        continue
    
    print("Feed status:", response.status_code)
    print("Number of entries:", len(feed.entries))
    print("First 5 article titles:", [entry.get("title", "") for entry in feed.entries[:5]])
    
    for entry in feed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        summary_text = BeautifulSoup(summary, "html.parser").get_text()
        summary_text = summary_text[:200] + "..." if len(summary_text) > 200 else summary_text
        link = entry.get("link", "")
        
        matched_keywords = []
        for kw in keywords_df['keyword']:
            if kw.lower() in title.lower() or kw.lower() in summary_text.lower():
                matched_keywords.append(kw)
        
        if matched_keywords:
            all_entries.append({
                "title": title,
                "summary": summary_text,
                "link": link,
                "matched_keywords": ", ".join(matched_keywords)
            })

# Step 5: save to CSV with timestamp
df = pd.DataFrame(all_entries)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # e.g., 20250909_153045
filename = f"data/rss_results_{timestamp}.csv"
df.to_csv(filename, index=False)
print(f"\n✅ RSS ingest complete! Results saved to {filename}")
