# scripts/tag_keywords.py
import pandas as pd
import os
import glob
import re
import sys
from datetime import datetime, timezone

def find_latest_raw_ingest():
    """Prefer files exactly like rss_results_YYYYMMDD_HHMMSS.csv (raw ingest files).
       If none found, fallback to newest rss_results_*.csv that contains 'ingested_at' column.
    """
    files = sorted(glob.glob("data/rss_results_*.csv"))
    if not files:
        raise FileNotFoundError("No files matching data/rss_results_*.csv found. Run ingest_rss.py first.")
    ingest_pattern = re.compile(r"rss_results_\d{8}_\d{6}\.csv$")
    raw_candidates = [f for f in files if ingest_pattern.search(os.path.basename(f))]
    if raw_candidates:
        return sorted(raw_candidates)[-1]
    # fallback: newest file that actually contains ingested_at
    for f in sorted(files, reverse=True):
        try:
            cols = pd.read_csv(f, nrows=0).columns.tolist()
            if "ingested_at" in cols:
                return f
        except Exception:
            continue
    raise FileNotFoundError("No RSS ingest file with ingested_at found. Run ingest_rss.py and try again.")

# --- choose input file robustly ---
try:
    latest_file = find_latest_raw_ingest()
except FileNotFoundError as e:
    print("ERROR:", e)
    sys.exit(1)

print("Using latest RSS file:", latest_file)
df = pd.read_csv(latest_file)

# Step 2: load seed keywords file expected to have columns 'category','keyword'
kw = pd.read_csv("data/seed_keywords.csv")
kw_map = {}
for _, row in kw.iterrows():
    cat = row['category']
    keyword = row['keyword']
    kw_map.setdefault(cat, []).append(keyword.lower())

# Step 3: function to generate tags for each row
def tags_for_row(title, summary):
    text = f"{title} {summary}".lower()
    tags = []
    for cat, words in kw_map.items():
        for w in words:
            if w in text:
                tags.append(cat)
                break
    return list(set(tags))

# Step 4: apply tagging
df['tags'] = df.apply(lambda r: tags_for_row(r.get('title', ''), r.get('summary', '')), axis=1)

# warn if ingested_at missing (helps debugging)
if 'ingested_at' not in df.columns:
    print("WARNING: input did not contain 'ingested_at' column. Tagging complete but timestamps are not present.")

# Step 5: save a timestamped CSV and (optionally) a convenience latest copy
os.makedirs("data", exist_ok=True)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
out_ts = f"data/rss_results_tagged_{timestamp}.csv"
out_latest = "data/rss_results_tagged.csv"

df.to_csv(out_ts, index=False)
# also update the non-timestamped "latest" file for compatibility with other scripts
df.to_csv(out_latest, index=False)

print("Tagged rows saved to", out_ts)
print("Also updated latest copy:", out_latest)





