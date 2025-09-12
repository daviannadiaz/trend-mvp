# scripts/tag_keywords.py
import pandas as pd
import os
import glob

# Step 1: automatically find the latest rss_results CSV
rss_files = glob.glob("data/rss_results_*.csv")
if not rss_files:
    raise FileNotFoundError("No RSS results files found in data/")

# sort by filename (timestamps in YYYYMMDD_HHMMSS format ensure correct order)
latest_file = sorted(rss_files)[-1]
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
df['tags'] = df.apply(lambda r: tags_for_row(r['title'], r['summary']), axis=1)

# Step 5: save tagged CSV
output_path = "data/rss_results_tagged.csv"
df.to_csv(output_path, index=False)
print("Tagged rows saved to", output_path)



