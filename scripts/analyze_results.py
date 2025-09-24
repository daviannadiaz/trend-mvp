# scripts/analyze_results.py
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
from datetime import datetime

# --- Step 1: Find latest tagged CSV ---
tagged_files = sorted(glob.glob("data/rss_results_tagged_*.csv"))
if tagged_files:
    latest_file = tagged_files[-1]
else:
    latest_file = "data/rss_results_tagged.csv"

if not os.path.exists(latest_file):
    print("⚠️ No tagged RSS results file found. Run ingest_rss.py → tag_keywords.py first.")
    exit()

try:
    df = pd.read_csv(latest_file)
except pd.errors.EmptyDataError:
    print("⚠️ Tagged results file is empty. Run ingest_rss.py with more feeds/keywords.")
    exit()

if df.empty:
    print("⚠️ No results found. Try running ingest_rss.py with more feeds/keywords.")
    exit()

print("Using input file:", latest_file)

# --- Step 2: Extract matched keywords ---
all_keywords = []
if "matched_keywords" in df.columns:
    for keywords in df["matched_keywords"].dropna():
        for kw in keywords.split(", "):
            all_keywords.append(kw.strip())

if not all_keywords:
    print("⚠️ No matched keywords found in the dataset.")
    exit()

keyword_counts = pd.Series(all_keywords).value_counts()

# --- Step 3: Print to terminal ---
print("\n📊 Keyword frequencies:")
print(keyword_counts)

# --- Step 4: Export results ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_csv = f"data/keyword_frequencies_{timestamp}.csv"
keyword_counts.to_csv(output_csv, header=["count"])
print(f"\n✅ Keyword frequencies exported to {output_csv}")

# --- Step 5: Plot bar chart ---
plt.figure(figsize=(10, 5))
keyword_counts.plot(kind="bar")
plt.title("Trend Keyword Frequency")
plt.xlabel("Keyword")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
