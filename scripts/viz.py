# scripts/viz.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os
from datetime import datetime

# --- Step 1: Find latest clustered CSV ---
clustered_files = sorted(glob.glob("data/rss_results_clustered_*.csv"))
if clustered_files:
    latest_file = clustered_files[-1]
else:
    latest_file = "data/rss_results_clustered.csv"

if not os.path.exists(latest_file):
    print("⚠️ No clustered results file found. Run cluster_topics.py first.")
    exit()

try:
    df = pd.read_csv(latest_file)
except pd.errors.EmptyDataError:
    print("⚠️ Clustered results file is empty. Run cluster_topics.py again.")
    exit()

if df.empty:
    print("⚠️ No data found in clustered results.")
    exit()

print("Using input file:", latest_file)

# --- Step 2: Count topics ---
if "topic" not in df.columns:
    print("⚠️ No 'topic' column found in the dataset.")
    exit()

topic_counts = df["topic"].value_counts().sort_values(ascending=False)

# --- Step 3: Plot ---
plt.figure(figsize=(8, 5))
sns.barplot(x=topic_counts.index.astype(str), y=topic_counts.values, color="steelblue")
plt.title("Topic Sizes")
plt.xlabel("Topic")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()

# --- Step 4: Save plot with timestamp ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"data/topic_counts_{timestamp}.png"
plt.savefig(output_path)
print(f"✅ Saved {output_path}")

