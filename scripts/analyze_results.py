import pandas as pd
import matplotlib.pyplot as plt
import os

# Check if the CSV exists
if not os.path.exists("data/rss_results.csv"):
    print("⚠️ No rss_results.csv file found. Run ingest_rss.py first.")
    exit()

try:
    # Load the RSS results
    df = pd.read_csv("data/rss_results.csv")
except pd.errors.EmptyDataError:
    print("⚠️ rss_results.csv is empty. Run ingest_rss.py with more feeds/keywords.")
    exit()

# If no rows, stop early
if df.empty:
    print("⚠️ No results found. Try running ingest_rss.py with more feeds/keywords.")
    exit()

# Split the matched_keywords column (because it can have multiple keywords per row)
all_keywords = []
for keywords in df["matched_keywords"].dropna():
    for kw in keywords.split(", "):
        all_keywords.append(kw)

# Count frequency
keyword_counts = pd.Series(all_keywords).value_counts()

# Print to terminal
print("\n📊 Keyword frequencies:")
print(keyword_counts)

# Plot a simple bar chart
plt.figure(figsize=(10, 5))
keyword_counts.plot(kind="bar")
plt.title("Trend Keyword Frequency")
plt.xlabel("Keyword")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
