# scripts/viz.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("data/rss_results_clustered.csv")
topic_counts = df['topic'].value_counts().sort_values(ascending=False)

plt.figure(figsize=(8,5))
sns.barplot(x=topic_counts.index.astype(str), y=topic_counts.values)
plt.title("Topic sizes")
plt.xlabel("Topic")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("data/topic_counts.png")
print("Saved data/topic_counts.png")
