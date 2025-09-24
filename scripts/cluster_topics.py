# scripts/cluster_topics.py
import numpy as np
import pandas as pd
from bertopic import BERTopic
from umap import UMAP
from hdbscan import HDBSCAN
import os, glob

# 🔑 Find the latest cleaned CSV
clean_files = glob.glob("data/rss_results_with_clean_*.csv")
if not clean_files:
    raise FileNotFoundError("No cleaned RSS results files found in data/")
latest_clean = sorted(clean_files)[-1]
print("Using input file:", latest_clean)
df = pd.read_csv(latest_clean)

# 🔑 Find the latest embeddings file
emb_files = glob.glob("models/embeddings_*.npy")
if not emb_files:
    raise FileNotFoundError("No embeddings files found in models/")
latest_emb = sorted(emb_files)[-1]
print("Using embeddings:", latest_emb)
emb = np.load(latest_emb)

# Optional: configure UMAP and HDBSCAN
umap_model = UMAP(n_neighbors=15, n_components=5, metric='cosine', random_state=42)
hdbscan_model = HDBSCAN(min_cluster_size=5, metric='euclidean', cluster_selection_method='eom')

# Fit BERTopic
topic_model = BERTopic(umap_model=umap_model, hdbscan_model=hdbscan_model, calculate_probabilities=False)
topics, probs = topic_model.fit_transform(df['text_clean'].astype(str).tolist(), embeddings=emb)

df['topic'] = topics

# Save topics summary
topics_info = topic_model.get_topic_info()
os.makedirs("models", exist_ok=True)
topic_model.save("models/bertopic_model")

# 🔑 Preserve columns
cols_to_keep = [c for c in df.columns if c in [
    'title', 'summary', 'link', 'matched_keywords', 'tags',
    'ingested_at', 'image_url', 'text_clean', 'topic'
]]
df_out = df[cols_to_keep]

# Save with timestamp
timestamp = os.path.basename(latest_clean).split("_")[-1].replace(".csv", "")
clustered_path = f"data/rss_results_clustered_{timestamp}.csv"
topics_info_path = f"data/topic_info_{timestamp}.csv"

df_out.to_csv(clustered_path, index=False)
topics_info.to_csv(topics_info_path, index=False)

print(f"✅ Clustering complete. Saved {clustered_path} and {topics_info_path} (ingested_at preserved).")




