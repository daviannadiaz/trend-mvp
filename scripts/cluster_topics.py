# scripts/cluster_topics.py
import numpy as np
import pandas as pd
from bertopic import BERTopic
from umap import UMAP
from hdbscan import HDBSCAN
import os

emb = np.load("models/embeddings.npy")
df = pd.read_csv("data/rss_results_with_clean.csv")

# Optional: configure UMAP and HDBSCAN
umap_model = UMAP(n_neighbors=15, n_components=5, metric='cosine', random_state=42)
hdbscan_model = HDBSCAN(min_cluster_size=5, metric='euclidean', cluster_selection_method='eom')

topic_model = BERTopic(umap_model=umap_model, hdbscan_model=hdbscan_model, calculate_probabilities=False)
topics, probs = topic_model.fit_transform(df['text_clean'].tolist(), embeddings=emb)

df['topic'] = topics
# Save topics summary
topics_info = topic_model.get_topic_info()
os.makedirs("models", exist_ok=True)
topic_model.save("models/bertopic_model")
df.to_csv("data/rss_results_clustered.csv", index=False)
topics_info.to_csv("data/topic_info.csv", index=False)
print("Clustering complete. Saved topic_info and clustered CSV.")

