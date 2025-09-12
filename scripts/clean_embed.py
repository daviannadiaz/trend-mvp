# scripts/clean_embed.py
import pandas as pd
import spacy
from sentence_transformers import SentenceTransformer
import numpy as np
import os

nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
embed_model = SentenceTransformer("all-MiniLM-L6-v2")  # small & fast

df = pd.read_csv("data/rss_results_tagged.csv")  # or rss_results_<timestamp>.csv

def clean_text(text):
    doc = nlp(text)
    tokens = [t.lemma_.lower() for t in doc if not t.is_stop and t.is_alpha and len(t.lemma_) > 2]
    return " ".join(tokens)

df['text_clean'] = (df['title'].fillna("") + " " + df['summary'].fillna("")).apply(clean_text)

# embed in batches
texts = df['text_clean'].astype(str).tolist()
embeddings = embed_model.encode(texts, show_progress_bar=True, convert_to_numpy=True, batch_size=32)

os.makedirs("models", exist_ok=True)
np.save("models/embeddings.npy", embeddings)
df.to_csv("data/rss_results_with_clean.csv", index=False)
print("Saved embeddings and cleaned CSV")

