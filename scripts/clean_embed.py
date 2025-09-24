# scripts/clean_embed.py
import pandas as pd
import spacy
from sentence_transformers import SentenceTransformer
import numpy as np
import os
from datetime import datetime, timezone
import glob
import re
import sys

nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
embed_model = SentenceTransformer("all-MiniLM-L6-v2")  # small & fast

def find_latest_raw_ingest():
    """Prefer files exactly like rss_results_YYYYMMDD_HHMMSS.csv (raw ingest files).
       If none found, fallback to newest rss_results_*.csv that contains 'ingested_at' column.
    """
    files = sorted(glob.glob("data/rss_results_*.csv"))
    if not files:
        raise FileNotFoundError("No files matching data/rss_results_*.csv found. Run ingest_rss.py first.")
    # Prefer strictly-named raw ingest files
    ingest_pattern = re.compile(r"rss_results_\d{8}_\d{6}\.csv$")
    raw_candidates = [f for f in files if ingest_pattern.search(os.path.basename(f))]
    if raw_candidates:
        return sorted(raw_candidates)[-1]
    # Fallback: pick newest file that actually contains an ingested_at header
    for f in sorted(files, reverse=True):
        try:
            cols = pd.read_csv(f, nrows=0).columns.tolist()
            if "ingested_at" in cols:
                return f
        except Exception:
            continue
    # nothing found
    raise FileNotFoundError("No RSS ingest file with ingested_at found. Run ingest_rss.py and try again.")

# choose input file robustly
try:
    latest_file = find_latest_raw_ingest()
except FileNotFoundError as e:
    print("ERROR:", e)
    sys.exit(1)

print("Using input file:", latest_file)
df = pd.read_csv(latest_file)

def clean_text(text):
    doc = nlp(text)
    tokens = [t.lemma_.lower() for t in doc if not t.is_stop and t.is_alpha and len(t.lemma_) > 2]
    return " ".join(tokens)

# clean text
df['text_clean'] = (df['title'].fillna("") + " " + df['summary'].fillna("")).apply(clean_text)

# embed in batches
texts = df['text_clean'].astype(str).tolist()
embeddings = embed_model.encode(
    texts, show_progress_bar=True, convert_to_numpy=True, batch_size=32
)

# save with timestamp
os.makedirs("models", exist_ok=True)
os.makedirs("data", exist_ok=True)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

emb_path = f"models/embeddings_{timestamp}.npy"
csv_path = f"data/rss_results_with_clean_{timestamp}.csv"

np.save(emb_path, embeddings)

# ✅ keep important columns including ingested_at (if present)
cols_to_keep = [c for c in df.columns if c in [
    'title', 'summary', 'link', 'matched_keywords', 'tags',
    'ingested_at', 'text_clean'
]]
# if ingested_at was missing from the input, this will simply omit it and we warn
if 'ingested_at' not in df.columns:
    print("WARNING: input did not contain 'ingested_at' column; cleaned CSV will not have ingested_at.")
df_out = df[cols_to_keep]

df_out.to_csv(csv_path, index=False)
print(f"Saved {csv_path} and {emb_path} (ingested_at preserved if present)")


