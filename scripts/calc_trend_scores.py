# scripts/calc_trend_scores.py
"""
Calculate weekly Trend Scores for each topic.

Outputs:
 - data/trend_scores_<timestamp>.csv  (timestamped snapshot)
 - data/trend_scores_latest.csv       (overwrites, for dashboard)
"""

import os, glob
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from urllib.parse import urlparse
from sklearn.preprocessing import MinMaxScaler

# ---------- Helper: find the latest clustered CSV ----------
def find_latest_clustered():
    files = sorted(glob.glob("data/rss_results_clustered_*.csv"))
    if not files:
        raise FileNotFoundError("No timestamped clustered CSV found. Run cluster_topics.py first.")
    return files[-1]

source_file = find_latest_clustered()
print("Using source:", source_file)

# ---------- Load data ----------
df = pd.read_csv(source_file, low_memory=False)

# Ensure ingested_at exists and is parsed
if 'ingested_at' not in df.columns:
    raise KeyError("'ingested_at' column not found. Make sure your ingest + cleaning pipeline preserved it.")

df['ingested_at'] = pd.to_datetime(df['ingested_at'], utc=True, errors='coerce')
if df['ingested_at'].isna().all():
    raise ValueError("ingested_at column could not be parsed as datetimes. Check its format.")

# If 'topic' is not numeric, keep as string (BERTopic gives ints)
df['topic'] = df['topic'].astype(str)

# ---------- Ensure there is a source/domain column ----------
if 'source' not in df.columns and 'link' in df.columns:
    df['source'] = df['link'].fillna("").apply(lambda u: urlparse(u).netloc.lower().replace('www.', '') if pd.notna(u) and u else "")
elif 'source' not in df.columns:
    df['source'] = "unknown"

# ---------- Define time bins (weeks) ----------
df['year_week'] = df['ingested_at'].dt.strftime('%Y-%W')  # ISO week
N_WEEKS = 4
all_weeks = sorted(df['year_week'].dropna().unique())
recent_weeks = all_weeks[-N_WEEKS:]
print("Weeks considered (up to last N):", recent_weeks)

# ---------- Aggregate mentions ----------
group = df.groupby(['topic','year_week']).size().rename('mentions').reset_index()
group_source = df.groupby(['topic','year_week','source']).size().rename('mentions_by_source').reset_index()

latest_week = recent_weeks[-1]
prev_week = recent_weeks[-2] if len(recent_weeks) >= 2 else None

mentions_this = group[group['year_week'] == latest_week].set_index('topic')['mentions'].to_dict()
mentions_prev = group[group['year_week'] == prev_week].set_index('topic')['mentions'].to_dict() if prev_week else {}
src_counts = group_source[group_source['year_week'] == latest_week].groupby('topic')['source'].nunique().to_dict()

# ---------- Compute Trend Metrics ----------
topics = sorted(set(group['topic'].unique()))
rows = []
for topic in topics:
    this_count = int(mentions_this.get(topic, 0))
    prev_count = int(mentions_prev.get(topic, 0)) if prev_week else 0
    denom = max(1, prev_count)
    velocity = (this_count - prev_count) / denom
    recency = 2 * this_count + prev_count
    source_count = int(src_counts.get(topic, 0))
    examples = df[(df['topic']==topic) & (df['year_week']==latest_week)]
    rep_headlines = examples['title'].dropna().astype(str).unique().tolist()[:3]
    rows.append({
        'topic': topic,
        'mentions_this_week': this_count,
        'mentions_prev_week': prev_count,
        'velocity': velocity,
        'recency': recency,
        'source_count': source_count,
        'rep_headlines': " || ".join(rep_headlines)
    })

metrics_df = pd.DataFrame(rows)

# ---------- Normalize metrics ----------
norm_cols = ['velocity', 'recency', 'source_count']
v = np.clip(metrics_df['velocity'].fillna(0).values.reshape(-1,1), -5, 5)
v_shifted = v - v.min()
r = metrics_df['recency'].fillna(0).values.reshape(-1,1)
s = metrics_df['source_count'].fillna(0).values.reshape(-1,1)

scaler = MinMaxScaler()
v_norm = scaler.fit_transform(v_shifted)
r_norm = scaler.fit_transform(r)
s_norm = scaler.fit_transform(s)

metrics_df['velocity_norm'] = v_norm.flatten()
metrics_df['recency_norm'] = r_norm.flatten()
metrics_df['source_norm'] = s_norm.flatten()

# ---------- Combine into Trend Score ----------
W_V, W_R, W_S = 0.4, 0.3, 0.3
metrics_df['trend_score'] = (W_V * metrics_df['velocity_norm'] + 
                             W_R * metrics_df['recency_norm'] +
                             W_S * metrics_df['source_norm']) * 100

# Compute delta vs previous
latest_path = "data/trend_scores_latest.csv"
if os.path.exists(latest_path):
    prev = pd.read_csv(latest_path).set_index('topic')['trend_score'].to_dict()
    metrics_df['score_prev'] = metrics_df['topic'].map(prev)
    metrics_df['score_delta'] = metrics_df['trend_score'] - metrics_df['score_prev']
else:
    metrics_df['score_prev'] = np.nan
    metrics_df['score_delta'] = np.nan

# ---------- Save outputs (timestamped + latest) ----------
os.makedirs("data", exist_ok=True)
ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
metrics_df.to_csv(f"data/trend_scores_{ts}.csv", index=False)
metrics_df.to_csv(latest_path, index=False)
print(f"✅ Saved trend scores: data/trend_scores_{ts}.csv and latest snapshot: {latest_path}")

# ---------- Quick terminal check ----------
print("\nTop 10 topics by trend_score:")
print(metrics_df.sort_values('trend_score', ascending=False).head(10)[['topic','trend_score','mentions_this_week','source_count']])
