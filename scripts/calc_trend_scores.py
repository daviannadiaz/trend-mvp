# scripts/calc_trend_scores.py
"""
Calculate weekly Trend Scores for each topic across *all* history.

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

# ---------- Load all clustered CSVs ----------
files = sorted(glob.glob("data/rss_results_clustered_*.csv"))
if not files:
    raise FileNotFoundError("No clustered CSVs found. Run cluster_topics.py first.")

print(f"Found {len(files)} clustered files.")
dfs = []
for f in files:
    try:
        df_tmp = pd.read_csv(f, low_memory=False)
        df_tmp['__source_file'] = os.path.basename(f)  # track origin
        dfs.append(df_tmp)
    except Exception as e:
        print(f"⚠️ Skipping {f}: {e}")

df = pd.concat(dfs, ignore_index=True)

# ---------- Ensure ingested_at exists ----------
if 'ingested_at' not in df.columns:
    raise KeyError("'ingested_at' column not found. Make sure ingest + cleaning pipeline preserved it.")

df['ingested_at'] = pd.to_datetime(df['ingested_at'], utc=True, errors='coerce')
df = df.dropna(subset=['ingested_at'])

# Deduplicate by link + title + topic
if 'link' in df.columns:
    df = df.drop_duplicates(subset=['link','title','topic'], keep='last')

# Ensure topic is string
df['topic'] = df['topic'].astype(str)

# ---------- Ensure source column ----------
if 'source' not in df.columns and 'link' in df.columns:
    df['source'] = df['link'].fillna("").apply(
        lambda u: urlparse(u).netloc.lower().replace('www.', '') if pd.notna(u) and u else ""
    )
elif 'source' not in df.columns:
    df['source'] = "unknown"

# ---------- Define time bins (weeks) ----------
df['year_week'] = df['ingested_at'].dt.strftime('%Y-%W')  # ISO week
N_WEEKS = 6  # keep last 6 weeks history
all_weeks = sorted(df['year_week'].dropna().unique())
recent_weeks = all_weeks[-N_WEEKS:]

print("\n🔎 Distinct weeks in dataset:")
print(df['year_week'].value_counts())
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
v = np.clip(metrics_df['velocity'].fillna(0).values.reshape(-1,1), -5, 5)
v_shifted = v - v.min()
r = metrics_df['recency'].fillna(0).values.reshape(-1,1)
s = metrics_df['source_count'].fillna(0).values.reshape(-1,1)

scaler = MinMaxScaler()
metrics_df['velocity_norm'] = scaler.fit_transform(v_shifted).flatten()
metrics_df['recency_norm'] = scaler.fit_transform(r).flatten()
metrics_df['source_norm'] = scaler.fit_transform(s).flatten()

# ---------- Combine into Trend Score ----------
W_V, W_R, W_S = 0.4, 0.3, 0.3
metrics_df['trend_score'] = (
    W_V * metrics_df['velocity_norm'] +
    W_R * metrics_df['recency_norm'] +
    W_S * metrics_df['source_norm']
) * 100

# ---------- Compare to previous snapshot ----------
latest_path = "data/trend_scores_latest.csv"
if os.path.exists(latest_path):
    prev = pd.read_csv(latest_path).set_index('topic')['trend_score'].to_dict()
    metrics_df['score_prev'] = metrics_df['topic'].map(prev)
    metrics_df['score_delta'] = metrics_df['trend_score'] - metrics_df['score_prev']
else:
    metrics_df['score_prev'] = np.nan
    metrics_df['score_delta'] = np.nan

# ---------- Save outputs ----------
os.makedirs("data", exist_ok=True)
ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
out_file = f"data/trend_scores_{ts}.csv"
metrics_df.to_csv(out_file, index=False)
metrics_df.to_csv(latest_path, index=False)
print(f"\n✅ Saved trend scores: {out_file} and {latest_path}")

# ---------- Quick terminal check ----------
print("\nTop 10 topics by trend_score:")
print(metrics_df.sort_values('trend_score', ascending=False).head(10)[['topic','trend_score','mentions_this_week','source_count']])
