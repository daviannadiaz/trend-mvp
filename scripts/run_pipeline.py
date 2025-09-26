# scripts/run_pipeline.py
"""
Master pipeline runner.
Runs all scripts in the correct order to refresh daily data.
Saves only today's outputs into a dated folder (DD-MM-YYYY).
"""

import subprocess
import sys
import os
from datetime import datetime
import shutil

# -------- Create dated folder --------
run_date = datetime.now().strftime("%d-%m-%Y")  # DD-MM-YYYY
base_data_dir = "data"
dated_dir = os.path.join(base_data_dir, run_date)

os.makedirs(dated_dir, exist_ok=True)
print(f"📂 Today's run will be archived in: {dated_dir}")

# -------- Scripts to run --------
pipeline = [
    "scripts/ingest_rss.py",
    "scripts/tag_keywords.py",
    "scripts/clean_embed.py",
    "scripts/cluster_topics.py",
    "scripts/calc_trend_scores.py",
    "scripts/analyze_frequencies.py",
    "scripts/analyze_results.py",
    "scripts/viz.py",
    "scripts/moda_new_scraper.py --max-products 50",
    "scripts/farf_new_scraper.py --max-products 50"
]

def run_script(script):
    print(f"\n🚀 Running: {script}")
    try:
        subprocess.run([sys.executable] + script.split(), check=True)
        print(f"✅ Finished: {script}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Snapshot of files before run
    before_files = set(os.listdir(base_data_dir))

    # Run pipeline scripts
    for script in pipeline:
        run_script(script)

    # Snapshot of files after run
    after_files = set(os.listdir(base_data_dir))

    # Identify only the new files created
    new_files = [f for f in (after_files - before_files) if os.path.isfile(os.path.join(base_data_dir, f))]

    # Move new files (except *_latest.csv) into today's folder
    for f in new_files:
        if f.endswith(".csv") and "_latest" in f:
            continue  # keep "latest" snapshots in root
        src = os.path.join(base_data_dir, f)
        dst = os.path.join(dated_dir, f)
        shutil.move(src, dst)

    print(f"\n🎉 All scripts completed. New results archived in: {dated_dir}")

