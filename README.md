# Trend-MVP

This project collects fashion and lifestyle news from multiple RSS feeds, tags them with relevant keywords, processes the text with NLP (using embeddings and clustering), and generates topic analysis and visualizations.  

It’s designed as a pipeline to track trends, keywords, and topics in fashion and tech news.

## Project Structure
- `scripts/` - Python scripts for the full pipeline:
    - `ingest_rss.py` - Collects RSS articles and saves them to CSV
    - `tag_keywords.py` - Adds category tags to each article
    - `clean_embed.py` - Cleans text and generates embeddings
    - `cluster_topics.py` - Clusters articles into topics
    - `analyze_frequencies.py` - Counts keyword and topic frequencies
    - `analyze_results.py` - Aggregates insights
    - `viz.py` - Creates visualizations (plots)
- `data/` - Stores CSV outputs and embeddings
- `models/` - Stores large models and embeddings (tracked with Git LFS)

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

# Install requirements
pip install -r requirements.txt

# (Optional) install Git LFS for large files
brew install git-lfs
git lfs install

# Pipeline order:
1. `python scripts/ingest_rss.py` - Collect RSS articles
2. `python scripts/tag_keywords.py` - Tag articles with keywords/categories
3. `python scripts/clean_embed.py` - Clean text & generate embeddings
4. `python scripts/cluster_topics.py` - Cluster articles into topics
5. `python scripts/analyze_frequencies.py` - Count frequencies
6. `python scripts/analyze_results.py` - Aggregate insights
7. `python scripts/viz.py` - Generate visualizations


# Davianna Diaz