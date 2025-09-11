import pandas as pd
import re
from collections import Counter
from itertools import islice
from nltk.util import ngrams
import nltk
from datetime import datetime

# ✅ Ensure stopwords are available
try:
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words("english"))
except LookupError:
    nltk.download("stopwords")
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words("english"))

# Load results
df = pd.read_csv("data/rss_results.csv")

if df.empty:
    print("⚠️ No results to analyze. Try running ingest_rss.py first.")
    exit()

# Combine all text (titles + summaries)
text_data = " ".join(df["title"].astype(str) + " " + df["summary"].astype(str))

# Clean text: remove punctuation, lower case
text_data = re.sub(r"[^a-zA-Z\s]", "", text_data).lower()

# Tokenize into words
words = text_data.split()

# Remove stopwords (common words like 'the', 'and', 'to')
filtered_words = [w for w in words if w not in stop_words and len(w) > 2]

# ---- WORD FREQUENCIES ----
word_counts = Counter(filtered_words)
print("\n🔝 Top 20 Words:")
for word, freq in word_counts.most_common(20):
    print(f"{word}: {freq}")

# ---- BIGRAM FREQUENCIES ----
bigrams = list(ngrams(filtered_words, 2))
bigram_counts = Counter(bigrams)
print("\n🔝 Top 20 Bigrams:")
for phrase, freq in bigram_counts.most_common(20):
    print(f"{' '.join(phrase)}: {freq}")

# ---- EXPORT TO EXCEL WITH TIMESTAMP ----
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
excel_path = f"data/frequency_analysis_{timestamp}.xlsx"

with pd.ExcelWriter(excel_path) as writer:
    # Word frequencies
    pd.DataFrame(word_counts.most_common(50), columns=["Word", "Count"]).to_excel(
        writer, sheet_name="Word Frequencies", index=False
    )
    # Bigram frequencies
    pd.DataFrame(
        [(" ".join(k), v) for k, v in bigram_counts.most_common(50)],
        columns=["Bigram", "Count"]
    ).to_excel(writer, sheet_name="Bigram Frequencies", index=False)

print(f"\n✅ Frequency analysis exported to {excel_path}")

# ---- INTERACTIVE SEARCH (Word or Bigram) ----
print("\n🔍 Search for words or bigrams in the articles (type 'exit' to quit):")
while True:
    user_input = input("Enter a word or bigram: ").strip()
    if user_input.lower() == "exit":
        break
    search_words = user_input.lower().split()
    
    if len(search_words) not in [1, 2]:
        print("⚠️ Please enter either one word or two words (bigram).")
        continue

    print(f"\nArticles containing '{user_input}':\n")
    found = False
    for idx, row in df.iterrows():
        text = (row['title'] + " " + row['summary']).lower()
        if all(word in text for word in search_words):
            # Highlight searched words in the summary
            summary_highlight = row['summary']
            for word in search_words:
                summary_highlight = re.sub(f"(?i)({word})", r"[\1]", summary_highlight)
            print(f"- {row['title']}\n  {summary_highlight}\n  Link: {row['link']}\n")
            found = True
    if not found:
        print("No articles found containing that search.\n")
