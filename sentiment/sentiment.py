import os
import csv
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Get today’s date and file paths
today = datetime.now().strftime('%Y-%m-%d')
raw_path = f'data/raw/news_raw_{today}.csv'
cleaned_path = f'data/cleaned/news_cleaned_{today}.csv'

# Ensure output folder exists
os.makedirs("data/cleaned", exist_ok=True)

# Function to analyze sentiment
def analyze_sentiment(text):
    score = analyzer.polarity_scores(text)['compound']
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

# Main function
def process_news():
    if not os.path.exists(raw_path):
        print(f"No raw news file found for today: {raw_path}")
        return

    cleaned_data = []
    with open(raw_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            full_text = f"{row['title']} {row['description']}"
            sentiment = analyze_sentiment(full_text)
            row['sentiment'] = sentiment
            cleaned_data.append(row)

    with open(cleaned_path, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = list(cleaned_data[0].keys())
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_data)

    print(f"Sentiment-tagged data saved to: {cleaned_path}")

if __name__ == "__main__":
    process_news()
