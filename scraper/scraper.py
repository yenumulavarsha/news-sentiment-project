
import os
import requests
import csv
import time
from datetime import datetime




API_KEY = 'add the key in here'

# Check if the key was loaded properly
if not API_KEY:
    print("API Key not found. Please check the .env file.")
    exit()

# Function to fetch news from GNews API
def fetch_news(query, language='en', page_size=10):
    url = f'https://gnews.io/api/v4/search?q={query}&lang={language}&token={API_KEY}&max={page_size}'
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data['articles']
    else:
        print(f"Error: {response.status_code}")
        return None

# Function to save news data to a dated CSV file
def save_to_csv(news_data):
    today = datetime.now().strftime('%Y-%m-%d')
    os.makedirs("data/raw", exist_ok=True)
    filename = f'data/raw/news_raw_{today}.csv'
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["topic", "title", "description", "publishedAt", "source", "url"])
        writer.writeheader()
        for article in news_data:
            writer.writerow(article)
    print(f"News data saved to {filename}")

# Main function to fetch news for multiple topics
def main():
    all_news = []
    seen_articles = set()  # To keep track of deduplicated articles
    topics = ["technology", "politics", "sports", "business", "world"]
    
    for topic in topics:
        news_data = fetch_news(topic)
        time.sleep(1)  # Add a slight delay to avoid hitting API rate limits
        if news_data:
            for article in news_data:
                # Deduplication logic: Check if article title + publishedAt is already seen
                article_key = (article.get("title", ""), article.get("publishedAt", ""))
                if article_key not in seen_articles:
                    seen_articles.add(article_key)
                    all_news.append({
                        "topic": topic,
                        "title": article.get("title", "N/A"),
                        "description": article.get("description", "N/A"),
                        "publishedAt": article.get("publishedAt", "N/A"),
                        "source": article.get("source", {}).get("name", "N/A"),
                        "url": article.get("url", "N/A")
                    })

    # Save to CSV
    save_to_csv(all_news)

if __name__ == "__main__":
    main()
