import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os

# Get today's date
today = datetime.now().strftime('%Y-%m-%d')
cleaned_path = f'data/cleaned/news_cleaned_{today}.csv'

# Check if cleaned file exists
if not os.path.exists(cleaned_path):
    print(f"Cleaned file not found: {cleaned_path}")
    exit()

# Load the cleaned data
df = pd.read_csv(cleaned_path)

# Count sentiment categories
sentiment_counts = df['sentiment'].value_counts()

# Create output folder
output_folder = "data/visuals"
os.makedirs(output_folder, exist_ok=True)

# --------------------------
# PIE CHART
# --------------------------
plt.figure(figsize=(8, 6))
colors = ['green', 'red', 'orange']
plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title(f'Sentiment Distribution (Pie) - {today}')
pie_path = os.path.join(output_folder, f'sentiment_pie_chart_{today}.png')
plt.savefig(pie_path)
plt.close()

# --------------------------
# BAR CHART
# --------------------------
plt.figure(figsize=(8, 6))
sentiment_counts.plot(kind='bar', color=['green', 'red', 'orange'])
plt.title(f'Sentiment Distribution (Bar) - {today}')
plt.xlabel('Sentiment')
plt.ylabel('Number of Articles')
plt.xticks(rotation=0)
bar_path = os.path.join(output_folder, f'sentiment_bar_chart_{today}.png')
plt.tight_layout()
plt.savefig(bar_path)
plt.close()

# --------------------------
# PRINT SUMMARY
# --------------------------
print(f"Sentiment Pie Chart saved to: {pie_path}")
print(f"Sentiment Bar Chart saved to: {bar_path}")
print("\nSentiment Counts:")
print(sentiment_counts)
