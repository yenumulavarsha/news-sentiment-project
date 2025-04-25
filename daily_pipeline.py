import subprocess
from datetime import datetime, timedelta
import logging
import os
from pymongo import MongoClient
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MongoDB connection URI
MONGO_URI = "mongodb+srv://MadhanKumarR:mypassword@cluster0.kgklnih.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def run_script(script_path, name):
    """Runs a Python script as a subprocess"""
    logger.info(f"Running {name}...")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"Error in {name}: {result.stderr}")
        return False

    logger.info(f"{name} completed successfully.")
    return True

def update_mongodb(csv_path):
    """Updates MongoDB with cleaned news data"""
    try:
        df = pd.read_csv(csv_path)

        if df.empty:
            logger.warning("CSV file is empty. Nothing to insert.")
            return False

        records = df.to_dict(orient='records')

        client = MongoClient(MONGO_URI)
        db = client["newsDB"]
        collection = db["headlines"]

        # Remove records from today's date to avoid duplicates
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_start = today_start + timedelta(days=1)
        collection.delete_many({
            "publishedAt": {
                "$gte": today_start.isoformat(),
                "$lt": tomorrow_start.isoformat()
            }
        })

        # Insert new records
        collection.insert_many(records)
        logger.info(f"Added {len(records)} new records to MongoDB")
        logger.info(f"Total documents in collection: {collection.count_documents({})}")

        client.close()
        return True

    except Exception as e:
        logger.error(f"Error updating MongoDB: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info(f"\n🚀 Starting Full Pipeline for {datetime.now().strftime('%Y-%m-%d')}...\n")

    # Step 1: Run Scraper
    if run_script("scraper/scraper.py", "Scraper"):
        # Step 2: Run Sentiment Analyzer
        if run_script("sentiment/sentiment.py", "Sentiment Analysis"):
            today = datetime.now().strftime('%Y-%m-%d')
            csv_path = f'data/cleaned/news_cleaned_{today}.csv'

            # Step 3: Update MongoDB
            if os.path.exists(csv_path):
                if update_mongodb(csv_path):
                    logger.info("✅ MongoDB updated successfully.")
                else:
                    logger.error("❌ Failed to update MongoDB.")
            else:
                logger.error(f"❌ Cleaned CSV not found: {csv_path}")

    logger.info("🎯 Pipeline execution complete.")
