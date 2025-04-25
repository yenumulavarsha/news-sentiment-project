# scheduler.py
import schedule
import time
import subprocess
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_scheduler.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_pipeline():
    """Run the daily news pipeline"""
    logger.info(f"Starting pipeline execution at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        result = subprocess.run(["python", "daily_pipeline.py"], 
                               capture_output=True, 
                               text=True)
        
        if result.returncode == 0:
            logger.info("Pipeline executed successfully")
            logger.debug(f"Output: {result.stdout}")
        else:
            logger.error(f"Pipeline failed with error code {result.returncode}")
            logger.error(f"Error: {result.stderr}")
    except Exception as e:
        logger.error(f"Failed to execute pipeline: {str(e)}")

def main():
    logger.info("Starting scheduler")
    
    # Schedule job to run daily at 6:00 AM
    schedule.every().day.at("06:00").do(run_pipeline)
    
    # Run once at startup to get initial data
    run_pipeline()
    
    logger.info("Scheduler is running, press Ctrl+C to exit")
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()