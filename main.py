import time
import schedule
import logging
from export_import import export_collection_to_postgres, check_collection_health, import_postgres_to_chroma

# Configure logging for the main scheduler as well.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def run_scheduler():
    logging.info("Scheduler starting. Scheduling tasks ...")
    # Schedule export to run hourly.
    schedule.every(1).hours.do(export_collection_to_postgres)
    logging.info("Scheduled export task every 1 hour.")
    # Schedule health check every 20 minutes.
    schedule.every(20).minutes.do(check_collection_health)
    logging.info("Scheduled health check task every 20 minutes.")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()