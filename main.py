import time
import schedule
from export_import import export_collection_to_postgres, check_collection_health

def run_scheduler():
    # Schedule export to run hourly.
    schedule.every(1).hours.do(export_collection_to_postgres)
    # Schedule collection health check every 20 minutes.
    schedule.every(20).minutes.do(check_collection_health)
    print("Scheduler started. Running tasks ...")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()
