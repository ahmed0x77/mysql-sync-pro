#!/usr/bin/env python3
"""
Auto Think: Simple loop that runs sync_mysql.sync_mysql() on an interval.
"""

import os
import time
from src.sync import sync_mysql


INTERVAL_SECS = int(os.getenv("AUTO_THINK_INTERVAL_SECS", "1"))


def run() -> None:
    print("Auto Think is running. Checking every", INTERVAL_SECS, "seconds...")
    print("Press Ctrl+C to stop.")

    dev_url = os.getenv("DEV_DB_URL")
    prod_url = os.getenv("PROD_DB_URL")
    
    if not dev_url or not prod_url:
        print("Error: Both DEV_DB_URL and PROD_DB_URL environment variables must be set.")
        print("Please set these variables in your environment or .env file.")
        exit(1)

    while True:
        try:
            sync_mysql(
                local_url=prod_url,
                prod_url=dev_url,
                allow_mysql_port_5432=True,
                verbosity=1,
                # change_detector=False,
            )
        except KeyboardInterrupt:
            print("\nStopping Auto Think...")
            break
        except Exception as exc:
            print(f"Error running sync: {exc}")

        time.sleep(INTERVAL_SECS)




if __name__ == "__main__":
    run()



