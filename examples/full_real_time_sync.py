#!/usr/bin/env python3
"""
Auto Think: Simple loop that runs sync_mysql.sync_mysql() on an interval.
"""

import os
import time
import threading
from dotenv import load_dotenv
from src.sync import sync_mysql
from utils.change_detector import (
    has_database_changes,
    get_quick_signature,
    save_current_signature,
    get_state_file_for_database,
)

# Load environment variables from .env file
load_dotenv()

INTERVAL_SECS = int(os.getenv("AUTO_THINK_INTERVAL_SECS", "1"))
sync_lock = threading.Lock()

def sync_side(
    name: str,
    source_url: str,
    target_url: str,
) -> None:
    print(f"[{name}] Auto Think is running. Checking every {INTERVAL_SECS} seconds...")
    print(f"[{name}] Press Ctrl+C to stop.")

    while True:
        try:
            source_state_file = get_state_file_for_database(source_url)
            target_state_file = get_state_file_for_database(target_url)

            # Add diagnostic printing
            from utils.change_detector import load_last_signature
            current_sig = get_quick_signature(source_url, signature_type="content")
            last_sig = load_last_signature(source_state_file)
            print(f"[{name}] Checking... | Current Sig: {current_sig} | Last Sig: {last_sig}")

            # Use "content" signature for reliable change detection
            if has_database_changes(source_url, signature_type="content"):
                with sync_lock:
                    print(f"[{name}] Changes detected in source. Syncing to target...")
                    sync_mysql(
                        local_url=source_url,
                        prod_url=target_url,
                        allow_mysql_port_5432=True,
                        verbosity=0,
                        change_detector=False, # Use our own logic
                    )
                    
                    # After sync, update both signatures to prevent feedback loops
                    new_sig = get_quick_signature(target_url, signature_type="content")
                    save_current_signature(new_sig, source_state_file)
                    save_current_signature(new_sig, target_state_file)
                    print(f"[{name}] Sync complete. Updated signatures.")

        except KeyboardInterrupt:
            print(f"\n[{name}] Stopping Auto Think...")
            break
        except Exception as exc:
            print(f"[{name}] Error running sync: {exc}")

        time.sleep(INTERVAL_SECS)


if __name__ == "__main__":
    dev_db_url = os.getenv("DEV_DB_URL")
    prod_db_url = os.getenv("PROD_DB_URL")
    
    if not dev_db_url or not prod_db_url:
        print("Error: Both DEV_DB_URL and PROD_DB_URL environment variables must be set.")
        print("Please set these variables in your environment or .env file.")
        exit(1)

    thread1 = threading.Thread(
        target=sync_side,
        args=("dev-to-prod", dev_db_url, prod_db_url),
        daemon=True,
    )
    thread2 = threading.Thread(
        target=sync_side,
        args=("prod-to-dev", prod_db_url, dev_db_url),
        daemon=True,
    )
    
    thread1.start()
    thread2.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMain thread caught KeyboardInterrupt. Exiting.")




