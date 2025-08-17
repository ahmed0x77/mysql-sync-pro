#!/usr/bin/env python3
"""
Simple MySQL Sync Runner as importable function.
"""

import os
from src.sync import sync_mysql


def run_default_sync() -> None:
    local_url = os.getenv("DEV_DB_URL")
    prod_url = os.getenv("PROD_DB_URL")
    
    if not local_url or not prod_url:
        print("Error: Both DEV_DB_URL and PROD_DB_URL environment variables must be set.")
        print("Please set these variables in your environment or .env file.")
        exit(1)
        
    sync_mysql(
        local_url=local_url,
        prod_url=prod_url,
        allow_mysql_port_5432=True,
        verbosity=1,
    )


if __name__ == "__main__":
    run_default_sync()
