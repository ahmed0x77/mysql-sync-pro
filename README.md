# SCSC - Smart, Concurrent Database Synchronization for MySQL

A high-performance, parallel utility for synchronizing MySQL databases, designed for efficiency and reliability.

## Overview

SCSC (Smart, Concurrent Sync) is a Python-based tool that provides a robust solution for synchronizing data between two MySQL databases. It is built with performance in mind, leveraging parallel processing to handle multiple tables at once. The tool features an intelligent change detection mechanism that can use either the MySQL binlog for high-speed checks or a content-based signature for environments where the binlog is not available.

## Key Features

- **Parallel Synchronization**: Syncs multiple tables concurrently, significantly reducing the time it takes to synchronize large databases.
- **Intelligent Change Detection**: Automatically skips synchronization if no changes are detected. Supports two detection methods:
  - **`binlog`**: High-performance method that checks the binary log position.
  - **`content`**: A reliable fallback that creates a hash of the database content.
- **Flexible Configuration**: Easily configure the synchronization process, including specifying tables to include or exclude, batch sizes, and foreign key check handling.
- **Multiple Sync Modes**: Supports several synchronization scenarios:
  - **One-Time Sync**: For manual, on-demand synchronization.
  - **Continuous One-Way Sync**: For automatically syncing changes from a source to a target database.
  - **Continuous Two-Way (Bi-Directional) Sync**: For keeping two databases in a real-time, synchronized state.
- **Dry Run Mode**: Preview the changes that will be made without actually modifying the target database.

## How It Works

The tool works by connecting to both a source and a target database and comparing their schemas to find common tables. For each table, it synchronizes the data in three main steps:

1.  **Inserts**: Rows that exist in the source but not in the target are inserted.
2.  **Updates**: For rows that exist in both, the tool efficiently compares a hash of the row data to see if an update is needed.
3.  **Deletes**: Rows that exist in the target but not in the source are deleted.

This process is parallelized, with multiple tables being synced at the same time to maximize speed.

## Installation

1.  Clone this repository to your local machine.
2.  Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

3.  **Required**: Configure your database connection URLs as environment variables. You can create a `.env` file in the project root:

    ```
    DEV_DB_URL="mysql+pymysql://username:password@localhost:3306/database_name"
    PROD_DB_URL="mysql+pymysql://username:password@production-host:3306/database_name"
    ```

    **Security Note**: Never commit database credentials to version control. Always use environment variables or secure configuration management.

## Usage

The tool provides several examples in the `examples/` directory to cover different use cases.

**Note**: If you're using a `.env` file, make sure to load it before running the scripts. You can do this by installing `python-dotenv` and adding `load_dotenv()` at the beginning of your scripts, or by sourcing the file in your shell.

### One-Time Synchronization

For a simple, one-time sync from a local database to a production database, you can use the `simple_runner.py` script.

```bash
python examples/simple_runner.py
```

### Continuous One-Way Synchronization

To continuously monitor a database for changes and automatically sync them to another database, use the `auto_sync.py` script. This script will check for changes at a set interval and sync if necessary.

```bash
python examples/auto_sync.py
```

### Continuous Two-Way (Bi-Directional) Sync

For keeping two databases in a constant, real-time synchronized state, use the `full_real_time_sync.py` script. This script runs two sync processes in parallel, one for each direction, and uses a locking mechanism to prevent conflicts.

```bash
python examples/full_real_time_sync.py
```

## Configuration

The core of the tool is the `sync_mysql` function in `src/sync.py`, which offers several parameters to customize the synchronization process:

| Parameter         | Description                                                                                                 | Default    |
| ----------------- | ----------------------------------------------------------------------------------------------------------- | ---------- |
| `local_url`       | The SQLAlchemy URL for the source database.                                                                 | (required) |
| `prod_url`        | The SQLAlchemy URL for the target database.                                                                 | (required) |
| `include`         | A list of table names to include in the sync. If `None`, all common tables are included.                    | `None`     |
| `exclude`         | A list of table names to exclude from the sync.                                                             | `None`     |
| `batch_size`      | The number of rows to process in each batch for inserts and deletes.                                        | `1000`     |
| `dry_run`         | If `True`, the script will only plan and log the changes without executing them.                            | `False`    |
| `keep_fk_checks`  | If `True`, foreign key checks will remain enabled during the sync.                                          | `False`    |
| `change_detector` | If `True`, the built-in change detection will be used to skip the sync if no changes are found.             | `True`     |
| `max_workers`     | The number of parallel workers to use for syncing tables. If `None`, it's auto-detected based on CPU cores. | `None`     |

## Change Detection

The change detection mechanism is a key feature for improving performance. It works by saving a "signature" of the database state in a `.state` file. Before running a full sync, it recalculates the signature and compares it to the last saved one.

You can influence the change detection by setting the `signature_type` parameter in the `has_database_changes` function:

- `signature_type="binlog"`: Forces the use of the high-performance binlog method.
- `signature_type="content"`: Forces the use of the content-hashing method.
- `signature_type=None` (or omitted): The tool will automatically try to use `binlog` first and fall back to `content` if it fails. This is the recommended setting.
