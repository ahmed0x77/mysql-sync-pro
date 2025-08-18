#!/usr/bin/env python3
"""
Quick Check: Fast database change detector.
Connects to any database and does lightweight change detection.
- Exit code 0: no changes since last saved signature
- Exit code 1: changes detected
- With --write-current-signature: save current signature and exit 0
"""

import os
import hashlib
from functools import lru_cache
from pathlib import Path
from sqlalchemy import MetaData, Table, create_engine, select, func, cast, String, literal
from sqlalchemy.engine import Engine

DEFAULT_DATABASE_URL = os.getenv("DATABASE_URL")


def get_state_file_for_database(database_url: str) -> Path:
    """
    Generate a unique state file name based on the database URL's MD5 hash.
    Uses the first 20 characters of the MD5 hash to create a unique filename.
    """
    md5_hash = hashlib.md5(database_url.encode('utf-8')).hexdigest()
    state_filename = f".sync_db_{md5_hash[:20]}.state"
    # Create .state directory if it doesn't exist
    state_dir = Path(".state")
    state_dir.mkdir(exist_ok=True)
    return state_dir / state_filename


def get_binlog_signature(engine: Engine) -> str:
    with engine.connect() as conn:
        try:
            result = conn.exec_driver_sql("SHOW MASTER STATUS")
            row = result.fetchone()
            if not row:
                return ""
            file_name = row[0]
            position = row[1]
            return f"binlog:{file_name}:{position}"
        except Exception:
            return ""


@lru_cache(maxsize=8)
def _get_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, future=True)





def content_dynamic_signature(engine: Engine) -> str:
    """
    Content-based signature using a dynamic SQL that sums OCTET_LENGTH of
    concatenated column values for each table. This reflects changes
    immediately when rows are inserted/updated/deleted.
    """
    import hashlib as _hashlib

    try:
        with engine.connect() as conn:
            # Ensure GROUP_CONCAT can hold large dynamic SQL
            conn.exec_driver_sql("SET SESSION group_concat_max_len = 1000000")

            # Build the combined UNION ALL query server-side
            row = conn.exec_driver_sql(
                """
                SELECT GROUP_CONCAT(query SEPARATOR ' UNION ALL ') AS all_queries
                FROM (
                    SELECT CONCAT(
                        'SELECT ''', TABLE_NAME, ''' AS table_name, ',
                        'SUM(OCTET_LENGTH(CONCAT_WS('''', ',
                        GROUP_CONCAT(CONCAT('COALESCE(`', COLUMN_NAME, '`, '''')') ORDER BY ORDINAL_POSITION),
                        '))) AS total_bytes FROM `', TABLE_NAME, '`'
                    ) AS query
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    GROUP BY TABLE_NAME
                ) t
                """
            ).first()

            if not row or not row[0]:
                return ""

            union_sql = row[0]
            # Order results for deterministic signature
            final_sql = f"SELECT * FROM ({union_sql}) AS q ORDER BY table_name"
            results = conn.exec_driver_sql(final_sql).all()

        # Build a stable signature from table_name and total_bytes
        parts = []
        for r in results:
            table_name = str(r[0])
            total_bytes = 0 if r[1] is None else int(r[1])
            parts.append(f"{table_name}:{total_bytes}")

        signature = _hashlib.md5("\n".join(parts).encode("utf-8")).hexdigest()
        return f"content:{signature}"
    except Exception:
        return ""


def table_quick_fingerprint(engine: Engine, table: Table, pk_column_names: list) -> str:
    count_expr = func.COUNT()
    if pk_column_names:
        pk_cols = [table.c[name] for name in pk_column_names]
        casted = [cast(c, String()) for c in pk_cols]
        concat = func.CONCAT_WS(literal("|"), *casted)
        crc = func.SUM(func.CRC32(concat))
        max_parts = [func.MAX(c) for c in pk_cols]
        stmt = select(count_expr, crc, *max_parts)
    else:
        stmt = select(count_expr)

    with engine.connect() as conn:
        row = conn.execute(stmt.select_from(table)).first()

    if not row:
        return "0:"

    if pk_column_names:
        row_count = int(row[0])
        sum_crc = int(row[1]) if row[1] is not None else 0
        max_vals = ["" if row[i + 2] is None else str(row[i + 2]) for i in range(len(pk_column_names))]
        key = f"{row_count}:{sum_crc}:{'|'.join(max_vals)}"
        return key
    else:
        row_count = int(row[0])
        return f"{row_count}:"





def load_last_signature(state_file: Path = None) -> str:
    if state_file is None:
        # Use default database URL to generate state file
        if not DEFAULT_DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable must be set when state_file is not provided")
        state_file = get_state_file_for_database(DEFAULT_DATABASE_URL)
    
    if state_file.exists():
        try:
            return state_file.read_text(encoding="utf-8").strip()
        except Exception:
            return ""
    return ""


def save_current_signature(sig: str, state_file: Path = None) -> None:
    if state_file is None:
        # Use default database URL to generate state file
        if not DEFAULT_DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable must be set when state_file is not provided")
        state_file = get_state_file_for_database(DEFAULT_DATABASE_URL)
    
    try:
        state_file.write_text(sig, encoding="utf-8")
    except Exception:
        pass


def get_quick_signature(database_url: str, signature_type: str = None) -> str:
    engine = _get_engine(database_url)
    if signature_type == "content":
        return content_dynamic_signature(engine)
    elif signature_type == "binlog":
        return get_binlog_signature(engine)
    

    # 1) Fast path: binlog signature if available
    sig = get_binlog_signature(engine)
    if sig:
        return sig

    # 2) Content-based signature that reflects immediate data changes
    sig = content_dynamic_signature(engine)
    if sig:
        return sig


    raise ValueError("can't get signature")


def has_database_changes(database_url: str, state_file: Path = None, signature_type: str = None) -> bool:
    if state_file is None:
        # Auto-generate state file name from database URL
        state_file = get_state_file_for_database(database_url)
    
    sig = get_quick_signature(database_url, signature_type=signature_type)
    last_sig = load_last_signature(state_file)
    return bool(sig and sig != last_sig)


def write_current_signature(database_url: str, state_file: Path = None) -> None:
    if state_file is None:
        # Auto-generate state file name from database URL
        state_file = get_state_file_for_database(database_url)
    
    sig = get_quick_signature(database_url)
    if sig:
        save_current_signature(sig, state_file)