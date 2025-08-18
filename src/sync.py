import logging
import os
from dataclasses import dataclass
from itertools import islice
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    def load_dotenv() -> None:  # type: ignore
        return None

from sqlalchemy import MetaData, Table, create_engine, select, text, func, cast, String, literal
from sqlalchemy.engine import Engine
from sqlalchemy.sql import tuple_ as sql_tuple
from sqlalchemy.engine.url import make_url
from sqlalchemy.dialects.mysql import insert as mysql_insert
from utils.change_detector import has_database_changes, write_current_signature


PKTuple = Tuple[object, ...]


def configure_logging(verbosity: int) -> None:
    log_level = logging.WARNING
    if verbosity == 1:
        log_level = logging.INFO
    elif verbosity >= 2:
        log_level = logging.DEBUG
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def chunked(seq: Sequence[PKTuple], size: int) -> Iterator[Sequence[PKTuple]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def chunked_iterable(it: Iterable[PKTuple], size: int) -> Iterator[List[PKTuple]]:
    iterator = iter(it)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            break
        yield batch


def is_mysql_url(url: str) -> bool:
    try:
        u = make_url(url)
        return (u.get_dialect().name or "").startswith("mysql")
    except Exception:
        return False


def get_port(url: str) -> Optional[int]:
    try:
        return make_url(url).port
    except Exception:
        return None


def load_metadata(engine: Engine) -> MetaData:
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return metadata


def get_common_table_names(local_meta: MetaData, prod_meta: MetaData) -> List[str]:
    local_names = set(local_meta.tables.keys())
    prod_names = set(prod_meta.tables.keys())
    common = sorted(local_names & prod_names)
    return common


def filter_tables(
    tables: List[str], include: Optional[List[str]], exclude: Optional[List[str]]
) -> List[str]:
    selected = tables
    if include:
        incl = set(n.strip() for n in include if n.strip())
        selected = [t for t in selected if t in incl]
    if exclude:
        excl = set(n.strip() for n in exclude if n.strip())
        selected = [t for t in selected if t not in excl]
    return selected


def get_primary_key_column_names(table: Table) -> List[str]:
    # Preserve PK column order as defined in the table
    return [col.name for col in table.primary_key.columns]


def fetch_pk_values(
    engine: Engine, table: Table, pk_column_names: List[str]
) -> Set[PKTuple]:
    if not pk_column_names:
        return set()

    columns = [table.c[name] for name in pk_column_names]
    stmt = select(*columns)
    results: Set[PKTuple] = set()
    with engine.connect() as conn:
        for row in conn.execute(stmt):
            # Always store PKs as tuples for uniform handling
            if len(pk_column_names) == 1:
                results.add((row[0],))
            else:
                results.add(tuple(row[i] for i in range(len(pk_column_names))))
    return results


def fetch_rows_by_pks(
    engine: Engine,
    table: Table,
    pk_column_names: List[str],
    pk_values: Sequence[PKTuple],
) -> List[Dict[str, object]]:
    if not pk_values:
        return []

    pk_columns = [table.c[name] for name in pk_column_names]
    if len(pk_columns) == 1:
        condition = pk_columns[0].in_([pk[0] for pk in pk_values])
    else:
        condition = sql_tuple(*pk_columns).in_(pk_values)

    stmt = select(table).where(condition)
    rows: List[Dict[str, object]] = []
    with engine.connect() as conn:
        result = conn.execute(stmt)
        for row in result.mappings():
            rows.append(dict(row))
    return rows


def insert_rows(
    engine: Engine,
    table: Table,
    rows: List[Dict[str, object]],
    use_insert_ignore: bool,
) -> int:
    if not rows:
        return 0
    # Skip generated/computed columns on insert
    allowed_cols = [col.name for col in table.columns if getattr(col, "computed", None) is None]
    filtered_rows: List[Dict[str, object]] = [
        {k: v for (k, v) in row.items() if k in allowed_cols}
        for row in rows
    ]
    insert_stmt = table.insert()
    if use_insert_ignore:
        insert_stmt = insert_stmt.prefix_with("IGNORE")  # MySQL-specific safety
    with engine.begin() as conn:
        conn.execute(insert_stmt, filtered_rows)
    return len(filtered_rows)


def delete_rows_by_pks(
    engine: Engine,
    table: Table,
    pk_column_names: List[str],
    pk_values: Sequence[PKTuple],
) -> int:
    if not pk_values:
        return 0

    pk_columns = [table.c[name] for name in pk_column_names]
    if len(pk_columns) == 1:
        condition = pk_columns[0].in_([pk[0] for pk in pk_values])
    else:
        condition = sql_tuple(*pk_columns).in_(pk_values)

    delete_stmt = table.delete().where(condition)
    with engine.begin() as conn:
        result = conn.execute(delete_stmt)
        # result.rowcount may be -1 for some drivers; treat as best-effort
        deleted = result.rowcount if result.rowcount is not None else 0
    return deleted if deleted >= 0 else 0


def get_computed_column_names(table: Table) -> List[str]:
    return [col.name for col in table.columns if getattr(col, "computed", None) is not None]


def get_updatable_column_names(table: Table, pk_column_names: List[str]) -> List[str]:
    pk_set = set(pk_column_names)
    computed_set = set(get_computed_column_names(table))
    return [
        col.name
        for col in table.columns
        if col.name not in pk_set and col.name not in computed_set
    ]


def get_version_column_name(table: Table) -> Optional[str]:
    # Prefer common update-timestamp columns to avoid hashing all columns
    candidates = [
        "updated_at",
        "updatedAt",
        "updated_on",
        "modified_at",
        "modifiedAt",
        "last_modified",
        "updated",
    ]
    computed = set(get_computed_column_names(table))
    for name in candidates:
        if name in table.c and name not in computed:
            return name
    return None


def build_row_fingerprint_expr(table: Table, data_column_names: List[str]):
    # Build MD5 hash over non-PK, non-generated columns; stable separator and NULL token
    null_token = literal("\u2400")  # visual null marker
    parts = [func.COALESCE(cast(table.c[name], String()), null_token) for name in data_column_names]
    if not parts:
        # If no data columns, hash an empty string for determinism
        return func.MD5(literal(""))
    return func.MD5(func.CONCAT_WS(literal("|"), *parts))


def fetch_hashes_for_pks(
    engine: Engine,
    table: Table,
    pk_column_names: List[str],
    data_column_names: List[str],
    pk_values: Sequence[PKTuple],
) -> Dict[PKTuple, str]:
    if not pk_values:
        return {}
    pk_columns = [table.c[name] for name in pk_column_names]
    if len(pk_columns) == 1:
        condition = pk_columns[0].in_([pk[0] for pk in pk_values])
    else:
        condition = sql_tuple(*pk_columns).in_(pk_values)

    hash_expr = build_row_fingerprint_expr(table, data_column_names).label("row_hash")
    stmt = select(*pk_columns, hash_expr).where(condition)
    hashes: Dict[PKTuple, str] = {}
    with engine.connect() as conn:
        result = conn.execute(stmt)
        for row in result:
            key: PKTuple
            if len(pk_columns) == 1:
                key = (row[0],)
                h = row[1]
            else:
                key = tuple(row[i] for i in range(len(pk_columns)))
                h = row[len(pk_columns)]
            hashes[key] = h
    return hashes


def upsert_rows(
    engine: Engine,
    table: Table,
    rows: List[Dict[str, object]],
    update_column_names: List[str],
) -> int:
    if not rows:
        return 0
    # Prepare MySQL-specific upsert
    insert_stmt = mysql_insert(table).values(rows)
    update_mapping = {name: insert_stmt.inserted[name] for name in update_column_names}
    upsert_stmt = insert_stmt.on_duplicate_key_update(**update_mapping)
    with engine.begin() as conn:
        conn.execute(upsert_stmt)
    return len(rows)


# ---------- Fast change detection (built-in short-circuit) ----------
# Create .state directory if it doesn't exist
state_dir = Path(".state")
state_dir.mkdir(exist_ok=True)
STATE_FILE = state_dir / ".sync_local.state"


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


def table_quick_fingerprint(engine: Engine, table: Table, pk_column_names: List[str]) -> str:
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


def database_quick_signature(engine: Engine, tables: List[Table]) -> str:
    # Build a single MD5 over quick per-table fingerprints (only PK-based)
    import hashlib as _hashlib

    parts: List[str] = []
    for t in tables:
        pk_cols = [col.name for col in t.primary_key.columns]
        key = table_quick_fingerprint(engine, t, pk_cols)
        parts.append(f"{t.name}:{key}")
    signature = _hashlib.md5("\n".join(sorted(parts)).encode("utf-8")).hexdigest()
    return f"quick:{signature}"


def load_last_local_signature() -> str:
    if STATE_FILE.exists():
        try:
            return STATE_FILE.read_text(encoding="utf-8").strip()
        except Exception:
            return ""
    return ""


def save_last_local_signature(sig: str) -> None:
    try:
        STATE_FILE.write_text(sig, encoding="utf-8")
    except Exception:
        pass


@dataclass
class SyncOptions:
    dry_run: bool
    batch_size: int
    disable_fk_checks: bool
    use_insert_ignore: bool


def set_foreign_key_checks(engine: Engine, enabled: bool) -> None:
    # MySQL session-level control; safe even if already in desired state
    with engine.begin() as conn:
        conn.execute(text(f"SET FOREIGN_KEY_CHECKS = {1 if enabled else 0}"))


def sync_table(
    local_engine: Engine,
    prod_engine: Engine,
    local_table: Table,
    prod_table: Table,
    options: SyncOptions,
) -> None:
    table_name = local_table.name
    pk_column_names = get_primary_key_column_names(local_table)
    if not pk_column_names:
        logging.warning(
            "Skipping table '%s' – no primary key defined; cannot safely diff rows.",
            table_name,
        )
        return

    logging.info("[%s] Fetching primary keys from local and production...", table_name)
    local_pks = fetch_pk_values(local_engine, local_table, pk_column_names)
    prod_pks = fetch_pk_values(prod_engine, prod_table, pk_column_names)

    to_insert = sorted(list(local_pks - prod_pks))
    to_delete = sorted(list(prod_pks - local_pks))
    overlap = sorted(list(local_pks & prod_pks))

    logging.info(
        "[%s] Planned changes | insert: %d, delete: %d",
        table_name,
        len(to_insert),
        len(to_delete),
    )

    if options.dry_run:
        return

    if options.disable_fk_checks:
        set_foreign_key_checks(prod_engine, enabled=False)

    try:
        # Inserts
        total_inserted = 0
        if to_insert:
            logging.info("[%s] Inserting rows in batches of %d...", table_name, options.batch_size)
            for batch in chunked(to_insert, options.batch_size):
                rows = fetch_rows_by_pks(local_engine, local_table, pk_column_names, batch)
                inserted = insert_rows(
                    prod_engine,
                    prod_table,
                    rows,
                    use_insert_ignore=options.use_insert_ignore,
                )
                total_inserted += inserted
        # Updates (smart diff using hashes of non-PK, non-generated columns)
        total_updated = 0
        if overlap:
            data_cols = get_updatable_column_names(local_table, pk_column_names)
            if data_cols:
                logging.info("[%s] Checking for updates in %d overlapping rows...", table_name, len(overlap))
                for batch in chunked(overlap, options.batch_size):
                    local_hashes = fetch_hashes_for_pks(
                        local_engine, local_table, pk_column_names, data_cols, batch
                    )
                    prod_hashes = fetch_hashes_for_pks(
                        prod_engine, prod_table, pk_column_names, data_cols, batch
                    )
                    changed_pks: List[PKTuple] = [
                        pk for pk in batch if local_hashes.get(pk) != prod_hashes.get(pk)
                    ]
                    if not changed_pks:
                        continue
                    # Fetch changed rows from local and upsert into production
                    changed_rows = fetch_rows_by_pks(
                        local_engine, local_table, pk_column_names, changed_pks
                    )
                    # Remove computed columns from payload
                    computed_cols = set(get_computed_column_names(local_table))
                    sanitized_rows = [
                        {k: v for (k, v) in row.items() if k not in computed_cols}
                        for row in changed_rows
                    ]
                    updated = upsert_rows(
                        prod_engine,
                        prod_table,
                        sanitized_rows,
                        update_column_names=data_cols,
                    )
                    total_updated += updated
        # Deletes
        total_deleted = 0
        if to_delete:
            logging.info("[%s] Deleting rows in batches of %d...", table_name, options.batch_size)
            for batch in chunked(to_delete, options.batch_size):
                deleted = delete_rows_by_pks(
                    prod_engine, prod_table, pk_column_names, batch
                )
                total_deleted += deleted

        logging.info(
            "[%s] Applied changes | inserted: %d, updated: %d, deleted: %d",
            table_name,
            total_inserted,
            total_updated,
            total_deleted,
        )
    finally:
        if options.disable_fk_checks:
            set_foreign_key_checks(prod_engine, enabled=True)





def sync_mysql(
    local_url: str,
    prod_url: str,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    batch_size: int = 1000,
    dry_run: bool = False,
    keep_fk_checks: bool = False,
    no_insert_ignore: bool = False,
    allow_mysql_port_5432: bool = False,
    max_workers: Optional[int] = None,
    verbosity: int = 1,
    change_detector: bool = True,
) -> None:
    """
    Sync MySQL databases from local to production.
    
    Args:
        local_url: SQLAlchemy URL for local MySQL database
        prod_url: SQLAlchemy URL for production MySQL database
        include: List of table names to include (None = all common tables)
        exclude: List of table names to exclude
        batch_size: Batch size for inserts/deletes (default: 1000)
        dry_run: If True, plan only without modifying production
        keep_fk_checks: If True, keep FOREIGN_KEY_CHECKS enabled during sync
        no_insert_ignore: If True, don't use INSERT IGNORE for inserts
        allow_mysql_port_5432: If True, allow MySQL on port 5432
        max_workers: Number of parallel workers (None = auto-detect)
        verbosity: Logging verbosity (0=WARNING, 1=INFO, 2=DEBUG)
        change_detector: If True, use built-in change detection to skip sync when no changes detected
    """
    configure_logging(verbosity)

    if not is_mysql_url(local_url) or not is_mysql_url(prod_url):
        logging.error("Both local_url and prod_url must be MySQL URLs (mysql+pymysql://...)")
        raise SystemExit(2)

    prod_port = get_port(prod_url)
    if prod_port == 5432 and not allow_mysql_port_5432:
        logging.error(
            "Production URL uses port 5432 (typical for PostgreSQL). If your MySQL server "
            "indeed listens on 5432, set allow_mysql_port_5432=True."
        )
        raise SystemExit(2)

    # Fast change detection: skip if no local changes and not a dry-run
    if change_detector:
        try:
            if not dry_run and not has_database_changes(local_url):
                logging.info("No local DB changes detected; skipping full sync.")
                return
        except Exception as exc:
            logging.debug("Fast change detection skipped due to: %s", exc)

    logging.info("Connecting to local and production databases...")
    local_engine = create_engine(local_url, pool_pre_ping=True, future=True)
    prod_engine = create_engine(prod_url, pool_pre_ping=True, future=True)

    logging.info("Reflecting database schemas...")
    local_meta = load_metadata(local_engine)
    prod_meta = load_metadata(prod_engine)

    tables = get_common_table_names(local_meta, prod_meta)
    if not tables:
        logging.error("No common tables found between local and production databases.")
        raise SystemExit(1)

    tables = filter_tables(tables, include, exclude)
    if not tables:
        logging.error("After filtering, no tables remain to synchronize.")
        raise SystemExit(1)

    options = SyncOptions(
        dry_run=bool(dry_run),
        batch_size=int(batch_size),
        disable_fk_checks=not bool(keep_fk_checks),
        use_insert_ignore=not bool(no_insert_ignore),
    )

    logging.info("Tables to synchronize (%d): %s", len(tables), ", ".join(tables))

    max_workers_env = os.getenv("SYNC_MAX_WORKERS") if max_workers is None else None
    if max_workers is None:
        try:
            max_workers = int(max_workers_env) if max_workers_env else None
        except ValueError:
            max_workers = None
        if not max_workers or max_workers <= 0:
            cpu = os.cpu_count() or 2
            max_workers = min(8, max(2, cpu * 2))
    logging.info("Using up to %d parallel workers for table syncs", max_workers)

    def _task(table_name: str) -> None:
        local_table = local_meta.tables[table_name]
        prod_table = prod_meta.tables[table_name]
        try:
            sync_table(local_engine, prod_engine, local_table, prod_table, options)
        except Exception as exc:
            logging.exception("Error while syncing table '%s': %s", table_name, exc)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_task, name) for name in tables]
        for _ in as_completed(futures):
            pass

    logging.info("Synchronization complete.")

    # Persist latest local signature using quick_check helpers
    try:
        if not dry_run:
            write_current_signature(local_url)
    except Exception as exc:
        logging.debug("Could not save local signature: %s", exc)





