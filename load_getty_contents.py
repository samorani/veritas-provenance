"""Partial load of Getty sales_contents files into Postgres table sales_contents.

For each file in FILES: download it into data/getty/sales_catalogs/ (kept on
disk), then load only rows whose catalog_number is one of the catalogs with
sale_begin_year 1900-1945 in the latest getty_sales_catalogs snapshot of
sales_descriptions.

Rows are committed in batches of BATCH_ROWS matching rows, in file order. Each
batch is its own COPY and its own transaction and records its batch_number
(1, 2, ...); all batches of a file share one loaded_at. If the connection
drops, the script reconnects, asks the table which batches committed, and
continues from the next one. If the script itself stops mid-file, set RESUME
to that file and the loaded_at printed when it started, then rerun. A resumed
load skips exactly the rows already committed, which holds as long as the file
and the sales_descriptions snapshot are unchanged; the row counts are checked.

Every CSV column is stored as TEXT exactly as it appears in the file: no
parsing, cleaning, or type guessing (empty fields are stored as '').
source_record_id is Getty's catalog_number; Getty gives lots no row-level ID.
Snapshots loaded before batching have batch_number NULL.

Each batch's transaction also rewrites the table comment to list every file
loaded so far, marking a partly loaded file IN PROGRESS. Earlier snapshots are
never modified or deleted.
"""

import csv
import os
import shutil
import time
import urllib.request
from datetime import datetime, timezone
from itertools import islice

import pandas as pd
import psycopg
from psycopg import sql
from dotenv import load_dotenv

from load_getty import GETTY_DIR, META_COLUMNS, SOURCE_ID, build_sql

TABLE = "sales_contents"
S3_BASE = "https://jpgt-or-prd-provenance-index-csv.s3.us-west-2.amazonaws.com/sales_catalogs"
ALL_FILES = [f"sales_contents_{n}.csv" for n in range(1, 14)]
FILES = [f"sales_contents_{n}.csv" for n in range(11, 13)]
# Completed loads from earlier runs, as (file, loaded_at, rows). Each is checked
# against the table before anything new is loaded. Add finished files here.
PRIOR_LOADS = [
    ("sales_contents_13.csv", "2026-09-14T18:50:40.359597+00:00", 26_237),
    ("sales_contents_7.csv", "2026-09-14T18:57:27.469921+00:00", 55_596),
    ("sales_contents_8.csv", "2026-09-14T18:58:00.936728+00:00", 150_000),
    ("sales_contents_9.csv", "2026-09-14T19:01:32.283194+00:00", 150_000),
    ("sales_contents_10.csv", "2026-09-14T19:21:15.135409+00:00", 150_000),
]
# A file left partly loaded by a run that stopped, as (file, loaded_at); it must
# be the first entry in FILES. None when there is nothing to resume.
RESUME = None
REFERENCE_HEADER_FILE = "sales_contents_13.csv"
CHUNK_ROWS = 20_000
BATCH_ROWS = 20_000
MAX_TRIES_PER_BATCH = 3
RETRY_SLEEP_SECONDS = 10

LATEST_DESCRIPTIONS = sql.SQL(
    "SELECT max(loaded_at) FROM sales_descriptions WHERE source_id = %s"
)

# The CASE keeps non-4-digit years from reaching the ::int cast.
PERIOD_CATALOGS = sql.SQL("""\
SELECT DISTINCT catalog_number
FROM sales_descriptions
WHERE source_id = %s AND loaded_at = %s
  AND CASE WHEN sale_begin_year ~ '^[0-9]{4}$'
           THEN sale_begin_year::int END BETWEEN 1900 AND 1945""")

ADD_BATCH_NUMBER = sql.SQL(
    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS batch_number INTEGER"
).format(sql.Identifier(TABLE))

COMMITTED_BATCHES = sql.SQL(
    "SELECT count(*), max(batch_number), count(*) FILTER (WHERE batch_number IS NULL) "
    "FROM {} WHERE source_id = %s AND loaded_at = %s"
).format(sql.Identifier(TABLE))

SNAPSHOT_SUMMARY = sql.SQL(
    "SELECT count(*), count(DISTINCT catalog_number) FROM {} WHERE source_id = %s AND loaded_at = %s"
).format(sql.Identifier(TABLE))

TABLE_SIZE = "SELECT pg_total_relation_size(%s::regclass), pg_size_pretty(pg_total_relation_size(%s::regclass))"


def copy_sql(header):
    columns = header + META_COLUMNS + ["batch_number"]
    return sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(TABLE), sql.SQL(", ").join(map(sql.Identifier, columns))
    )


def connect():
    # DIRECT_URL skips Neon's connection pooler.
    return psycopg.connect(os.environ["DIRECT_URL"], autocommit=True)


def log(message=""):
    print(message, flush=True)


def read_header(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f))


def download(filename):
    """Download a file unless a same-size copy is already on disk. Returns (bytes, downloaded)."""
    path = GETTY_DIR / filename
    url = f"{S3_BASE}/{filename}"
    with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60) as r:
        expected = int(r.headers["Content-Length"])
    if path.exists() and path.stat().st_size == expected:
        return expected, False
    part = path.with_name(filename + ".part")
    with urllib.request.urlopen(url, timeout=600) as r, open(part, "wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)
    if part.stat().st_size != expected:
        raise ValueError(f"{filename}: downloaded {part.stat().st_size:,} bytes, expected {expected:,}")
    part.replace(path)
    return expected, True


def read_raw_chunks(path, header, stats):
    """Yield DataFrames of raw strings; counts unparseable rows in stats['failed']."""
    too_many_fields = []

    def reject(fields):
        too_many_fields.append(fields)
        return None  # skip the line

    reader = pd.read_csv(
        path,
        header=None,
        skiprows=1,
        names=list(range(len(header))),
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8-sig",
        engine="python",
        on_bad_lines=reject,
        chunksize=CHUNK_ROWS,
    )
    for chunk in reader:
        chunk.columns = header
        # Short rows are padded with NaN; rows without a record id can't be traced.
        bad = chunk.isna().any(axis=1) | (chunk["catalog_number"] == "")
        stats["failed"] += int(bad.sum())
        yield chunk[~bad]
    stats["failed"] += len(too_many_fields)


def matching_rows(path, header, period, stats):
    """Yield rows whose catalog_number is in period, in file order; counts parsed rows in stats['valid']."""
    for chunk in read_raw_chunks(path, header, stats):
        stats["valid"] += len(chunk)
        yield from chunk[chunk["catalog_number"].isin(period)].itertuples(index=False, name=None)


def batches(rows, size):
    """Yield (batch_number, rows, is_last) with batch_number counting from 1."""
    rows = iter(rows)
    current = list(islice(rows, size))
    number = 0
    while current:
        upcoming = list(islice(rows, size))
        number += 1
        yield number, current, not upcoming
        current = upcoming


def table_comment(loads, descriptions_loaded_at, n_catalogs):
    order = {name: i for i, name in enumerate(ALL_FILES)}
    loads = sorted(loads, key=lambda load: order[load[0]])
    listed = {name for name, _, _, _ in loads}
    entries = "; ".join(
        f"{name} (loaded_at {ts}, {rows:,} rows{', ' + note if note else ''})" for name, ts, rows, note in loads
    )
    not_loaded = ", ".join(name for name in ALL_FILES if name not in listed) or "none"
    return (
        f"PARTIAL LOAD. Contains rows from these Getty sales_contents files only, each filtered to "
        f"rows whose catalog_number is one of the {n_catalogs:,} catalogs with sale_begin_year "
        f"1900-1945 in sales_descriptions snapshot loaded_at {descriptions_loaded_at.isoformat()}: "
        f"{entries}. Not loaded: {not_loaded}. A file marked IN PROGRESS is only partly loaded; "
        f"catalogs whose lots continue in files not loaded are incomplete. batch_number is NULL "
        f"for files loaded before batching."
    )


def comment_sql(text):
    # COMMENT ON cannot take bind parameters, so the text goes in as a quoted literal.
    return sql.SQL("COMMENT ON TABLE {} IS {}").format(sql.Identifier(TABLE), sql.Literal(text))


def committed_state(conn, loaded_at):
    """Return (rows, highest batch_number) already committed for this loaded_at."""
    with conn.cursor() as cur:
        cur.execute(COMMITTED_BATCHES, (SOURCE_ID, loaded_at))
        rows, max_batch, unbatched = cur.fetchone()
    if unbatched:
        raise SystemExit(f"{TABLE} has {unbatched:,} rows without batch_number at loaded_at "
                         f"{loaded_at.isoformat()}; that snapshot was not loaded in batches.")
    return rows, max_batch or 0


def run_batches(conn, filename, header, copy_stmt, period, loads, descriptions_loaded_at,
                loaded_at, committed_rows, committed_batches, stats):
    """Read the whole file, skip batches already committed, commit the rest. Returns (batches, rows)."""
    id_pos = header.index("catalog_number")
    skipped_rows = 0
    loaded_rows = committed_rows
    last_number = 0
    for number, batch, is_last in batches(matching_rows(GETTY_DIR / filename, header, period, stats), BATCH_ROWS):
        last_number = number
        if number <= committed_batches:
            skipped_rows += len(batch)
            if number == committed_batches and skipped_rows != committed_rows:
                raise SystemExit(f"{filename}: {TABLE} holds {committed_rows:,} rows in batches 1-{committed_batches}, "
                                 f"but those batches cover {skipped_rows:,} rows of the file; stopping.")
            continue
        with conn.cursor() as cur, conn.transaction():
            with cur.copy(copy_stmt) as cp:
                for row in batch:
                    cp.write_row([*row, SOURCE_ID, row[id_pos], loaded_at, number])
            note = f"{number} batches" if is_last else f"IN PROGRESS: batches 1-{number} committed"
            entry = (filename, loaded_at.isoformat(), loaded_rows + len(batch), note)
            cur.execute(comment_sql(table_comment(loads + [entry], descriptions_loaded_at, len(period))))
        loaded_rows += len(batch)
        log(f"  batch {number}: {len(batch):,} rows committed ({loaded_rows:,} so far)")
    if last_number < committed_batches:
        raise SystemExit(f"{filename}: {TABLE} holds {committed_batches} batches for loaded_at "
                         f"{loaded_at.isoformat()}, but the file yields only {last_number}; stopping.")
    if last_number == 0:
        with conn.cursor() as cur, conn.transaction():
            entry = (filename, loaded_at.isoformat(), 0, "no matching rows")
            cur.execute(comment_sql(table_comment(loads + [entry], descriptions_loaded_at, len(period))))
    return last_number, loaded_rows


def load_file(conn, reconnect, filename, header, copy_stmt, period, loads, descriptions_loaded_at, loaded_at):
    """Load one file in batches, reconnecting and resuming after connection errors.

    Returns (conn, stats, batches, rows, connection_errors); conn may be a new connection.
    """
    failing_batch, tries, errors = None, 0, 0
    while True:
        committed_rows, committed_batches = committed_state(conn, loaded_at)
        stats = {"failed": 0, "valid": 0}
        try:
            total_batches, total_rows = run_batches(conn, filename, header, copy_stmt, period, loads,
                                                    descriptions_loaded_at, loaded_at,
                                                    committed_rows, committed_batches, stats)
            return conn, stats, total_batches, total_rows, errors
        except psycopg.OperationalError as exc:
            errors += 1
            log(f"  connection error: {str(exc).splitlines()[0]}")
            try:
                conn.close()
            except Exception:
                pass
            conn = reconnect()
            _, now_committed = committed_state(conn, loaded_at)
            batch = now_committed + 1
            tries = tries + 1 if batch == failing_batch else 1
            failing_batch = batch
            if tries >= MAX_TRIES_PER_BATCH:
                raise SystemExit(f"{filename}: batch {batch} failed {tries} times; batches 1-{now_committed} are "
                                 f"committed. To resume later set RESUME = (\"{filename}\", \"{loaded_at.isoformat()}\").")
            log(f"  batches 1-{now_committed} committed; retrying from batch {batch} (try {tries + 1} of {MAX_TRIES_PER_BATCH})")
            time.sleep(RETRY_SLEEP_SECONDS * tries)


def main():
    load_dotenv()
    header = read_header(GETTY_DIR / REFERENCE_HEADER_FILE)
    statements = build_sql(TABLE, header)
    copy_stmt = copy_sql(header)
    if RESUME is not None and RESUME[0] != FILES[0]:
        raise SystemExit(f"RESUME is for {RESUME[0]}, but FILES starts with {FILES[0]}.")

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(statements["create"])
            cur.execute(ADD_BATCH_NUMBER)
            cur.execute(LATEST_DESCRIPTIONS, (SOURCE_ID,))
            (descriptions_loaded_at,) = cur.fetchone()
            if descriptions_loaded_at is None:
                raise SystemExit("No getty_sales_catalogs snapshot in sales_descriptions; run load_getty.py first.")
            cur.execute(PERIOD_CATALOGS, (SOURCE_ID, descriptions_loaded_at))
            period = {r[0] for r in cur.fetchall()}
            log(f"Filter: {len(period):,} catalogs dated 1900-1945 "
                f"(sales_descriptions snapshot {descriptions_loaded_at.isoformat()})")

            loads = []
            for name, ts, expected_rows in PRIOR_LOADS:
                cur.execute(SNAPSHOT_SUMMARY, (SOURCE_ID, datetime.fromisoformat(ts)))
                rows, _ = cur.fetchone()
                if rows != expected_rows:
                    raise SystemExit(f"PRIOR_LOADS expects {expected_rows:,} rows for {name} at {ts}; {TABLE} has {rows:,}.")
                loads.append((name, ts, rows, ""))
                log(f"Prior load verified: {name} loaded_at {ts}, {rows:,} rows")
            if RESUME is not None:
                cur.execute(SNAPSHOT_SUMMARY, (SOURCE_ID, datetime.fromisoformat(RESUME[1])))
                rows, _ = cur.fetchone()
                if rows == 0:
                    raise SystemExit(f"RESUME names {RESUME[0]} at {RESUME[1]}, but nothing is committed for it.")
                log(f"Partial load to resume: {RESUME[0]} loaded_at {RESUME[1]}, {rows:,} rows committed")
            cur.execute(statements["snapshots"], (SOURCE_ID,))
            (snapshots,) = cur.fetchone()
            expected_snapshots = len(PRIOR_LOADS) + (RESUME is not None)
            if snapshots != expected_snapshots:
                raise SystemExit(f"{TABLE} holds {snapshots} snapshots but PRIOR_LOADS and RESUME account for "
                                 f"{expected_snapshots}; resolve before loading more.")
            log(f"Snapshots in {TABLE}: {snapshots} (matches PRIOR_LOADS and RESUME)")

        for filename in FILES:
            started = time.perf_counter()
            size, downloaded = download(filename)
            download_secs = time.perf_counter() - started
            if read_header(GETTY_DIR / filename) != header:
                raise SystemExit(f"{filename}: header differs from {REFERENCE_HEADER_FILE}; nothing loaded from it.")

            resuming = RESUME is not None and RESUME[0] == filename
            loaded_at = datetime.fromisoformat(RESUME[1]) if resuming else datetime.now(timezone.utc)
            log(f"\n{filename}: {'resuming' if resuming else 'starting'} loaded_at {loaded_at.isoformat()} "
                f"(if this run stops mid-file: RESUME = (\"{filename}\", \"{loaded_at.isoformat()}\"))")

            started = time.perf_counter()
            conn, stats, total_batches, total_rows, errors = load_file(
                conn, connect, filename, header, copy_stmt, period, loads, descriptions_loaded_at, loaded_at)
            loads.append((filename, loaded_at.isoformat(), total_rows, f"{total_batches} batches"))

            with conn.cursor() as cur:
                cur.execute(SNAPSHOT_SUMMARY, (SOURCE_ID, loaded_at))
                db_rows, db_catalogs = cur.fetchone()
                cur.execute(TABLE_SIZE, (TABLE, TABLE))
                size_bytes, size_pretty = cur.fetchone()
            if db_rows != total_rows:
                raise SystemExit(f"{filename}: loader counted {total_rows:,} rows but {TABLE} has {db_rows:,}.")
            load_secs = time.perf_counter() - started

            log(f"{filename} -> {TABLE}")
            log(f"  File: {size:,} bytes, {'downloaded' if downloaded else 'already on disk'} ({download_secs:.0f}s)")
            log(f"  Rows in file:          {stats['valid'] + stats['failed']:,}")
            log(f"  Rows failed to parse:  {stats['failed']:,}")
            log(f"  Rows loaded:           {total_rows:,} in {total_batches} batches")
            log(f"  Rows skipped (catalog not dated 1900-1945): {stats['valid'] - total_rows:,}")
            log(f"  Distinct catalogs loaded: {db_catalogs:,}")
            log(f"  Connection errors recovered: {errors}")
            log(f"  Running table size: {size_bytes:,} bytes ({size_pretty})  | load {load_secs:.0f}s")
    finally:
        conn.close()

    log(f"\nAll files done. Rows in {TABLE} across loads: {sum(r for _, _, r, _ in loads):,}")


if __name__ == "__main__":
    main()
