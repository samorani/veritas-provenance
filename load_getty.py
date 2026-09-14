"""Load a snapshot of Getty Sales Catalogs CSVs into Postgres, one table per file.

Every CSV column is stored as TEXT exactly as it appears in the file: no
parsing, cleaning, or type guessing (empty fields are stored as '').
Each row also carries source_id, source_record_id, and loaded_at.

source_record_id is Getty's catalog_number. In sales_descriptions that is
one row per catalog; in sales_catalogs_info there is one row per physical
copy, so several rows share a catalog_number (Getty gives no row-level ID).

Columns with a blank header are dropped, but only after checking that every
value in them is empty; the load stops if any of them holds data.

Each run appends a new snapshot identified by loaded_at. Earlier snapshots
are never modified or deleted.
"""

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql
from dotenv import load_dotenv

GETTY_DIR = Path(__file__).parent / "data" / "getty" / "sales_catalogs"
FILES = ["sales_descriptions.csv", "sales_catalogs_info.csv"]
SOURCE_ID = "getty_sales_catalogs"
RECORD_ID_COLUMN = "catalog_number"
META_COLUMNS = ["source_id", "source_record_id", "loaded_at"]


def read_raw_csv(path):
    """Return (DataFrame of raw strings, rows that failed, blank-header columns dropped)."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f))

    too_many_fields = []

    def reject(fields):
        too_many_fields.append(fields)
        return None  # skip the line

    df = pd.read_csv(
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
    )

    blank = [i for i, name in enumerate(header) if name == ""]
    if (df[blank].fillna("") != "").any().any():
        raise ValueError(f"{path.name}: a column with a blank header holds data")
    df = df.drop(columns=blank)
    df.columns = [name for name in header if name != ""]

    # Short rows are padded with NaN; rows without a record id can't be traced.
    bad = df.isna().any(axis=1) | (df[RECORD_ID_COLUMN] == "")
    failures = len(too_many_fields) + int(bad.sum())
    return df[~bad], failures, len(blank)


def build_sql(table_name, header):
    table = sql.Identifier(table_name)
    columns = sql.SQL(", ").join(
        [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in header]
        + [
            sql.SQL("source_id TEXT NOT NULL"),
            sql.SQL("source_record_id TEXT NOT NULL"),
            sql.SQL("loaded_at TIMESTAMPTZ NOT NULL"),
        ]
    )
    return {
        "create": sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, columns),
        "copy": sql.SQL("COPY {} ({}) FROM STDIN").format(
            table, sql.SQL(", ").join(map(sql.Identifier, header + META_COLUMNS))
        ),
        "snapshot_count": sql.SQL(
            "SELECT count(*) FROM {} WHERE source_id = %s AND loaded_at = %s"
        ).format(table),
        "snapshots": sql.SQL(
            "SELECT count(DISTINCT loaded_at) FROM {} WHERE source_id = %s"
        ).format(table),
    }


def main():
    load_dotenv()
    loaded_at = datetime.now(timezone.utc)
    reports = []

    # One transaction for all files: a failed load adds nothing to any table.
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        for filename in FILES:
            table = Path(filename).stem
            df, failed, dropped = read_raw_csv(GETTY_DIR / filename)
            header = list(df.columns)
            statements = build_sql(table, header)

            cur.execute(statements["create"])
            id_pos = header.index(RECORD_ID_COLUMN)
            with cur.copy(statements["copy"]) as cp:
                for row in df.itertuples(index=False, name=None):
                    cp.write_row([*row, SOURCE_ID, row[id_pos], loaded_at])

            cur.execute(statements["snapshot_count"], (SOURCE_ID, loaded_at))
            (db_count,) = cur.fetchone()
            cur.execute(statements["snapshots"], (SOURCE_ID,))
            (snapshots,) = cur.fetchone()
            reports.append((filename, table, len(df), failed, len(header), dropped, db_count, snapshots))

    for filename, table, loaded, failed, ncols, dropped, db_count, snapshots in reports:
        print(f"{filename} -> {table}")
        print(f"  Rows read:    {loaded + failed:,}")
        print(f"  Rows loaded:  {loaded:,}")
        print(f"  Rows failed:  {failed:,}")
        print(f"  Columns loaded: {ncols} (+ source_id, source_record_id, loaded_at); "
              f"empty blank-header columns dropped: {dropped}")
        print(f"  Rows in {table} for this snapshot: {db_count:,} ({snapshots} snapshot(s) of {SOURCE_ID})")
    print(f"Snapshot loaded_at: {loaded_at.isoformat()}")


if __name__ == "__main__":
    main()
