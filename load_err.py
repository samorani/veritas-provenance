"""Load a snapshot of data/err.csv into Postgres table err_raw.

Every CSV column is stored as TEXT exactly as it appears in the file: no
parsing, cleaning, or type guessing (empty fields are stored as '').
Each row also carries source_id, source_record_id (the source's artwork_id),
and loaded_at.

Each run appends a new snapshot identified by loaded_at. Earlier snapshots
are never modified or deleted.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql
from dotenv import load_dotenv

CSV_PATH = Path(__file__).parent / "data" / "err.csv"
TABLE = "err_raw"
SOURCE_ID = "err_jeudepaume"
RECORD_ID_COLUMN = "artwork_id"
META_COLUMNS = ["source_id", "source_record_id", "loaded_at"]


def read_raw_csv(path):
    """Return (DataFrame of raw strings, number of rows that failed to parse)."""
    too_many_fields = []

    def reject(fields):
        too_many_fields.append(fields)
        return None  # skip the line

    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8",
        engine="python",
        on_bad_lines=reject,
    )
    # Short rows are padded with NaN; rows without a record id can't be traced.
    bad = df.isna().any(axis=1) | (df[RECORD_ID_COLUMN] == "")
    failures = len(too_many_fields) + int(bad.sum())
    return df[~bad], failures


def build_sql(header):
    table = sql.Identifier(TABLE)
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

    df, failed = read_raw_csv(CSV_PATH)
    header = list(df.columns)
    statements = build_sql(header)

    # One transaction: a failed load adds nothing.
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(statements["create"])
        id_pos = header.index(RECORD_ID_COLUMN)
        with cur.copy(statements["copy"]) as cp:
            for row in df.itertuples(index=False, name=None):
                cp.write_row([*row, SOURCE_ID, row[id_pos], loaded_at])

        cur.execute(statements["snapshot_count"], (SOURCE_ID, loaded_at))
        (db_count,) = cur.fetchone()
        cur.execute(statements["snapshots"], (SOURCE_ID,))
        (snapshots,) = cur.fetchone()

    print(f"Rows read:    {len(df) + failed:,}")
    print(f"Rows loaded:  {len(df):,}")
    print(f"Rows failed:  {failed:,}")
    print(f"Rows in {TABLE} for this snapshot: {db_count:,}")
    print(f"Snapshot loaded_at: {loaded_at.isoformat()} ({snapshots} snapshot(s) of {SOURCE_ID})")


if __name__ == "__main__":
    main()
