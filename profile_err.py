"""Profile the latest err_raw snapshot: row count, per-column fill counts, top values."""

import os

import psycopg
from psycopg import sql
from dotenv import load_dotenv

TABLE = "err_raw"
SOURCE_ID = "err_jeudepaume"
TOP_N = 20
TOP_COLUMNS = ["owner", "collection", "medium", "intake_place"]
MAX_VALUE_WIDTH = 80

LATEST_SNAPSHOT = sql.SQL(
    "SELECT max(loaded_at), count(DISTINCT loaded_at) FROM {} WHERE source_id = %s"
).format(sql.Identifier(TABLE))

COLUMNS = (
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = %s "
    "ORDER BY ordinal_position"
)


def fill_counts_sql(columns):
    # Casting to text covers loaded_at; whitespace-only values count as non-empty.
    counts = sql.SQL(", ").join(
        sql.SQL("count(*) FILTER (WHERE {c}::text <> '')").format(c=sql.Identifier(c))
        for c in columns
    )
    return sql.SQL(
        "SELECT count(*), {} FROM {} WHERE source_id = %s AND loaded_at = %s"
    ).format(counts, sql.Identifier(TABLE))


def top_values_sql(column):
    return sql.SQL(
        "SELECT {c}, count(*) AS n FROM {t} "
        "WHERE source_id = %s AND loaded_at = %s AND {c} <> '' "
        "GROUP BY {c} ORDER BY n DESC, {c} LIMIT %s"
    ).format(c=sql.Identifier(column), t=sql.Identifier(TABLE))


def main():
    load_dotenv()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(COLUMNS, (TABLE,))
        columns = [r[0] for r in cur.fetchall()]
        if not columns:
            raise SystemExit(f"Table {TABLE} not found; run load_err.py first.")

        cur.execute(LATEST_SNAPSHOT, (SOURCE_ID,))
        loaded_at, snapshots = cur.fetchone()
        if loaded_at is None:
            raise SystemExit(f"No {SOURCE_ID} snapshots in {TABLE}; run load_err.py first.")
        snapshot = (SOURCE_ID, loaded_at)

        cur.execute(fill_counts_sql(columns), snapshot)
        total, *filled = cur.fetchone()

        print(f"Snapshot: {SOURCE_ID} loaded_at {loaded_at.isoformat()} "
              f"(latest of {snapshots} snapshot(s))")
        print(f"Total rows: {total:,}\n")
        print("Non-null, non-empty values per column")
        width = max(map(len, columns))
        for c, n in zip(columns, filled):
            pct = 100 * n / total if total else 0
            print(f"  {c:<{width}}  {n:>7,}  {pct:5.1f}%")

        for c in TOP_COLUMNS:
            cur.execute(top_values_sql(c), (*snapshot, TOP_N))
            print(f"\nTop {TOP_N} values: {c}")
            for value, n in cur.fetchall():
                shown = value if len(value) <= MAX_VALUE_WIDTH else value[: MAX_VALUE_WIDTH - 1] + "…"
                print(f"  {n:>6,}  {shown!r}")


if __name__ == "__main__":
    main()
