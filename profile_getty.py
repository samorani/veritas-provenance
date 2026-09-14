"""Profile the latest Getty Sales Catalogs snapshot.

For each table: row count, filled/missing per column, and the 20 most
frequent values in artist, title, buyer, and seller columns. For
sales_descriptions, also a breakdown of catalog_number country-code prefixes
with the number of catalogs whose sale_begin_year is 1900-1945.
"""

import os
import re

import psycopg
from psycopg import sql
from dotenv import load_dotenv

TABLES = ["sales_descriptions", "sales_catalogs_info"]
SOURCE_ID = "getty_sales_catalogs"
TOP_N = 20
TOP_COLUMN_PATTERN = re.compile(r"artist|title|buy|sell")
SKIP_COLUMN_PATTERN = re.compile(r"_q_\d+$")  # "[?]" uncertainty flags, not names
MAX_VALUE_WIDTH = 80

COLUMNS = (
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = %s "
    "ORDER BY ordinal_position"
)


def latest_snapshot_sql(table):
    return sql.SQL(
        "SELECT max(loaded_at), count(DISTINCT loaded_at) FROM {} WHERE source_id = %s"
    ).format(sql.Identifier(table))


def fill_counts_sql(table, columns):
    # Casting to text covers loaded_at; whitespace-only values count as filled.
    counts = sql.SQL(", ").join(
        sql.SQL("count(*) FILTER (WHERE {c}::text <> '')").format(c=sql.Identifier(c))
        for c in columns
    )
    return sql.SQL(
        "SELECT count(*), {} FROM {} WHERE source_id = %s AND loaded_at = %s"
    ).format(counts, sql.Identifier(table))


def top_values_sql(table, column):
    return sql.SQL(
        "SELECT {c}, count(*) AS n FROM {t} "
        "WHERE source_id = %s AND loaded_at = %s AND {c} <> '' "
        "GROUP BY {c} ORDER BY n DESC, {c} LIMIT %s"
    ).format(c=sql.Identifier(column), t=sql.Identifier(table))


# The CASE keeps non-4-digit years from reaching the ::int cast.
PREFIX_BREAKDOWN = sql.SQL("""\
SELECT split_part(catalog_number, '-', 1) AS prefix,
       count(*) AS catalogs,
       count(*) FILTER (WHERE CASE WHEN sale_begin_year ~ '^[0-9]{4}$'
                                   THEN sale_begin_year::int END BETWEEN 1900 AND 1945) AS dated_1900_1945,
       min(sale_begin_year) AS min_year,
       max(sale_begin_year) AS max_year
FROM sales_descriptions
WHERE source_id = %s AND loaded_at = %s
GROUP BY prefix
ORDER BY catalogs DESC, prefix""")

COUNTRY_1900_1945 = sql.SQL("""\
SELECT split_part(catalog_number, '-', 1) AS prefix,
       country_auth,
       count(*) AS catalogs
FROM sales_descriptions
WHERE source_id = %s AND loaded_at = %s
  AND CASE WHEN sale_begin_year ~ '^[0-9]{4}$'
           THEN sale_begin_year::int END BETWEEN 1900 AND 1945
GROUP BY prefix, country_auth
ORDER BY catalogs DESC, prefix, country_auth""")


def shorten(value):
    return value if len(value) <= MAX_VALUE_WIDTH else value[: MAX_VALUE_WIDTH - 1] + "…"


def profile_table(cur, table):
    cur.execute(COLUMNS, (table,))
    columns = [r[0] for r in cur.fetchall()]
    if not columns:
        raise SystemExit(f"Table {table} not found; run load_getty.py first.")

    cur.execute(latest_snapshot_sql(table), (SOURCE_ID,))
    loaded_at, snapshots = cur.fetchone()
    if loaded_at is None:
        raise SystemExit(f"No {SOURCE_ID} snapshots in {table}; run load_getty.py first.")
    snapshot = (SOURCE_ID, loaded_at)

    cur.execute(fill_counts_sql(table, columns), snapshot)
    total, *filled = cur.fetchone()

    print(f"===== {table} =====")
    print(f"Snapshot: {SOURCE_ID} loaded_at {loaded_at.isoformat()} (latest of {snapshots} snapshot(s))")
    print(f"Total rows: {total:,}\n")
    width = max(map(len, columns))
    print(f"  {'column':<{width}}  {'filled':>7}  {'missing':>7}")
    for c, n in zip(columns, filled):
        print(f"  {c:<{width}}  {n:>7,}  {total - n:>7,}")

    top_columns = [
        c for c, n in zip(columns, filled)
        if TOP_COLUMN_PATTERN.search(c) and not SKIP_COLUMN_PATTERN.search(c)
    ]
    empty = [c for c, n in zip(columns, filled) if c in top_columns and n == 0]
    if not top_columns:
        print("\nNo artist, title, buyer, or seller columns in this table.")
    for c in top_columns:
        if c in empty:
            continue
        cur.execute(top_values_sql(table, c), (*snapshot, TOP_N))
        print(f"\nTop {TOP_N} values: {c}")
        for value, n in cur.fetchall():
            print(f"  {n:>6,}  {shorten(value)!r}")
    if empty:
        print(f"\nArtist/title/buyer/seller columns with no values: {', '.join(empty)}")

    if table == "sales_descriptions":
        cur.execute(PREFIX_BREAKDOWN, snapshot)
        rows = cur.fetchall()
        print("\ncatalog_number prefix breakdown")
        print(f"  {'prefix':<6}  {'catalogs':>8}  {'1900-1945':>9}  {'min_year':>8}  {'max_year':>8}")
        for prefix, catalogs, modern, lo, hi in rows:
            print(f"  {prefix:<6}  {catalogs:>8,}  {modern:>9,}  {lo:>8}  {hi:>8}")
        print(f"  {'total':<6}  {sum(r[1] for r in rows):>8,}  {sum(r[2] for r in rows):>9,}")

        cur.execute(COUNTRY_1900_1945, snapshot)
        print("\nCatalogs dated 1900-1945 by prefix and country_auth")
        for prefix, country, catalogs in cur.fetchall():
            print(f"  {prefix:<6}  {country or '<empty>':<16}  {catalogs:>6,}")
    print()


def main():
    load_dotenv()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        for table in TABLES:
            profile_table(cur, table)


if __name__ == "__main__":
    main()
