# Veritas Provenance

Search tool over Nazi-era art looting records. Ingests archival
databases into Postgres, resolves people and firms across them,
and reports what corpus was searched and what was not.

## Stack — do not substitute
Python 3.14, psycopg, python-dotenv, pandas.
Postgres on Neon, reached via DATABASE_URL in .env.
Flask + Cloud Run later. No ORM — plain SQL.
Pipeline scripts are batch, run manually from my laptop.

## Data rules
- Raw source values are never overwritten. Normalized columns sit
  beside their _raw original.
- Every record carries source_id, source_record_id, loaded_at.
- Never store an inferred value as if a source stated it.
- Sources delete records; we don't. Loads are versioned snapshots.

## How I work
- One small change per request. I commit whenever it works.
- Show me SQL before you run it.
- Dev database only unless I say otherwise.
- Report row counts and failure counts after every transform.