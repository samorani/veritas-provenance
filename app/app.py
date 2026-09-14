"""Veritas Provenance search app.

Searches the ERR (err_raw) and Getty Sales Catalogs (sales_contents) data in
Postgres by artist surname, behind an email/password login.

Surname keys come from match_artists.err_surname and getty_surname. The
database holds only raw artist strings, so the app reads each side's distinct
artist values with counts, keys them in Python, and turns a search key back
into the raw values it covers to query with = ANY(...). That index is rebuilt
only when the loaded data changes (see name_index).

Run locally:  venv/Scripts/python.exe app/app.py
On Cloud Run: gunicorn (see Dockerfile).
"""

import hmac
import os
import re
import sys
import threading
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from flask import Flask, g, redirect, render_template, request, session, url_for
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from werkzeug.security import check_password_hash, generate_password_hash

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from match_artists import (  # noqa: E402
    ERR_ARTIST_COUNTS,
    ERR_SOURCE_ID,
    GETTY_AUTHORITY_COUNTS,
    GETTY_SOURCE_ID,
    err_surname,
    getty_surname,
    keyed,
)

load_dotenv(ROOT / ".env")

DISPLAY_LIMIT = 100
MAX_QUERY_LENGTH = 200
MIN_PASSWORD_LENGTH = 8
NOT_SEARCHED = [
    "Munich Central Collecting Point (CCP) database",
    "Lost Art database",
    "national looted-art registers",
]

FIND_USER = "SELECT id, password_hash FROM app_user WHERE lower(email) = lower(%s)"
CREATE_USER = "INSERT INTO app_user (email, password_hash) VALUES (%s, %s) RETURNING id"

# What the search index is built from. The Getty loaders rewrite the
# sales_contents comment on every load, so a changed comment means new rows.
INDEX_VERSION = """\
SELECT (SELECT max(loaded_at) FROM err_raw WHERE source_id = %s),
       (SELECT max(loaded_at) FROM sales_descriptions WHERE source_id = %s),
       obj_description('sales_contents'::regclass, 'pg_class')"""

ERR_RESULTS = """\
SELECT artist, title, medium, measurements, owner, collection, intake_date, restituted, munich_no,
       count(*) OVER () AS total
FROM err_raw
WHERE source_id = %s AND loaded_at = %s AND artist = ANY(%s)
ORDER BY artist, title, artwork_id
LIMIT %s"""

GETTY_RESULTS = """\
SELECT c.art_authority_1, c.title, c.lot_number, c.auction_house_1, c.lot_sale_year, c.catalog_number,
       c.price_amount_1, c.price_currency_1, d.heidelberg_link,
       count(*) OVER () AS total
FROM sales_contents c
LEFT JOIN sales_descriptions d
       ON d.catalog_number = c.catalog_number AND d.source_id = %s AND d.loaded_at = %s
WHERE c.source_id = %s AND c.art_authority_1 = ANY(%s)
ORDER BY c.lot_sale_year, c.catalog_number, c.lot_number
LIMIT %s"""

LOADED_FILE = re.compile(r"sales_contents_(\d+)\.csv \(loaded_at ([^,]+), ([\d,]+) rows(?:, ([^)]*))?\)")
COMMENT_FILTER = re.compile(r"each filtered to (.*?): sales_contents_")

app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ["SECRET_KEY"],
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true",
)

# Checked when an email is unknown, so login takes similar time either way.
_DUMMY_HASH = generate_password_hash("not a real password")


# --- database -----------------------------------------------------------------

def db():
    if "db" not in g:
        # prepare_threshold=None: no server-side prepared statements, which a
        # transaction-mode connection pooler cannot keep track of.
        g.db = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, prepare_threshold=None)
    return g.db


@app.teardown_appcontext
def close_db(exc):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def find_user(email):
    with db().cursor() as cur:
        cur.execute(FIND_USER, (email,))
        return cur.fetchone()


def create_user(email, password_hash):
    with db().cursor() as cur:
        cur.execute(CREATE_USER, (email, password_hash))
        return cur.fetchone()[0]


# --- authentication -----------------------------------------------------------

PUBLIC_ENDPOINTS = {"login", "signup", "static"}


@app.before_request
def require_login():
    if request.endpoint not in PUBLIC_ENDPOINTS and "user_id" not in session:
        return redirect(url_for("login"))


def signup_error(email, password, confirm, invite_code):
    # TEMPORARY BARRIER: signup requires an invite code equal to the APP_PASSWORD
    # env var. Remove this check (and the invite code field in signup.html) to
    # open signup. With APP_PASSWORD unset, every signup is rejected.
    expected = os.environ.get("APP_PASSWORD", "")
    if not expected or not hmac.compare_digest(invite_code.encode(), expected.encode()):
        return "Invalid invite code."

    if "@" not in email or email.startswith("@") or email.endswith("@"):
        return "Enter a valid email address."
    if len(password) < MIN_PASSWORD_LENGTH:
        return f"Password must be at least {MIN_PASSWORD_LENGTH} characters."
    if password != confirm:
        return "Passwords do not match."
    return None


def start_session(user_id):
    session.clear()
    session["user_id"] = user_id


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "GET":
        return render_template("signup.html")
    email = request.form.get("email", "").strip()
    password = request.form.get("password", "")
    error = signup_error(email, password, request.form.get("confirm_password", ""),
                         request.form.get("invite_code", ""))
    if error is None:
        try:
            user_id = create_user(email, generate_password_hash(password))
        except UniqueViolation:
            error = "An account with that email already exists."
    if error:
        return render_template("signup.html", error=error, email=email), 400
    start_session(user_id)
    return redirect(url_for("search"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")
    email = request.form.get("email", "").strip()
    user = find_user(email)
    password_ok = check_password_hash(user[1] if user else _DUMMY_HASH, request.form.get("password", ""))
    if user is None or not password_ok:
        # Same message whether the email is unknown or the password is wrong.
        return render_template("login.html", error="Invalid email or password.", email=email), 401
    start_session(user[0])
    return redirect(url_for("search"))


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# --- search index and coverage ------------------------------------------------

_index = None
_index_lock = threading.Lock()


def index_version(conn):
    """(ERR snapshot, sales_descriptions snapshot, sales_contents comment) currently in the database."""
    with conn.cursor() as cur:
        cur.execute(INDEX_VERSION, (ERR_SOURCE_ID, GETTY_SOURCE_ID))
        return tuple(cur.fetchone())


def build_index(conn):
    version = index_version(conn)
    err_loaded_at, descriptions_loaded_at, getty_comment = version
    with conn.cursor() as cur:
        cur.execute(ERR_ARTIST_COUNTS, (ERR_SOURCE_ID, ERR_SOURCE_ID))
        err_counts = dict(cur.fetchall())
        cur.execute(GETTY_AUTHORITY_COUNTS, (GETTY_SOURCE_ID,))
        getty_counts = dict(cur.fetchall())
    err_keys, err_excluded = keyed(err_counts, err_surname)
    getty_keys, getty_excluded = keyed(getty_counts, getty_surname)
    return {
        "version": version,
        "err_loaded_at": err_loaded_at,
        "err_rows": sum(err_counts.values()),
        "err_keys": err_keys,
        "err_unsearchable": sum(err_excluded.values()),
        "descriptions_loaded_at": descriptions_loaded_at,
        "getty_rows": sum(getty_counts.values()),
        "getty_keys": getty_keys,
        "getty_unsearchable": sum(getty_excluded.values()),
        "getty_comment": getty_comment,
    }


def name_index():
    """Return the search index, rebuilding it only when the loaded data has changed."""
    global _index
    version = index_version(db())
    if _index is not None and _index["version"] == version:
        return _index
    with _index_lock:
        # Another request may have rebuilt it while this one waited for the lock.
        if _index is None or _index["version"] != version:
            _index = build_index(db())
        return _index


def file_ranges(numbers):
    """[1, 2, 3, 6] -> '1–3, 6'."""
    groups = []
    for n in sorted(numbers):
        if groups and n == groups[-1][1] + 1:
            groups[-1][1] = n
        else:
            groups.append([n, n])
    return ", ".join(f"{a}–{b}" if a != b else str(a) for a, b in groups)


def parse_getty_comment(comment):
    """Read loaded files, files not loaded, and the row filter from sales_contents' table comment."""
    if not comment:
        return None
    loaded = [
        {"number": int(m.group(1)), "loaded_at": m.group(2), "rows": int(m.group(3).replace(",", "")),
         "note": m.group(4) or ""}
        for m in LOADED_FILE.finditer(comment)
    ]
    if not loaded or "Not loaded:" not in comment:
        return None
    not_loaded_text = comment.split("Not loaded:", 1)[1].split(". ", 1)[0]
    row_filter = COMMENT_FILTER.search(comment)
    return {
        "loaded": sorted(loaded, key=lambda f: f["number"]),
        "not_loaded": sorted(int(n) for n in re.findall(r"sales_contents_(\d+)\.csv", not_loaded_text)),
        "filter": row_filter.group(1) if row_filter else None,
    }


def coverage(index):
    getty = parse_getty_comment(index["getty_comment"])
    not_searched = list(NOT_SEARCHED)
    info = {
        "err_rows": index["err_rows"],
        "err_loaded": index["err_loaded_at"].strftime("%Y-%m-%d") if index["err_loaded_at"] else "unknown",
        "err_unsearchable": index["err_unsearchable"],
        "getty_rows": index["getty_rows"],
        "getty_unsearchable": index["getty_unsearchable"],
        "getty_parsed": getty is not None,
        "getty_comment": index["getty_comment"],
    }
    if getty:
        dates = sorted(f["loaded_at"][:10] for f in getty["loaded"])
        info.update(
            getty_files=file_ranges(f["number"] for f in getty["loaded"]),
            getty_loaded=dates[0] if dates[0] == dates[-1] else f"{dates[0]} to {dates[-1]}",
            getty_filter=getty["filter"],
            getty_in_progress=[f"file {f['number']} ({f['note']})" for f in getty["loaded"] if "IN PROGRESS" in f["note"]],
            getty_comment_rows=sum(f["rows"] for f in getty["loaded"]),
        )
        if getty["not_loaded"]:
            not_searched.append(f"Getty sales_contents files {file_ranges(getty['not_loaded'])}")
    else:
        not_searched.append("Getty sales_contents files not recorded (table comment missing or unreadable)")
    info["not_searched"] = not_searched
    return info


# --- search -------------------------------------------------------------------

def err_results(index, raw_names):
    with db().cursor(row_factory=dict_row) as cur:
        cur.execute(ERR_RESULTS, (ERR_SOURCE_ID, index["err_loaded_at"], raw_names, DISPLAY_LIMIT))
        return cur.fetchall()


def getty_results(index, raw_names):
    with db().cursor(row_factory=dict_row) as cur:
        cur.execute(GETTY_RESULTS, (GETTY_SOURCE_ID, index["descriptions_loaded_at"], GETTY_SOURCE_ID,
                                    raw_names, DISPLAY_LIMIT))
        return cur.fetchall()


def panel(query, extract, keys, fetch):
    key, reason = extract(query)
    if key is None:
        return {"key": None, "reason": reason, "names": 0, "rows": [], "total": 0}
    raw_names = sorted(keys.get(key, ()))
    rows = fetch(raw_names) if raw_names else []
    return {"key": key, "reason": None, "names": len(raw_names), "rows": rows,
            "total": rows[0]["total"] if rows else 0}


@app.get("/")
def search():
    query = request.args.get("q", "").strip()[:MAX_QUERY_LENGTH]
    index = name_index()
    context = {"q": query, "limit": DISPLAY_LIMIT, "coverage": coverage(index)}
    if query:
        context["err"] = panel(query, err_surname, index["err_keys"], lambda names: err_results(index, names))
        context["getty"] = panel(query, getty_surname, index["getty_keys"], lambda names: getty_results(index, names))
    return render_template("search.html", **context)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")), debug=False)
