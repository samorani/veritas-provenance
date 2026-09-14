"""First cross-source artist match: ERR (err_raw.artist) vs Getty (sales_contents.art_authority_1).

Both sides are reduced to a surname key: accents stripped, lowercased, dots and
apostrophes removed, particles (van, von, de, ...) skipped. Keys are compared
exactly. Nothing is written to the database; this is a read-only report.

ERR artist is free text. Before taking a surname, the ERR side drops bracketed
text, anything after "nach" (a copy after another artist), generation markers
("d. Ält.", "d. J."), dates, "geb./gest." and "in <place>" phrases, print
credits and generation words ("pinx.", "le Jeune"), and comma parts made only
of descriptor words (nationalities, periods, styles, manufactories such as
"Französisch", "Louis XV", "Sèvres"). If the first remaining part is a single
word followed by more parts, it is read as "Surname, Forename"; otherwise the
last word of the first part is the surname.

Getty art_authority_1 is mostly "SURNAME, FORENAMES (qualifier)". The Getty
side skips empty values, "NEW" (not yet assigned an authority name), and
bracketed placeholders such as "[ANONYMOUS]" or "[GERMAN - 18TH C.]". Names
without a comma use their last word.

A surname key is not an identity: different artists who share a surname are
merged (the report shows how many Getty authority names fall under each key),
and spelling variants (Schröder/Schroeder, Rembrandt/RIJN) are missed.
"""

import os
import re
import unicodedata
from collections import Counter, defaultdict

import psycopg
from psycopg import sql
from dotenv import load_dotenv

ERR_SOURCE_ID = "err_jeudepaume"
GETTY_SOURCE_ID = "getty_sales_catalogs"
# sales_contents holds one snapshot per loaded file (7-13); more would double-count.
EXPECTED_GETTY_SNAPSHOTS = 7
TOP_N = 30

ERR_ARTIST_COUNTS = sql.SQL("""\
SELECT artist, count(*)
FROM err_raw
WHERE source_id = %s
  AND loaded_at = (SELECT max(loaded_at) FROM err_raw WHERE source_id = %s)
GROUP BY artist""")

GETTY_SNAPSHOTS = sql.SQL("SELECT count(DISTINCT loaded_at) FROM sales_contents WHERE source_id = %s")

GETTY_AUTHORITY_COUNTS = sql.SQL("""\
SELECT art_authority_1, count(*)
FROM sales_contents
WHERE source_id = %s
GROUP BY art_authority_1""")

PARTICLES = {
    "van", "von", "der", "den", "de", "du", "des", "la", "le", "les", "di", "da", "del", "della", "dal",
    "dei", "ten", "ter", "zu", "zum", "y", "d", "l", "st", "sankt", "a", "au", "aux", "the",
}

# Words that describe an object's origin, period or style in ERR's artist field
# rather than naming a person. Applied to the ERR side only: several are real
# Getty surnames (Deutsch, Franz, Holland, Meister, Paris).
ERR_DESCRIPTORS = {
    "franzosisch", "franz", "francais", "francaise", "france", "frankreich", "deutsch", "deutschland",
    "allemand", "allemande", "italienisch", "italien", "italienne", "italie", "niederlandisch",
    "hollandisch", "hollandais", "hollandaise", "holland", "flamisch", "flamand", "flamande", "englisch",
    "anglais", "anglaise", "spanisch", "espagnol", "espagnole", "osterreichisch", "wien", "wiener",
    "russisch", "russland", "persisch", "persien", "persan", "perse", "china", "chine", "chinesisch",
    "chinois", "chinoise", "japan", "japanisch", "japonais", "japonaise", "agyptisch", "agypten", "egypte",
    "egyptien", "afrikanisch", "africain", "romisch", "romain", "romane", "romanisch", "griechisch", "grec",
    "antik", "antike", "antique", "modern", "moderne", "gotisch", "gothique", "barock", "baroque", "rokoko",
    "empire", "regence", "renaissance", "biedermeier", "klassizismus", "jugendstil", "directoire",
    "restauration", "epoque", "style", "stil", "art", "ecole", "schule", "werkstatt", "umkreis", "manier",
    "meister", "maitre", "anonym", "anonyme", "unbekannt", "inconnu", "unknown", "louis", "ming", "tang",
    "sevres", "meissen", "limoges", "aubusson", "urbino", "venedig", "venezianisch", "venise", "paris",
    "compagnie", "indes", "jh", "jhd", "jhdt", "jhrh", "jahrhundert", "siecle", "sec", "mitte", "anf",
    "anfang", "ende", "halfte", "h", "und", "oder", "et", "attribue", "attr", "zugeschrieben", "kopie",
    "copie", "signiert", "sign", "bez", "monogrammiert", "burgundisch", "brugge", "ostasiatisch", "indisch",
    "indien", "luristan", "byzantinisch", "koreanisch", "korea", "tibetisch", "turkisch", "islamisch",
    "koptisch", "etruskisch", "assyrisch", "syrisch", "keltisch",
}

# Words next to a name that are not the surname: generation markers and print
# credits. Skipped on the ERR side.
ERR_QUALIFIERS = {
    "jeune", "aine", "fils", "pere", "cadet", "junior", "jun", "jr", "senior", "sen", "pinx", "pinxit",
    "sculp", "sculpsit", "fecit", "fec", "delin", "inv", "invenit", "invento", "exc", "excudit", "incisi",
    "disegno", "gen", "genannt", "dit", "dite", "schuler", "schulerin", "nachfolger",
}

ROMAN_NUMERAL = re.compile(r"(?=[ivxlc])(x{0,3})(ix|iv|v?i{0,3})")
VALID_KEY = re.compile(r"[a-z][a-z-]*[a-z]")
BRACKETED = re.compile(r"\[[^\]]*\]|\([^)]*\)")
AFTER = re.compile(r"\b(nach|d'apres|d’apres|after)\b.*")
GENERATION = re.compile(r"\bd\s*\.\s*(alt\w*|aelt\w*|jung\w*|j(?![a-z]))\.?")
PLACE_OR_DATE = re.compile(r"(\b(geb|gest|um|ca|circa|dat|datiert|in)\b|\d).*")


def fold(text):
    """Strip accents and lowercase; letters NFKD does not decompose are mapped by hand."""
    for src, dst in (("ß", "ss"), ("ø", "o"), ("Ø", "O"), ("æ", "ae"), ("Æ", "AE"), ("œ", "oe"), ("Œ", "OE"),
                     ("ł", "l"), ("Ł", "L"), ("đ", "d"), ("Đ", "D")):
        text = text.replace(src, dst)
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposed if not unicodedata.combining(c)).lower()


def words(part):
    part = re.sub(r"['`’]", "", part.replace(".", " "))
    return [w for w in re.split(r"[^a-z-]+", part) if w.strip("-")]


def surname_from_words(ws):
    for w in reversed(ws):
        w = w.strip("-")
        if w not in PARTICLES:
            return w
    return None


def valid(key):
    return bool(key) and VALID_KEY.fullmatch(key) is not None and not ROMAN_NUMERAL.fullmatch(key)


def is_noise(word):
    return word in ERR_DESCRIPTORS or word in PARTICLES or ROMAN_NUMERAL.fullmatch(word) is not None


def err_surname(raw):
    """Return (key, None) or (None, reason) for an ERR artist value."""
    if raw.strip() == "":
        return None, "empty"
    text = AFTER.sub("", GENERATION.sub(" ", BRACKETED.sub(" ", fold(raw))))
    parts = []
    for part in text.split(","):
        ws = [w for w in words(PLACE_OR_DATE.sub("", part)) if w not in ERR_QUALIFIERS]
        if ws and not all(is_noise(w) for w in ws):
            parts.append([w for w in ws if w not in ERR_DESCRIPTORS])
    if not parts:
        return None, "no name (dates or descriptors only)"
    first = [w for w in parts[0] if w not in PARTICLES]
    key = first[0] if len(parts) > 1 and len(first) == 1 else surname_from_words(parts[0])
    if not valid(key) or key in ERR_DESCRIPTORS:
        return None, "no usable surname"
    return key, None


def getty_surname(raw):
    """Return (key, None) or (None, reason) for a Getty art_authority_1 value."""
    value = raw.strip()
    if value == "":
        return None, "empty"
    if value == "NEW":
        return None, "NEW (no authority name)"
    if value.startswith("["):
        return None, "bracketed placeholder"
    text = BRACKETED.sub(" ", fold(value))
    key = surname_from_words(words(text.split(",", 1)[0]))
    if not valid(key):
        return None, "no usable surname"
    return key, None


def keyed(counts, extract):
    """Group {raw value: objects} by surname key. Returns (key -> {raw: objects}, reason -> objects)."""
    by_key = defaultdict(Counter)
    excluded = Counter()
    for raw, n in counts.items():
        key, reason = extract(raw)
        if key is None:
            excluded[reason] += n
        else:
            by_key[key][raw] += n
    return by_key, excluded


def match(err_counts, getty_counts):
    err_keys, err_excluded = keyed(err_counts, err_surname)
    getty_keys, getty_excluded = keyed(getty_counts, getty_surname)
    shared = set(err_keys) & set(getty_keys)
    rows = []
    for key in shared:
        e, g = err_keys[key], getty_keys[key]
        rows.append({
            "key": key,
            "err_objects": sum(e.values()),
            "getty_objects": sum(g.values()),
            "err_top": e.most_common(1)[0][0],
            "getty_top": g.most_common(1)[0][0],
            "getty_authorities": len(g),
        })
    rows.sort(key=lambda r: (-(r["err_objects"] + r["getty_objects"]), r["key"]))
    return {
        "err_total": sum(err_counts.values()),
        "err_excluded": err_excluded,
        "err_keyed": sum(sum(c.values()) for c in err_keys.values()),
        "err_distinct_keys": len(err_keys),
        "getty_total": sum(getty_counts.values()),
        "getty_excluded": getty_excluded,
        "getty_keyed": sum(sum(c.values()) for c in getty_keys.values()),
        "getty_distinct_keys": len(getty_keys),
        "shared": rows,
        "err_objects_shared": sum(r["err_objects"] for r in rows),
        "getty_objects_shared": sum(r["getty_objects"] for r in rows),
    }


def shorten(text, width):
    return text if len(text) <= width else text[: width - 1] + "…"


def print_table(title, rows):
    print(f"\n{title}")
    print(f"  {'#':>2}  {'key':<16} {'ERR':>5} {'Getty':>6} {'total':>6}  {'most common ERR artist':<34} "
          f"{'most common Getty authority':<40} {'Getty names':>11}")
    for i, r in enumerate(rows, 1):
        print(f"  {i:>2}  {r['key']:<16} {r['err_objects']:>5,} {r['getty_objects']:>6,} "
              f"{r['err_objects'] + r['getty_objects']:>6,}  {shorten(r['err_top'], 34):<34} "
              f"{shorten(r['getty_top'], 40):<40} {r['getty_authorities']:>11,}")


def report(result):
    print("Normalization (surname keys)")
    for side, total, excluded, keyed_n, distinct in (
        ("ERR artist", result["err_total"], result["err_excluded"], result["err_keyed"], result["err_distinct_keys"]),
        ("Getty art_authority_1", result["getty_total"], result["getty_excluded"], result["getty_keyed"], result["getty_distinct_keys"]),
    ):
        print(f"  {side}: {total:,} objects -> {keyed_n:,} with a surname key ({distinct:,} distinct keys)")
        for reason, n in excluded.most_common():
            print(f"    excluded, {reason}: {n:,}")

    print(f"\nDistinct surname keys in both: {len(result['shared']):,}")
    print(f"ERR objects whose artist surname appears in Getty: {result['err_objects_shared']:,} "
          f"of {result['err_total']:,} ({100 * result['err_objects_shared'] / result['err_total']:.1f}%); "
          f"{result['err_keyed']:,} had a surname key")
    print(f"Getty objects whose surname appears in ERR: {result['getty_objects_shared']:,} of {result['getty_total']:,}")

    shared = result["shared"]
    print_table(f"Top {TOP_N} shared surnames by combined object count", shared[:TOP_N])
    by_err = sorted(shared, key=lambda r: (-r["err_objects"], -r["getty_objects"], r["key"]))
    print_table(f"Top {TOP_N} shared surnames by ERR object count", by_err[:TOP_N])


def main():
    load_dotenv()
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(ERR_ARTIST_COUNTS, (ERR_SOURCE_ID, ERR_SOURCE_ID))
        err_counts = dict(cur.fetchall())
        cur.execute(GETTY_SNAPSHOTS, (GETTY_SOURCE_ID,))
        (snapshots,) = cur.fetchone()
        if snapshots != EXPECTED_GETTY_SNAPSHOTS:
            raise SystemExit(f"sales_contents has {snapshots} snapshots, expected {EXPECTED_GETTY_SNAPSHOTS}; "
                             f"counts would include reloaded files.")
        cur.execute(GETTY_AUTHORITY_COUNTS, (GETTY_SOURCE_ID,))
        getty_counts = dict(cur.fetchall())
    report(match(err_counts, getty_counts))


if __name__ == "__main__":
    main()
