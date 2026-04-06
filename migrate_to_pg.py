"""Migrate SQLite mlb.db to PostgreSQL."""
import sqlite3
import psycopg2
import psycopg2.extras
import time

SQLITE_PATH = "/home/chrislan/mlb/mlb.db"
PG_DSN = "host=127.0.0.1 port=5432 dbname=mlb user=mlb password=mlb2026"
BATCH_SIZE = 10000

TYPE_MAP = {
    "INTEGER": "BIGINT",
    "TEXT": "TEXT",
    "REAL": "DOUBLE PRECISION",
    "BLOB": "BYTEA",
    "": "TEXT",
}


def get_sqlite_tables(sconn):
    cur = sconn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [r[0] for r in cur.fetchall()]


def get_columns(sconn, table):
    cur = sconn.execute("PRAGMA table_info([{}])".format(table))
    return cur.fetchall()


def create_pg_table(pgconn, table, columns):
    cols = []
    for c in columns:
        name = c[1]
        stype = c[2].upper() if c[2] else "TEXT"
        pgtype = TYPE_MAP.get(stype, "TEXT")
        cols.append('"{}" {}'.format(name, pgtype))
    col_str = ", ".join(cols)
    with pgconn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS "{}" CASCADE'.format(table))
        cur.execute('CREATE TABLE IF NOT EXISTS "{}" ({})'.format(table, col_str))
    pgconn.commit()


def migrate_table(sconn, pgconn, table, columns):
    col_names = [c[1] for c in columns]
    quoted = ['"{}"'.format(c) for c in col_names]
    placeholders = ",".join(["%s"] * len(col_names))
    insert_sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(
        table, ",".join(quoted), placeholders
    )

    # Build type lookup for cleaning empty strings
    int_cols = {i for i, c in enumerate(columns) if (c[2].upper() if c[2] else "") in ("INTEGER", "REAL")}

    cur_s = sconn.cursor()
    cur_s.execute("SELECT * FROM [{}]".format(table))

    total = 0
    while True:
        rows = cur_s.fetchmany(BATCH_SIZE)
        if not rows:
            break
        # Convert empty strings to None for numeric columns
        cleaned = [
            tuple(None if (i in int_cols and v == "") else v for i, v in enumerate(row))
            for row in rows
        ]
        with pgconn.cursor() as cur_p:
            psycopg2.extras.execute_batch(cur_p, insert_sql, cleaned, page_size=BATCH_SIZE)
        pgconn.commit()
        total += len(rows)
        print("  {}: {:,} rows".format(table, total), end="\r", flush=True)

    print("  {}: {:,} rows".format(table, total))
    return total


def main():
    sconn = sqlite3.connect(SQLITE_PATH)
    pgconn = psycopg2.connect(PG_DSN)

    tables = get_sqlite_tables(sconn)
    print("Migrating {} tables...\n".format(len(tables)))

    grand_total = 0
    start = time.time()

    for table in tables:
        columns = get_columns(sconn, table)
        create_pg_table(pgconn, table, columns)
        count = migrate_table(sconn, pgconn, table, columns)
        grand_total += count

    elapsed = time.time() - start
    print("\nDone. {:,} total rows in {:.1f}s".format(grand_total, elapsed))

    sconn.close()
    pgconn.close()


if __name__ == "__main__":
    main()
