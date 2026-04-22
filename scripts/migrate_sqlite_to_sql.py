#!/usr/bin/env python3
"""
migrate_sqlite_to_sql.py — Migrate a Media Library Dashboard SQLite database
to PostgreSQL or MySQL/MariaDB.

This script is invoked by the Dashboard Settings panel (Database > Migrate to SQL).
It can also be run manually from the command line.

SQLite remains the default and recommended backend for single-user installs.
Use this script only if you need multi-service write access or network DB access.

Usage:
  python3 migrate_sqlite_to_sql.py \
    --src /path/to/library_history.db \
    --dest-type mysql \
    --dest-host localhost \
    --dest-port 3306 \
    --dest-name media_library \
    --dest-user myuser \
    --dest-pass mypassword \
    [--wipe-dest]

Requirements:
  SQLite   — no driver needed (default)
  MySQL/MariaDB — pip install PyMySQL>=1.1.0
  PostgreSQL    — pip install psycopg2-binary
"""

import argparse
import sys
import sqlite3
from sqlalchemy import create_engine, text as sql_text, MetaData, Table, Column
from sqlalchemy import Integer, Float, Text, inspect as sa_inspect

BATCH_SIZE = 500

def build_dest_engine(args):
    if args.dest_type == "postgres":
        url = "postgresql://{}:{}@{}:{}/{}".format(
            args.dest_user, args.dest_pass,
            args.dest_host, args.dest_port, args.dest_name)
    elif args.dest_type == "mysql":
        url = "mysql+pymysql://{}:{}@{}:{}/{}".format(
            args.dest_user, args.dest_pass,
            args.dest_host, args.dest_port, args.dest_name)
    else:
        print("ERROR: --dest-type must be 'postgres' or 'mysql'")
        sys.exit(1)
    return create_engine(url)

def sqlite_type_to_sa(col_type):
    t = col_type.upper()
    if "INT" in t:
        return Integer()
    if any(x in t for x in ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")):
        return Float()
    return Text()

def main():
    parser = argparse.ArgumentParser(
        description="Migrate SQLite DB to PostgreSQL or MySQL/MariaDB")
    parser.add_argument("--src",       required=True,
                        help="Path to source SQLite .db file")
    parser.add_argument("--dest-type", required=True, choices=["postgres", "mysql"],
                        help="Destination backend type")
    parser.add_argument("--dest-host", required=True)
    parser.add_argument("--dest-port", required=True)
    parser.add_argument("--dest-name", required=True)
    parser.add_argument("--dest-user", required=True)
    parser.add_argument("--dest-pass", required=True)
    parser.add_argument("--wipe-dest", action="store_true",
                        help="Drop and recreate destination tables before migrating")
    args = parser.parse_args()

    # Connect to source SQLite
    try:
        src_conn = sqlite3.connect(args.src)
        src_conn.row_factory = sqlite3.Row
    except Exception as e:
        print("ERROR: Cannot open source SQLite DB: {}".format(e))
        sys.exit(1)

    # Connect to destination
    try:
        dest_engine = build_dest_engine(args)
        with dest_engine.connect() as c:
            c.execute(sql_text("SELECT 1"))
        print("Connected to destination {} successfully.".format(args.dest_type))
    except Exception as e:
        print("ERROR: Cannot connect to destination DB: {}".format(e))
        sys.exit(1)

    # Discover all tables in source
    cursor = src_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cursor.fetchall()]
    if not tables:
        print("ERROR: No tables found in source SQLite DB.")
        sys.exit(1)
    print("Found {} tables: {}".format(len(tables), ", ".join(tables)))

    meta = MetaData()

    for table_name in tables:
        # Read column info from SQLite
        col_cursor = src_conn.execute("PRAGMA table_info('{}')".format(table_name))
        cols = col_cursor.fetchall()
        if not cols:
            print("Skipping {} — no columns found.".format(table_name))
            continue

        sa_columns = []
        for col in cols:
            cid, name, col_type, notnull, dflt, pk = col
            sa_col = Column(
                name,
                sqlite_type_to_sa(col_type or ""),
                primary_key=(pk == 1),
                autoincrement=(pk == 1),
                nullable=(not notnull),
            )
            sa_columns.append(sa_col)

        dest_table = Table(table_name, meta, *sa_columns)

        try:
            if args.wipe_dest:
                with dest_engine.begin() as conn:
                    conn.execute(sql_text(
                        "DROP TABLE IF EXISTS `{}` ".format(table_name)
                        if args.dest_type == "mysql"
                        else "DROP TABLE IF EXISTS \"{}\" ".format(table_name)
                    ))
                print("Dropped existing table: {}".format(table_name))

            meta.create_all(dest_engine, tables=[dest_table], checkfirst=True)
        except Exception as e:
            print("ERROR creating table {}: {}".format(table_name, e))
            sys.exit(1)

        # Migrate rows in batches
        row_cursor = src_conn.execute("SELECT * FROM '{}'".format(table_name))
        col_names = [d[0] for d in row_cursor.description]
        total = 0
        batch = row_cursor.fetchmany(BATCH_SIZE)
        while batch:
            rows = [dict(zip(col_names, r)) for r in batch]
            try:
                with dest_engine.begin() as conn:
                    conn.execute(dest_table.insert(), rows)
            except Exception as e:
                print("ERROR inserting into {}: {}".format(table_name, e))
                sys.exit(1)
            total += len(batch)
            batch = row_cursor.fetchmany(BATCH_SIZE)
        print("Migrating {}... {} rows done.".format(table_name, total))

    src_conn.close()
    print("Migration complete. All tables migrated successfully.")
    sys.exit(0)

if __name__ == "__main__":
    main()
