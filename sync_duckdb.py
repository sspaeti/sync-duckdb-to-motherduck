#!/usr/bin/env python3
"""Sync a local DuckDB database (or parquet files) to MotherDuck."""

import argparse
import os
import duckdb


def sync_database(local_path: str, remote_db: str, tables: list[str] | None = None):
    """Sync tables from a local DuckDB file to MotherDuck.

    Discovers all tables via information_schema if none are specified.
    Uses CREATE OR REPLACE TABLE ... AS SELECT * for each table.
    """
    con = duckdb.connect(f"md:{remote_db}")
    con.execute(f"ATTACH '{local_path}' AS local_db (READ_ONLY)")

    if tables is None:
        tables = [
            row[0]
            for row in con.execute(
                "SELECT table_name FROM local_db.information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        ]

    if not tables:
        print("No tables found in local database.")
        return

    for table in tables:
        con.execute(
            f"CREATE OR REPLACE TABLE {remote_db}.main.{table} AS "
            f"SELECT * FROM local_db.main.{table}"
        )
        count = con.execute(f"SELECT count(*) FROM {remote_db}.main.{table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    con.execute("DETACH local_db")
    con.close()


def sync_parquet(pattern: str, remote_db: str, table_name: str):
    """Sync parquet files matching a glob pattern to a MotherDuck table."""
    con = duckdb.connect(f"md:{remote_db}")

    con.execute(
        f"CREATE OR REPLACE TABLE {remote_db}.main.{table_name} AS "
        f"SELECT * FROM read_parquet('{pattern}')"
    )
    count = con.execute(f"SELECT count(*) FROM {remote_db}.main.{table_name}").fetchone()[0]
    print(f"  {table_name}: {count:,} rows")

    con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Sync local DuckDB data to MotherDuck",
        epilog="Examples:\n"
               "  python sync_duckdb.py local.db my_remote_db\n"
               "  python sync_duckdb.py local.db my_remote_db --tables users,orders\n"
               "  python sync_duckdb.py --parquet 'data/*.parquet' my_remote_db events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("source", help="Path to local .db file, or parquet glob with --parquet")
    parser.add_argument("remote_db", help="MotherDuck database name")
    parser.add_argument("table_name", nargs="?", help="Target table name (required with --parquet)")
    parser.add_argument("--tables", help="Comma-separated list of tables to sync (default: all)")
    parser.add_argument("--parquet", action="store_true", help="Treat source as a parquet glob pattern")

    args = parser.parse_args()

    if not os.environ.get("MOTHERDUCK_TOKEN"):
        print("Error: MOTHERDUCK_TOKEN environment variable is not set.")
        print("  export MOTHERDUCK_TOKEN=your_token_here")
        raise SystemExit(1)

    if args.parquet:
        if not args.table_name:
            parser.error("--parquet requires a table_name argument")
        print(f"Syncing parquet '{args.source}' → md:{args.remote_db}.{args.table_name}")
        sync_parquet(args.source, args.remote_db, args.table_name)
    else:
        table_list = args.tables.split(",") if args.tables else None
        print(f"Syncing '{args.source}' → md:{args.remote_db}")
        sync_database(args.source, args.remote_db, table_list)

    print("Done.")


if __name__ == "__main__":
    main()
