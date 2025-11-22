#!/usr/bin/env python3
"""
Export data from MotherDuck to local DuckDB.

This script connects to your MotherDuck database, extracts all Spotify play data,
and imports it into your local DuckDB database with deduplication.

Usage:
    python export_motherduck_to_local.py                     # Use defaults
    python export_motherduck_to_local.py --md-db my_custom_db # Custom MotherDuck DB
    python export_motherduck_to_local.py --local-db ./custom.duckdb # Custom local DB
    python export_motherduck_to_local.py --table historical_plays # Export different table
"""

import argparse
import os
import sys
import duckdb
from local_db_utils import init_local_db, print_database_stats


def export_motherduck_to_local(
    motherduck_db="my_db",
    motherduck_table="spotify_plays",
    local_db_path="./data/spotify_plays.duckdb"
):
    """
    Export data from MotherDuck to local DuckDB.

    Args:
        motherduck_db: Name of the MotherDuck database
        motherduck_table: Name of the table to export
        local_db_path: Path to local DuckDB file
    """
    # Get MotherDuck token from environment variable
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        print("Error: MOTHERDUCK_TOKEN environment variable must be set")
        sys.exit(1)

    print("\n" + "="*60)
    print("MOTHERDUCK TO LOCAL EXPORT")
    print("="*60)

    # Connect to MotherDuck
    print(f"\nConnecting to MotherDuck database: {motherduck_db}")
    try:
        md_con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
    except Exception as e:
        print(f"Error connecting to MotherDuck: {e}")
        sys.exit(1)

    # Check if table exists in MotherDuck
    print(f"Checking for table: {motherduck_table}")
    try:
        tables = md_con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]

        if motherduck_table not in table_names:
            print(f"Error: Table '{motherduck_table}' not found in MotherDuck database")
            print(f"Available tables: {', '.join(table_names)}")
            md_con.close()
            sys.exit(1)
    except Exception as e:
        print(f"Error checking tables: {e}")
        md_con.close()
        sys.exit(1)

    # Get count from MotherDuck
    try:
        md_count = md_con.execute(f"SELECT COUNT(*) FROM {motherduck_table}").fetchone()[0]
        print(f"Found {md_count:,} records in MotherDuck table '{motherduck_table}'")
    except Exception as e:
        print(f"Error querying MotherDuck table: {e}")
        md_con.close()
        sys.exit(1)

    if md_count == 0:
        print("No data to export!")
        md_con.close()
        return

    # Get schema information
    print("\nFetching table schema...")
    try:
        schema_info = md_con.execute(f"DESCRIBE {motherduck_table}").fetchall()
        print("Table schema:")
        for col in schema_info:
            print(f"  - {col[0]}: {col[1]}")
    except Exception as e:
        print(f"Warning: Could not fetch schema: {e}")

    # Export all data from MotherDuck
    print(f"\nExporting data from MotherDuck...")
    try:
        # Fetch all data into a DataFrame, filtering out rows with null track_id
        df = md_con.execute(f"""
            SELECT * FROM {motherduck_table}
            WHERE track_id IS NOT NULL AND track_id != ''
        """).fetchdf()
        print(f"✅ Exported {len(df):,} records from MotherDuck (excluding null/empty track_id)")
    except Exception as e:
        print(f"Error exporting data: {e}")
        md_con.close()
        sys.exit(1)

    # Close MotherDuck connection
    md_con.close()
    print("Closed MotherDuck connection")

    # Connect to local database
    print("\n" + "-"*60)
    print("IMPORTING TO LOCAL DATABASE")
    print("-"*60)

    try:
        # Use init_local_db which creates the table if it doesn't exist
        local_con = init_local_db(local_db_path)
    except Exception as e:
        print(f"Error connecting to local database: {e}")
        sys.exit(1)

    # Get count before import
    count_before = local_con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    print(f"Local database before import: {count_before:,} records")

    # Insert data into local database with deduplication
    print("\nInserting data into local database...")
    try:
        # Get the columns from the DataFrame
        df_columns = list(df.columns)

        # Map common MotherDuck column names to local schema
        # This handles cases where MotherDuck might have different column names
        column_mapping = {
            'played_at': 'played_at',
            'track_name': 'track_name',
            'artist_name': 'artist_name',
            'album_name': 'album_name',
            'track_id': 'track_id',
            'artist_id': 'artist_id',
            'album_id': 'album_id',
            'duration_ms': 'duration_ms'
        }

        # Check which columns are present in the DataFrame
        available_cols = [col for col in column_mapping.keys() if col in df_columns]

        if not available_cols:
            print(f"Error: No matching columns found in exported data")
            print(f"DataFrame columns: {df_columns}")
            local_con.close()
            sys.exit(1)

        # Build the INSERT statement dynamically based on available columns
        col_list = ', '.join(available_cols)
        select_cols = ', '.join([f'df.{col}' for col in available_cols])

        # Use INSERT OR IGNORE to skip duplicates based on primary key
        local_con.execute(f"""
            INSERT OR IGNORE INTO spotify_plays ({col_list})
            SELECT {select_cols}
            FROM df
        """)

        # Get count after import
        count_after = local_con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
        inserted = count_after - count_before
        duplicates = len(df) - inserted

        print(f"\n✅ Import complete!")
        print(f"   → Inserted: {inserted:,} new records")
        print(f"   → Skipped: {duplicates:,} duplicates")
        print(f"   → Total in local DB: {count_after:,} records")

    except Exception as e:
        print(f"Error inserting data: {e}")
        local_con.close()
        sys.exit(1)

    # Show database statistics
    print_database_stats(local_con)

    # Close local connection
    local_con.close()

    print("="*60)
    print("✅ EXPORT COMPLETE")
    print("="*60 + "\n")

    # Summary
    print("Summary:")
    print(f"  - Exported from MotherDuck: {len(df):,} records")
    print(f"  - Imported to local: {inserted:,} new records")
    print(f"  - Duplicates skipped: {duplicates:,} records")
    print(f"  - Local database path: {local_db_path}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Export Spotify plays from MotherDuck to local DuckDB'
    )
    parser.add_argument(
        '--md-db',
        default='my_db',
        help='MotherDuck database name (default: my_db)'
    )
    parser.add_argument(
        '--table',
        default='spotify_plays',
        help='Table name to export (default: spotify_plays)'
    )
    parser.add_argument(
        '--local-db',
        default='./data/spotify_plays.duckdb',
        help='Local DuckDB file path (default: ./data/spotify_plays.duckdb)'
    )
    parser.add_argument(
        '--also-historical',
        action='store_true',
        help='Also export historical_plays table (if it exists)'
    )

    args = parser.parse_args()

    # Export main table
    export_motherduck_to_local(
        motherduck_db=args.md_db,
        motherduck_table=args.table,
        local_db_path=args.local_db
    )

    # Optionally export historical_plays table
    if args.also_historical:
        print("\n" + "="*60)
        print("EXPORTING HISTORICAL PLAYS TABLE")
        print("="*60 + "\n")

        try:
            export_motherduck_to_local(
                motherduck_db=args.md_db,
                motherduck_table='historical_plays',
                local_db_path=args.local_db
            )
        except SystemExit:
            print("Note: historical_plays table may not exist, skipping...")


if __name__ == "__main__":
    main()
