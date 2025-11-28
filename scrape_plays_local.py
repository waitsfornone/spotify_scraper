#!/usr/bin/env python3
"""
Local DuckDB scraper for Spotify plays.

This script:
1. Fetches recent Spotify plays from the API
2. Stores them in a local DuckDB database
3. Exports the database to CSV
4. Uploads the CSV to B2 cloud storage

Usage:
    python scrape_plays_local.py                    # Normal incremental run
    python scrape_plays_local.py --restart          # Restart from beginning
    python scrape_plays_local.py --no-upload        # Skip B2 upload (testing)
    python scrape_plays_local.py --keep-csv         # Keep CSV after upload
    python scrape_plays_local.py --db-path ./custom.duckdb  # Custom DB path
"""

import argparse
import os
import sys
from datetime import datetime
from spotify_scraper import get_recent_tracks
from local_db_utils import (
    init_local_db,
    insert_plays_to_local_db,
    export_to_csv,
    upload_csv_to_b2,
    print_database_stats
)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Scrape Spotify plays to local DuckDB and export to B2'
    )
    parser.add_argument(
        '--restart',
        action='store_true',
        help='Restart from starting timestamp instead of current'
    )
    parser.add_argument(
        '--no-upload',
        action='store_true',
        help='Skip B2 upload (for testing)'
    )
    parser.add_argument(
        '--keep-csv',
        action='store_true',
        help='Keep CSV file after upload (default: delete)'
    )
    parser.add_argument(
        '--db-path',
        default='./data/spotify_plays.duckdb',
        help='Path to DuckDB database file (default: ./data/spotify_plays.duckdb)'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics and exit'
    )

    args = parser.parse_args()

    # Initialize database connection
    print("\n" + "="*60)
    print("SPOTIFY PLAYS LOCAL SCRAPER")
    print("="*60)

    try:
        con = init_local_db(args.db_path)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    # If --stats flag is set, just show stats and exit
    if args.stats:
        print_database_stats(con)
        con.close()
        return

    # Determine which timestamp file to use
    timestamp_file = "./starting_timestamp.txt" if args.restart else "./current_timestamp.txt"

    print(f"\nReading timestamp from: {timestamp_file}")

    try:
        with open(timestamp_file, "r") as f:
            starting_timestamp = f.read().strip()
    except FileNotFoundError:
        print(f"Error: {timestamp_file} not found!")
        print("Please create this file with a Unix timestamp in milliseconds")
        con.close()
        sys.exit(1)

    start_datetime = datetime.fromtimestamp(int(starting_timestamp) / 1000)
    print(f"Fetching plays since: {start_datetime}")

<<<<<<< Updated upstream
=======
    # Show current database stats before fetching
    current_count = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    print(f"Current plays in database: {current_count:,}")

>>>>>>> Stashed changes
    # Fetch recent tracks from Spotify
    print("\n" + "-"*60)
    print("FETCHING TRACKS FROM SPOTIFY")
    print("-"*60)
<<<<<<< Updated upstream
=======
    print("Note: This may take a while depending on how many new tracks there are...")
    print("")
>>>>>>> Stashed changes

    try:
        plays_data, new_cursor = get_recent_tracks(starting_timestamp)
    except Exception as e:
<<<<<<< Updated upstream
        print(f"Error fetching tracks from Spotify: {e}")
=======
        print(f"\nError fetching tracks from Spotify: {e}")
>>>>>>> Stashed changes
        con.close()
        sys.exit(1)

    # Check if we got any new tracks
    if not plays_data['items']:
<<<<<<< Updated upstream
        print("\nNo new tracks found since last check")
=======
        print("\n✅ No new tracks found since last check")
>>>>>>> Stashed changes
        print_database_stats(con)
        con.close()
        return

<<<<<<< Updated upstream
    print(f"\nFetched {len(plays_data['items'])} tracks from Spotify API")
=======
    print(f"\n✅ Fetched {len(plays_data['items'])} tracks from Spotify API")

    # Show a sample of what was fetched
    if plays_data['items']:
        latest = plays_data['items'][0]
        oldest = plays_data['items'][-1]
        print(f"\nDate range of fetched tracks:")
        print(f"  Latest: {latest['played_at']} - {latest['track']['name']}")
        print(f"  Oldest: {oldest['played_at']} - {oldest['track']['name']}")
>>>>>>> Stashed changes

    # Insert plays into local database
    print("\n" + "-"*60)
    print("INSERTING INTO LOCAL DATABASE")
    print("-"*60)

    try:
        new_plays = insert_plays_to_local_db(con, plays_data)
    except Exception as e:
        print(f"Error inserting plays into database: {e}")
        con.close()
        sys.exit(1)

    # Update timestamp file (always write to current_timestamp.txt)
    print("\nUpdating timestamp file...")
    try:
        with open("./current_timestamp.txt", "w") as f:
            f.write(new_cursor)
        new_cursor_datetime = datetime.fromtimestamp(int(new_cursor) / 1000)
        print(f"New cursor timestamp: {new_cursor_datetime}")
    except Exception as e:
        print(f"Warning: Could not update timestamp file: {e}")

    # Export to CSV
    print("\n" + "-"*60)
    print("EXPORTING TO CSV")
    print("-"*60)
<<<<<<< Updated upstream
=======
    print("Exporting full database to CSV for backup...")
>>>>>>> Stashed changes

    try:
        csv_path = export_to_csv(con)
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        con.close()
        sys.exit(1)

    # Upload to B2 (unless --no-upload flag is set)
    if not args.no_upload:
        print("\n" + "-"*60)
        print("UPLOADING TO B2")
        print("-"*60)
<<<<<<< Updated upstream

        try:
            uploaded_filename = upload_csv_to_b2(csv_path)
            print(f"Successfully uploaded: {uploaded_filename}")
        except Exception as e:
            print(f"Error uploading to B2: {e}")
=======
        print("Uploading CSV to Backblaze B2 cloud storage...")

        try:
            uploaded_filename = upload_csv_to_b2(csv_path)
            print(f"✅ Successfully uploaded: {uploaded_filename}")
        except Exception as e:
            print(f"⚠️  Error uploading to B2: {e}")
>>>>>>> Stashed changes
            print(f"CSV file saved locally at: {csv_path}")
            con.close()
            sys.exit(1)

        # Clean up CSV file (unless --keep-csv flag is set)
        if not args.keep_csv:
            try:
                os.remove(csv_path)
<<<<<<< Updated upstream
                print(f"Cleaned up local CSV file: {csv_path}")
            except Exception as e:
                print(f"Warning: Could not delete CSV file: {e}")
        else:
            print(f"CSV file kept at: {csv_path}")
    else:
        print("\n⚠️  Skipping B2 upload (--no-upload flag set)")
        print(f"CSV file saved at: {csv_path}")
=======
                print(f"🗑️  Cleaned up local CSV file")
            except Exception as e:
                print(f"Warning: Could not delete CSV file: {e}")
        else:
            print(f"📁 CSV file kept at: {csv_path}")
    else:
        print("\n⚠️  Skipping B2 upload (--no-upload flag set)")
        print(f"📁 CSV file saved at: {csv_path}")
>>>>>>> Stashed changes

    # Show final statistics
    print_database_stats(con)

    # Close database connection
    con.close()

    print("="*60)
    print("✅ SCRAPING COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
