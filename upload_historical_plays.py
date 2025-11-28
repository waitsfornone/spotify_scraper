#!/usr/bin/env python3
"""
Upload historical Spotify play data from JSON files to local DuckDB.

This script processes Spotify export JSON files and adds them to the local
DuckDB database using incremental inserts (no table drops).

Supported formats:
- StreamingHistory_music_*.json (new export format)
- Old export format with trackId, ts, etc.

Usage:
    python upload_historical_plays.py                           # Use default directory
    python upload_historical_plays.py --data-dir /path/to/json  # Custom directory
    python upload_historical_plays.py --db-path ./custom.duckdb # Custom database
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime
from local_db_utils import init_local_db, print_database_stats
import pandas as pd


def parse_streaming_history_format(item):
    """
    Parse Spotify's StreamingHistory JSON format.

    Format:
    {
        "endTime": "2024-10-10 12:58",
        "artistName": "Artist Name",
        "trackName": "Track Name",
        "msPlayed": 193226
    }
    """
    try:
        # Parse endTime format: "YYYY-MM-DD HH:MM"
        played_at = datetime.strptime(item['endTime'], "%Y-%m-%d %H:%M")

        return {
            'played_at': played_at,
            'track_name': item.get('trackName', ''),
            'artist_name': item.get('artistName', ''),
            'album_name': '',  # Not available in this format
            'track_id': '',    # Not available in this format
            'artist_id': '',   # Not available in this format
            'album_id': '',    # Not available in this format
            'duration_ms': item.get('msPlayed', 0)
        }
    except (KeyError, ValueError):
        return None


def parse_old_export_format(item):
    """
    Parse older Spotify export format.

    Format:
    {
        "ts": "2023-01-01T12:00:00Z" or milliseconds,
        "trackName": "Track Name",
        "artistName": "Artist Name",
        "albumName": "Album Name",
        "trackId": "spotify:track:abc123" or "abc123",
        "skipped": true/false
    }
    """
    try:
        # Handle track_id
        track_uri = item.get('trackId', item.get('spotify_track_uri', ''))
        track_id = track_uri.split(':')[2] if track_uri and ':' in track_uri else track_uri

        # Parse timestamp
        try:
            # Try ISO format first
            played_at = datetime.strptime(item['ts'], "%Y-%m-%dT%H:%M:%SZ")
        except (KeyError, ValueError):
            try:
                # Try milliseconds format
                played_at = datetime.fromtimestamp(int(item['ts']) / 1000)
            except (KeyError, ValueError):
                return None

        return {
            'played_at': played_at,
            'track_name': item.get('trackName', item.get('master_metadata_track_name', '')),
            'artist_name': item.get('artistName', item.get('master_metadata_album_artist_name', '')),
            'album_name': item.get('albumName', item.get('master_metadata_album_album_name', '')),
            'track_id': track_id,
            'artist_id': '',  # Not typically available
            'album_id': '',   # Not typically available
            'duration_ms': item.get('ms_played', 0)
        }
    except Exception:
        return None


def detect_and_parse_item(item):
    """
    Auto-detect JSON format and parse accordingly.
    """
    # Check for StreamingHistory format (has endTime field)
    if 'endTime' in item:
        return parse_streaming_history_format(item)
    # Check for old export format (has ts field)
    elif 'ts' in item or 'trackName' in item:
        return parse_old_export_format(item)
    else:
        return None


def upload_historical_plays(data_dir, db_path="./data/spotify_plays.duckdb"):
    """
    Upload historical Spotify play data to local DuckDB database.

    Args:
        data_dir: Directory containing JSON files
        db_path: Path to DuckDB database file
    """
    # Check if data directory exists
    if not os.path.exists(data_dir):
        print(f"Error: Directory not found: {data_dir}")
        sys.exit(1)

    # Initialize database connection
    print("\n" + "="*60)
    print("HISTORICAL PLAYS UPLOAD")
    print("="*60)

    con = init_local_db(db_path)

    # Get count before insertion
    count_before = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    print(f"Plays in database before upload: {count_before:,}")

    # Get all JSON files in the directory
    json_files = glob.glob(os.path.join(data_dir, "*.json"))

    if not json_files:
        print(f"\nNo JSON files found in {data_dir}")
        con.close()
        sys.exit(1)

    print(f"\nFound {len(json_files)} JSON files to process")
    print("-"*60)

    total_processed = 0
    total_inserted = 0
    total_skipped = 0

    for json_file in json_files:
        filename = os.path.basename(json_file)
        print(f"\nProcessing: {filename}")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  ⚠️  Error reading JSON: {e}")
            continue
        except Exception as e:
            print(f"  ⚠️  Error: {e}")
            continue

        # Handle both list and dict formats
        if isinstance(data, dict):
            # Could be a wrapper object, try to find the items
            if 'items' in data:
                data = data['items']
            else:
                print(f"  ⚠️  Skipping - not a recognized format")
                continue

        if not isinstance(data, list):
            print(f"  ⚠️  Skipping - expected a list of plays")
            continue

        # Transform data into rows
        rows = []
        for item in data:
            parsed = detect_and_parse_item(item)
            if parsed:
                rows.append(parsed)

        if not rows:
            print(f"  ⚠️  No valid plays found in file")
            continue

        # Convert to DataFrame and insert
        df = pd.DataFrame(rows)

        # Count before this file's insert
        file_count_before = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]

        try:
            # Insert with deduplication
            con.execute("""
                INSERT OR IGNORE INTO spotify_plays (
                    played_at, track_name, artist_name, album_name,
                    track_id, artist_id, album_id, duration_ms
                )
                SELECT * FROM df
            """)

            # Count after insert
            file_count_after = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
            inserted = file_count_after - file_count_before
            duplicates = len(rows) - inserted

            print(f"  ✅ Processed {len(rows):,} plays")
            print(f"     → Inserted: {inserted:,} new plays")
            print(f"     → Skipped: {duplicates:,} duplicates")

            total_processed += len(rows)
            total_inserted += inserted
            total_skipped += duplicates

        except Exception as e:
            print(f"  ⚠️  Error inserting data: {e}")
            continue

    # Show summary
    count_after = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]

    print("\n" + "="*60)
    print("UPLOAD SUMMARY")
    print("="*60)
    print(f"Total plays processed: {total_processed:,}")
    print(f"New plays inserted: {total_inserted:,}")
    print(f"Duplicates skipped: {total_skipped:,}")
    print(f"Database before: {count_before:,}")
    print(f"Database after: {count_after:,}")
    print(f"Net change: +{count_after - count_before:,}")

    # Show database statistics
    print_database_stats(con)

    # Close connection
    con.close()

    print("="*60)
    print("✅ UPLOAD COMPLETE")
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Upload historical Spotify plays from JSON files to local DuckDB'
    )
    parser.add_argument(
        '--data-dir',
        default='spotify_play_data',
        help='Directory containing JSON files (default: spotify_play_data)'
    )
    parser.add_argument(
        '--db-path',
        default='./data/spotify_plays.duckdb',
        help='Path to DuckDB database file (default: ./data/spotify_plays.duckdb)'
    )

    args = parser.parse_args()

    upload_historical_plays(args.data_dir, args.db_path)


if __name__ == "__main__":
    main() 