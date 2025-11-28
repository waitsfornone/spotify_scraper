#!/usr/bin/env python3
"""
Enrich track data from Spotify API and store in local DuckDB.

This script:
1. Finds all tracks in spotify_plays that don't have enrichment data
2. Fetches detailed track information from Spotify API
3. Stores the enriched data in the all_tracks table

Usage:
    python enrich_tracks_local.py                    # Enrich missing tracks
    python enrich_tracks_local.py --limit 100        # Process max 100 tracks
    python enrich_tracks_local.py --db-path ./custom.duckdb # Custom DB
"""

import argparse
import sys
from time import sleep
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from local_db_utils import (
    init_local_db,
    init_tracks_table,
    get_tracks_needing_enrichment,
    insert_track_to_local_db
)


def get_track_info(spotify, track_id):
    """
    Get track information from Spotify API.

    Args:
        spotify: Spotipy client instance
        track_id: Spotify track ID

    Returns:
        dict: Track details from Spotify API, or None on error
    """
    try:
        track_details = spotify.track(track_id)
        return track_details
    except Exception as e:
        print(f"  ⚠️  Error fetching track {track_id}: {str(e)}")
        return None


def enrich_tracks(db_path="./data/spotify_plays.duckdb", limit=None):
    """
    Enrich tracks from spotify_plays with detailed Spotify API data.

    Args:
        db_path: Path to local DuckDB file
        limit: Maximum number of tracks to process (None for all)
    """
    print("\n" + "="*60)
    print("TRACK ENRICHMENT")
    print("="*60)

    # Initialize database connection
    try:
        con = init_local_db(db_path)
        init_tracks_table(con)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    # Initialize Spotify client
    print("\nConnecting to Spotify API...")
    try:
        scope = 'user-read-recently-played'
        spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
        print("✅ Connected to Spotify API")
    except Exception as e:
        print(f"Error connecting to Spotify: {e}")
        con.close()
        sys.exit(1)

    # Get tracks that need enrichment
    print("\nFinding tracks that need enrichment...")
    try:
        tracks_df = get_tracks_needing_enrichment(con)
    except Exception as e:
        print(f"Error querying database: {e}")
        con.close()
        sys.exit(1)

    if tracks_df.empty:
        print("\n✅ All tracks are already enriched!")
        con.close()
        return

    total_tracks = len(tracks_df)
    tracks_to_process = min(total_tracks, limit) if limit else total_tracks

    print(f"Found {total_tracks:,} tracks needing enrichment")
    if limit and total_tracks > limit:
        print(f"Processing first {limit:,} tracks (use --limit to change)")
        tracks_df = tracks_df.head(limit)

    print("\n" + "-"*60)
    print("PROCESSING TRACKS")
    print("-"*60)

    # Process each track
    success_count = 0
    error_count = 0

    for idx, row in tracks_df.iterrows():
        track_id = row['track_id']
        track_name = row['track_name']
        play_count = row['play_count']

        progress = f"[{idx + 1}/{tracks_to_process}]"
        print(f"\n{progress} {track_name}")
        print(f"  Track ID: {track_id}")
        print(f"  Play count: {play_count}")

        # Fetch track details from Spotify
        track_details = get_track_info(spotify, track_id)
        if not track_details:
            error_count += 1
            continue

        # Insert into database
        try:
            was_inserted = insert_track_to_local_db(con, track_details)
            if was_inserted:
                print(f"  ✅ Enriched and saved to database")
                success_count += 1
            else:
                print(f"  ℹ️  Already exists (updated)")
                success_count += 1
        except Exception as e:
            print(f"  ⚠️  Error saving to database: {e}")
            error_count += 1
            continue

        # Sleep to avoid rate limits (Spotify allows ~180 requests per minute)
        sleep(0.35)

    # Summary
    print("\n" + "="*60)
    print("ENRICHMENT SUMMARY")
    print("="*60)
    print(f"Total tracks found: {total_tracks:,}")
    print(f"Tracks processed: {tracks_to_process:,}")
    print(f"Successfully enriched: {success_count:,}")
    print(f"Errors: {error_count:,}")

    # Get final counts
    total_enriched = con.execute("SELECT COUNT(*) FROM all_tracks").fetchone()[0]
    print(f"\nTotal enriched tracks in database: {total_enriched:,}")

    # Close connection
    con.close()

    print("="*60)
    print("✅ ENRICHMENT COMPLETE")
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Enrich track data from Spotify API into local DuckDB'
    )
    parser.add_argument(
        '--db-path',
        default='./data/spotify_plays.duckdb',
        help='Path to DuckDB database file (default: ./data/spotify_plays.duckdb)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of tracks to process (default: all)'
    )

    args = parser.parse_args()

    enrich_tracks(db_path=args.db_path, limit=args.limit)


if __name__ == "__main__":
    main()
