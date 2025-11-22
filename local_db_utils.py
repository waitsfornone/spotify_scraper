import duckdb
import pandas as pd
import os
from datetime import datetime
from b2_utils import get_b2_resource


def init_local_db(db_path="./data/spotify_plays.duckdb"):
    """
    Initialize local DuckDB database and create table if it doesn't exist.
    
    Args:
        db_path: Path to the DuckDB database file
        
    Returns:
        duckdb.DuckDBPyConnection: Database connection object
    """
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to local DuckDB file
    con = duckdb.connect(db_path)
    
    # Create table with primary key for deduplication
    con.execute("""
        CREATE TABLE IF NOT EXISTS spotify_plays (
            played_at TIMESTAMP,
            track_name VARCHAR,
            artist_name VARCHAR,
            album_name VARCHAR,
            track_id VARCHAR,
            artist_id VARCHAR,
            album_id VARCHAR,
            duration_ms INTEGER,
            PRIMARY KEY (played_at, track_id)
        )
    """)
    
    print(f"Connected to local DuckDB at: {db_path}")
    return con


def insert_plays_to_local_db(con, plays_data):
    """
    Insert plays into local DuckDB with automatic deduplication.
    
    Args:
        con: DuckDB connection object
        plays_data: Dictionary containing 'items' list from Spotify API
        
    Returns:
        int: Number of new plays inserted
    """
    if not plays_data.get('items'):
        print("No plays to insert")
        return 0
    
    # Transform data into rows
    rows = []
    for item in plays_data['items']:
        track = item['track']
        rows.append({
            'played_at': item['played_at'],
            'track_name': track['name'],
            'artist_name': track['artists'][0]['name'],
            'album_name': track['album']['name'],
            'track_id': track['id'],
            'artist_id': track['artists'][0]['id'],
            'album_id': track['album']['id'],
            'duration_ms': track['duration_ms']
        })
    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    
    # Count existing plays before insert
    count_before = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    
    # Insert with ON CONFLICT DO NOTHING (deduplication)
    # DuckDB uses INSERT OR IGNORE for this
    con.execute("""
        INSERT OR IGNORE INTO spotify_plays (
            played_at, track_name, artist_name, album_name,
            track_id, artist_id, album_id, duration_ms
        )
        SELECT * FROM df
    """)
    
    # Count after insert
    count_after = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    new_plays = count_after - count_before
    
    print(f"Inserted {new_plays} new plays ({len(rows) - new_plays} duplicates skipped)")
    print(f"Total plays in database: {count_after}")
    
    return new_plays


def export_to_csv(con, csv_path=None):
    """
    Export the entire spotify_plays table to a CSV file.
    
    Args:
        con: DuckDB connection object
        csv_path: Optional path for CSV file. If None, generates timestamped filename.
        
    Returns:
        str: Path to the exported CSV file
    """
    # Generate timestamped filename if not provided
    if csv_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = f"./exports/spotify_plays_{timestamp}.csv"
    
    # Ensure exports directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # Export to CSV using DuckDB's COPY command (faster than pandas)
    con.execute(f"""
        COPY (
            SELECT * FROM spotify_plays 
            ORDER BY played_at DESC
        ) TO '{csv_path}' (HEADER, DELIMITER ',')
    """)
    
    # Get row count and file size
    row_count = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    file_size = os.path.getsize(csv_path) / (1024 * 1024)  # Size in MB
    
    print(f"Exported {row_count} plays to: {csv_path}")
    print(f"CSV file size: {file_size:.2f} MB")
    
    return csv_path


def upload_csv_to_b2(csv_path, bucket_name='spotify-plays-csv'):
    """
    Upload CSV file to Backblaze B2 bucket.
    
    Args:
        csv_path: Path to the CSV file to upload
        bucket_name: Name of the B2 bucket (default: 'spotify-plays-csv')
        
    Returns:
        str: B2 object key (filename in bucket)
    """
    # Get B2 resource
    b2_resource = get_b2_resource()
    bucket = b2_resource.Bucket(bucket_name)
    
    # Get filename from path
    filename = os.path.basename(csv_path)
    
    # Read CSV file
    with open(csv_path, 'rb') as f:
        csv_data = f.read()
    
    # Upload to B2
    bucket.Object(filename).put(Body=csv_data)
    
    file_size = len(csv_data) / (1024 * 1024)  # Size in MB
    print(f"Uploaded {filename} to B2 bucket '{bucket_name}'")
    print(f"Upload size: {file_size:.2f} MB")
    
    return filename


def get_database_stats(con):
    """
    Get statistics about the local database.
    
    Args:
        con: DuckDB connection object
        
    Returns:
        dict: Dictionary containing database statistics
    """
    stats = {}
    
    # Total plays
    stats['total_plays'] = con.execute("SELECT COUNT(*) FROM spotify_plays").fetchone()[0]
    
    # Date range
    date_range = con.execute("""
        SELECT MIN(played_at) as earliest, MAX(played_at) as latest 
        FROM spotify_plays
    """).fetchone()
    stats['earliest_play'] = date_range[0]
    stats['latest_play'] = date_range[1]
    
    # Unique tracks
    stats['unique_tracks'] = con.execute("""
        SELECT COUNT(DISTINCT track_id) FROM spotify_plays
    """).fetchone()[0]
    
    # Unique artists
    stats['unique_artists'] = con.execute("""
        SELECT COUNT(DISTINCT artist_id) FROM spotify_plays
    """).fetchone()[0]
    
    # Top 5 tracks
    stats['top_tracks'] = con.execute("""
        SELECT track_name, artist_name, COUNT(*) as play_count
        FROM spotify_plays
        GROUP BY track_name, artist_name
        ORDER BY play_count DESC
        LIMIT 5
    """).fetchdf()
    
    return stats


def print_database_stats(con):
    """
    Print formatted database statistics.

    Args:
        con: DuckDB connection object
    """
    stats = get_database_stats(con)

    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)
    print(f"Total plays: {stats['total_plays']:,}")
    print(f"Unique tracks: {stats['unique_tracks']:,}")
    print(f"Unique artists: {stats['unique_artists']:,}")
    print(f"Date range: {stats['earliest_play']} to {stats['latest_play']}")
    print("\nTop 5 Most Played Tracks:")
    print("-"*60)
    for idx, row in stats['top_tracks'].iterrows():
        print(f"  {idx+1}. {row['track_name']} - {row['artist_name']} ({row['play_count']} plays)")
    print("="*60 + "\n")


def init_tracks_table(con):
    """
    Initialize the all_tracks table if it doesn't exist.

    Args:
        con: DuckDB connection object
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS all_tracks (
            track_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            album_id VARCHAR,
            album_name VARCHAR,
            album_release_date VARCHAR,
            album_total_tracks INTEGER,
            artist_id VARCHAR,
            artist_name VARCHAR,
            duration_ms INTEGER,
            explicit BOOLEAN,
            popularity INTEGER,
            preview_url VARCHAR,
            spotify_url VARCHAR,
            is_local BOOLEAN,
            disc_number INTEGER,
            track_number INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Tracks table ready")


def insert_track_to_local_db(con, track_data):
    """
    Insert a single track into the all_tracks table with deduplication.

    Args:
        con: DuckDB connection object
        track_data: Dictionary containing track information from Spotify API

    Returns:
        bool: True if inserted, False if already existed
    """
    import pandas as pd

    # Transform track data into row
    row = {
        'track_id': track_data['id'],
        'name': track_data['name'],
        'album_id': track_data['album']['id'],
        'album_name': track_data['album']['name'],
        'album_release_date': track_data['album']['release_date'],
        'album_total_tracks': track_data['album']['total_tracks'],
        'artist_id': track_data['artists'][0]['id'],
        'artist_name': track_data['artists'][0]['name'],
        'duration_ms': track_data['duration_ms'],
        'explicit': track_data['explicit'],
        'popularity': track_data['popularity'],
        'preview_url': track_data.get('preview_url'),
        'spotify_url': track_data['external_urls']['spotify'],
        'is_local': track_data['is_local'],
        'disc_number': track_data['disc_number'],
        'track_number': track_data['track_number']
    }

    # Convert to DataFrame
    df = pd.DataFrame([row])

    # Count before insert
    count_before = con.execute("SELECT COUNT(*) FROM all_tracks").fetchone()[0]

    # Insert with deduplication
    con.execute("""
        INSERT OR REPLACE INTO all_tracks (
            track_id, name, album_id, album_name, album_release_date,
            album_total_tracks, artist_id, artist_name, duration_ms,
            explicit, popularity, preview_url, spotify_url, is_local,
            disc_number, track_number, last_updated
        )
        SELECT
            track_id, name, album_id, album_name, album_release_date,
            album_total_tracks, artist_id, artist_name, duration_ms,
            explicit, popularity, preview_url, spotify_url, is_local,
            disc_number, track_number, CURRENT_TIMESTAMP
        FROM df
    """)

    # Count after insert
    count_after = con.execute("SELECT COUNT(*) FROM all_tracks").fetchone()[0]

    return count_after > count_before


def get_tracks_needing_enrichment(con):
    """
    Get list of track IDs from spotify_plays that don't have enrichment data.

    Args:
        con: DuckDB connection object

    Returns:
        DataFrame with track_id, track_name, and play_count
    """
    return con.execute("""
        SELECT DISTINCT
            p.track_id,
            p.track_name,
            COUNT(*) as play_count
        FROM spotify_plays p
        LEFT JOIN all_tracks t ON p.track_id = t.track_id
        WHERE p.track_id IS NOT NULL
          AND p.track_id != ''
          AND t.track_id IS NULL
        GROUP BY p.track_id, p.track_name
        ORDER BY play_count DESC
    """).fetchdf()
