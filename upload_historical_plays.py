import duckdb
import pandas as pd
from datetime import datetime
import json
import os
import glob

def upload_historical_plays(data_dir="spotify_play_data", db_name="my_db"):
    """Upload historical Spotify play data to MotherDuck database"""
    
    # Get MotherDuck token from environment variable
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable must be set")
    
    # Initialize MotherDuck connection
    con = duckdb.connect(f"md:{db_name}?motherduck_token={motherduck_token}")
    
    # Drop and recreate historical plays table
    con.execute("DROP TABLE IF EXISTS historical_plays")
    con.execute("""
        CREATE TABLE historical_plays (
            played_at TIMESTAMP,
            track_name VARCHAR,
            artist_name VARCHAR,
            album_name VARCHAR,
            track_id VARCHAR,
            skipped BOOLEAN
        )
    """)
    
    # Get all JSON files in the directory
    json_files = glob.glob(os.path.join(data_dir, "*.json"))
    
    for json_file in json_files:
        print(f"Processing file: {json_file}")
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Transform data into rows
        rows = []
        for item in data:
            # Handle both old and new format JSON files
            track_name = item.get('trackName', item.get('master_metadata_track_name', ''))
            artist_name = item.get('artistName', item.get('master_metadata_album_artist_name', ''))
            album_name = item.get('albumName', item.get('master_metadata_album_album_name', ''))
            
            # Get track_id and split on ':' if it exists
            track_uri = item.get('trackId', item.get('spotify_track_uri', ''))
            track_id = track_uri.split(':')[2] if track_uri and ':' in track_uri else track_uri
            
            # Get skipped status directly from JSON
            skipped = item.get('skipped', None)
            
            # Convert timestamp format
            try:
                # Try new format first (ISO format)
                played_at = datetime.strptime(item['ts'], "%Y-%m-%dT%H:%M:%SZ")
            except (KeyError, ValueError):
                try:
                    # Try old format (milliseconds)
                    played_at = datetime.fromtimestamp(int(item['ts'])/1000)
                except (KeyError, ValueError):
                    print(f"Warning: Could not parse timestamp for track {track_name}")
                    continue
            
            rows.append({
                'played_at': played_at,
                'track_name': track_name,
                'artist_name': artist_name,
                'album_name': album_name,
                'track_id': track_id,
                'skipped': skipped
            })
        
        if rows:
            # Convert to DataFrame and insert into DuckDB
            df = pd.DataFrame(rows)
            con.execute("""
                INSERT INTO historical_plays (
                    played_at, track_name, artist_name, album_name,
                    track_id, skipped
                )
                SELECT * FROM df
            """)
            
            print(f"Inserted {len(rows)} rows from {json_file}")
    
    # Create index on played_at for better query performance
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_played_at ON historical_plays(played_at)
    """)
    
    con.close()
    print("Processing complete!")

if __name__ == "__main__":
    upload_historical_plays() 