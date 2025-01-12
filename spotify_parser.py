import duckdb
import pandas as pd
from datetime import datetime
import json
import os

def parse_spotify_plays_to_db(b2_resource, bucket_name, db_name="YOUR_MOTHERDUCK_DB"):
    """Parse JSON files from B2 bucket into MotherDuck database"""
    
    # Get MotherDuck token from environment variable
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable must be set")
    
    # Initialize MotherDuck connection
    con = duckdb.connect(f"md:{db_name}?motherduck_token={motherduck_token}")
    
    # Drop table if exists and recreate
    con.execute("DROP TABLE IF EXISTS spotify_plays")
    con.execute("""
        CREATE TABLE spotify_plays (
            played_at TIMESTAMP,
            track_name VARCHAR,
            artist_name VARCHAR,
            album_name VARCHAR,
            track_id VARCHAR,
            artist_id VARCHAR,
            album_id VARCHAR,
            duration_ms INTEGER,
            file_timestamp TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Get bucket
    bucket = b2_resource.Bucket(bucket_name)
    
    for obj in bucket.objects.all():
        if not obj.key.endswith('.json'):
            continue
            
        # Extract timestamp from filename
        try:
            file_timestamp = datetime.strptime(
                obj.key.replace('spotify_plays_', '').replace('.json', ''),
                "%Y%m%d_%H%M%S"
            )
        except ValueError:
            print(f"Skipping file with invalid timestamp format: {obj.key}")
            continue
            
        print(f"Processing file: {obj.key}")
        
        # Download and parse JSON
        response = obj.get()
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Transform data into rows
        rows = []
        for item in data['items']:
            track = item['track']
            rows.append({
                'played_at': item['played_at'],
                'track_name': track['name'],
                'artist_name': track['artists'][0]['name'],
                'album_name': track['album']['name'],
                'track_id': track['id'],
                'artist_id': track['artists'][0]['id'],
                'album_id': track['album']['id'],
                'duration_ms': track['duration_ms'],
                'file_timestamp': file_timestamp
            })
        
        if rows:
            # Convert to DataFrame and insert into DuckDB
            df = pd.DataFrame(rows)
            con.execute("""
                INSERT INTO spotify_plays (
                    played_at, track_name, artist_name, album_name,
                    track_id, artist_id, album_id, duration_ms, file_timestamp
                )
                SELECT * FROM df
            """)
            
            print(f"Inserted {len(rows)} rows from {obj.key}")
    
    # Create index on played_at for better query performance
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_played_at ON spotify_plays(played_at)
    """)
    
    con.close()
    print("Processing complete!") 