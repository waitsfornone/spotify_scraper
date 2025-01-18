import duckdb
import pandas as pd
import json
import os
from b2_utils import get_b2_resource

def parse_tracks_to_db(db_name="my_db"):
    """Parse track JSON files from B2 bucket into MotherDuck database"""
    
    # Get MotherDuck token from environment variable
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable must be set")
    
    # Initialize MotherDuck connection
    con = duckdb.connect(f"md:{db_name}?motherduck_token={motherduck_token}")
    
    # Drop and recreate tracks table
    con.execute("DROP TABLE IF EXISTS all_tracks")
    con.execute("""
        CREATE TABLE all_tracks (
            track_id VARCHAR,
            name VARCHAR,
            album_id VARCHAR,
            album_name VARCHAR,
            album_release_date STRING,
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
    
    # Initialize B2
    b2_resource = get_b2_resource()
    bucket = b2_resource.Bucket('spotify-tracks-raw')
    
    # Process each JSON file
    for obj in bucket.objects.all():
        if not obj.key.startswith('track_'):
            continue
            
        print(f"Processing file: {obj.key}")
        
        # Download and parse JSON
        response = obj.get()
        track_data = json.loads(response['Body'].read().decode('utf-8'))
        
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
            'preview_url': track_data['preview_url'],
            'spotify_url': track_data['external_urls']['spotify'],
            'is_local': track_data['is_local'],
            'disc_number': track_data['disc_number'],
            'track_number': track_data['track_number']
        }
        
        # Convert to DataFrame for easy insertion
        df = pd.DataFrame([row])
        
        # Insert into database
        con.execute("""
            INSERT INTO all_tracks (
                track_id, name, album_id, album_name, album_release_date,
                album_total_tracks, artist_id, artist_name, duration_ms,
                explicit, popularity, preview_url, spotify_url, is_local,
                disc_number, track_number
            )
            SELECT 
                track_id, name, album_id, album_name, album_release_date,
                album_total_tracks, artist_id, artist_name, duration_ms,
                explicit, popularity, preview_url, spotify_url, is_local,
                disc_number, track_number
            FROM df
            WHERE NOT EXISTS (
                SELECT 1 FROM all_tracks 
                WHERE all_tracks.track_id = df.track_id
            )
        """)
        
        print(f"Processed track: {row['name']}")
    
    # Create index on track_id for better query performance
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_track_id ON all_tracks(track_id)
    """)
    
    con.close()
    print("Track parsing complete!")

if __name__ == "__main__":
    parse_tracks_to_db() 