import spotipy
from spotipy.oauth2 import SpotifyOAuth
import duckdb
import os
import json
from time import sleep
from datetime import datetime
import argparse
from b2_utils import get_b2_resource

def get_track_info(spotify, track_id):
    """Get track information from Spotify API"""
    try:
        # Get track details
        track_details = spotify.track(track_id)
        return track_details
    except Exception as e:
        print(f"Error processing track {track_id}: {str(e)}")
        return None

def get_distinct_tracks(db_name="my_db", restart=False):
    """Get distinct tracks from the database and fetch their Spotify data"""
    
    # Get MotherDuck token from environment variable
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if not motherduck_token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable must be set")
    
    # Initialize Spotify client
    scope = 'user-read-recently-played'
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    
    # Initialize MotherDuck connection
    con = duckdb.connect(f"md:{db_name}?motherduck_token={motherduck_token}")
    
    # Initialize B2
    b2_resource = get_b2_resource()
    bucket = b2_resource.Bucket('spotify-tracks-raw')
    
    # If restart flag is set, delete all existing files
    if restart:
        print("Restart flag set, deleting all existing files...")
        bucket.objects.all().delete()
        print("Bucket cleared")
    
    # Get list of existing track IDs in B2
    existing_tracks = set()
    if not restart:
        for obj in bucket.objects.all():
            if obj.key.startswith('track_'):
                track_id = obj.key.split('_')[1]  # Extract ID from filename
                existing_tracks.add(track_id)
        print(f"Found {len(existing_tracks)} existing tracks in B2")
    
    # Get distinct tracks from the all_plays table
    tracks_df = con.execute("""
        SELECT DISTINCT
            track_id,
            track_name,
            COUNT(*) as play_count
        FROM all_plays
        WHERE track_id IS NOT NULL
        GROUP BY track_id, track_name
        ORDER BY play_count DESC
    """).df()
    
    print(f"Found {len(tracks_df)} tracks to process")
    
    # Process each track
    for _, row in tracks_df.iterrows():
        track_id = row['track_id']
        track_name = row['track_name']
        
        # Skip if track already exists in B2 and not in restart mode
        if not restart and track_id in existing_tracks:
            print(f"Skipping existing track: {track_name} (ID: {track_id})")
            continue
            
        print(f"\nProcessing track: {track_name} (ID: {track_id})")
        
        track_details = get_track_info(spotify, track_id)
        if track_details:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'track_{track_id}_{timestamp}.json'
            
            # Upload to B2
            data_as_bytes = json.dumps(track_details).encode('utf-8')
            bucket.Object(filename).put(Body=bytes(data_as_bytes))
            print(f"Uploaded track details to B2: {filename}")
            
            # Sleep to avoid hitting rate limits
            sleep(0.1)
    
    con.close()
    print("Track enrichment complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--restart', action='store_true', help='Clear bucket and fetch all tracks again')
    args = parser.parse_args()
    
    get_distinct_tracks(restart=args.restart) 