import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
from datetime import datetime
import time

def get_recent_tracks(starting_timestamp, max_size_bytes=1024*1024*1024):  # 1GB default
    all_tracks = {'items': []}
    current_size = 0
    
    # Start from current time
    before_timestamp = int(time.time() * 1000)  # Current time in milliseconds
    starting_timestamp = int(starting_timestamp)  # Ensure it's an integer
    
    scope = 'user-read-recently-played'
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    while True:
        cursor_datetime = datetime.fromtimestamp(before_timestamp/1000)
        print(f"Fetching tracks before: {cursor_datetime}")
        
        results = spotify.current_user_recently_played(before=before_timestamp, limit=50)

        if not results['items']:
            print("No more tracks found")
            break

        # Get timestamp of oldest track in this batch
        oldest_track = results['items'][-1]
        oldest_track_time = oldest_track['played_at']
        print(f"Oldest track in batch: {oldest_track['track']['name']} by {oldest_track['track']['artists'][0]['name']} played at {oldest_track_time}")
        
        # Convert ISO timestamp to Unix timestamp in milliseconds
        new_before_timestamp = int(datetime.strptime(oldest_track_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
        
        # Check if timestamp hasn't changed
        if new_before_timestamp >= before_timestamp:
            print(f"Warning: Timestamp not decreasing, breaking to avoid infinite loop")
            break
            
        before_timestamp = new_before_timestamp
        
        # Add current batch of items and check size
        all_tracks['items'].extend(results['items'])
        current_size = len(json.dumps(all_tracks).encode('utf-8'))
        
        # If we've hit our starting timestamp or earlier, break
        if before_timestamp <= starting_timestamp:
            print(f"Reached or passed starting timestamp: {datetime.fromtimestamp(starting_timestamp/1000)}")
            break
            
        # If we've exceeded max size, break
        if current_size >= max_size_bytes:
            print(f"Reached max size limit of {max_size_bytes} bytes")
            break
            
        # If we're under max size, continue fetching more tracks
        if current_size < max_size_bytes:
            cursor_datetime = datetime.fromtimestamp(before_timestamp/1000)
            print(f"Getting next batch before: {cursor_datetime}")
            continue
    
    return all_tracks, str(before_timestamp) 