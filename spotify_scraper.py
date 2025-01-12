import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json

def get_recent_tracks(after_date, max_size_bytes=1024*1024*1024):  # 1GB default
    all_tracks = {'items': []}
    latest_cursor = after_date
    current_size = 0
    
    scope = 'user-read-recently-played'
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    while True:
        results = spotify.current_user_recently_played(after=latest_cursor)
        
        if not results['items']:
            break

        # Update the cursor for next iteration
        latest_cursor = results['cursors']['after']
        
        # Add current batch of items and check size
        all_tracks['items'].extend(results['items'])
        current_size = len(json.dumps(all_tracks).encode('utf-8'))
        
        # If we've exceeded max size, return current batch and cursor
        if current_size >= max_size_bytes:
            break
            
        # If no more items or no next cursor, break
        if 'cursors' not in results or not results['cursors'].get('after'):
            break
    
    return all_tracks, latest_cursor 