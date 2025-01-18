import os
import json
from datetime import datetime
from spotify_scraper import get_recent_tracks
from spotify_parser import parse_spotify_plays_to_db
import argparse
from b2_utils import get_b2_resource

def main():
    # Add command line argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--restart', action='store_true', help='Restart from starting timestamp')
    args = parser.parse_args()

    # Choose which timestamp file to use based on --restart flag
    timestamp_file = "./starting_timestamp.txt" if args.restart else "./current_timestamp.txt"
    
    with open(timestamp_file, "r") as inf:
        after_date = inf.read()

    # Get tracks and new cursor
    data, new_cursor = get_recent_tracks(after_date)
    
    # Only upload new data if we got new tracks
    if data['items']:
        # Always save to current_timestamp.txt, regardless of which file we read from
        with open("./current_timestamp.txt", "w") as outf:
            outf.write(new_cursor)
        
        # Convert after_date (milliseconds) to datetime and format filename
        start_datetime = datetime.fromtimestamp(int(after_date)/1000)
        timestamp = start_datetime.strftime("%Y%m%d_%H%M%S")
        filename = f'spotify_plays_{timestamp}.json'
        
        data_as_bytes = json.dumps(data).encode('utf-8')
        
        b2_resource = get_b2_resource()
        spotify_bucket = b2_resource.Bucket('spotify-plays-raw')
        spotify_bucket.Object(filename).put(Body=bytes(data_as_bytes))
        
        print("Uploaded new tracks to B2")
    else:
        print("No new tracks found since last check")
        b2_resource = get_b2_resource()

    # Always parse and reload the database, regardless of new tracks
    parse_spotify_plays_to_db(b2_resource, 'spotify-plays-raw')

if __name__ == "__main__":
    main()