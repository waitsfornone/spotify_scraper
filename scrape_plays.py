import spotipy
from spotipy.oauth2 import SpotifyOAuth
import boto3
from botocore.config import Config
import os
import json


def get_recent_tracks(after_date):
    scope = 'user-read-recently-played'
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    results = spotify.current_user_recently_played(after=after_date)
    # print(results)

    return results

def get_b2_resource(endpoint, key_id, application_key):
    b2 = boto3.resource(service_name='s3',
                        endpoint_url=endpoint,
                        aws_access_key_id=key_id,
                        aws_secret_access_key=application_key,
                        config = Config(
                            signature_version='s3v4',
                    ))
    return b2

def main():
    b2_key_id = os.getenv("B2_SPOTIFY_KEY_ID")
    b2_app_key = os.getenv("B2_APP_KEY")
    b2_endpoint = 'https://s3.us-west-004.backblazeb2.com'

    with open("./current_timestamp.txt", "r") as inf:
        after_date = inf.read()

    data = get_recent_tracks(after_date)
    cursors = data['cursors']
    print(cursors)
    data_as_bytes = json.dumps(data).encode('utf-8')
    b2_resource = get_b2_resource(b2_endpoint, b2_key_id, b2_app_key)
    spotify_bucket = b2_resource.Bucket('spotify-plays-raw')
    # spotify_bucket.upload_fileobj(data_as_bytes, 'test_file.json')
    spotify_bucket.Object('test_file.json').put(Body=bytes(data_as_bytes))
    # breakpoint()


if __name__ == "__main__":
    main()