import spotipy
from spotipy.oauth2 import SpotifyOAuth


def get_recent_tracks(after_date):
    scope = 'user-read-recently-played'
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    results = spotify.current_user_recently_played(after=after_date)
    print(results)

    return results


with open("./current_timestamp.txt", "r") as inf:
    after_date = inf.read()

get_recent_tracks(after_date)