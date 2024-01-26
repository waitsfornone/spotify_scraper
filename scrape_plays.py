import spotipy
from spotipy.oauth2 import SpotifyOAuth
from prefect import flow, task
from prefect.blocks.system import String


@task
def get_recent_tracks(after_date):
    scope = 'user-read-recently-played'
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    results = spotify.current_user_recently_played(after=after_date)

    return results


@flow
def scrape_plays(full_load=False):
    if full_load:
        start_date = String.load("backfill-start").value()
    else:
        start_date = String.load("current-timestamp").value()
    print(start_date)
    # get_recent_tracks(start_date)


if __name__ == "__main__":
    plays = scrape_plays()
    # print(plays['items'])