from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spotify", "etl"],
)

def spotify_recent_tracks_etl():
    pass

spotify_recent_tracks_dag = spotify_recent_tracks_etl

@task(retries=3, retry_delay=timedelta(minutes=5))
def extract_recent_tracks():
    token_url = "https://accounts.spotify.com/api/token"
    
    response = requests.post(
        token_url,
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.getenv("SPOTIFY_REFRESH_TOKEN"),
        },
        auth=(
            os.getenv("SPOTIFY_CLIENT_ID"),
            os.getenv("SPOTIFY_CLIENT_SECRET"),
        ),
    )
    
    access_token = response.json()[access_token]
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    recent_tracks = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50",
        headers=headers,
    )
    
    return recent_tracks.json()['items']


@task
def transform_tracks(raw_tracks):
    transformed = []
    
    for item in raw_tracks:
        transformed.append({
            "played_at": item["played_at"],
            "track_name": item["track"]["name"],
            "artist_name": item["track"]["artists"][0]["name"],
            "album_name": item["track"]["album"]["name"],
            "duration_ms": item["track"]["duration_ms"]
        })
        
    return transformed


@task
def load_to_postgres(clean_tracks):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    sql = """
    INSERT INTO spotify_recent_tracks (
        played_at, track_name, artist_name, album_name, duration_ms
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (played_at) DO NOTHING;
    """
    
    for track in clean_tracks:
        hook.run(
            sql,
            parameters=(
                track["played_at"],
                track["track_name"],
                track["artist_name"],
                track["album_name"],
                track["duration_ms"],
            )
        )

def spotify_recent_tracks_etl():
    raw = extract_recent_tracks()
    clean = transform_tracks(raw)
    load_to_postgres(clean)
