"""
Spotify Recent Tracks ETL Pipeline
Extracts recently played tracks, transforms, and loads to PostgreSQL
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import logging
from typing import List, Dict, Any

# Initialize logger
logger = logging.getLogger(__name__)

SPOTIFY_API_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_RECENT_TRACKS_URL = "https://api.spotify.com/v1/me/player/recently-played"
DEFAULT_LIMIT = 50
BATCH_SIZE = 100

@dag(
    dag_id="spotify_recent_tracks_etl",
    description="ETL pipeline for Spotify recently played tracks",
    start="@daily",
    catchup=False
    max_active_runs=1
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "owner": "data_engineering",
        "depends_on_past": False,
    },
    tags=["spotify", "etl", "music"],
    doc_md=__doc__
)

def spotify_recent_tracks():
    """
    # Spotify Recent Tracks ETL Pipeline
    
    ## Overview
    This DAG extracts your recently played tracks from Spotify API,
    transforms the data, and loads it into a PostgreSQL database.
    
    ## Tasks
    1. **extract_recent_tracks**: Get OAuth token and fetch recent plays
    2. **validate_tracks**: Validate API response structure
    3. **transform_tracks**: Extract relevant fields and clean data
    4. **load_to_postgres**: Bulk insert into database
    
    ## Prerequisites
    1. Spotify Developer App credentials
    2. PostgreSQL database with spotify_recent_tracks table
    3. Airflow connections/variables set up
    """
    # EXTRACT
    @task(
        task_id="extract_recent_tracks",
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=10)
    )
