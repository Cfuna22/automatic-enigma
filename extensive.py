"""
Spotify Recent Tracks ETL Pipeline
Extracts recently played tracks, transforms, and loads to PostgreSQL
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import logging
import json
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
    catchup=False,
    max_active_runs=1,
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
    def extract_recent_tracks() -> List[Dict[str, Any]]:
        """
        Extract recently played tracks from Spotify API.
        
        Returns:
            List of raw track dictionaries from Spotify API
        
        Raises:
            AirflowException: If API calls fail or credentials missing
        """
        logger.info("Starting Spotify API extraction")
        
        # GET CREDENTIALS
        try:
            client_id = Variable.get("SPOTIFY_CLIENT_ID")
            client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")
            refresh_token = Variable.get("SPOTIFY_REFRESH_TOKEN")
            
            if not all([client_id, client_secret, refresh_token]):
                raise ValueError("Missing one or more Spotify credentials")
            
        except Exception as e:
            logger.error(f"Failed to load Spotify credentials: {e}")
            raise AirflowException(f"Spotify credentials error: {e}")
        
        # GET ACCESS TOKEN
        logger.info("Requesting access token from Spotify")
        try:
            token_response = requests.post(
                SPOTIFY_API_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token
                },
                auth=(client_id, client_secret),
                timeout=30
            )
            
            # Check for HTTP errors
            token_response.raise_for_status()
            
            token_data = token_response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                raise ValueError("No access token in response")
            
            logger.info("Successfully obtained access token")
            
        except Exception as e:
            logger.error(f"Token request failed: {e}")
            
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status failed: {e.response.status_code}")
                raise AirflowException(f"Spotify token request failed: {e}")
            
            # FETCH RECENT TRACKS
            logger.info(f"Fetching recent tracks (limit: {DEFAULT_LIMIT})")
            try:
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
                
                params = {
                    "limit": DEFAULT_LIMIT,
                    "after": get_yesterday_timestamp()
                }
                
                tracks_response = requests.get(
                    SPOTIFY_RECENT_TRACKS_URL,
                    headers=headers,
                    params=params,
                    timeout=30,
                )
                
                tracks_response.raise_for_status()
                tracks_data = tracks_response.json()
                
                items = tracks_data.get("items", [])
                logger.info(f"Successfully extracted {len(items)} tracks")
                
                if items:
                    sample = {
                        "track": items[0]["track"]["name"],
                        "artist": items[0]["track"]["artists"][0]["name"],
                        "played_at": items[0]["played_at"],
                    }
                    logger.debug(f"Sample track {json.dumps(sample, indent=2)}")
                    return items
            except requests.exceptions.RequestException as e:
                logger.error(f"Tracks request failed: {e}")
                raise AirflowException(f"Spotify tracks request failed: {e}")
            
        # VALIDATE
        @task(
            task_is="validate_tracks",
            retries=2,
            retry_delay=timedelta(minutes=2),
        )
        def validate_tracks(raw_tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            logger.info(f"Validating {len(raw_tracks)} tracks")
            
            if not raw_tracks:
                logger.warning("No tracks to validate")
                return []
            
            validate_tracks = []
            validation_errors = []
            
            for i, item in enumerate(raw_tracks):
                try:
                    required_fields = [
                        ("played_at", str),
                        ("track", dict)
                    ]
                    for field_name, field_type in required_fields:
                        if field_name not in item:
                            raise ValueError(f"Missing field: {field_name}")
                        if not isinstance(item[field_name], field_type):
                            raise ValueError(f"Invalid type for {field_name}")
                        
                        track = item["track"]
                        nested_required = [
                            ("name", str),
                            ("artists", list),
                            ("album", dict),
                            ("duration_ms", int)
                        ]
                        
                        for field_name, field_type in nested_required:
                            if field_name not in track:
                                raise ValueError(f"Missing track field: {field_name}")
                            if not isinstance(track[field_name], field_type):
                                raise ValueError(f"Invalid type for track. {field_name}")
                            
                            # Validate artists list
                            artists = track.get("artists", [])
                            if not artists:
                                logger.warning(f"Track {i} has empty artists list")
                                
                                validate_tracks.append(item)
                                
                except Exception as e:
                    error_msg = f"Validation failed for track {i}: {e}"
                    validation_errors.append(error_msg)
                    logger.warning(error_msg)
