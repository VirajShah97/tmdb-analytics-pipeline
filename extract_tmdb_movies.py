import os
import json
import time
import logging
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests

# --- Config ---
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "vshah-tmdb-pipeline")
NUM_PAGES = int(os.getenv("NUM_PAGES", "5"))  # Configurable via env — useful for Lambda deploys
BASE_URL = "https://api.themoviedb.org/3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

s3 = boto3.client("s3")


# --- TMDB Helpers ---


def _get_with_retries(url: str, params: dict, max_retries: int = 3) -> dict:
    """GET request with exponential backoff for transient failures.
    
    Client errors (4xx) are raised immediately without retrying.
    Server errors (5xx) and connection issues are retried up to max_retries times.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            # Don't retry client errors — retrying won't fix a bad request or missing resource
            if e.response.status_code in range(400, 500):
                raise
            if attempt == max_retries - 1:
                raise
            wait = (2**attempt) + 0.25
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait:.1f}s")
            time.sleep(wait)
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait = (2**attempt) + 0.25
            logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait:.1f}s")
            time.sleep(wait)


def get_popular_movies(page: int) -> dict:
    """Fetch one page of popular movies."""
    url = f"{BASE_URL}/movie/popular"
    params = {"api_key": TMDB_API_KEY, "language": "en-US", "page": page}
    return _get_with_retries(url, params)


def get_movie_details(movie_id: int) -> dict:
    """Fetch full movie details including budget, revenue, and genres."""
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    return _get_with_retries(url, params)


# --- Core Logic ---

def fetch_movies(num_pages: int = NUM_PAGES) -> list[dict]:
    """
    Fetch movie details across num_pages of popular movies.
    Includes budget + revenue for Genre ROI analysis.
    Skips movies where both budget and revenue are 0 (unreported).
    Note: page_start_count reflects post-filter count (movies with financial data only).
    """
    movies = []

    for page in range(1, num_pages + 1):
        logging.info(f"Fetching popular movies page {page}/{num_pages}")
        popular = get_popular_movies(page)
        page_start_count = len(movies)

        for item in popular.get("results", []):
            movie_id = item.get("id")
            if movie_id is None:
                logging.debug("Skipping result with missing id")
                continue
            try:
                details = get_movie_details(movie_id)

                # Skip if financial data is missing (not useful for ROI)
                if details.get("budget", 0) == 0 and details.get("revenue", 0) == 0:
                    logging.debug(f"Skipping movie {movie_id} — no financial data")
                    continue

                movies.append({
                    "movie_id": details["id"],
                    "title": details["title"],
                    "release_date": details.get("release_date"),
                    "budget": details.get("budget", 0),
                    "revenue": details.get("revenue", 0),
                    "popularity": details.get("popularity"),
                    "vote_average": details.get("vote_average"),
                    "vote_count": details.get("vote_count"),
                    "runtime": details.get("runtime"),
                    "genres": [g["name"] for g in details.get("genres", [])],
                    "genre_ids": [g["id"] for g in details.get("genres", [])],
                    "original_language": details.get("original_language"),
                    "status": details.get("status"),
                    "_ingested_at": datetime.now(timezone.utc).isoformat()
                })

            except requests.HTTPError as e:
                logging.warning(f"HTTP error for movie {movie_id}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for movie {movie_id}: {e}")
            finally:
                time.sleep(0.25)  # Respect TMDB rate limit (40 req/10s)

        new_on_page = len(movies) - page_start_count
        logging.info(f"Page {page} complete — {new_on_page} new (total so far: {len(movies)})")

    return movies


def upload_to_s3(movies: list[dict]) -> str:
    """Upload movie data as newline-delimited JSON to S3."""
    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%H-%M-%S")
    s3_key = f"raw/movies/{date}/movies_{timestamp}.json"

    ndjson = "\n".join(json.dumps(m) for m in movies)

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=ndjson.encode("utf-8"),
            ContentType="application/x-ndjson"
        )
    except Exception as e:
        logging.error(f"S3 upload failed: {e}")
        raise

    logging.info(f"Uploaded {len(movies)} movies to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# --- Entry Point ---

def main():
    logging.info("Starting TMDB ingestion")

    if not TMDB_API_KEY:
        raise ValueError("TMDB_API_KEY not found in .env file")

    movies = fetch_movies()  # num_pages controlled by NUM_PAGES env var (default: 5)
    logging.info(f"Total movies with financial data: {len(movies)}")

    if not movies:
        logging.warning("No movies fetched — check API key and filters")
        return

    try:
        s3_key = upload_to_s3(movies)
        logging.info(f"Ingestion complete. S3 key: {s3_key}")
    except Exception as e:
        logging.error(f"Ingestion failed at S3 upload: {e}")
        raise


if __name__ == "__main__":
    main()