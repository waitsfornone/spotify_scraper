# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

A Python-based data pipeline that scrapes Spotify play history, stores raw data in Backblaze B2, loads it into MotherDuck (DuckDB cloud), and transforms it using dbt. The project tracks both current plays (via Spotify API) and historical plays (from Spotify data exports).

## Environment Setup

### Required Environment Variables
- `MOTHERDUCK_TOKEN`: MotherDuck authentication token
- `B2_SPOTIFY_KEY_ID`: Backblaze B2 key ID
- `B2_APP_KEY`: Backblaze B2 application key
- `SPOTIPY_CLIENT_ID`: Spotify API client ID
- `SPOTIPY_CLIENT_SECRET`: Spotify API client secret
- `SPOTIPY_REDIRECT_URI`: Spotify OAuth redirect URI

### Python Environment
```bash
# Install dependencies
pip install -r requirements.txt

# Python 3.11+ required (see .python-version)
```

## Common Commands

### Running the Scraper
```bash
# Normal run (incremental scrape from last timestamp)
python scrape_plays.py

# Restart from beginning timestamp
python scrape_plays.py --restart

# Run via shell script (includes environment setup)
./run_spotify_scraper.sh
```

### Track Enrichment
```bash
# Enrich new tracks (fetches detailed track info from Spotify)
python spotify_track_enrichment.py

# Restart track enrichment (clears bucket and re-fetches all)
python spotify_track_enrichment.py --restart

# Parse enriched tracks to database
python spotify_track_parser.py
```

### Historical Data Upload
```bash
# Upload historical play data from local JSON files
python upload_historical_plays.py
```

### dbt Commands
```bash
# Navigate to dbt project
cd dbt/spotify_plays

# Run all models
dbt run

# Run specific model
dbt run --select all_plays

# Test data quality
dbt test

# Build docs and serve
dbt docs generate
dbt docs serve
```

## Architecture

### Data Flow Pipeline

1. **Extraction Layer** (`scrape_plays.py`, `spotify_scraper.py`)
   - Fetches recent plays from Spotify API using cursor-based pagination
   - Tracks progress via timestamps in `current_timestamp.txt` and `starting_timestamp.txt`
   - Uploads raw JSON to Backblaze B2 (`spotify-plays-raw` bucket)

2. **Loading Layer** (`spotify_parser.py`)
   - Reads JSON files from B2
   - Parses and loads into MotherDuck `spotify_plays` table
   - Drops and recreates table on each run (full refresh pattern)

3. **Enrichment Pipeline** (`spotify_track_enrichment.py`, `spotify_track_parser.py`)
   - Fetches detailed track metadata from Spotify API
   - Stores in separate B2 bucket (`spotify-tracks-raw`)
   - Loads into `all_tracks` table with deduplication

4. **Historical Data** (`upload_historical_plays.py`)
   - Processes Spotify's official data export JSON files
   - Loads into `historical_plays` table
   - Handles multiple timestamp and field name formats

5. **Transformation Layer** (dbt)
   - Combines historical and current plays into unified `all_plays` table
   - Deduplicates by `played_at` timestamp
   - Materialized as table for performance

### Key Tables in MotherDuck

- `spotify_plays`: Current plays from Spotify API (raw)
- `historical_plays`: Historical plays from data exports
- `all_plays`: Unified, deduplicated view (dbt model)
- `all_tracks`: Enriched track metadata

### Storage Architecture

- **Backblaze B2 Buckets:**
  - `spotify-plays-raw`: Raw play history JSON files
  - `spotify-tracks-raw`: Detailed track metadata JSON files
- **MotherDuck Database:** `my_db` (cloud DuckDB instance)

## Code Organization

### Core Scripts
- `scrape_plays.py`: Main orchestrator for scraping pipeline
- `spotify_scraper.py`: Spotify API interaction logic with cursor pagination
- `spotify_parser.py`: JSON to database loader for plays
- `spotify_track_enrichment.py`: Fetches detailed track info
- `spotify_track_parser.py`: Loads track metadata to database
- `upload_historical_plays.py`: One-time historical data loader
- `b2_utils.py`: Backblaze B2 client initialization

### dbt Project Structure
- `dbt/spotify_plays/`: dbt project root
- `dbt/spotify_plays/models/all_plays.sql`: Main transformation model
- `dbt/spotify_plays/profiles.yml`: MotherDuck connection config

## Important Implementation Details

### Cursor-Based Pagination
The scraper uses cursor-based pagination with `before` timestamps. Each run:
1. Reads last cursor from `current_timestamp.txt`
2. Fetches tracks backward in time using Spotify's `recently_played` endpoint
3. Stops when reaching `starting_timestamp` or size limit (1GB default)
4. Updates cursor for next run

### Deduplication Strategy
The `all_plays` dbt model deduplicates using:
```sql
ROW_NUMBER() OVER (PARTITION BY played_at ORDER BY source DESC)
```
This prefers 'historical' source over 'current' when timestamps match.

### Rate Limiting
Track enrichment includes `sleep(0.1)` between API calls to avoid rate limits.

### Data Freshness
The scraper is designed for scheduled execution (e.g., cron job). Each run is incremental, processing only new plays since last cursor.

## Testing

### Manual Testing
```bash
# Test Spotify API connection
python -c "from spotify_scraper import get_recent_tracks; print('API connected')"

# Test B2 connection
python -c "from b2_utils import get_b2_resource; b2_resource = get_b2_resource(); print('B2 connected')"

# Test MotherDuck connection
python -c "import duckdb, os; con = duckdb.connect(f\"md:my_db?motherduck_token={os.getenv('MOTHERDUCK_TOKEN')}\"); print('MotherDuck connected'); con.close()"

# Run dbt tests
cd dbt/spotify_plays && dbt test
```

## Development Workflow

### Adding New Data Sources
1. Create new scraper script following pattern in `spotify_scraper.py`
2. Upload raw data to B2 bucket
3. Create parser script following `spotify_parser.py` pattern
4. Add dbt model to combine with existing data
5. Update this documentation

### Modifying Transformations
1. Edit SQL in `dbt/spotify_plays/models/`
2. Run `dbt run --select <model_name>` to test
3. Use `dbt test` to validate data quality
4. Deploy by committing changes

### Debugging Failed Runs
- Check timestamp files: `current_timestamp.txt`, `starting_timestamp.txt`
- Verify B2 bucket contents for uploaded files
- Query MotherDuck directly to inspect loaded data
- Review script output for API errors or parsing issues
