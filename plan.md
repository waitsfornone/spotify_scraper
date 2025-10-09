# Plan: Local DuckDB Scraper with CSV Upload to B2

## Overview
Transform the current architecture from **Spotify API → B2 (JSON) → MotherDuck** to **Spotify API → Local DuckDB → B2 (CSV)**.

---

## Phase 1: Create New Local DuckDB Scraper Script

**File to create:** `scrape_plays_local.py`

**Changes needed:**
1. Remove B2 JSON upload step
2. Write directly to local DuckDB file
3. Use incremental inserts instead of full refresh
4. Export to CSV after scraping
5. Upload CSV to B2
6. Clean up local files (optional)

**Key implementation details:**
- Database file: `./data/spotify_plays.duckdb` (local file)
- Create table with `IF NOT EXISTS` for incremental loading
- Use `INSERT OR IGNORE` pattern to prevent duplicates (based on `played_at` + `track_id`)
- Export full table to CSV after each run
- Upload timestamped CSV to B2

---

## Phase 2: Create Helper Function for CSV Export & Upload

**File to create:** `local_db_utils.py`

**Functions needed:**
1. `init_local_db()` - Create/connect to local DuckDB
2. `insert_plays_to_local_db()` - Insert new plays with deduplication
3. `export_to_csv()` - Export table to CSV file
4. `upload_csv_to_b2()` - Upload CSV to B2 bucket

---

## Phase 3: Update or Create Configuration

**Considerations:**
- Where to store local DuckDB file (suggested: `./data/` directory)
- CSV export location (suggested: `./exports/` directory)
- B2 bucket name for CSVs (could be new bucket or existing one)
- Whether to keep or delete local CSVs after upload

---

## Phase 4: Testing Strategy

1. **Test with small dataset** - Use `--restart` with a recent timestamp
2. **Verify DuckDB contents** - Query local database to confirm data
3. **Verify CSV generation** - Check CSV file format and contents
4. **Verify B2 upload** - Confirm CSV appears in B2 bucket
5. **Test incremental runs** - Run multiple times to ensure no duplicates

---

## Detailed Implementation Plan

### Step 1: Create directory structure
```bash
mkdir -p data exports
```

### Step 2: Create `local_db_utils.py`
**Purpose:** Utility functions for local DuckDB operations

**Key functions:**
- Connect to local DuckDB file
- Create table schema
- Insert with deduplication
- Export to CSV
- Upload to B2

### Step 3: Create `scrape_plays_local.py`
**Purpose:** Main script for local scraping workflow

**Flow:**
1. Read timestamp file
2. Call `spotify_scraper.get_recent_tracks()`
3. Insert plays into local DuckDB
4. Update timestamp file
5. Export DuckDB table to CSV
6. Upload CSV to B2
7. Optionally clean up local CSV

### Step 4: Add command-line options
```bash
--restart         # Start from beginning timestamp
--no-upload       # Skip B2 upload (for testing)
--keep-csv        # Don't delete CSV after upload
--db-path         # Custom DuckDB file path
```

---

## Database Schema Changes

**Current approach:** Drop and recreate table each time (MotherDuck)
**New approach:** Persistent local table with incremental inserts

**Table schema:**
```sql
CREATE TABLE IF NOT EXISTS spotify_plays (
    played_at TIMESTAMP,
    track_name VARCHAR,
    artist_name VARCHAR,
    album_name VARCHAR,
    track_id VARCHAR,
    artist_id VARCHAR,
    album_id VARCHAR,
    duration_ms INTEGER,
    PRIMARY KEY (played_at, track_id)
)
```

**Unique constraint:** Composite of `played_at` + `track_id`

---

## File Naming Conventions

**Local DuckDB:** `data/spotify_plays.duckdb`
**CSV Export:** `exports/spotify_plays_YYYYMMDD_HHMMSS.csv`
**B2 CSV:** `spotify-plays-csv/spotify_plays_YYYYMMDD_HHMMSS.csv`

---

## Environment Variables Needed

**Existing (still required):**
- `SPOTIPY_CLIENT_ID`
- `SPOTIPY_CLIENT_SECRET`
- `SPOTIPY_REDIRECT_URI`
- `B2_SPOTIFY_KEY_ID`
- `B2_APP_KEY`

**No longer needed for local version:**
- `MOTHERDUCK_TOKEN` ✗

---

## Advantages of This Approach

1. **No cloud database costs** - Local DuckDB is free
2. **Faster development** - No network latency
3. **Data portability** - CSV format is universal
4. **Backup friendly** - Both DuckDB file and CSV serve as backups
5. **Easy analysis** - Can query local DuckDB with any tool
6. **Incremental loading** - No need to reload all data each time

---

## Migration Path

**Option A: Parallel systems** (Recommended for testing)
- Keep existing scripts unchanged
- Create new `scrape_plays_local.py` alongside
- Test thoroughly before switching

**Option B: Full replacement**
- Replace current scripts
- Archive old scripts
- Update documentation

---

## Optional Enhancements

1. **Data validation** - Check for gaps in timestamps
2. **CSV compression** - Use gzip before upload to B2
3. **Logging** - Add structured logging to file
4. **Error handling** - Retry logic for B2 uploads
5. **Metrics** - Track number of new plays per run

---

## Testing Checklist

- [ ] Create `data/` and `exports/` directories
- [ ] Implement `local_db_utils.py` functions
- [ ] Implement `scrape_plays_local.py` main script
- [ ] Test local DuckDB connection and table creation
- [ ] Test Spotify API scraping
- [ ] Test data insertion with deduplication
- [ ] Test CSV export
- [ ] Test B2 upload
- [ ] Test incremental run (no duplicates)
- [ ] Verify data integrity (compare with existing data)
- [ ] Update WARP.md documentation

---

## Implementation Timeline

**Phase 1 (Core Functionality):** 1-2 hours
- Create helper utilities
- Basic scraper script
- CSV export and upload

**Phase 2 (Testing & Refinement):** 30-60 minutes
- Test with small dataset
- Fix bugs
- Add error handling

**Phase 3 (Documentation & Cleanup):** 30 minutes
- Update documentation
- Add comments
- Clean up code

**Total estimated time:** 2-3 hours

---

## Rollback Plan

If issues arise:
1. Keep existing scripts untouched during testing
2. Local DuckDB file can be deleted without affecting production
3. B2 CSV bucket is separate from existing JSON bucket
4. Easy to switch back to original workflow

---

## Success Criteria

✅ Local DuckDB file created and populated
✅ CSV exported with correct format
✅ CSV uploaded to B2 successfully
✅ No duplicate records on incremental runs
✅ Timestamp tracking works correctly
✅ Script handles errors gracefully
✅ Documentation updated
