{{ config(materialized='table') }}

WITH historical AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        album_name,
        track_id,
        skipped,
        'historical' as source
    FROM historical_plays
    WHERE artist_name is not null
),

current AS (
    SELECT 
        played_at,
        track_name,
        artist_name,
        album_name,
        track_id,
        NULL as skipped,
        'current' as source
    FROM spotify_plays
),

combined AS (
    SELECT * FROM historical
    UNION ALL
    SELECT * FROM current
),

-- Deduplicate by taking the most recent record for each played_at timestamp
deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY played_at 
               ORDER BY source DESC
           ) as rn
    FROM combined
)

SELECT 
    played_at,
    track_name,
    artist_name,
    album_name,
    track_id,
    skipped,
    source
FROM deduplicated
WHERE rn = 1
ORDER BY played_at DESC 