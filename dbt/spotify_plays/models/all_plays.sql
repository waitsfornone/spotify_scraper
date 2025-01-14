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
)

SELECT * FROM historical
UNION ALL
SELECT * FROM current 