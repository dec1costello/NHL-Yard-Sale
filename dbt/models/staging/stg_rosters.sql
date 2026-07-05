-- models/staging/stg_rosters.sql
-- Staging model for raw roster data

WITH source AS (
    SELECT
        team,
        season,
        run_id,
        ingestion_timestamp,
        source,
        player_id,
        player_url,
        number,
        player AS full_name,
        position,
        CASE 
            WHEN position IN ('C', 'LW', 'RW') THEN 'Forward'
            WHEN position IN ('F', 'RW/C') THEN 'Forward'
            WHEN position = 'D' THEN 'Defenseman'
            WHEN position = 'G' THEN 'Goalie'
            ELSE 'Unknown'
        END AS position_group,
        age,
        height,
        weight,
        shoots_catches,
        experience,
        birth_date,
        summary,
        roster_hash,
        bronze_loaded_at,
        load_timestamp
    FROM raw_rosters
),

-- Get the latest data for each player per team
latest_season AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team, player_id 
            ORDER BY ingestion_timestamp DESC
        ) as rn
    FROM source
    WHERE season = '2025'
)

SELECT
    team,
    season,
    -- If player_id is NULL, use the player name as a fallback
    COALESCE(player_id, 'unknown_' || REPLACE(full_name, ' ', '_')) AS player_id,
    player_url,
    number,
    full_name,
    position,
    position_group,
    age,
    height,
    weight,
    shoots_catches,
    experience,
    birth_date,
    roster_hash,
    ingestion_timestamp
FROM latest_season
WHERE rn = 1