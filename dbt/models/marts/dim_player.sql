WITH player_attributes AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY player_id) AS player_id_sk,
        -- If player_id is NULL, create one from the player name
        COALESCE(player_id, 'unknown_' || REPLACE(full_name, ' ', '_')) AS player_id,
        full_name,
        team,
        season,
        number,
        position,
        position_group,
        shoots_catches,
        height,
        weight,
        birth_date,
        ingestion_timestamp
    FROM main_staging.stg_rosters
),

-- Generate hash for change detection
with_hash AS (
    SELECT
        *,
        MD5(
            COALESCE(team, '') || '|' ||
            COALESCE(number, '') || '|' ||
            COALESCE(position, '') || '|' ||
            COALESCE(shoots_catches, '') || '|' ||
            COALESCE(height, '') || '|' ||
            COALESCE(weight, '')
        ) AS hash_diff
    FROM player_attributes
),

-- Determine effective dates
with_effective_dates AS (
    SELECT
        *,
        ingestion_timestamp AS effective_date,
        LEAD(ingestion_timestamp) OVER (
            PARTITION BY player_id 
            ORDER BY ingestion_timestamp
        ) AS next_effective_date,
        ROW_NUMBER() OVER (
            PARTITION BY player_id 
            ORDER BY ingestion_timestamp DESC
        ) AS latest_rank
    FROM with_hash
)

SELECT
    ROW_NUMBER() OVER (ORDER BY player_id, effective_date) AS player_sk,
    player_id,
    player_id_sk AS player_nk,
    full_name,
    SPLIT_PART(full_name, ' ', 1) AS first_name,
    SPLIT_PART(full_name, ' ', -1) AS last_name,
    team,
    number,
    position,
    position_group,
    shoots_catches,
    height,
    weight,
    birth_date,
    effective_date,
    CASE 
        WHEN latest_rank = 1 THEN '9999-12-31'::TIMESTAMP
        ELSE next_effective_date
    END AS end_date,
    latest_rank = 1 AS is_current,
    hash_diff
FROM with_effective_dates