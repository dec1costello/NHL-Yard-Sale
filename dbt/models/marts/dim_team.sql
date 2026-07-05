-- models/marts/dim_team.sql
-- Team dimension table

WITH team_data AS (
    SELECT DISTINCT
        team AS team_abbrev,
        -- Map abbreviations to full names
        CASE team
            WHEN 'ANA' THEN 'Anaheim Ducks'
            WHEN 'BOS' THEN 'Boston Bruins'
            WHEN 'BUF' THEN 'Buffalo Sabres'
            WHEN 'CAR' THEN 'Carolina Hurricanes'
            WHEN 'CGY' THEN 'Calgary Flames'
            WHEN 'CHI' THEN 'Chicago Blackhawks'
            WHEN 'COL' THEN 'Colorado Avalanche'
            WHEN 'CBJ' THEN 'Columbus Blue Jackets'
            WHEN 'DAL' THEN 'Dallas Stars'
            WHEN 'DET' THEN 'Detroit Red Wings'
            WHEN 'EDM' THEN 'Edmonton Oilers'
            WHEN 'FLA' THEN 'Florida Panthers'
            WHEN 'LAK' THEN 'Los Angeles Kings'
            WHEN 'MIN' THEN 'Minnesota Wild'
            WHEN 'MTL' THEN 'Montreal Canadiens'
            WHEN 'NSH' THEN 'Nashville Predators'
            WHEN 'NJD' THEN 'New Jersey Devils'
            WHEN 'NYI' THEN 'New York Islanders'
            WHEN 'NYR' THEN 'New York Rangers'
            WHEN 'OTT' THEN 'Ottawa Senators'
            WHEN 'PHI' THEN 'Philadelphia Flyers'
            WHEN 'PIT' THEN 'Pittsburgh Penguins'
            WHEN 'SJS' THEN 'San Jose Sharks'
            WHEN 'SEA' THEN 'Seattle Kraken'
            WHEN 'STL' THEN 'St. Louis Blues'
            WHEN 'TBL' THEN 'Tampa Bay Lightning'
            WHEN 'TOR' THEN 'Toronto Maple Leafs'
            WHEN 'UTA' THEN 'Utah Hockey Club'
            WHEN 'VAN' THEN 'Vancouver Canucks'
            WHEN 'VGK' THEN 'Vegas Golden Knights'
            WHEN 'WPG' THEN 'Winnipeg Jets'
            WHEN 'WSH' THEN 'Washington Capitals'
            ELSE team
        END AS team_name,
        1 AS active_flag
    FROM raw_rosters
)

SELECT
    ROW_NUMBER() OVER (ORDER BY team_abbrev) AS team_sk,
    team_abbrev,
    team_name,
    active_flag,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM team_data