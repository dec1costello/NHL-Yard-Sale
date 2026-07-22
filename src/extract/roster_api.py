"""NHL Roster API - Official NHL API ingestion."""

import uuid
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path

from src.extract.client import NHLAPIClient
from src.config import get_config
from src.utils.logging import get_logger

logger = get_logger(__name__)


class NHLRosterAPI:
    """Roster ingestion from official NHL API."""
    
    def __init__(self):
        """Initialize with API client."""
        self.config = get_config()
        self.client = NHLAPIClient()
    
    def fetch_roster(self, team: str, season: str) -> Dict[str, Any]:
        """Fetch roster JSON from NHL API."""
        logger.info(f"Fetching roster for {team} season {season}")
        return self.client.get_roster_by_season(team, int(season))
    
    def parse_player(self, player_data: Dict[str, Any], team: str, season: str, 
                     run_id: str, ingest_timestamp: str, position_type: str) -> Dict[str, Any]:
        """Parse a single player from API response into our schema."""
        # Extract fields - handle i18n {'default': 'value'} format
        first_name = player_data.get('firstName', {})
        last_name = player_data.get('lastName', {})
        full_name = f"{first_name.get('default', '')} {last_name.get('default', '')}".strip()
        
        # Handle birth date
        birth_date = player_data.get('birthDate', {})
        birth_date_str = birth_date.get('default', '') if isinstance(birth_date, dict) else str(birth_date)
        
        # Handle height/weight
        height = player_data.get('heightInInches', 'N/A')
        weight = player_data.get('weightInPounds', 'N/A')
        
        # Map position codes
        pos_code = player_data.get('positionCode', 'N/A')
        position_map = {
            'C': 'C', 'LW': 'LW', 'RW': 'RW', 
            'L': 'LW', 'R': 'RW',
            'D': 'D', 'G': 'G'
        }
        position = position_map.get(pos_code, pos_code)
        
        # Shoots/Catches
        shoots_catches = player_data.get('shootsCatches', 'N/A')
        
        return {
            # Lineage metadata
            "team": team,
            "season": season,
            "run_id": run_id,
            "ingestion_timestamp": ingest_timestamp,
            "source": "nhl-api",
            
            # Player identity
            "player_id": str(player_data.get('id', '')),
            "player_url": f"https://www.nhl.com/player/{player_data.get('id', '')}",
            
            # Player attributes
            "number": str(player_data.get('sweaterNumber', 'N/A')),
            "player": full_name,
            "position": position,
            "age": None,
            "height": str(height),
            "weight": str(weight),
            "shoots_catches": shoots_catches,
            "experience": None,
            "birth_date": birth_date_str,
            "summary": None,
            
            # Additional NHL API fields
            "birth_city": player_data.get('birthCity', {}).get('default', None),
            "birth_country": player_data.get('birthCountry', ''),
            "nationality": player_data.get('nationality', {}).get('default', None),
            "rookie": player_data.get('rookie', False),
            "current_age": player_data.get('currentAge', None),
            "captain": player_data.get('captain', False),
            "alternate_captain": player_data.get('alternateCaptain', False),
            
            # Stats (not in roster endpoint)
            "games_played": None,
            "goals": None,
            "assists": None,
            "points": None,
            "plus_minus": None,
            "penalty_minutes": None,
            "time_on_ice": None,
            
            # Position group
            "position_group": position_type,
            
            # Raw data for debugging
            "_raw": player_data
        }
    
    def parse_roster(
        self,
        raw: Dict[str, Any],
        team: str,
        season: str,
        run_id: str,
        ingest_timestamp: str,
        raw_json: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Parse API response into structured roster data."""
        players = []
        
        position_groups = [
            ('forwards', 'Forward'),
            ('defensemen', 'Defenseman'),
            ('goalies', 'Goalie')
        ]
        
        for group_key, position_type in position_groups:
            group_data = raw.get(group_key, [])
            
            for player_data in group_data:
                player = self.parse_player(
                    player_data=player_data,
                    team=team,
                    season=season,
                    run_id=run_id,
                    ingest_timestamp=ingest_timestamp,
                    position_type=position_type
                )
                players.append(player)
        
        logger.info(f"Parsed {len(players)} players from API response")
        return players
    
    def get_team_roster(
        self,
        team: str,
        season: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch and parse roster for a single team."""
        raw = self.fetch_roster(team, season)
        raw_json = json.dumps(raw, indent=2)
        
        players = self.parse_roster(
            raw=raw,
            team=team,
            season=season,
            run_id=metadata["run_id"],
            ingest_timestamp=metadata["ingest_timestamp"],
            raw_json=raw_json
        )
        
        return {
            "players": players,
            "raw_json": raw_json,
            "raw_data": raw,
            "player_count": len(players)
        }
    
    def scrape_and_load_team(
        self,
        team: str,
        metadata: Dict[str, Any],
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Fetch, write bronze, and load to DuckDB for a team.
        """
        from src.load.bronze import BronzeLoader
        from src.load.duckdb_loader import DuckDBLoader
        from src.state.state_manager import RosterStateManager
        
        bronze = BronzeLoader()
        duckdb = DuckDBLoader()
        state_manager = RosterStateManager()
        
        # Fetch and parse
        result = self.get_team_roster(
            team=team,
            season=metadata["season"],
            metadata=metadata
        )
        
        players = result["players"]
        raw_json = result["raw_json"]
        
        if not players:
            return {
                "team": team,
                "status": "failed",
                "players": 0,
                "error": "No players found"
            }
        
        # Compute hash
        roster_hash = state_manager.compute_roster_hash(players)
        
        # Check if this is a change
        current_state = state_manager.get_state(team, metadata["season_display"])
        is_change = False
        
        if current_state and current_state.get('current_hash') != roster_hash:
            is_change = True
            logger.info(f"🔄 Change detected for {team}")
        elif force:
            is_change = True
            logger.info(f"🔄 Force loading {team}")
        elif not current_state:
            is_change = True
            logger.info(f"🔄 First time loading {team}")
        
        # Check if bronze exists
        bronze_dir = self.config.bronze_path / f"season={metadata['season_display']}" / f"team={team}"
        bronze_exists = bronze_dir.exists() and list(bronze_dir.glob("*.parquet"))
        
        # Write bronze only if change detected
        wrote_bronze = False
        if force or not bronze_exists or is_change:
            result = bronze.write_roster(
                players=players,
                team=team,
                season=metadata["season_display"],
                roster_hash=roster_hash,
                run_id=metadata["run_id"],
                raw_html=raw_json,
                force=True
            )
            wrote_bronze = bool(result)
        else:
            logger.info(f"⏭️ Skipping bronze write for {team} (no changes)")
        
        # Load to DuckDB (always load, it's append-only)
        rows_loaded = duckdb.load_roster(players)
        
        # Update state with change flag
        state_manager.update_state(
            team=team,
            season=metadata["season_display"],
            current_hash=roster_hash,
            run_id=metadata["run_id"],
            player_count=len(players),
            is_change=is_change
        )
        
        # Log change if it happened
        if is_change and current_state:
            state_manager.log_change(
                run_id=metadata["run_id"],
                team=team,
                season=metadata["season_display"],
                changed=True,
                current_hash=roster_hash,
                previous_hash=current_state.get('current_hash'),
                player_count=len(players)
            )
        
        return {
            "team": team,
            "status": "success",
            "players": len(players),
            "hash": roster_hash,
            "wrote_bronze": wrote_bronze,
            "loaded_to_duckdb": rows_loaded > 0,
            "changed": is_change
        }
    
    def scrape_and_load_all_teams(self) -> Dict[str, Any]:
        """Fetch, write bronze, and load for all teams."""
        teams = self._load_teams()
        config = get_config()
        
        # Use API season format
        season_api = config.current_season_api
        season_display = config.current_season
        
        metadata = {
            "run_id": str(uuid.uuid4()),
            "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
            "season": season_api,  # API format for fetching
            "season_display": season_display,  # Display format for storage
        }
        
        results = {}
        total_players = 0
        teams_written = 0
        
        for i, team in enumerate(teams, 1):
            logger.info(f"[{i}/{len(teams)}] Processing {team}")
            
            try:
                result = self.scrape_and_load_team(team, metadata)
                results[team] = result
                total_players += result.get("players", 0)
                if result.get("wrote_bronze"):
                    teams_written += 1
                logger.info(
                    f"✅ {team}: {result['players']} players, "
                    f"bronze_written={result.get('wrote_bronze', False)}, "
                    f"changed={result.get('changed', False)}"
                )
            except Exception as e:
                logger.error(f"Failed to process {team}: {e}")
                results[team] = {
                    "team": team,
                    "status": "error",
                    "error": str(e)
                }
        
        return {
            "teams": results,
            "summary": {
                "total_teams": len(teams),
                "total_players": total_players,
                "teams_with_bronze": teams_written
            }
        }
    
    def _load_teams(self) -> List[str]:
        """Load team abbreviations from configuration."""
        teams_file = self.config.teams_file
        if teams_file and teams_file.exists():
            with open(teams_file, "r") as f:
                return [line.strip() for line in f if line.strip()]
        
        return [
            "ANA", "BOS", "BUF", "CAR", "CGY", "CHI", "COL", "CBJ",
            "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH",
            "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA",
            "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WPG", "WSH"
        ]