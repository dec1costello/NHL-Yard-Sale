"""Bronze layer operations with change detection and DuckDB loading."""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import get_config
from src.state.state_manager import RosterStateManager
from src.load.duckdb_loader import DuckDBLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


class BronzeLoader:
    """Handle bronze layer storage operations with change detection."""
    
    def __init__(self):
        """Initialize bronze loader with configuration."""
        self.config = get_config()
        self.bronze_path = self.config.bronze_path
        self.state_manager = RosterStateManager()
        self.duckdb = DuckDBLoader()
    
    def _check_bronze_exists(self, team: str, season: str) -> bool:
        """Check if bronze files exist for a team."""
        team_dir = self.bronze_path / f"season={season}" / f"team={team}"
        return team_dir.exists() and list(team_dir.glob("*.parquet"))
    
    def _get_bronze_hash(self, team: str, season: str) -> Optional[str]:
        """Get the hash from the latest bronze file."""
        team_dir = self.bronze_path / f"season={season}" / f"team={team}"
        if not team_dir.exists():
            return None
        
        parquet_files = list(team_dir.glob("*.parquet"))
        if not parquet_files:
            return None
        
        # Read the latest parquet file
        latest = sorted(parquet_files)[-1]
        try:
            df = pd.read_parquet(latest)
            if 'roster_hash' in df.columns:
                return df['roster_hash'].iloc[0]
        except Exception as e:
            logger.warning(f"Could not read hash from {latest}: {e}")
        
        return None
    
    def write_roster(
        self,
        players: List[Dict[str, Any]],
        team: str,
        season: str,
        roster_hash: str,
        run_id: str,
        raw_html: Optional[str] = None,
        force: bool = False,
        load_to_duckdb: bool = True
    ) -> Dict[str, Any]:
        """
        Write roster to bronze layer with change detection.
        
        Args:
            players: List of player dictionaries
            team: Team abbreviation
            season: NHL season year
            roster_hash: Computed hash of roster
            run_id: Unique run identifier
            raw_html: Optional raw HTML for debugging
            force: Force write even if no changes
            load_to_duckdb: Load to DuckDB after writing
            
        Returns:
            Dict with results including paths and status
        """
        if not players:
            logger.warning(f"No players to write for {team}")
            return {"status": "skipped", "reason": "No players"}
        
        result = {
            "team": team,
            "season": season,
            "run_id": run_id,
            "player_count": len(players),
            "wrote_bronze": False,
            "loaded_to_duckdb": False,
            "paths": {}
        }
        
        # Check if bronze exists
        bronze_exists = self._check_bronze_exists(team, season)
        bronze_hash = self._get_bronze_hash(team, season) if bronze_exists else None
        
        # Determine if we should write
        should_write = force or not bronze_exists or (bronze_hash != roster_hash)
        
        if not should_write:
            logger.info(f"⏭️ Skipping bronze write for {team} (no changes)")
            result["status"] = "skipped"
            result["reason"] = "No changes detected"
            return result
        
        # Create partitioned directory structure
        team_dir = self.bronze_path / f"season={season}" / f"team={team}"
        team_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Write structured data as Parquet
        parquet_path = team_dir / f"run_{run_id}.parquet"
        
        # Add metadata to each player record
        enriched_players = []
        for player in players:
            player_copy = player.copy()
            player_copy["roster_hash"] = roster_hash
            player_copy["bronze_loaded_at"] = datetime.now().isoformat()
            enriched_players.append(player_copy)
        
        # Convert to DataFrame and write
        df = pd.DataFrame(enriched_players)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path, compression="snappy")
        result["paths"]["parquet"] = str(parquet_path)
        logger.info(f"✅ Wrote {len(players)} players to {parquet_path}")
        
        # 2. Write raw HTML (optional)
        if raw_html:
            raw_str = raw_html.strip()
            # Check if it's JSON (starts with { or [)
            if raw_str.startswith('{') or raw_str.startswith('['):
                # It's JSON - save as .json
                raw_path = team_dir / f"run_{run_id}_raw.json"
                with open(raw_path, "w", encoding="utf-8") as f:
                    f.write(raw_str)
                result["paths"]["json"] = str(raw_path)
                logger.info(f"✅ Saved raw JSON to {raw_path}")
            else:
                # It's HTML - save as .html
                raw_path = team_dir / f"run_{run_id}_raw.html"
                with open(raw_path, "w", encoding="utf-8") as f:
                    f.write(raw_str)
                result["paths"]["html"] = str(raw_path)
                logger.info(f"✅ Saved raw HTML to {raw_path}")
        
        # 3. Write metadata
        metadata = {
            "team": team,
            "season": season,
            "run_id": run_id,
            "roster_hash": roster_hash,
            "player_count": len(players),
            "timestamp": datetime.now().isoformat(),
            "source": "nhl-api",  # Changed from "hockey-reference"
            "files": result["paths"]
        }
        
        metadata_path = team_dir / f"run_{run_id}_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        result["paths"]["metadata"] = str(metadata_path)
        logger.info(f"✅ Wrote metadata to {metadata_path}")
        
        result["wrote_bronze"] = True
        result["status"] = "success"
        
        # 4. Load to DuckDB
        if load_to_duckdb:
            try:
                rows = self.duckdb.load_roster(players)
                result["loaded_to_duckdb"] = rows > 0
                logger.info(f"✅ Loaded {rows} rows to DuckDB for {team}")
            except Exception as e:
                logger.error(f"Failed to load {team} to DuckDB: {e}")
                result["loaded_to_duckdb"] = False
        
        return result
    
    def get_latest_run(self, team: str, season: str) -> Optional[Dict[str, Any]]:
        """Get the latest run for a team."""
        team_dir = self.bronze_path / f"season={season}" / f"team={team}"
        
        if not team_dir.exists():
            return None
        
        metadata_files = list(team_dir.glob("run_*_metadata.json"))
        
        if not metadata_files:
            return None
        
        latest = sorted(metadata_files)[-1]
        
        with open(latest, "r") as f:
            return json.load(f)
    
    def list_runs(self, team: str, season: str) -> List[Dict[str, Any]]:
        """List all runs for a team."""
        team_dir = self.bronze_path / f"season={season}" / f"team={team}"
        
        if not team_dir.exists():
            return []
        
        metadata_files = list(team_dir.glob("run_*_metadata.json"))
        runs = []
        
        for file in sorted(metadata_files):
            with open(file, "r") as f:
                runs.append(json.load(f))
        
        return runs
