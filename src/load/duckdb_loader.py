"""DuckDB loading operations for raw roster data."""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import duckdb
import pandas as pd

from src.config import get_config
from src.utils.logging import get_logger

logger = get_logger(__name__)


class DuckDBLoader:
    """Handle loading data into DuckDB."""
    
    def __init__(self):
        """Initialize DuckDB loader with configuration."""
        self.config = get_config()
        self.warehouse_path = self.config.warehouse_path
        self._ensure_schema()
    
    def _ensure_schema(self) -> None:
        """Ensure database schema exists."""
        conn = self._get_connection()
        
        try:
            # Raw rosters table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_rosters (
                    team VARCHAR,
                    season VARCHAR,
                    run_id VARCHAR,
                    ingestion_timestamp TIMESTAMP,
                    source VARCHAR,
                    player_id VARCHAR,
                    player_url VARCHAR,
                    number VARCHAR,
                    player VARCHAR,
                    position VARCHAR,
                    age VARCHAR,
                    height VARCHAR,
                    weight VARCHAR,
                    shoots_catches VARCHAR,
                    experience VARCHAR,
                    birth_date VARCHAR,
                    summary VARCHAR,
                    roster_hash VARCHAR,
                    bronze_loaded_at TIMESTAMP,
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # State table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS roster_state (
                    team VARCHAR,
                    season VARCHAR,
                    current_hash VARCHAR,
                    last_ingested TIMESTAMP,
                    last_run_id VARCHAR,
                    player_count INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (team, season)
                )
            """)
            
            # Change log table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS roster_changes_log (
                    run_id VARCHAR,
                    team VARCHAR,
                    season VARCHAR,
                    changed BOOLEAN,
                    current_hash VARCHAR,
                    previous_hash VARCHAR,
                    player_count INTEGER,
                    timestamp TIMESTAMP
                )
            """)
            
            # Create indexes
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_rosters_team_season 
                ON raw_rosters(team, season)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_rosters_run_id 
                ON raw_rosters(run_id)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_rosters_player_id 
                ON raw_rosters(player_id)
            """)
            
            logger.info("✅ Database schema verified")
            
        finally:
            conn.close()
    
    def load_roster(self, players: List[Dict[str, Any]]) -> int:
        """Load roster data into DuckDB with deduplication."""
        if not players:
            return 0
        
        conn = self._get_connection()
        
        try:
            df = pd.DataFrame(players)
            
            # Define expected columns
            columns = [
                "team", "season", "run_id", "ingestion_timestamp",
                "source", "player_id", "player_url", "number", 
                "player", "position", "age", "height", "weight",
                "shoots_catches", "experience", "birth_date", 
                "summary", "roster_hash", "bronze_loaded_at"
            ]
            
            # Only keep columns that exist in the DataFrame
            existing_columns = [col for col in columns if col in df.columns]
            df = df[existing_columns]
            
            # Register and insert
            conn.register("df_temp", df)
            col_names = ", ".join(existing_columns)
            
            conn.execute(f"""
                INSERT INTO raw_rosters ({col_names})
                SELECT {col_names} FROM df_temp
            """)
            
            row_count = len(df)
            logger.info(f"✅ Loaded {row_count} roster records into DuckDB")
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to load roster: {e}")
            raise
        finally:
            conn.close()
    
    def load_state(self, team: str, season: str, roster_hash: str, 
                   run_id: str, player_count: int) -> None:
        """Update state for a team."""
        conn = self._get_connection()
        
        try:
            conn.execute("""
                INSERT OR REPLACE INTO roster_state 
                (team, season, current_hash, last_ingested, last_run_id, player_count, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP)
            """, [team, season, roster_hash, run_id, player_count])
            
            logger.info(f"✅ Updated state for {team}")
            
        finally:
            conn.close()
    
    def log_change(self, run_id: str, team: str, season: str, changed: bool,
                   current_hash: str, previous_hash: Optional[str], 
                   player_count: int) -> None:
        """Log a change detection event."""
        conn = self._get_connection()
        
        try:
            conn.execute("""
                INSERT INTO roster_changes_log 
                (run_id, team, season, changed, current_hash, previous_hash, player_count, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [run_id, team, season, changed, current_hash, previous_hash, player_count])
            
        finally:
            conn.close()
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        conn = self._get_connection()
        try:
            return conn.execute(sql).df()
        finally:
            conn.close()
    
    def _get_connection(self):
        """Get DuckDB connection, creating directory if needed."""
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.warehouse_path))
