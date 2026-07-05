"""State management using DuckDB instead of Parquet."""

import hashlib
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import duckdb

from src.config import get_config
from src.utils.logging import get_logger

logger = get_logger(__name__)


class RosterStateManager:
    """Manages ingestion state using DuckDB."""
    
    def __init__(self):
        """Initialize state manager with DuckDB connection."""
        self.config = get_config()
        self.conn = self._get_connection()
        self._ensure_state_tables()
    
    def _get_connection(self):
        """Get DuckDB connection."""
        return duckdb.connect(str(self.config.warehouse_path))
    
    def _ensure_state_tables(self):
        """Ensure state tables exist."""
        self.conn.execute("""
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
        
        self.conn.execute("""
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
    
    def get_state(self, team: str, season: str) -> Optional[Dict[str, Any]]:
        """Get current state for a team."""
        result = self.conn.execute("""
            SELECT team, season, current_hash, last_ingested, last_run_id, player_count
            FROM roster_state
            WHERE team = ? AND season = ?
        """, [team, season]).fetchone()
        
        if result:
            return {
                "team": result[0],
                "season": result[1],
                "current_hash": result[2],
                "last_ingested": result[3],
                "last_run_id": result[4],
                "player_count": result[5]
            }
        return None
    
    def update_state(self, team: str, season: str, current_hash: str, 
                    run_id: str, player_count: int) -> None:
        """Update state for a team."""
        self.conn.execute("""
            INSERT OR REPLACE INTO roster_state 
            (team, season, current_hash, last_ingested, last_run_id, player_count, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP)
        """, [team, season, current_hash, run_id, player_count])
        
        logger.info(f"Updated state for {team} season {season}")
    
    def log_change(self, run_id: str, team: str, season: str, changed: bool,
                   current_hash: str, previous_hash: Optional[str], player_count: int):
        """Log a change detection event."""
        self.conn.execute("""
            INSERT INTO roster_changes_log 
            (run_id, team, season, changed, current_hash, previous_hash, player_count, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [run_id, team, season, changed, current_hash, previous_hash, player_count])
    
    def get_all_states(self) -> List[Dict[str, Any]]:
        """Get all current states."""
        result = self.conn.execute("""
            SELECT team, season, current_hash, last_ingested, last_run_id, player_count
            FROM roster_state
            ORDER BY team
        """).fetchall()
        
        return [
            {
                "team": r[0],
                "season": r[1],
                "current_hash": r[2],
                "last_ingested": r[3],
                "last_run_id": r[4],
                "player_count": r[5]
            }
            for r in result
        ]
    
    @staticmethod
    def compute_roster_hash(players: List[Dict[str, Any]]) -> str:
        """Compute a deterministic hash of the roster."""
        # Sort by player name for consistency
        sorted_players = sorted(
            players,
            key=lambda x: (
                x.get("player", ""),
                x.get("number", ""),
                x.get("position", "")
            )
        )
        
        # Create a tuple of key identifiers
        identifiers = []
        for p in sorted_players:
            name = p.get("player", "").strip()
            number = p.get("number", "").strip()
            position = p.get("position", "").strip()
            
            if name:
                identifiers.append(f"{name}|{number}|{position}")
        
        hash_input = "||".join(identifiers)
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def close(self):
        """Close database connection."""
        self.conn.close()