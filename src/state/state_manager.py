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
        """Ensure state tables exist with proper schema."""
        # State table with clean schema
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS roster_state (
                team VARCHAR,
                season VARCHAR,
                current_hash VARCHAR,
                last_polled TIMESTAMP,
                last_changed TIMESTAMP,
                last_run_id VARCHAR,
                player_count INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (team, season)
            )
        """)
        
        # Safe migration: rename last_ingested to last_polled if it exists
        try:
            # Check if last_ingested exists
            cols = self.conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'roster_state' 
                AND column_name = 'last_ingested'
            """).fetchall()
            
            if cols:
                logger.info("🔄 Migrating: last_ingested → last_polled")
                self.conn.execute("""
                    ALTER TABLE roster_state 
                    RENAME COLUMN last_ingested TO last_polled
                """)
            
            # Add last_changed if it doesn't exist
            cols = self.conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'roster_state' 
                AND column_name = 'last_changed'
            """).fetchall()
            
            if not cols:
                logger.info("➕ Adding last_changed column")
                self.conn.execute("""
                    ALTER TABLE roster_state 
                    ADD COLUMN last_changed TIMESTAMP
                """)
                
                # Backfill last_changed from last_polled
                self.conn.execute("""
                    UPDATE roster_state 
                    SET last_changed = last_polled 
                    WHERE last_changed IS NULL
                """)
            
            # Drop last_checked if it exists (redundant)
            cols = self.conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'roster_state' 
                AND column_name = 'last_checked'
            """).fetchall()
            
            if cols:
                logger.info("🗑️  Dropping redundant last_checked column")
                self.conn.execute("""
                    ALTER TABLE roster_state 
                    DROP COLUMN last_checked
                """)
                
        except Exception as e:
            logger.warning(f"Migration note: {e} (table may already be migrated)")
        
        # Change log table
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
        
        # Monitoring view
        self.conn.execute("""
            CREATE OR REPLACE VIEW v_roster_monitoring AS
            SELECT 
                team,
                season,
                last_polled AS last_checked,
                last_changed,
                player_count,
                CASE 
                    WHEN last_polled = last_changed THEN '⚠️  No changes since last check'
                    WHEN last_polled > last_changed THEN '✅ Checked after change'
                    WHEN last_polled < last_changed THEN '🔴 Data inconsistency!'
                    ELSE '❓ Unknown'
                END as status,
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_polled)) / 3600 AS hours_since_check,
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_changed)) / 86400 AS days_since_change
            FROM roster_state
        """)
    
    def get_state(self, team: str, season: str) -> Optional[Dict[str, Any]]:
        """Get current state for a team."""
        result = self.conn.execute("""
            SELECT team, season, current_hash, last_polled, last_changed, last_run_id, player_count
            FROM roster_state
            WHERE team = ? AND season = ?
        """, [team, season]).fetchone()
        
        if result:
            return {
                "team": result[0],
                "season": result[1],
                "current_hash": result[2],
                "last_polled": result[3],
                "last_changed": result[4],
                "last_run_id": result[5],
                "player_count": result[6]
            }
        return None
    
    def update_state(self, team: str, season: str, current_hash: str, 
                    run_id: str, player_count: int, is_change: bool = False) -> None:
        """
        Update state for a team.
        
        Args:
            team: Team abbreviation
            season: NHL season
            current_hash: Current roster hash
            run_id: Pipeline run ID
            player_count: Number of players
            is_change: Whether the roster actually changed
        """
        if is_change:
            # Data changed - update both timestamps
            self.conn.execute("""
                INSERT OR REPLACE INTO roster_state 
                (team, season, current_hash, last_polled, last_changed, last_run_id, player_count, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP)
            """, [team, season, current_hash, run_id, player_count])
            logger.info(f"✅ State updated for {team}: CHANGE DETECTED")
        else:
            # No change - only update last_polled, preserve last_changed
            self.conn.execute("""
                INSERT OR REPLACE INTO roster_state 
                (team, season, current_hash, last_polled, last_changed, last_run_id, player_count, updated_at)
                VALUES (
                    ?, ?, ?, 
                    CURRENT_TIMESTAMP, 
                    COALESCE(
                        (SELECT last_changed FROM roster_state WHERE team = ? AND season = ?),
                        CURRENT_TIMESTAMP
                    ),
                    ?, ?, CURRENT_TIMESTAMP
                )
            """, [team, season, current_hash, team, season, run_id, player_count])
            logger.info(f"✅ State updated for {team}: no changes")
    
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
            SELECT team, season, current_hash, last_polled, last_changed, last_run_id, player_count
            FROM roster_state
            ORDER BY team
        """).fetchall()
        
        return [
            {
                "team": r[0],
                "season": r[1],
                "current_hash": r[2],
                "last_polled": r[3],
                "last_changed": r[4],
                "last_run_id": r[5],
                "player_count": r[6]
            }
            for r in result
        ]
    
    def get_teams_with_changes(self, since_hours: int = 24) -> List[Dict[str, Any]]:
        """Get teams that have changed in the last N hours."""
        # FIXED: Use f-string instead of parameter for INTERVAL
        result = self.conn.execute(f"""
            SELECT 
                team,
                season,
                last_polled AS last_checked,
                last_changed,
                player_count,
                EXTRACT(EPOCH FROM (last_polled - last_changed)) / 3600 AS hours_since_change
            FROM roster_state
            WHERE last_polled > last_changed
            AND last_changed > CURRENT_TIMESTAMP - INTERVAL {since_hours} HOURS
            ORDER BY last_changed DESC
        """).fetchall()
        
        return [
            {
                "team": r[0],
                "season": r[1],
                "last_checked": r[2],
                "last_changed": r[3],
                "player_count": r[4],
                "hours_since_change": r[5]
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