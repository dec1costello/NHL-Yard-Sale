"""Verify the warehouse data."""

import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.load.duckdb_loader import DuckDBLoader

def verify():
    print("🐪 Verifying warehouse...")
    print("=" * 60)
    
    loader = DuckDBLoader()
    
    # Teams in raw_rosters
    teams = loader.query('SELECT COUNT(DISTINCT team) as teams FROM raw_rosters')
    print(f"Teams in raw_rosters: {teams['teams'].iloc[0]}")
    
    # Players
    players = loader.query('SELECT COUNT(DISTINCT player_id) as players FROM raw_rosters')
    print(f"Unique players: {players['players'].iloc[0]}")
    
    # Records
    records = loader.query('SELECT COUNT(*) as records FROM raw_rosters')
    print(f"Total records: {records['records'].iloc[0]}")
    
    # dim_team
    dim_teams = loader.query('SELECT COUNT(*) as teams FROM main_marts.dim_team')
    print(f"Teams in dim_team: {dim_teams['teams'].iloc[0]}")
    
    # dim_player
    dim_players = loader.query('''
        SELECT 
            COUNT(DISTINCT player_id) as players,
            SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) as current
        FROM main_marts.dim_player
    ''')
    print(f"Players in dim_player: {dim_players['players'].iloc[0]}")
    print(f"Current players: {dim_players['current'].iloc[0]}")

if __name__ == "__main__":
    verify()