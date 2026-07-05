"""Run the full NHL API pipeline."""

import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.extract.roster_api import NHLRosterAPI
from src.config import get_config

def main():
    print("🐪 Running NHL API Pipeline...")
    print("=" * 60)
    
    config = get_config()
    api = NHLRosterAPI()
    
    print(f"📅 Season: {config.current_season} ({config.current_season_api})")
    print()
    
    result = api.scrape_and_load_all_teams()
    
    summary = result['summary']
    print("\n📊 SUMMARY")
    print(f"   Total teams: {summary['total_teams']}")
    print(f"   Total players: {summary['total_players']}")
    print(f"   Teams with new bronze: {summary['teams_with_bronze']}")
    
    print("\n🐪 Pipeline complete!")

if __name__ == "__main__":
    main()