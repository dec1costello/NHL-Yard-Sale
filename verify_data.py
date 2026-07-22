"""Verify the warehouse data with SCD Type 2 awareness."""

import sys
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.load.duckdb_loader import DuckDBLoader
from src.state.state_manager import RosterStateManager


class WarehouseVerifier:
    """Verify and report on warehouse health - SCD Type 2 aware."""
    
    def __init__(self):
        self.loader = DuckDBLoader()
        self.state_manager = RosterStateManager()
        
        # ANSI color codes for pretty output
        self.GREEN = '\033[92m'
        self.YELLOW = '\033[93m'
        self.RED = '\033[91m'
        self.BLUE = '\033[94m'
        self.PURPLE = '\033[95m'
        self.CYAN = '\033[96m'
        self.RESET = '\033[0m'
        self.BOLD = '\033[1m'
    
    def verify(self):
        """Run all verification checks."""
        print(f"{self.BOLD}🐪 NHL-Yard-Sale SCD Type 2 Warehouse Verification{self.RESET}")
        print("=" * 80)
        print(f"🕐 Verification time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        self.check_raw_data()
        print("\n" + "-" * 80)
        self.check_state_tracking()
        print("\n" + "-" * 80)
        self.check_scd_health()
        print("\n" + "-" * 80)
        self.check_change_detection()
        print("\n" + "-" * 80)
        self.check_data_quality()
        print("\n" + "=" * 80)
        self.print_summary()
    
    def check_raw_data(self):
        """Check raw_rosters table - SCD aware."""
        print(f"\n{self.BOLD}📊 Raw Data Summary (Historical Snapshots){self.RESET}")
        
        # Basic counts
        teams = self.loader.query('SELECT COUNT(DISTINCT team) as count FROM raw_rosters')
        players = self.loader.query('SELECT COUNT(DISTINCT player_id) as count FROM raw_rosters')
        records = self.loader.query('SELECT COUNT(*) as count FROM raw_rosters')
        
        print(f"  🏒 Teams:          {teams['count'].iloc[0]:>6}")
        print(f"  👤 Unique players: {players['count'].iloc[0]:>6} (historical)")
        print(f"  📝 Total records:  {records['count'].iloc[0]:>6} (SCD history)")
        
        # Run count per team (this is GOOD for SCD!)
        runs = self.loader.query("""
            SELECT 
                team,
                COUNT(DISTINCT run_id) as runs,
                COUNT(*) as records,
                ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT run_id), 1) as avg_per_run
            FROM raw_rosters
            GROUP BY team
            ORDER BY team
        """)
        
        print(f"\n  📈 Historical runs per team:")
        for _, row in runs.iterrows():
            print(f"    • {row['team']}: {row['runs']} runs, {row['records']} records (avg {row['avg_per_run']}/run)")
        
        # Latest ingestion
        latest = self.loader.query("""
            SELECT 
                MAX(ingestion_timestamp) as latest,
                MIN(ingestion_timestamp) as earliest
            FROM raw_rosters
        """)
        
        if not latest.empty:
            latest_time = latest['latest'].iloc[0]
            earliest_time = latest['earliest'].iloc[0]
            print(f"\n  🕐 Latest snapshot:  {latest_time}")
            print(f"  🕐 Earliest snapshot: {earliest_time}")
            
            # Calculate time range
            if latest_time and earliest_time:
                days = (latest_time - earliest_time).days
                print(f"  📅 Data span:      {days} days of SCD history")
    
    def check_state_tracking(self):
        """Check roster_state tracking."""
        print(f"\n{self.BOLD}📈 State Tracking (Current State){self.RESET}")
        
        states = self.state_manager.get_all_states()
        
        if not states:
            print(f"  {self.RED}❌ No state data found!{self.RESET}")
            return
        
        # Calculate current total players
        current_total = sum(s.get('player_count', 0) for s in states)
        print(f"  🏒 Teams tracked:  {len(states)}")
        print(f"  👤 Current players: {current_total} (active rosters)")
        
        # Check when teams were last polled
        now = datetime.now()
        recently_polled = 0
        stale = 0
        
        for s in states:
            if s['last_polled']:
                hours_ago = (now - s['last_polled']).total_seconds() / 3600
                if hours_ago < 24:
                    recently_polled += 1
                else:
                    stale += 1
        
        print(f"  ✅ Recently polled (<24h): {recently_polled}")
        if stale > 0:
            print(f"  {self.YELLOW}⚠️  Stale (>24h):        {stale}{self.RESET}")
        
        # Show when tracking started
        oldest = min(states, key=lambda x: x.get('last_polled') or datetime.min)
        if oldest['last_polled']:
            print(f"  🕐 Tracking since:   {oldest['last_polled']}")
    
    def check_scd_health(self):
        """Check SCD Type 2 health metrics."""
        print(f"\n{self.BOLD}🔄 SCD Type 2 Health{self.RESET}")
        
        # 1. History depth per player
        history_depth = self.loader.query("""
            SELECT 
                player_id,
                player,
                COUNT(*) as changes,
                MIN(ingestion_timestamp) as first_seen,
                MAX(ingestion_timestamp) as last_seen
            FROM raw_rosters
            GROUP BY player_id, player
            HAVING COUNT(*) > 1
            ORDER BY changes DESC
            LIMIT 10
        """)
        
        if not history_depth.empty:
            print(f"  📊 Players with most changes (SCD history):")
            for _, row in history_depth.iterrows():
                print(f"    • {row['player']}: {row['changes']} changes")
        
        # 2. SCD distribution
        scd_dist = self.loader.query("""
            SELECT 
                CASE 
                    WHEN change_count = 1 THEN 'New (1 change)'
                    WHEN change_count BETWEEN 2 AND 3 THEN 'Moderate (2-3 changes)'
                    WHEN change_count BETWEEN 4 AND 5 THEN 'Active (4-5 changes)'
                    ELSE 'Very Active (6+ changes)'
                END as change_category,
                COUNT(*) as players
            FROM (
                SELECT 
                    player_id,
                    COUNT(*) as change_count
                FROM raw_rosters
                GROUP BY player_id
            )
            GROUP BY change_category
            ORDER BY MIN(change_count)
        """)
        
        print(f"\n  📊 SCD Change Distribution:")
        for _, row in scd_dist.iterrows():
            print(f"    • {row['change_category']}: {row['players']} players")
        
        # 3. Historical snapshot count
        snapshot_stats = self.loader.query("""
            SELECT 
                team,
                COUNT(DISTINCT run_id) as snapshots,
                MIN(ingestion_timestamp) as first_snapshot,
                MAX(ingestion_timestamp) as last_snapshot
            FROM raw_rosters
            GROUP BY team
            ORDER BY snapshots DESC
        """)
        
        print(f"\n  📸 Snapshot count per team (SCD history):")
        for _, row in snapshot_stats.iterrows():
            print(f"    • {row['team']}: {row['snapshots']} snapshots")
    
    def check_change_detection(self):
        """Check change detection metrics."""
        print(f"\n{self.BOLD}🔍 Change Detection{self.RESET}")
        
        # Teams with changes
        changed = self.state_manager.get_teams_with_changes(since_hours=168)  # 7 days
        
        if changed:
            print(f"  {self.GREEN}✅ Teams with changes (7d): {len(changed)}{self.RESET}")
            for t in changed[:5]:
                hours = t.get('hours_since_change', 0)
                print(f"    • {t['team']}: {hours:.1f} hours ago")
            if len(changed) > 5:
                print(f"    ... and {len(changed) - 5} more")
        else:
            print(f"  {self.YELLOW}ℹ️  No changes detected in last 7 days{self.RESET}")
        
        # Check change log
        log_count = self.loader.query('SELECT COUNT(*) as count FROM roster_changes_log')
        print(f"  📝 Change log entries: {log_count['count'].iloc[0]}")
        
        # Latest change
        latest_change = self.loader.query("""
            SELECT 
                team,
                timestamp,
                player_count
            FROM roster_changes_log
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        
        if not latest_change.empty and latest_change['timestamp'].iloc[0]:
            print(f"  🕐 Latest change:    {latest_change['timestamp'].iloc[0]} ({latest_change['team'].iloc[0]})")
    
    def check_data_quality(self):
        """Check data quality metrics - SCD aware."""
        print(f"\n{self.BOLD}🔍 Data Quality (SCD-Aware){self.RESET}")
        
        # 1. Check for TRUE duplicates (same player, same run - this is BAD)
        duplicates = self.loader.query("""
            SELECT 
                team,
                run_id,
                player_id,
                COUNT(*) as dup_count
            FROM raw_rosters
            WHERE player_id IS NOT NULL AND player_id != ''
            GROUP BY team, run_id, player_id
            HAVING COUNT(*) > 1
            LIMIT 10
        """)
        
        if len(duplicates) > 0:
            print(f"  {self.RED}❌ Found {len(duplicates)} TRUE duplicates (same player in same run)!{self.RESET}")
            for _, row in duplicates.iterrows():
                # Safe handling of potentially None values
                team = row['team'] or 'Unknown'
                run_id = str(row['run_id'])[:8] if row['run_id'] else 'Unknown'
                player_id = str(row['player_id'])[:8] if row['player_id'] else 'NULL'
                dup_count = row['dup_count']
                print(f"    • {team} run {run_id}: player {player_id} appears {dup_count}x")
        else:
            print(f"  ✅ No TRUE duplicates (SCD history is clean)")
        
        # 2. Check current state matches latest snapshot (THIS IS THE CORRECT SCD CHECK)
        state_match = self.loader.query("""
            WITH latest_snapshot AS (
                SELECT 
                    team,
                    COUNT(*) as snapshot_count,
                    MAX(ingestion_timestamp) as snapshot_time
                FROM raw_rosters
                GROUP BY team
            )
            SELECT 
                s.team,
                s.player_count as state_count,
                l.snapshot_count as latest_snapshot_count,
                CASE 
                    WHEN s.player_count = l.snapshot_count THEN '✅'
                    ELSE '❌'
                END as match_status
            FROM roster_state s
            JOIN latest_snapshot l ON s.team = l.team
            WHERE s.player_count != l.snapshot_count
        """)
        
        if len(state_match) == 0:
            print(f"  ✅ All teams match their latest snapshot (SCD tracking correct)")
        else:
            print(f"  {self.YELLOW}⚠️  {len(state_match)} teams have state/snapshot mismatch:{self.RESET}")
            for _, row in state_match.iterrows():
                print(f"    • {row['team']}: state={row['state_count']}, latest_snapshot={row['latest_snapshot_count']}")
        
        # 3. Check hash consistency (state hash should match latest snapshot)
        hash_check = self.loader.query("""
            WITH latest_hash AS (
                SELECT 
                    team,
                    roster_hash,
                    ROW_NUMBER() OVER (PARTITION BY team ORDER BY ingestion_timestamp DESC) as rn
                FROM raw_rosters
            )
            SELECT 
                s.team,
                s.current_hash as state_hash,
                l.roster_hash as snapshot_hash
            FROM roster_state s
            JOIN latest_hash l ON s.team = l.team AND l.rn = 1
            WHERE s.current_hash != l.roster_hash
        """)
        
        if len(hash_check) == 0:
            print(f"  ✅ Hash consistency verified (state = latest snapshot)")
        else:
            print(f"  {self.YELLOW}⚠️  {len(hash_check)} teams have hash mismatch:{self.RESET}")
            for _, row in hash_check.iterrows():
                state_hash = str(row['state_hash'])[:8] if row['state_hash'] else 'NULL'
                snapshot_hash = str(row['snapshot_hash'])[:8] if row['snapshot_hash'] else 'NULL'
                print(f"    • {row['team']}: state={state_hash}... snapshot={snapshot_hash}...")
        
        # 4. Check for NULL player_ids (data quality issue)
        null_ids = self.loader.query("""
            SELECT COUNT(*) as count 
            FROM raw_rosters 
            WHERE player_id IS NULL OR player_id = ''
        """)
        
        if null_ids['count'].iloc[0] > 0:
            print(f"  {self.YELLOW}⚠️  NULL player_ids: {null_ids['count'].iloc[0]}{self.RESET}")
        else:
            print(f"  ✅ No NULL player_ids")
    
    def print_summary(self):
        """Print final summary with health status."""
        print(f"\n{self.BOLD}📊 SCD Type 2 Health Summary{self.RESET}")
        
        # Check if everything is healthy
        states = self.state_manager.get_all_states()
        recent_polled = sum(1 for s in states if s.get('last_polled') and 
                           (datetime.now() - s['last_polled']).total_seconds() < 86400)
        
        # Check SCD health - state matches latest snapshot
        state_match = self.loader.query("""
            WITH latest_snapshot AS (
                SELECT 
                    team,
                    COUNT(*) as snapshot_count
                FROM raw_rosters
                GROUP BY team
            )
            SELECT COUNT(*) as mismatches
            FROM roster_state s
            JOIN latest_snapshot l ON s.team = l.team
            WHERE s.player_count != l.snapshot_count
        """)
        
        mismatches = state_match['mismatches'].iloc[0] if not state_match.empty else 0
        
        # Check for TRUE duplicates
        duplicates = self.loader.query("""
            SELECT COUNT(*) as count
            FROM (
                SELECT 
                    team, run_id, player_id
                FROM raw_rosters
                GROUP BY team, run_id, player_id
                HAVING COUNT(*) > 1
            )
        """)
        
        true_dups = duplicates['count'].iloc[0] if not duplicates.empty else 0
        
        # Overall health
        if recent_polled == len(states) and len(states) == 32 and mismatches == 0 and true_dups == 0:
            print(f"  {self.GREEN}✅ SCD Type 2 system is HEALTHY{self.RESET}")
            print(f"     • All 32 teams tracked and recently polled")
            print(f"     • State matches latest snapshots")
            print(f"     • No TRUE duplicates found")
            status = "HEALTHY 🟢"
        elif recent_polled > 0 and mismatches == 0:
            print(f"  {self.YELLOW}⚠️  SCD Type 2 system is DEGRADED{self.RESET}")
            print(f"     • {recent_polled}/{len(states)} teams recently polled")
            status = "DEGRADED 🟡"
        else:
            print(f"  {self.RED}❌ SCD Type 2 system is UNHEALTHY{self.RESET}")
            if mismatches > 0:
                print(f"     • {mismatches} teams have state/snapshot mismatches")
            if true_dups > 0:
                print(f"     • {true_dups} TRUE duplicates found")
            status = "UNHEALTHY 🔴"
        
        print(f"\n  {self.BOLD}Overall Status: {status}{self.RESET}")
        
        # Suggestions
        if status != "HEALTHY 🟢":
            print(f"\n  {self.YELLOW}💡 Recommended actions:{self.RESET}")
            if true_dups > 0:
                print(f"     • Fix duplicates in raw_rosters")
            if mismatches > 0:
                print(f"     • Sync state with latest snapshots")
            if recent_polled < len(states):
                print(f"     • Run pipeline: uv run python run_pipeline.py")


def main():
    """Run verification."""
    verifier = WarehouseVerifier()
    verifier.verify()


if __name__ == "__main__":
    main()