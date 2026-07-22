"""
SCD Type 2 Pipeline Health Diagnostics

Validates:
- State management consistency (state vs latest snapshot)
- Raw ingestion integrity
- Snapshot uniqueness
- SCD Type 2 dimension correctness
- Pipeline freshness
- Hash stability
- Change tracking (last_polled vs last_changed)
- Data quality metrics
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.load.duckdb_loader import DuckDBLoader
from src.state.state_manager import RosterStateManager


class Colors:
    """Terminal formatting."""

    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    BOLD = "\033[1m"
    
    @classmethod
    def status_color(cls, status: str) -> str:
        """Get color for status."""
        return {
            "PASS": cls.GREEN,
            "WARN": cls.YELLOW,
            "FAIL": cls.RED,
            "INFO": cls.BLUE,
            "SKIP": cls.CYAN,
        }.get(status, cls.RESET)
    
    @classmethod
    def status_emoji(cls, status: str) -> str:
        """Get emoji for status."""
        return {
            "PASS": "✅",
            "WARN": "⚠️",
            "FAIL": "❌",
            "INFO": "ℹ️",
            "SKIP": "⏭️",
        }.get(status, "❓")


class DiagnosticResult:
    """Stores diagnostic check result."""

    def __init__(
        self,
        name: str,
        status: str,  # "PASS" | "WARN" | "FAIL" | "INFO" | "SKIP"
        message: str,
        failures: int = 0,
        details: Optional[pd.DataFrame] = None,
    ) -> None:
        self.name = name
        self.status = status
        self.message = message
        self.failures = failures
        self.details = details


class SCDDiagnostic:
    """
    Diagnose SCD Type 2 pipeline health.

    Performs operational checks across:
    - roster_state
    - raw_rosters
    - dim_player (if available)
    """

    def __init__(self, verbose: bool = False) -> None:
        self.loader = DuckDBLoader()
        self.state_manager = RosterStateManager()
        self.results: List[DiagnosticResult] = []
        self.verbose = verbose

    def diagnose(self) -> None:
        """Run all diagnostic checks."""
        print(
            f"{Colors.BOLD}"
            "🔬 SCD Type 2 System Diagnosis"
            f"{Colors.RESET}"
        )
        print("=" * 90)
        print(datetime.now().strftime("🕐 %Y-%m-%d %H:%M:%S"))
        print("=" * 90)

        checks = [
            ("State Consistency", self.check_state_consistency),
            ("Snapshot Integrity", self.check_snapshot_integrity),
            ("Duplicate Records", self.check_duplicates),
            ("Player IDs", self.check_player_ids),
           # ("SCD Integrity", self.check_scd_integrity),
            ("Pipeline Freshness", self.check_pipeline_freshness),
            ("SCD History Depth", self.check_scd_depth),
            ("Hash Stability", self.check_hash_stability),
            ("Change Tracking", self.check_change_tracking),
            ("Data Quality", self.check_data_quality),
        ]

        for name, check in checks:
            print("\n" + "-" * 90)
            result = check()
            self.results.append(result)
            self.print_result(result)
            
            # Show details if verbose or if there's a failure
            if self.verbose or result.status == "FAIL":
                if result.details is not None and not result.details.empty:
                    print(f"\n  {Colors.BLUE}Details:{Colors.RESET}")
                    print(result.details.to_string(index=False))

        print("\n" + "=" * 90)
        self.print_summary()

    # ---------------------------------------------------------
    # STATE VALIDATION
    # ---------------------------------------------------------

    def check_state_consistency(self) -> DiagnosticResult:
        """
        Validate roster_state matches latest raw snapshot.
        Uses ingestion_timestamp, NOT run_id (UUIDs are lexicographic).
        """
        df = self.loader.query("""
            WITH latest_snapshot AS (
                SELECT 
                    team,
                    COUNT(*) as snapshot_players
                FROM raw_rosters
                WHERE (team, ingestion_timestamp) IN (
                    SELECT team, MAX(ingestion_timestamp)
                    FROM raw_rosters
                    GROUP BY team
                )
                GROUP BY team
            )
            SELECT 
                s.team,
                s.player_count as state_count,
                l.snapshot_players,
                (l.snapshot_players - s.player_count) as difference
            FROM roster_state s
            JOIN latest_snapshot l ON s.team = l.team
            WHERE s.player_count != l.snapshot_players
            ORDER BY s.team
        """)

        if df.empty:
            return DiagnosticResult(
                "State Consistency",
                "PASS",
                "All 32 teams match their latest snapshots"
            )

        return DiagnosticResult(
            "State Consistency",
            "FAIL",
            f"{len(df)} teams have state/latest-snapshot mismatches",
            len(df),
            df
        )

    # ---------------------------------------------------------
    # RAW VALIDATION
    # ---------------------------------------------------------

    def check_snapshot_integrity(self) -> DiagnosticResult:
        """
        Ensure each run_id represents a valid snapshot.
        """
        df = self.loader.query("""
            SELECT 
                team,
                run_id,
                COUNT(*) as player_count,
                MIN(ingestion_timestamp) as first_seen,
                MAX(ingestion_timestamp) as last_seen
            FROM raw_rosters
            GROUP BY team, run_id
            HAVING COUNT(*) < 15
            ORDER BY team, run_id
        """)

        if df.empty:
            return DiagnosticResult(
                "Snapshot Integrity",
                "PASS",
                "All snapshots have complete records (>=15 players)"
            )

        return DiagnosticResult(
            "Snapshot Integrity",
            "WARN",
            f"{len(df)} snapshots have incomplete records (<15 players)",
            len(df),
            df
        )

    def check_duplicates(self) -> DiagnosticResult:
        """
        Detect duplicate players within a snapshot.
        """
        df = self.loader.query("""
            SELECT
                team,
                run_id,
                player_id,
                COUNT(*) as duplicate_count,
                MIN(ingestion_timestamp) as first_seen,
                MAX(ingestion_timestamp) as last_seen
            FROM raw_rosters
            WHERE player_id IS NOT NULL AND player_id != ''
            GROUP BY team, run_id, player_id
            HAVING COUNT(*) > 1
            ORDER BY team, run_id
            LIMIT 20
        """)

        if df.empty:
            return DiagnosticResult(
                "Duplicate Records",
                "PASS",
                "No duplicate players detected"
            )

        total_dups = self.loader.query("""
            SELECT COUNT(*) as total
            FROM (
                SELECT team, run_id, player_id
                FROM raw_rosters
                WHERE player_id IS NOT NULL AND player_id != ''
                GROUP BY team, run_id, player_id
                HAVING COUNT(*) > 1
            )
        """).iloc[0]['total']

        return DiagnosticResult(
            "Duplicate Records",
            "FAIL",
            f"{total_dups} duplicate player records found",
            total_dups,
            df
        )

    def check_player_ids(self) -> DiagnosticResult:
        """
        Ensure every player has a stable identifier.
        """
        df = self.loader.query("""
            SELECT 
                COUNT(*) as count,
                COUNT(DISTINCT team) as teams_affected
            FROM raw_rosters
            WHERE player_id IS NULL OR player_id = ''
        """)

        count = df.iloc[0]["count"] if not df.empty else 0
        teams = df.iloc[0]["teams_affected"] if not df.empty else 0

        if count == 0:
            return DiagnosticResult(
                "Player IDs",
                "PASS",
                "No NULL or empty player IDs"
            )

        # Show which teams have NULL IDs
        details = self.loader.query("""
            SELECT 
                team,
                COUNT(*) as null_count
            FROM raw_rosters
            WHERE player_id IS NULL OR player_id = ''
            GROUP BY team
            ORDER BY null_count DESC
        """)

        return DiagnosticResult(
            "Player IDs",
            "WARN",
            f"{count} records have NULL/empty player IDs across {teams} teams",
            count,
            details
        )

    # ---------------------------------------------------------
    # SCD TYPE 2 VALIDATION
    # ---------------------------------------------------------

    def check_scd_integrity(self) -> DiagnosticResult:
        """
        Validate SCD Type 2 rules if dim_player exists.
        Auto-detects column names (valid_to/end_date, valid_from/start_date).
        """
        # Check if dim_player exists
        table_check = self.loader.query("""
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = 'dim_player'
            AND table_schema = 'main_marts'
        """)

        if table_check['count'].iloc[0] == 0:
            return DiagnosticResult(
                "SCD Integrity",
                "SKIP",
                "dim_player not found - run dbt first"
            )

        # Detect column names
        cols = self.loader.query("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'dim_player'
            AND table_schema = 'main_marts'
            AND column_name IN ('valid_to', 'end_date', 'valid_from', 'start_date')
        """)
        
        col_names = cols['column_name'].tolist() if not cols.empty else []
        
        date_to = 'end_date' if 'end_date' in col_names else 'valid_to'
        date_from = 'start_date' if 'start_date' in col_names else 'valid_from'
        
        # Check for multiple current records
        multiple_current = self.loader.query("""
            SELECT player_id, COUNT(*) as current_count
            FROM main_marts.dim_player
            WHERE is_current = true
            GROUP BY player_id
            HAVING COUNT(*) > 1
        """)

        # Check for invalid date patterns
        invalid_dates = self.loader.query(f"""
            SELECT player_id, is_current, {date_to}, {date_from}
            FROM main_marts.dim_player
            WHERE (is_current = true AND {date_to} IS NOT NULL)
            OR (is_current = false AND {date_to} IS NULL)
            LIMIT 20
        """)

        failures = len(multiple_current) + len(invalid_dates)

        if failures == 0:
            return DiagnosticResult(
                "SCD Integrity",
                "PASS",
                "SCD Type 2 rules validated"
            )

        # Build details
        details = pd.DataFrame()
        if len(multiple_current) > 0:
            details = pd.concat([
                details,
                multiple_current.assign(issue="Multiple current records")
            ])
        if len(invalid_dates) > 0:
            details = pd.concat([
                details,
                invalid_dates.assign(issue="Invalid date pattern")
            ])

        return DiagnosticResult(
            "SCD Integrity",
            "FAIL",
            f"{failures} SCD violations found",
            failures,
            details
        )

    # ---------------------------------------------------------
    # OPERATIONAL MONITORING
    # ---------------------------------------------------------

    def check_pipeline_freshness(self) -> DiagnosticResult:
        """
        Check when teams were last polled.
        """
        df = self.loader.query("""
            SELECT 
                COUNT(*) as stale_count,
                STRING_AGG(team, ', ') as stale_teams
            FROM roster_state
            WHERE last_polled < CURRENT_TIMESTAMP - INTERVAL '24 hours'
        """)

        stale = df.iloc[0]["stale_count"] if not df.empty else 0
        teams = df.iloc[0]["stale_teams"] if not df.empty and df.iloc[0]["stale_teams"] else ""

        if stale == 0:
            return DiagnosticResult(
                "Pipeline Freshness",
                "PASS",
                "All 32 teams checked within 24 hours"
            )

        return DiagnosticResult(
            "Pipeline Freshness",
            "WARN",
            f"{stale} stale teams: {teams}",
            stale
        )

    def check_scd_depth(self) -> DiagnosticResult:
        """
        Report average SCD history depth with distribution.
        """
        df = self.loader.query("""
            SELECT 
                ROUND(AVG(history_count), 2) as avg_history,
                MIN(history_count) as min_history,
                MAX(history_count) as max_history,
                COUNT(*) as total_players
            FROM (
                SELECT
                    player_id,
                    COUNT(*) as history_count
                FROM raw_rosters
                WHERE player_id IS NOT NULL AND player_id != ''
                GROUP BY player_id
            )
        """)

        if df.empty:
            return DiagnosticResult(
                "SCD History Depth",
                "INFO",
                "No player history found"
            )

        avg = float(df.iloc[0]["avg_history"])
        min_h = int(df.iloc[0]["min_history"])
        max_h = int(df.iloc[0]["max_history"])
        players = int(df.iloc[0]["total_players"])

        # Get distribution
        dist = self.loader.query("""
            SELECT 
                CASE 
                    WHEN history_count = 1 THEN '1 (Single)'
                    WHEN history_count BETWEEN 2 AND 3 THEN '2-3 (Few)'
                    WHEN history_count BETWEEN 4 AND 6 THEN '4-6 (Moderate)'
                    WHEN history_count BETWEEN 7 AND 10 THEN '7-10 (Active)'
                    ELSE '11+ (Very Active)'
                END as category,
                COUNT(*) as players,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percent
            FROM (
                SELECT player_id, COUNT(*) as history_count
                FROM raw_rosters
                WHERE player_id IS NOT NULL AND player_id != ''
                GROUP BY player_id
            )
            GROUP BY category
            ORDER BY MIN(history_count)
        """)

        message = (
            f"Avg: {avg} changes/player "
            f"(min: {min_h}, max: {max_h}, n={players})"
        )

        return DiagnosticResult(
            "SCD History Depth",
            "PASS" if avg > 3 else "INFO",
            message,
            0,
            dist
        )

    def check_hash_stability(self) -> DiagnosticResult:
        """
        Verify hashes are consistent between state and raw.
        """
        df = self.loader.query("""
            WITH latest_raw AS (
                SELECT 
                    team,
                    roster_hash,
                    ingestion_timestamp
                FROM raw_rosters
                WHERE (team, ingestion_timestamp) IN (
                    SELECT team, MAX(ingestion_timestamp)
                    FROM raw_rosters
                    GROUP BY team
                )
            )
            SELECT 
                s.team,
                s.current_hash as state_hash,
                r.roster_hash as raw_hash
            FROM roster_state s
            JOIN latest_raw r ON s.team = r.team
            WHERE s.current_hash != r.roster_hash
            ORDER BY s.team
        """)

        if df.empty:
            return DiagnosticResult(
                "Hash Stability",
                "PASS",
                "All 32 teams have consistent hashes"
            )

        return DiagnosticResult(
            "Hash Stability",
            "FAIL",
            f"{len(df)} teams have inconsistent hashes",
            len(df),
            df
        )

    def check_change_tracking(self) -> DiagnosticResult:
        """
        Verify last_polled vs last_changed tracking.
        """
        df = self.loader.query("""
            SELECT 
                team,
                last_polled,
                last_changed,
                CASE 
                    WHEN last_polled = last_changed THEN 'NO_CHANGE'
                    WHEN last_polled > last_changed THEN 'CHANGE_DETECTED'
                    ELSE 'ERROR'
                END as change_status
            FROM roster_state
            ORDER BY team
        """)

        changes = df[df['change_status'] == 'CHANGE_DETECTED']
        errors = df[df['change_status'] == 'ERROR']
        no_changes = df[df['change_status'] == 'NO_CHANGE']

        if len(errors) > 0:
            return DiagnosticResult(
                "Change Tracking",
                "FAIL",
                f"{len(errors)} teams have invalid change tracking",
                len(errors),
                errors
            )

        if len(changes) > 0:
            return DiagnosticResult(
                "Change Tracking",
                "INFO",
                f"{len(changes)} teams had changes detected",
                0,
                changes
            )

        return DiagnosticResult(
            "Change Tracking",
            "PASS",
            "All 32 teams tracked (no changes detected)"
        )

    def check_data_quality(self) -> DiagnosticResult:
        """
        Additional data quality checks.
        """
        issues = []
        details_list = []

        # Check for null positions
        null_pos = self.loader.query("""
            SELECT COUNT(*) as count
            FROM raw_rosters
            WHERE position IS NULL OR position = ''
        """)
        if null_pos.iloc[0]['count'] > 0:
            issues.append(f"{null_pos.iloc[0]['count']} NULL positions")
            details_list.append(("NULL Positions", null_pos.iloc[0]['count']))

        # Check for invalid numbers
        invalid_num = self.loader.query("""
            SELECT COUNT(*) as count
            FROM raw_rosters
            WHERE number IS NULL OR number = '' OR number = 'N/A'
        """)
        if invalid_num.iloc[0]['count'] > 0:
            issues.append(f"{invalid_num.iloc[0]['count']} invalid numbers")
            details_list.append(("Invalid Numbers", invalid_num.iloc[0]['count']))

        # Check for missing roster_hash
        missing_hash = self.loader.query("""
            SELECT COUNT(*) as count
            FROM raw_rosters
            WHERE roster_hash IS NULL OR roster_hash = ''
        """)
        if missing_hash.iloc[0]['count'] > 0:
            issues.append(f"{missing_hash.iloc[0]['count']} missing hashes")
            details_list.append(("Missing Hashes", missing_hash.iloc[0]['count']))

        if not issues:
            return DiagnosticResult(
                "Data Quality",
                "PASS",
                "All data quality checks passed"
            )

        details = pd.DataFrame(details_list, columns=['Issue', 'Count'])
        return DiagnosticResult(
            "Data Quality",
            "WARN",
            f"{len(issues)} data quality issues: " + ", ".join(issues),
            len(issues),
            details
        )

    # ---------------------------------------------------------
    # OUTPUT
    # ---------------------------------------------------------

    def print_result(self, result: DiagnosticResult) -> None:
        """Print a single diagnostic result."""
        color = Colors.status_color(result.status)
        emoji = Colors.status_emoji(result.status)

        print(
            f"{color}"
            f"{emoji} {result.status:<5}"
            f"{Colors.RESET}"
            f" {result.name}: "
            f"{result.message}"
        )

    def print_summary(self) -> None:
        """Print final summary."""
        print("\n📊 Summary")
        print("-" * 40)

        # Count by status
        status_counts = {}
        for r in self.results:
            status_counts[r.status] = status_counts.get(r.status, 0) + 1

        print(f"  {Colors.GREEN}✅ PASS: {status_counts.get('PASS', 0)}{Colors.RESET}")
        print(f"  {Colors.YELLOW}⚠️  WARN: {status_counts.get('WARN', 0)}{Colors.RESET}")
        print(f"  {Colors.RED}❌ FAIL: {status_counts.get('FAIL', 0)}{Colors.RESET}")
        print(f"  {Colors.BLUE}ℹ️  INFO: {status_counts.get('INFO', 0)}{Colors.RESET}")
        print(f"  {Colors.CYAN}⏭️  SKIP: {status_counts.get('SKIP', 0)}{Colors.RESET}")

        failures = sum(
            r.failures
            for r in self.results
            if r.status == "FAIL"
        )

        print("-" * 40)

        if failures:
            print(
                f"{Colors.RED}"
                f"❌ System unhealthy: {failures} failures detected"
                f"{Colors.RESET}"
            )
            sys.exit(1)

        print(
            f"{Colors.GREEN}"
            "✅ SCD System Healthy - All checks passed!"
            f"{Colors.RESET}"
        )


def main() -> None:
    """Run the diagnostic."""
    import argparse
    
    parser = argparse.ArgumentParser(description="SCD Type 2 System Diagnostics")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed output for each check"
    )
    args = parser.parse_args()
    
    diagnostic = SCDDiagnostic(verbose=args.verbose)
    diagnostic.diagnose()


if __name__ == "__main__":
    main()