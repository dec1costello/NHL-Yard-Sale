"""
SCD Type 2 Pipeline Health Diagnostics

Validates:
- State management consistency
- Raw ingestion integrity
- Snapshot uniqueness
- SCD Type 2 dimension correctness
- Pipeline freshness
- Hash stability
- Change tracking (last_polled vs last_changed)
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Optional

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
        }.get(status, cls.RESET)


class DiagnosticResult:
    """Stores diagnostic check result."""

    def __init__(
        self,
        name: str,
        status: str,  # "PASS" | "WARN" | "FAIL" | "INFO"
        message: str,
        failures: int = 0,
    ) -> None:
        self.name = name
        self.status = status
        self.message = message
        self.failures = failures


class SCDDiagnostic:
    """
    Diagnose SCD Type 2 pipeline health.

    Performs operational checks across:
    - roster_state
    - raw_rosters
    - dim_player (if available)
    """

    def __init__(self) -> None:
        self.loader = DuckDBLoader()
        self.state_manager = RosterStateManager()
        self.results: List[DiagnosticResult] = []

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
            self.check_state_consistency,
            self.check_snapshot_integrity,
            self.check_duplicates,
            self.check_player_ids,
            self.check_scd_integrity,
            self.check_pipeline_freshness,
            self.check_scd_depth,
            self.check_hash_stability,
            self.check_change_tracking,
        ]

        for check in checks:
            print("\n" + "-" * 90)
            result = check()
            self.results.append(result)
            self.print_result(result)

        print("\n" + "=" * 90)
        self.print_summary()

    # ---------------------------------------------------------
    # STATE VALIDATION
    # ---------------------------------------------------------

    def check_state_consistency(self) -> DiagnosticResult:
        """
        Validate roster_state matches latest raw snapshot.
        """
        df = self.loader.query("""
            WITH latest_runs AS (
                SELECT
                    team,
                    MAX(run_id) AS latest_run
                FROM raw_rosters
                GROUP BY team
            ),
            latest_counts AS (
                SELECT
                    r.team,
                    COUNT(*) AS snapshot_players
                FROM raw_rosters r
                JOIN latest_runs l
                    ON r.team = l.team
                    AND r.run_id = l.latest_run
                GROUP BY r.team
            )
            SELECT
                s.team,
                s.player_count,
                l.snapshot_players
            FROM roster_state s
            JOIN latest_counts l
                ON s.team = l.team
            WHERE s.player_count != l.snapshot_players
        """)

        if df.empty:
            return DiagnosticResult(
                "State Consistency",
                "PASS",
                "roster_state matches latest snapshots"
            )

        return DiagnosticResult(
            "State Consistency",
            "FAIL",
            f"{len(df)} teams have state mismatches",
            len(df)
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
                COUNT(*) as player_count
            FROM raw_rosters
            GROUP BY team, run_id
            HAVING COUNT(*) < 15
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
            f"{len(df)} snapshots have incomplete records",
            len(df)
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
                COUNT(*) as duplicates
            FROM raw_rosters
            WHERE player_id IS NOT NULL
            GROUP BY team, run_id, player_id
            HAVING COUNT(*) > 1
        """)

        if df.empty:
            return DiagnosticResult(
                "Duplicate Records",
                "PASS",
                "No duplicate players detected"
            )

        return DiagnosticResult(
            "Duplicate Records",
            "FAIL",
            f"{len(df)} duplicate player records",
            len(df)
        )

    def check_player_ids(self) -> DiagnosticResult:
        """
        Ensure every player has a stable identifier.
        """
        df = self.loader.query("""
            SELECT COUNT(*) as count
            FROM raw_rosters
            WHERE player_id IS NULL OR player_id = ''
        """)

        count = df.iloc[0]["count"] if not df.empty else 0

        if count == 0:
            return DiagnosticResult(
                "Player IDs",
                "PASS",
                "No NULL or empty player IDs"
            )

        return DiagnosticResult(
            "Player IDs",
            "WARN",
            f"{count} records have NULL/empty player IDs",
            count
        )

    # ---------------------------------------------------------
    # SCD TYPE 2 VALIDATION
    # ---------------------------------------------------------

    def check_scd_integrity(self) -> DiagnosticResult:
        """
        Validate SCD Type 2 rules if dim_player exists.
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
                "WARN",
                "dim_player not found - run dbt first"
            )

        multiple_current = self.loader.query("""
            SELECT player_id
            FROM main_marts.dim_player
            WHERE is_current = true
            GROUP BY player_id
            HAVING COUNT(*) > 1
        """)

        invalid_dates = self.loader.query("""
            SELECT *
            FROM main_marts.dim_player
            WHERE (is_current = true AND valid_to IS NOT NULL)
            OR (is_current = false AND valid_to IS NULL)
        """)

        failures = len(multiple_current) + len(invalid_dates)

        if failures == 0:
            return DiagnosticResult(
                "SCD Integrity",
                "PASS",
                "SCD Type 2 rules validated"
            )

        return DiagnosticResult(
            "SCD Integrity",
            "FAIL",
            f"{failures} SCD violations",
            failures
        )

    # ---------------------------------------------------------
    # OPERATIONAL MONITORING
    # ---------------------------------------------------------

    def check_pipeline_freshness(self) -> DiagnosticResult:
        """
        Check when teams were last polled.
        """
        df = self.loader.query("""
            SELECT COUNT(*) as teams
            FROM roster_state
            WHERE last_polled < NOW() - INTERVAL '24 hours'
        """)

        stale = df.iloc[0]["teams"] if not df.empty else 0

        if stale == 0:
            return DiagnosticResult(
                "Pipeline Freshness",
                "PASS",
                "All teams checked within 24 hours"
            )

        return DiagnosticResult(
            "Pipeline Freshness",
            "WARN",
            f"{stale} stale teams (not checked in 24h)",
            stale
        )

    def check_scd_depth(self) -> DiagnosticResult:
        """
        Report average SCD history depth.
        """
        df = self.loader.query("""
            SELECT ROUND(AVG(history_count), 2) as avg_history
            FROM (
                SELECT
                    player_id,
                    COUNT(*) as history_count
                FROM raw_rosters
                WHERE player_id IS NOT NULL
                GROUP BY player_id
            )
        """)

        avg = float(df.iloc[0]["avg_history"]) if not df.empty else 0.0

        if avg > 5:
            status = "PASS"
            message = f"Good SCD depth: avg {avg} changes per player"
        elif avg > 2:
            status = "WARN"
            message = f"Moderate SCD depth: avg {avg} changes per player"
        else:
            status = "INFO"
            message = f"Low SCD depth: avg {avg} changes per player"

        return DiagnosticResult(
            "SCD History Depth",
            status,
            message
        )

    def check_hash_stability(self) -> DiagnosticResult:
        """
        Verify hashes are consistent across runs.
        """
        df = self.loader.query("""
            WITH current_hash AS (
                SELECT 
                    team,
                    roster_hash
                FROM raw_rosters
                WHERE (team, ingestion_timestamp) IN (
                    SELECT team, MAX(ingestion_timestamp)
                    FROM raw_rosters
                    GROUP BY team
                )
            ),
            state_hash AS (
                SELECT 
                    team,
                    current_hash
                FROM roster_state
            )
            SELECT 
                c.team
            FROM current_hash c
            JOIN state_hash s ON c.team = s.team
            WHERE c.roster_hash != s.current_hash
        """)

        if df.empty:
            return DiagnosticResult(
                "Hash Stability",
                "PASS",
                "Hashes consistent between state and raw"
            )

        return DiagnosticResult(
            "Hash Stability",
            "FAIL",
            f"{len(df)} teams have inconsistent hashes",
            len(df)
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
        """)

        changes = df[df['change_status'] == 'CHANGE_DETECTED']
        errors = df[df['change_status'] == 'ERROR']

        if len(errors) > 0:
            return DiagnosticResult(
                "Change Tracking",
                "FAIL",
                f"{len(errors)} teams have invalid change tracking",
                len(errors)
            )

        if len(changes) > 0:
            return DiagnosticResult(
                "Change Tracking",
                "INFO",
                f"{len(changes)} teams had recent changes"
            )

        return DiagnosticResult(
            "Change Tracking",
            "PASS",
            "Change tracking working correctly"
        )

    # ---------------------------------------------------------
    # OUTPUT
    # ---------------------------------------------------------

    def print_result(self, result: DiagnosticResult) -> None:
        """Print a single diagnostic result."""
        color = Colors.status_color(result.status)

        print(
            f"{color}"
            f"{result.status:<5}"
            f"{Colors.RESET}"
            f" {result.name}: "
            f"{result.message}"
        )

    def print_summary(self) -> None:
        """Print final summary."""
        failures = sum(
            r.failures
            for r in self.results
            if r.status == "FAIL"
        )

        print("\n📊 Summary")

        if failures:
            print(
                f"{Colors.RED}"
                f"❌ System unhealthy: {failures} failures"
                f"{Colors.RESET}"
            )
            sys.exit(1)

        print(
            f"{Colors.GREEN}"
            "✅ SCD System Healthy"
            f"{Colors.RESET}"
        )


def main() -> None:
    """Run the diagnostic."""
    diagnostic = SCDDiagnostic()
    diagnostic.diagnose()


if __name__ == "__main__":
    main()