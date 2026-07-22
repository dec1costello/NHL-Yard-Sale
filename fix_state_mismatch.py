# fix_state_mismatch.py
"""Find and fix the single state mismatch."""

import duckdb

conn = duckdb.connect('warehouse/duckdb.db')

print("🔍 Finding state mismatch...")

# Find the team with mismatch
result = conn.execute("""
    WITH latest_runs AS (
        SELECT team, MAX(run_id) AS latest_run
        FROM raw_rosters
        GROUP BY team
    ),
    latest_counts AS (
        SELECT r.team, COUNT(*) AS snapshot_players
        FROM raw_rosters r
        JOIN latest_runs l ON r.team = l.team AND r.run_id = l.latest_run
        GROUP BY r.team
    )
    SELECT 
        s.team,
        s.player_count as state_count,
        l.snapshot_players,
        s.current_hash as state_hash,
        MAX(r.roster_hash) as raw_hash
    FROM roster_state s
    JOIN latest_counts l ON s.team = l.team
    JOIN raw_rosters r ON s.team = r.team
    GROUP BY s.team, s.player_count, l.snapshot_players, s.current_hash
    WHERE s.player_count != l.snapshot_players
""").df()

if len(result) == 0:
    print("✅ No mismatches found!")
else:
    print("❌ Mismatch found:")
    print(result)
    
    # Fix it by syncing state with latest snapshot
    print("\n🔧 Fixing state...")
    conn.execute("""
        WITH latest_counts AS (
            SELECT team, COUNT(*) as snapshot_count
            FROM raw_rosters
            GROUP BY team
        ),
        latest_hash AS (
            SELECT team, roster_hash
            FROM raw_rosters
            WHERE (team, ingestion_timestamp) IN (
                SELECT team, MAX(ingestion_timestamp)
                FROM raw_rosters
                GROUP BY team
            )
        )
        UPDATE roster_state
        SET 
            player_count = lc.snapshot_count,
            current_hash = lh.roster_hash,
            updated_at = CURRENT_TIMESTAMP
        FROM latest_counts lc
        JOIN latest_hash lh ON lc.team = lh.team
        WHERE roster_state.team = lc.team
        AND roster_state.player_count != lc.snapshot_count
    """)
    print("✅ State fixed!")
    
    # Verify
    verify = conn.execute("""
        WITH latest_counts AS (
            SELECT team, COUNT(*) as snapshot_count
            FROM raw_rosters
            GROUP BY team
        )
        SELECT COUNT(*) as mismatches
        FROM roster_state s
        JOIN latest_counts l ON s.team = l.team
        WHERE s.player_count != l.snapshot_count
    """).fetchone()[0]
    
    if verify == 0:
        print("✅ All states now match!")
    else:
        print(f"⚠️  {verify} mismatches still remain")

conn.close()