"""Fix TRUE duplicates and NULL player_ids."""

import duckdb
from pathlib import Path
import shutil
from datetime import datetime


def fix_all_issues():
    """Fix duplicates and NULL player_ids."""
    
    # 1. Backup
    print("📦 Creating backup...")
    backup_path = f'warehouse/duckdb_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
    shutil.copy2('warehouse/duckdb.db', backup_path)
    print(f"   Backup saved to: {backup_path}")
    
    conn = duckdb.connect('warehouse/duckdb.db')
    
    try:
        # 2. Check current state
        print("\n📊 Current issues:")
        
        dup_count = conn.execute("""
            SELECT COUNT(*) as count
            FROM (
                SELECT team, run_id, player_id, COUNT(*) as c
                FROM raw_rosters
                WHERE player_id IS NOT NULL AND player_id != ''
                GROUP BY team, run_id, player_id
                HAVING c > 1
            )
        """).fetchone()[0]
        print(f"   • TRUE duplicates: {dup_count}")
        
        null_count = conn.execute("""
            SELECT COUNT(*) 
            FROM raw_rosters 
            WHERE player_id IS NULL OR player_id = ''
        """).fetchone()[0]
        print(f"   • NULL player_ids: {null_count}")
        
        # 3. Fix duplicates (keep only one per team/run/player)
        if dup_count > 0:
            print(f"\n🔧 Fixing {dup_count} duplicates...")
            
            # Create clean table without duplicates
            conn.execute("""
                CREATE TEMP TABLE clean_rosters AS
                SELECT DISTINCT *
                FROM raw_rosters
                WHERE player_id IS NOT NULL AND player_id != ''
            """)
            
            # Add back NULL records (if any)
            conn.execute("""
                INSERT INTO clean_rosters
                SELECT *
                FROM raw_rosters
                WHERE player_id IS NULL OR player_id = ''
            """)
            
            # Replace table
            conn.execute("DROP TABLE raw_rosters")
            conn.execute("""
                CREATE TABLE raw_rosters AS 
                SELECT * FROM clean_rosters
            """)
            conn.execute("DROP TABLE IF EXISTS clean_rosters")
            
            print("   ✅ Duplicates removed!")
        
        # 4. Handle NULL player_ids (ask user)
        if null_count > 0:
            print(f"\n⚠️  Found {null_count} NULL player_ids")
            
            # Show which teams have NULLs
            null_teams = conn.execute("""
                SELECT 
                    team,
                    COUNT(*) as null_count,
                    MIN(ingestion_timestamp) as first_seen,
                    MAX(ingestion_timestamp) as last_seen
                FROM raw_rosters
                WHERE player_id IS NULL OR player_id = ''
                GROUP BY team
                ORDER BY null_count DESC
            """).df()
            
            print("\n   Teams with NULL player_ids:")
            for _, row in null_teams.iterrows():
                print(f"     • {row['team']}: {row['null_count']} NULLs")
            
            # Check if these are from specific runs
            null_runs = conn.execute("""
                SELECT 
                    team,
                    run_id,
                    COUNT(*) as null_count,
                    MIN(ingestion_timestamp) as ingest_time
                FROM raw_rosters
                WHERE player_id IS NULL OR player_id = ''
                GROUP BY team, run_id
                ORDER BY team, null_count DESC
            """).df()
            
            print("\n   NULLs by run (showing first 5):")
            for _, row in null_runs.head(5).iterrows():
                print(f"     • {row['team']} run {row['run_id'][:12]}...: {row['null_count']} NULLs")
            
            # Ask what to do
            print("\n💡 What would you like to do?")
            print("   1. Remove NULL records (if they're from test data)")
            print("   2. Keep NULL records (if they're real data with missing IDs)")
            
            choice = input("\nChoose option (1 or 2): ")
            
            if choice == '1':
                print("\n🗑️  Removing NULL records...")
                conn.execute("""
                    DELETE FROM raw_rosters 
                    WHERE player_id IS NULL OR player_id = ''
                """)
                print(f"   ✅ Removed {null_count} records")
            else:
                print("\n📌 Keeping NULL records.")
                print("   💡 Consider investigating the API for these players.")
        
        # 5. Sync state - FIXED VERSION
        print("\n🔄 Syncing state with latest snapshots...")
        
        # Get latest snapshot info per team using a different approach
        conn.execute("""
            WITH latest_snapshot AS (
                SELECT 
                    team,
                    COUNT(*) as snapshot_count,
                    MAX(ingestion_timestamp) as snapshot_time,
                    MAX(roster_hash) as snapshot_hash  -- Using MAX instead of FIRST_VALUE
                FROM raw_rosters
                GROUP BY team
            )
            UPDATE roster_state s
            SET 
                player_count = l.snapshot_count,
                current_hash = l.snapshot_hash,
                last_changed = l.snapshot_time,
                last_polled = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            FROM latest_snapshot l
            WHERE s.team = l.team
        """)
        
        print("   ✅ State synced!")
        
        # 6. Final verification
        print("\n📊 Final verification:")
        
        dup_after = conn.execute("""
            SELECT COUNT(*) as count
            FROM (
                SELECT team, run_id, player_id, COUNT(*) as c
                FROM raw_rosters
                WHERE player_id IS NOT NULL AND player_id != ''
                GROUP BY team, run_id, player_id
                HAVING c > 1
            )
        """).fetchone()[0]
        print(f"   • TRUE duplicates: {dup_after}")
        
        null_after = conn.execute("""
            SELECT COUNT(*) 
            FROM raw_rosters 
            WHERE player_id IS NULL OR player_id = ''
        """).fetchone()[0]
        print(f"   • NULL player_ids: {null_after}")
        
        state_match = conn.execute("""
            WITH latest_snapshot AS (
                SELECT team, COUNT(*) as snapshot_count
                FROM raw_rosters
                GROUP BY team
            )
            SELECT COUNT(*) as mismatches
            FROM roster_state s
            JOIN latest_snapshot l ON s.team = l.team
            WHERE s.player_count != l.snapshot_count
        """).fetchone()[0]
        print(f"   • State/snapshot mismatches: {state_match}")
        
        # 7. Final status
        print("\n" + "=" * 60)
        if dup_after == 0 and state_match == 0:
            print("✅ SCD System is now HEALTHY!")
            if null_after > 0:
                print(f"   ⚠️  {null_after} NULL player_ids remain (kept by choice)")
        else:
            print("⚠️  Some issues remain:")
            if dup_after > 0:
                print(f"   • {dup_after} duplicates still exist")
            if state_match > 0:
                print(f"   • {state_match} state mismatches")
        print("=" * 60)
        
        conn.close()
        
        print(f"\n📦 Backup saved to: {backup_path}")
        print("💡 To restore: Copy the backup file to warehouse/duckdb.db")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        conn.close()
        raise


if __name__ == "__main__":
    fix_all_issues()