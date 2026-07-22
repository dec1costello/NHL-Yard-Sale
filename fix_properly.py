"""Fix TRUE duplicates using ROW_NUMBER()."""

import duckdb
from pathlib import Path
import shutil
from datetime import datetime


def fix_duplicates_properly():
    """Remove duplicates using ROW_NUMBER()."""
    
    # 1. Backup
    print("📦 Creating backup...")
    backup_path = f'warehouse/duckdb_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
    shutil.copy2('warehouse/duckdb.db', backup_path)
    print(f"   Backup saved to: {backup_path}")
    
    conn = duckdb.connect('warehouse/duckdb.db')
    
    try:
        # 2. Check current duplicates
        print("\n📊 Checking for duplicates...")
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
        print(f"   Found {dup_count} duplicate groups")
        
        if dup_count == 0:
            print("   ✅ No duplicates to fix!")
            return
        
        # 3. Show sample of duplicates
        print("\n🔍 Sample duplicates:")
        sample = conn.execute("""
            SELECT 
                team,
                run_id,
                player_id,
                COUNT(*) as dup_count,
                MIN(ingestion_timestamp) as first_time,
                MAX(ingestion_timestamp) as last_time
            FROM raw_rosters
            WHERE player_id IS NOT NULL AND player_id != ''
            GROUP BY team, run_id, player_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """).df()
        print(sample.to_string(index=False))
        
        # 4. Fix duplicates using ROW_NUMBER
        print("\n🔧 Removing duplicates using ROW_NUMBER()...")
        
        # Create a temp table with row numbers
        conn.execute("""
            CREATE TEMP TABLE deduped AS
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY team, run_id, player_id 
                    ORDER BY ingestion_timestamp
                ) as rn
            FROM raw_rosters
            WHERE player_id IS NOT NULL AND player_id != ''
        """)
        
        # Keep only the first occurrence of each duplicate
        conn.execute("""
            CREATE TEMP TABLE clean_rosters AS
            SELECT 
                team, season, run_id, ingestion_timestamp, source,
                player_id, player_url, number, player, position, age,
                height, weight, shoots_catches, experience, birth_date,
                summary, roster_hash, bronze_loaded_at, load_timestamp
            FROM deduped
            WHERE rn = 1
        """)
        
        # Add back NULL records
        conn.execute("""
            INSERT INTO clean_rosters
            SELECT *
            FROM raw_rosters
            WHERE player_id IS NULL OR player_id = ''
        """)
        
        # Replace raw_rosters
        conn.execute("DROP TABLE raw_rosters")
        conn.execute("""
            CREATE TABLE raw_rosters AS 
            SELECT * FROM clean_rosters
        """)
        
        # Clean up
        conn.execute("DROP TABLE IF EXISTS deduped")
        conn.execute("DROP TABLE IF EXISTS clean_rosters")
        
        print("   ✅ Duplicates removed!")
        
        # 5. Verify
        print("\n📊 Verification:")
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
        print(f"   • TRUE duplicates remaining: {dup_after}")
        
        total_records = conn.execute("SELECT COUNT(*) FROM raw_rosters").fetchone()[0]
        print(f"   • Total records: {total_records}")
        
        # 6. Update state
        print("\n🔄 Updating state...")
        conn.execute("""
            WITH latest_snapshot AS (
                SELECT 
                    team,
                    COUNT(*) as snapshot_count,
                    MAX(ingestion_timestamp) as snapshot_time,
                    MAX(roster_hash) as snapshot_hash
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
        print("   ✅ State updated!")
        
        # 7. Final status
        print("\n" + "=" * 60)
        if dup_after == 0:
            print("✅ SCD System is now HEALTHY!")
        else:
            print(f"⚠️  {dup_after} duplicates still remain")
        print("=" * 60)
        
        conn.close()
        print(f"\n📦 Backup saved to: {backup_path}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        conn.close()
        raise


if __name__ == "__main__":
    fix_duplicates_properly()