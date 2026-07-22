"""
Migration: 002_cleanup_duplicate_columns (FIXED)

What this does:
1. Drops redundant last_checked (duplicate of last_ingested)
2. Renames last_ingested → last_polled (clearer name)
3. Ensures last_changed is properly populated
4. Creates helper view for monitoring
"""

import duckdb
from pathlib import Path
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CleanupMigration:
    def __init__(self):
        self.db_path = Path("warehouse/duckdb.db")
        self.backup_path = Path("warehouse/duckdb_backup.db")
        
        # Don't auto-create backup - let user handle it
        if not self.backup_path.exists():
            logger.warning("⚠️  No backup found! Please create one first:")
            logger.warning("   Copy-Item warehouse\\duckdb.db warehouse\\duckdb_backup.db")
            sys.exit(1)
        
        logger.info(f"✅ Backup verified: {self.backup_path}")
    
    def run(self):
        """Execute cleanup migration."""
        conn = duckdb.connect(str(self.db_path))
        
        try:
            conn.execute("BEGIN TRANSACTION")
            logger.info("🔄 Starting cleanup migration...")
            
            # 1. Check current columns
            cols = conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'roster_state'
                AND table_schema = 'main'
                ORDER BY column_name
            """).fetchall()
            
            col_names = [c[0] for c in cols]
            logger.info(f"📊 Current columns: {col_names}")
            
            # 2. Drop redundant last_checked (if it exists)
            if 'last_checked' in col_names:
                logger.info("🗑️  Dropping redundant last_checked column...")
                conn.execute("""
                    ALTER TABLE roster_state 
                    DROP COLUMN last_checked
                """)
                logger.info("✅ Removed last_checked")
            
            # 3. Rename last_ingested → last_polled (if it exists)
            if 'last_ingested' in col_names:
                logger.info("🔄 Renaming last_ingested → last_polled...")
                conn.execute("""
                    ALTER TABLE roster_state 
                    RENAME COLUMN last_ingested TO last_polled
                """)
                logger.info("✅ Renamed to last_polled")
            
            # 4. Ensure last_changed exists (it does, but just in case)
            if 'last_changed' not in col_names:
                logger.info("➕ Adding last_changed column...")
                conn.execute("""
                    ALTER TABLE roster_state 
                    ADD COLUMN last_changed TIMESTAMP
                """)
            
            # 5. Backfill last_changed where NULL
            logger.info("📝 Backfilling last_changed...")
            conn.execute("""
                UPDATE roster_state 
                SET last_changed = COALESCE(last_polled, CURRENT_TIMESTAMP)
                WHERE last_changed IS NULL
            """)
            
            # 6. Verify no NULLs
            null_count = conn.execute("""
                SELECT COUNT(*) 
                FROM roster_state 
                WHERE last_changed IS NULL
            """).fetchone()[0]
            
            if null_count > 0:
                logger.warning(f"⚠️  {null_count} rows have NULL last_changed")
                conn.execute("""
                    UPDATE roster_state 
                    SET last_changed = CURRENT_TIMESTAMP 
                    WHERE last_changed IS NULL
                """)
            
            # 7. Drop existing view if it exists (to avoid conflicts)
            logger.info("🗑️  Dropping existing view if it exists...")
            conn.execute("DROP VIEW IF EXISTS v_roster_monitoring")
            
            # 8. Create helper view (FIXED - proper column references)
            logger.info("📊 Creating monitoring view...")
            conn.execute("""
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
            
            # 9. Validate final schema
            final_cols = conn.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'roster_state'
                AND table_schema = 'main'
            """).fetchall()
            
            logger.info(f"✅ Final columns: {[c[0] for c in final_cols]}")
            
            # 10. Show sample data (FIXED - query the view instead)
            logger.info("\n📊 Sample data after migration:")
            sample = conn.execute("""
                SELECT 
                    team,
                    last_checked,
                    last_changed,
                    status,
                    player_count
                FROM v_roster_monitoring
                LIMIT 5
            """).df()
            print(sample.to_string(index=False))
            
            # Commit transaction
            conn.execute("COMMIT")
            logger.info("✅ Migration completed successfully!")
            
            # 11. Show summary
            summary = conn.execute("""
                SELECT 
                    COUNT(*) as total_teams,
                    COUNT(CASE WHEN last_polled = last_changed THEN 1 END) as no_changes,
                    COUNT(CASE WHEN last_polled > last_changed THEN 1 END) as had_changes
                FROM roster_state
            """).fetchone()
            
            logger.info(f"\n📈 Summary:")
            logger.info(f"   Total teams: {summary[0]}")
            logger.info(f"   Teams with no changes: {summary[1]}")
            logger.info(f"   Teams with changes: {summary[2]}")
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            conn.execute("ROLLBACK")
            logger.info("🔄 Rolled back transaction")
            raise
        
        finally:
            conn.close()

if __name__ == "__main__":
    migration = CleanupMigration()
    migration.run()