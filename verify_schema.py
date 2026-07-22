"""Verify the database schema after cleanup."""

import duckdb
from pathlib import Path

def verify_schema():
    """Check that the schema is correct."""
    conn = duckdb.connect('warehouse/duckdb.db')
    
    print('📊 Tables in database:')
    print('=' * 40)
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").df()
    print(tables.to_string(index=False))
    
    print('\n📊 roster_state columns:')
    print('=' * 40)
    columns = conn.execute('DESCRIBE roster_state').df()
    print(columns.to_string(index=False))
    
    print('\n📊 Sample data:')
    print('=' * 40)
    sample = conn.execute("""
        SELECT 
            team,
            season,
            last_polled,
            last_changed,
            player_count
        FROM roster_state
        LIMIT 5
    """).df()
    print(sample.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    verify_schema()