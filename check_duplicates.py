"""Check TRUE duplicates with full run_id."""

import duckdb

conn = duckdb.connect('warehouse/duckdb.db')

print("🔍 Checking for TRUE duplicates (same player, same run_id):")
print("=" * 60)

# Check if there are actually any duplicates
result = conn.execute("""
    SELECT 
        team,
        run_id,
        player_id,
        COUNT(*) as dup_count,
        MIN(ingestion_timestamp) as first_ingest,
        MAX(ingestion_timestamp) as last_ingest
    FROM raw_rosters
    WHERE player_id IS NOT NULL AND player_id != ''
    GROUP BY team, run_id, player_id
    HAVING COUNT(*) > 1
    ORDER BY team, run_id
    LIMIT 10
""").df()

if len(result) == 0:
    print("✅ No TRUE duplicates found!")
else:
    print(f"⚠️  Found {len(result)} duplicates")
    for _, row in result.iterrows():
        print(f"\n  Team: {row['team']}")
        print(f"  Run ID: {row['run_id']}")
        print(f"  Player ID: {row['player_id']}")
        print(f"  Count: {row['dup_count']}")
        print(f"  First ingest: {row['first_ingest']}")
        print(f"  Last ingest: {row['last_ingest']}")

# Also check the ANA run that was flagged
print("\n" + "=" * 60)
print("🔍 Checking ANA run 50340ecb...")

ana_check = conn.execute("""
    SELECT 
        run_id,
        COUNT(*) as total_records,
        COUNT(DISTINCT player_id) as unique_players,
        MAX(ingestion_timestamp) as ingest_time
    FROM raw_rosters
    WHERE team = 'ANA' 
    AND run_id LIKE '50340ecb%'
    GROUP BY run_id
""").df()

print(ana_check.to_string(index=False))

conn.close()