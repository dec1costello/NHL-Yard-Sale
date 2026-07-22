# save as check_impact.py and run it
import duckdb
from pathlib import Path
import re

conn = duckdb.connect(str(Path("warehouse/duckdb.db")))

print("🔍 IMPACT ANALYSIS: Changing roster_state schema")
print("=" * 60)

# 1. Check what queries use roster_state
print("\n📊 1. What references roster_state?")
print("-" * 40)

# Check dbt models
dbt_models = Path("dbt/models")
if dbt_models.exists():
    for model_file in dbt_models.rglob("*.sql"):
        content = model_file.read_text()
        if "roster_state" in content.lower():
            print(f"   ⚠️  dbt model uses it: {model_file}")
            # Show the lines
            for i, line in enumerate(content.split('\n')):
                if 'roster_state' in line.lower():
                    print(f"      Line {i+1}: {line.strip()}")

# 2. Check Python code
print("\n📊 2. What Python code references roster_state?")
print("-" * 40)
python_files = list(Path("src").rglob("*.py")) + list(Path(".").glob("*.py"))
for py_file in python_files:
    if py_file.name == "check_impact.py":
        continue
    try:
        content = py_file.read_text()
        if "roster_state" in content:
            print(f"   📄 {py_file}")
            # Show context
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if 'roster_state' in line:
                    print(f"      Line {i+1}: {line.strip()}")
    except:
        pass

# 3. Check current schema
print("\n📊 3. Current roster_state schema:")
print("-" * 40)
schema = conn.execute("DESCRIBE roster_state").df()
print(schema.to_string(index=False))

# 4. Check if any views or materialized views depend on it
print("\n📊 4. Check for views using roster_state:")
print("-" * 40)
views = conn.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_type = 'VIEW' 
    AND table_schema = 'main'
""").df()

for view in views['table_name']:
    try:
        view_def = conn.execute(f"SELECT sql FROM sqlite_master WHERE type='view' AND name='{view}'").df()
        if 'roster_state' in view_def.to_string().lower():
            print(f"   ⚠️  View depends on it: {view}")
    except:
        pass

conn.close()

print("\n" + "=" * 60)
print("✅ Impact analysis complete!")
print("📝 Check for any dbt models or views above that need updating.")