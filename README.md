# 🏒 NHL Slowly Changing Dimensions

### A Production Grade NHL Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.0-orange.svg)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5.4-yellow.svg)](https://duckdb.org/)
---

## Overview

This project builds a modern analytics pipeline around the NHL's public roster API.

Rather than simply downloading today's roster, the pipeline captures historical snapshots, detects roster changes, stores immutable bronze data, and builds a dimensional warehouse using **Slowly Changing Dimension (Type 2)** modeling.

```mermaid

flowchart TB
    subgraph EXTRACT["📡 EXTRACT"]
        API[("NHL API")]
        RosterAPI[Orchestrator]
    end

    subgraph PROCESS["⚙️ PROCESS"]
        SM[State Manager<br/>Change Detection]
        BL[Bronze Writer<br/>Immutable Storage]
        DL[DuckDB Loader<br/>Append-Only]
    end

    subgraph STORE["💾 STORE"]
        StateDB[(roster_state<br/>last_polled ⏰<br/>last_changed 🔄)]
        Bronze[(Parquet Files)]
        Raw[(raw_rosters)]
    end

    subgraph DBT["📈 DBT"]
        DimPlayer[(dim_player<br/>SCD Type 2)]
        DimTeam[(dim_team)]
    end

    subgraph MONITOR["🔍 MONITOR"]
        Diagnose[Health Check]
        Verify[Verification]
    end

    API -->|Fetch| RosterAPI
    RosterAPI -->|Hash| SM
    SM -->|Changed?| BL
    BL -->|Write| Bronze
    RosterAPI -->|Load| DL
    DL -->|Insert| Raw
    Raw -->|Transform| DimPlayer
    Raw -->|Transform| DimTeam
    StateDB -.->|Monitor| Diagnose
    Raw -.->|Monitor| Verify
```


## What It Solves

| Problem | Solution |
|---------|----------|
| "Who was on the roster on opening night?" | Point in time queries via SCD2 |
| "When did a player switch numbers" | Full player history tracking |
| "How many players has Chicago used this season?" | Historical roster counts |
| "Are we wasting storage on unchanged rosters?" | Hash based change detection |

## 🚀 Key Features

| Feature | Why It Matters |
|---------|----------------|
| **✅ Hash-based change detection** | Skip unchanged rosters → save storage & time |
| **✅ Immutable bronze snapshots** | Complete audit trail, never overwrite |
| **✅ DuckDB warehouse** | Embedded analytics, zero infrastructure |
| **✅ SCD Type 2 modeling** | Full player history (team, number, position) |
| **✅ dbt tests** | 12 tests ensuring data quality |

## Pipeline Breakdown

```mermaid
flowchart LR
    A[🏒 NHL API] --> B[📡 Fetch Roster]
    B --> C{🧮 Hash<br/>Changed?}
    C -->|No| D[⏭️ Skip<br/>Update last_polled]
    C -->|Yes| E[💾 Save Bronze<br/>Parquet + JSON]
    E --> F[🗄️ Load to DuckDB]
    F --> G[📝 Update State<br/>last_changed = NOW]
    D --> H[✅ Ready]
    G --> H
    H --> I[🏁 All 32 Teams]

    style A fill:#4A90D9,color:#fff
    style B fill:#50C878,color:#fff
    style C fill:#FFD700,color:#333
    style D fill:#D3D3D3,color:#333
    style E fill:#FF6B6B,color:#fff
    style F fill:#9B59B6,color:#fff
    style G fill:#F39C12,color:#fff
    style H fill:#2ECC71,color:#fff
    style I fill:#1ABC9C,color:#fff
```

run_pipeline.py kicks off the entire process by telling roster_api.py to scrape all 32 NHL teams. For each team, client.py handles the heavy lifting fetching the JSON roster data from the NHL API with automatic retries if anything fails.

Once the raw JSON arrives, roster_api.py transforms it into a clean list of player records. Then state_manager.py steps in: it computes a unique hash of the roster and checks DuckDB for the previous version. If the hashes match, nothing has changed, so it skips the team entirely and only updates `last_polled` (when we checked). If they differ, that means the roster has updated.

When a change is detected, bronze.py writes an immutable snapshot as Parquet + JSON to a folder organized by season and team. duckdb_loader.py appends the new data to raw_rosters, and state_manager.py updates the stored hash, `last_polled`, and `last_changed` (when the data actually changed) for future comparisons. This cycle repeats for all 32 teams, leaving you with a complete, auditable historical record of every roster change.

| Module | What It Does | Key Decision |
|--------|-------------|--------------|
| `client.py` | Generic HTTP client with retries, timeouts, and session management | Exponential backoff (2^attempt) for transient failures; single `_get()` method reused across all endpoints |
| `roster_api.py` | Orchestrates full pipeline: fetch → parse → bronze → DuckDB → state | `scrape_and_load_all_teams()` is the single entry point; calls every module in sequence |
| `state_manager.py` | Computes MD5 roster hash, checks DuckDB for previous hash, updates state | Tracks `last_polled` (when checked) vs `last_changed` (when data changed); state lives in DuckDB |
| `bronze.py` | Writes immutable Parquet snapshots + JSON + metadata | **ONLY** writes bronze (no DuckDB loading); partitioned storage `season=YYYY/team=XXX/run_*.parquet` |
| `duckdb_loader.py` | Appends roster data to `raw_rosters` table; manages schema initialization | Append-only loading; called **ONCE** per team; indexes on `team`, `season`, `run_id`, `player_id` |
| `config.py` | Auto detects NHL season (2025 → 20252026); manages all paths | Season detection logic: July–September = previous season, October–June = current season |
| `logging.py` | Structured logging with timestamps, levels, and file output | All modules share same logger; logs written to `data/logs/` |



## DuckDB Tables
| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `raw_rosters` | Append-only historical data | team, season, run_id, player_id, player, number, position, roster_hash, ingestion_timestamp |
| `roster_state` | Current state per team | team, season, current_hash, **last_polled** ⏰ (when checked), **last_changed** 🔄 (when data changed), player_count |
| `roster_changes_log` | Audit trail of changes | run_id, team, changed, current_hash, previous_hash, timestamp |
| `v_roster_monitoring` | Monitoring view | team, last_checked, last_changed, status, hours_since_check, days_since_change |
| `main_marts.dim_team` | Team dimension | dbt run |
| `main_marts.dim_player` | SCD Type 2 player history | dbt run |

```mermaid
sequenceDiagram
    participant RP as Pipeline
    participant RA as RosterAPI
    participant SM as StateManager
    participant BL as BronzeLoader
    participant DL as DuckDBLoader

    RP->>RA: Process all teams
    loop Each Team
        RA->>RA: Fetch & Parse
        RA->>SM: compute_hash()
        SM-->>RA: hash
        
        RA->>SM: get_state()
        SM-->>RA: current_state
        
        alt Hash Changed
            RA->>BL: write_bronze()
            BL-->>RA: done
            
            RA->>DL: load_to_duckdb()
            DL-->>RA: done
            
            RA->>SM: update_state(changed=True)
            SM->>SM: last_polled = NOW()<br/>last_changed = NOW()
        else No Change
            RA->>SM: update_state(changed=False)
            SM->>SM: last_polled = NOW()<br/>last_changed unchanged
        end
    end
    RA-->>RP: Complete
```
