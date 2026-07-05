# 🏒 NHL Slowly Changing Dimensions

### A Production Grade NHL Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.0-orange.svg)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5.4-yellow.svg)](https://duckdb.org/)

---

## 📋 Table of Contents
- [Overview](#overview)
- [Why This Project Matters](#why-this-project-matters)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Data Flow](#data-flow)
- [Dimensional Models](#dimensional-models)

---

## 🎯 Overview

This is a **production ready NHL data platform** that mirrors what an NHL Hockey Operations department would maintain. It ingests official NHL API data, applies change detection, stores immutable bronze snapshots, and builds a dimensional warehouse with SCD Type 2 tracking for historical player analysis.

> The pipeline demonstrates **software engineering best practices**, **data modeling excellence**, and **production-grade orchestration** all with a focused scope that's realistic to complete and easy to explain in an interview.

---

## 🤔 Why This Project Matters

### For NHL Teams
- **Single source of truth** for roster data
- **Historical tracking** of player movements, jersey changes, and position changes
- **Immutable audit trail** of every roster change
- **Daily automated updates** with zero manual intervention

### For Data Engineers
- **Layered architecture** (Bronze → DuckDB → dbt)
- **SCD Type 2 dimensional modeling** for historical tracking
- **Change detection** to avoid redundant storage
- **Clean separation of concerns** (extract, load, state, transform)

---

## 🚀 Key Features

### 🔄 Change Detection
- Computes MD5 hash of sorted player identifiers (name, number, position)
- Compares with stored hash in DuckDB
- Skips bronze write if unchanged → saves bandwidth and storage
- Ideal for daily runs where most rosters are stable

### 📸 Immutable Bronze Layer
- Partitioned Parquet storage (`season=YYYY/team=XXX/run_*.parquet`)
- Raw JSON snapshots for debugging and backfilling
- Metadata files with hash, timestamp, and file references
- Never overwrite existing files

### 👤 SCD Type 2 Player Dimension
- Tracks historical changes to: team, jersey number, position, height, weight, shoots/catches
- New row created when ANY attribute changes
- `effective_date` and `end_date` for point-in-time queries
- `is_current` flag for active players

### 🗄️ State Management in DuckDB
- `roster_state` table with current hash per team
- `roster_changes_log` table for audit trail
- No external Parquet files for state → single source of truth



### ✅ Data Quality
- 12 dbt tests all passing:
  - Unique constraints (`team_sk`, `player_sk`)
  - Not null constraints (required fields)
  - Referential integrity (team references)
  - Data type validation

---

## 💻 Tech Stack

### Core Technologies
| Component | Technology | Purpose |
|-----------|------------|---------|
| **Extraction** | Python 3.10+, Requests | NHL API client |
| **Storage** | DuckDB, Parquet | Data warehouse & bronze layer |
| **Transformations** | dbt-duckdb 1.7+ | Dimensional modeling |


---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- uv (or pip)

### Step 1: Clone and Set Up
```bash
git clone https://github.com/yourusername/nhl-ops-data-platform.git
cd nhl-ops-data-platform

# Create virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv sync


# run the pipeline 

# Run full pipeline with NHL API
python -c "
from src.extract.roster_api import NHLRosterAPI
api = NHLRosterAPI()
result = api.scrape_and_load_all_teams()
print(f\"✅ {result['summary']['total_teams']} teams, {result['summary']['total_players']} players\")
"

# Run dbt transformations
cd dbt
dbt run
dbt test
