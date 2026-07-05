




# 🏒 NHL Ops Data Platform

### A Production-Grade NHL Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.0-orange.svg)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.11.0-blue.svg)](https://airflow.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5.4-yellow.svg)](https://duckdb.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

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
- [Airflow Orchestration](#airflow-orchestration)
- [Performance Metrics](#performance-metrics)
- [Future Extensions](#future-extensions)
- [About The Developer](#about-the-developer)

---

## 🎯 Overview

This is a **production-ready NHL data platform** that mirrors what an NHL Hockey Operations department would maintain. It ingests official NHL API data, applies change detection, stores immutable bronze snapshots, and builds a dimensional warehouse with SCD Type 2 tracking for historical player analysis.

> "This is not a scraper project. This is a data platform."

The pipeline demonstrates **software engineering best practices**, **data modeling excellence**, and **production-grade orchestration**—all with a focused scope that's realistic to complete and easy to explain in an interview.

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
- **Production orchestration** with Airflow
- **Clean separation of concerns** (extract, load, state, transform)

---

## 🏗️ Architecture




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

### ⏰ Airflow Orchestration
- Daily DAG runs at 6 AM UTC
- Idempotent tasks with retry logic
- Dockerized for consistent execution
- Email notifications on success/failure

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
| **Orchestration** | Apache Airflow 2.11+ | Pipeline scheduling |
| **Containerization** | Docker, Docker Compose | Consistent execution |


---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Docker Desktop
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


# start airflow

# Start Airflow services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080  # Username: admin, Password: admin

# Trigger the DAG manually
# Click the play button on nhl_roster_pipeline
















# Dir Structure

        nhl-ops-data-platform/
        │
        ├── 📁 airflow/
        │   └── 📁 dags/
        │       └── roster_pipeline.py          # Airflow DAG
        │
        ├── 📁 config/
        │   └── teams.txt                       # NHL team abbreviations
        │
        ├── 📁 data/
        │   ├── 📁 bronze/
        │   │   └── 📁 rosters/                 # Parquet files go here
        │   ├── 📁 logs/                        # Pipeline logs
        │   └── 📁 silver/                      # Silver layer outputs
        │
        ├── 📁 dbt/
        │   ├── 📁 models/
        │   │   ├── 📁 marts/
        │   │   │   ├── dim_team.sql
        │   │   │   ├── dim_player.sql
        │   │   │   └── schema.yml
        │   │   └── 📁 staging/
        │   │       └── stg_rosters.sql
        │   ├── dbt_project.yml                 # dbt configuration
        │   └── profiles.yml                    # dbt profiles
        │
        ├── 📁 src/
        │   ├── 📁 extract/
        │   │   └── scraper.py                  # Web scraping logic
        │   ├── 📁 compare/
        │   │   └── hash_compare.py             # Change detection
        │   ├── 📁 load/
        │   │   ├── bronze.py                   # Bronze layer writes
        │   │   └── duckdb_loader.py            # DuckDB operations
        │   ├── 📁 utils/
        │   │   └── logging.py                  # Logging utilities
        │   ├── config.py                       # Configuration
        │   └── cli.py                          # CLI entry point
        │
        ├── 📁 warehouse/
        │   └── duckdb.db                       # DuckDB database file
        │
        ├── .env                                # Environment variables
        ├── .gitignore
        ├── docker-compose.yml                  # Airflow services
        ├── Dockerfile                          # Airflow container
        ├── pyproject.toml                      # Project dependencies
        ├── setup.sh                            # Setup script
        └── README.md                           # Documentation