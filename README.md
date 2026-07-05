# 🏒 NHL Slowly Changing Dimensions

### A Production Grade NHL Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.0-orange.svg)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5.4-yellow.svg)](https://duckdb.org/)

---

## Overview

This project builds a modern analytics pipeline around the NHL's public roster API.

Rather than simply downloading today's roster, the pipeline captures historical snapshots, detects roster changes, stores immutable bronze data, and builds a dimensional warehouse using **Slowly Changing Dimension (Type 2)** modeling.

## What It Solves

| Problem | Solution |
|---------|----------|
| "Who was on the roster on opening night?" | Point in time queries via SCD2 |
| "When did Caufield switch numbers" | Full player history tracking |
| "How many players has COL used this season?" | Historical roster counts |
| "Are we wasting storage on unchanged rosters?" | Hash based change detection |

---

## 🚀 Key Features

| Feature | Why It Matters |
|---------|----------------|
| **✅ Hash-based change detection** | Skip unchanged rosters → save storage & time |
| **✅ Immutable bronze snapshots** | Complete audit trail, never overwrite |
| **✅ DuckDB warehouse** | Embedded analytics, zero infrastructure |
| **✅ SCD Type 2 modeling** | Full player history (team, number, position) |
| **✅ dbt tests** | 12 tests ensuring data quality |
| **✅ Idempotent pipeline** | Safe to run daily, no duplicate data |



## Quick Start
```
    # 1. Clone
    git clone https://github.com/dec1costello/NHL-Yard-Sale.git
    cd nhl-slowly-changing-dimensions
    
    # 2. Setup
    uv venv && uv sync
    
    # 3. Run the pipeline
    python run_pipeline.py
    
    # 4. Run dbt
    cd dbt && dbt run && dbt test
```
> [!TIP]
> Use `uv run` before any Python command to guarantee execution with the locked environment. This ensures consistent Python versions and dependency trees across all machines.
