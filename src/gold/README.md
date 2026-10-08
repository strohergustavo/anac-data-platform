<div align="center">

# 🥇 Gold Layer

### Consumption-ready — dimensional model + OBT for AI & BI

</div>

---

## 📊 Tables

| Table | Pattern | Rows | Cols | Consumption | Description |
|-------|---------|-----:|-----:|-------------|-------------|
| `gold.fact_flights` | fact | 1,014,664 | 31 | bi | Flight steps with resolved airline names, op code descriptions, delay plausibility, punctuality flags |
| `gold.dim_airport` | dimension | 396 | 7 | bi | Unified airport dimension (origin + destination), with ANAC registry fallback |
| `gold.obt_flights` | obt | 1,014,664 | 39 | genie | Denormalized single-table view for NL2SQL — no joins needed |

## 📓 Notebooks

| File | Description |
|------|-------------|
| `01_gold_obt_flights.py` | Builds OBT by joining fact + dim, adding route columns and country derivation |
| `02_gold_dim_airport.py` | Builds airport dimension from all ICAO codes in VRA, LEFT JOIN to ANAC registry |
| `03_gold_fact_flights.py` | Builds fact table: deduplication (41 rows), airline resolution, delay checks, punctuality (15-min threshold) |
| `04_gold_governance.py` | Applies column comments, UC tags, runs validation queries (coverage, lineage, DQ) |

## 📋 Execution Order

1. `02_gold_dim_airport.py` — dimension first (no dependencies on fact)
2. `03_gold_fact_flights.py` — fact table (independent of dimension)
3. `01_gold_obt_flights.py` — OBT (joins fact + dimension)
4. `04_gold_governance.py` — governance (after all gold tables exist)

## 📐 Business Rules

- **Deduplication:** 41 exact duplicates removed via `ROW_NUMBER() OVER (PARTITION BY ...)`
- **Punctuality threshold:** 15 minutes (aligned with ANAC reporting standard)
- **Delay plausibility:** Values outside ±2h to +24h are nullified, not dropped
- **Foreign airport fallback:** `AEROPORTO FORA DO CADASTRO ANAC (ICAO)`
- **Unknown airline fallback:** `COMPANHIA NAO CADASTRADA (ICAO)`
- **Flight scope:** Derived from line type code (N/C = Domestico, I/G = Internacional)

## 🔗 Dependencies

- Silver layer must be complete (vra, airlines, aerodromes, operation_codes)
- SDP quarantine can exist but is not required for gold

<p align="right"><a href="#top">⬆ Back to top</a></p>
