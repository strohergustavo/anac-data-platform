<div align="center">

# 🥉 Bronze Layer

### Raw ingestion — zero transformation, all-string schema

</div>

---

## 📊 Tables

| Table | Source | Rows | Description |
|-------|--------|-----:|-------------|
| `bronze.vra` | VRA CSV (monthly) | 1,014,705 | Flight operations, all-string, with `_arquivo_origem` and `_ingerido_em` audit columns |
| `bronze.aerodromos` | AerodromosPublicos.csv | 496 | Brazilian public airport registry |
| `bronze.national_airlines` | pda_empresas_aereas_nacionais.csv | 729 | National airline registry |
| `bronze.foreign_airlines` | pda_empresas_aereas_estrangeiros.csv | 148 | Foreign airline registry |
| `bronze.operation_codes` | (seed) | 13 | DI and line-type code descriptions |

## 📓 Notebooks

| File | Description |
|------|-------------|
| `01_ingest_vra.py` | Reads all VRA CSVs from volume, loads as strings, applies Delta properties (deletion vectors, column mapping), sets UC tags and column comments |
| `02_ingest_reference_data.py` | Ingests aerodromes, national/foreign airlines, and seed operation codes |

## 📐 Conventions

- All columns preserved as `string` — no casting, no filtering
- Audit columns: `_arquivo_origem` (source file name), `_ingerido_em` (ingestion timestamp)
- Delta properties: `deletionVectors = true`, `columnMapping.mode = name`
- Full refresh: `mode("overwrite")` with `overwriteSchema = true`
- UC tags applied at ingestion: `layer`, `domain`, `source`, `grain`
- Column comments in Portuguese

## 🔗 Dependencies

- UC Volume: `/Volumes/airline_operations/bronze/data/VRA/*.csv`
- UC Volume: `/Volumes/airline_operations/bronze/data/references/*.csv`

<p align="right"><a href="#top">⬆ Back to top</a></p>
