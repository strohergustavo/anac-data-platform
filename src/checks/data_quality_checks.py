# Databricks notebook source
# DBTITLE 1,Checks — Post-load Data Quality Gate
# MAGIC %md
# MAGIC # Checks — Post-load Data Quality Gate
# MAGIC
# MAGIC Last task of the job. Verifies the invariants the pipeline promises across layers and **fails the run** if any of them break, so a bad load never goes unnoticed. Read-only: it changes no table.

# COMMAND ----------

import logging, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.checks")
_start = time.time()

CATALOG = "airline_operations"
DEDUP_KEYS = [
    "icao_airline", "flight_number", "di_code", "line_type_code", "icao_origin", "icao_destination",
    "scheduled_departure", "actual_departure", "scheduled_arrival", "actual_arrival", "flight_status",
]

def scalar(query):
    return spark.sql(query).collect()[0][0]

def count(table):
    return spark.table(f"{CATALOG}.{table}").count()

results = []

def check(name, passed, detail):
    results.append((name, bool(passed), detail))

# COMMAND ----------

bronze_vra = count("bronze.vra")
silver_vra = count("silver.vra")
fact = count("gold.fact_flights")
obt = count("gold.obt_flights")
quarantine = count("silver.vra_quarentena")
keys = ", ".join(DEDUP_KEYS)
exact_duplicates = scalar(f"""
    SELECT COALESCE(SUM(n - 1), 0)
    FROM (SELECT COUNT(*) AS n FROM {CATALOG}.silver.vra GROUP BY {keys})
""")

check("bronze is not empty", bronze_vra > 0, f"bronze.vra = {bronze_vra:,}")
check("silver mirrors bronze", silver_vra == bronze_vra, f"bronze {bronze_vra:,} vs silver {silver_vra:,}")
check("gold drops only exact duplicates", fact == silver_vra - exact_duplicates,
      f"silver {silver_vra:,} - duplicates {exact_duplicates:,} vs fact {fact:,}")
check("OBT keeps every fact row", obt == fact, f"fact {fact:,} vs obt {obt:,}")
check("quarantine is diagnostic, never the whole table", 0 < quarantine < silver_vra,
      f"quarantine {quarantine:,} of {silver_vra:,}")

fact_dupes = scalar(f"""
    SELECT COUNT(*) FROM (
      SELECT 1 FROM {CATALOG}.gold.fact_flights GROUP BY {keys} HAVING COUNT(*) > 1
    )
""")
check("fact grain is unique", fact_dupes == 0, f"{fact_dupes} duplicated keys")

orphan_airports = scalar(f"""
    SELECT COUNT(*) FROM {CATALOG}.gold.fact_flights f
    LEFT ANTI JOIN {CATALOG}.gold.dim_airport d ON f.icao_origin = d.icao_airport
    WHERE f.icao_origin IS NOT NULL AND f.icao_origin <> ''
""")
check("every origin airport exists in dim_airport", orphan_airports == 0, f"{orphan_airports} orphan rows")

bad_punctuality = scalar(f"""
    SELECT COUNT(*) FROM {CATALOG}.gold.fact_flights
    WHERE (departure_punctual AND departure_delay_min > 15)
       OR (NOT departure_punctual AND departure_delay_min <= 15)
""")
check("punctuality follows the 15 minute rule", bad_punctuality == 0, f"{bad_punctuality} inconsistent rows")

untagged = scalar(f"""
    SELECT COUNT(*) FROM {CATALOG}.information_schema.tables t
    WHERE t.table_schema = 'gold'
      AND t.table_name IN ('fact_flights', 'dim_airport', 'obt_flights')
      AND NOT EXISTS (
        SELECT 1 FROM {CATALOG}.information_schema.table_tags g
        WHERE g.schema_name = 'gold' AND g.table_name = t.table_name AND g.tag_name = 'consumption'
      )
""")
check("gold tables carry the consumption tag", untagged == 0, f"{untagged} untagged tables")

# COMMAND ----------

for name, passed, detail in results:
    level = logging.INFO if passed else logging.ERROR
    logger.log(level, "%s  %s  (%s)", 'PASS' if passed else 'FAIL', name, detail)

failed = [name for name, passed, _ in results if not passed]
if failed:
    logger.error("%d data quality check(s) failed: %s", len(failed), ', '.join(failed))
    raise AssertionError(f"{len(failed)} data quality check(s) failed: {', '.join(failed)}")
logger.info("All %d checks passed in %.1fs", len(results), time.time() - _start)