# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold — Airport Dimension
# MAGIC %md
# MAGIC # Gold — Airport Dimension
# MAGIC
# MAGIC Builds `<catalog>.gold.dim_airport` from every distinct ICAO code present in the silver VRA table, left-joined to the ANAC aerodrome registry. Airports not in the ANAC cadastre (foreign airports) receive a fallback name and are flagged `in_anac_registry = false`. Serves both origin and destination of the fact table.

# COMMAND ----------

# MAGIC %run ../common/setup

# COMMAND ----------

# DBTITLE 1,Logging setup
logger = get_logger("gold.dim_airport")
_start = time.time()

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_airport AS
WITH fact_airports AS (
  SELECT DISTINCT icao_origin      AS icao FROM {CATALOG}.silver.vra WHERE icao_origin      IS NOT NULL AND icao_origin      <> ''
  UNION
  SELECT DISTINCT icao_destination AS icao FROM {CATALOG}.silver.vra WHERE icao_destination IS NOT NULL AND icao_destination <> ''
),

registry AS (
  SELECT icao, name, municipality, state_name, served_municipality, served_state_name
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY icao ORDER BY name) AS rn
    FROM {CATALOG}.silver.aerodromes
    WHERE icao IS NOT NULL AND icao <> ''
  )
  WHERE rn = 1
)
SELECT
  a.icao                                                              AS icao_airport,

  COALESCE(c.name, concat('AEROPORTO FORA DO CADASTRO ANAC (', a.icao, ')'))
                                                                      AS airport_name,
  c.municipality                                                      AS airport_municipality,
  c.state_name                                                        AS airport_state,
  CASE WHEN a.icao RLIKE '^S[BDIJNSW]' THEN 'Brasil' ELSE 'Exterior' END
                                                                      AS airport_country,
  (c.icao IS NOT NULL)                                                AS in_anac_registry,
  current_timestamp()                                                 AS _processed_at
FROM fact_airports a
LEFT JOIN registry c ON a.icao = c.icao
""")

# COMMAND ----------

# DBTITLE 1,Log result
_rows = spark.table(f"{CATALOG}.gold.dim_airport").count()
logger.info("gold.dim_airport: %s rows in %.1fs", f"{_rows:,}", time.time() - _start)
