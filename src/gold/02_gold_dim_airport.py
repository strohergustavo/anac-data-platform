# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold — Airport Dimension
# MAGIC %md
# MAGIC # Gold — Airport Dimension
# MAGIC
# MAGIC Builds `airline_operations.gold.dim_airport` from every distinct ICAO code present in the silver VRA table, left-joined to the ANAC aerodrome registry. Airports not in the ANAC cadastre (foreign airports) receive a fallback name and are flagged `in_anac_registry = false`. Serves both origin and destination of the fact table.

# COMMAND ----------

# DBTITLE 1,Logging setup
import logging, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.gold.dim_airport")
_start = time.time()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE airline_operations.gold.dim_airport AS
# MAGIC WITH fact_airports AS (
# MAGIC   SELECT DISTINCT icao_origin      AS icao FROM airline_operations.silver.vra WHERE icao_origin      IS NOT NULL AND icao_origin      <> ''
# MAGIC   UNION
# MAGIC   SELECT DISTINCT icao_destination AS icao FROM airline_operations.silver.vra WHERE icao_destination IS NOT NULL AND icao_destination <> ''
# MAGIC ),
# MAGIC
# MAGIC registry AS (
# MAGIC   SELECT icao, name, municipality, state_name, served_municipality, served_state_name
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY icao ORDER BY name) AS rn
# MAGIC     FROM airline_operations.silver.aerodromes
# MAGIC     WHERE icao IS NOT NULL AND icao <> ''
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   a.icao                                                              AS icao_airport,
# MAGIC
# MAGIC   COALESCE(c.name, concat('AEROPORTO FORA DO CADASTRO ANAC (', a.icao, ')'))
# MAGIC                                                                       AS airport_name,
# MAGIC   c.municipality                                                      AS airport_municipality,
# MAGIC   c.state_name                                                        AS airport_state,
# MAGIC   CASE WHEN a.icao RLIKE '^S[BDIJNSW]' THEN 'Brasil' ELSE 'Exterior' END
# MAGIC                                                                       AS airport_country,
# MAGIC   (c.icao IS NOT NULL)                                                AS in_anac_registry,
# MAGIC   current_timestamp()                                                 AS _processed_at
# MAGIC FROM fact_airports a
# MAGIC LEFT JOIN registry c ON a.icao = c.icao

# COMMAND ----------

# DBTITLE 1,Log result
_rows = spark.table("airline_operations.gold.dim_airport").count()
logger.info("gold.dim_airport: %s rows in %.1fs", f"{_rows:,}", time.time() - _start)
