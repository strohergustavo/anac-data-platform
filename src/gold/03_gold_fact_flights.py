# Databricks notebook source
# DBTITLE 1,Gold — Flight Fact Table
# MAGIC %md
# MAGIC # Gold — Flight Fact Table
# MAGIC
# MAGIC Builds `airline_operations.gold.fact_flights` from `silver.vra` with exact deduplication (41 duplicate rows removed), airline name resolution, operation-code descriptions, domestic/international scope derivation, delay plausibility checks (±2 h to ±24 h), and the project's 15-minute punctuality metric. One row per flight step. This is where business rules live.

# COMMAND ----------

# DBTITLE 1,Logging setup
import logging, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.gold.fact_flights")
_start = time.time()

# COMMAND ----------

# DBTITLE 1,Log result
_rows = spark.table("airline_operations.gold.fact_flights").count()
logger.info("gold.fact_flights: %s rows in %.1fs", f"{_rows:,}", time.time() - _start)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE airline_operations.gold.fact_flights AS
# MAGIC WITH vra_deduplicated AS (
# MAGIC   SELECT * FROM (
# MAGIC     SELECT *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY icao_airline, flight_number, di_code, line_type_code,
# MAGIC                      icao_origin, icao_destination, scheduled_departure, actual_departure,
# MAGIC                      scheduled_arrival, actual_arrival, flight_status
# MAGIC         ORDER BY _ingerido_em
# MAGIC       ) AS _rn
# MAGIC     FROM airline_operations.silver.vra
# MAGIC   )
# MAGIC   WHERE _rn = 1
# MAGIC ),
# MAGIC airline AS (
# MAGIC   SELECT icao, legal_name, registry_origin
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (
# MAGIC       PARTITION BY icao
# MAGIC       ORDER BY CASE WHEN status = 'ATIVA' THEN 0 ELSE 1 END, legal_name
# MAGIC     ) AS rn
# MAGIC     FROM airline_operations.silver.airlines
# MAGIC     WHERE icao IS NOT NULL AND icao <> ''
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC di AS (
# MAGIC   SELECT code, description FROM airline_operations.silver.operation_codes WHERE domain = 'di_code'
# MAGIC ),
# MAGIC line_type AS (
# MAGIC   SELECT code, description FROM airline_operations.silver.operation_codes WHERE domain = 'line_type_code'
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     v.*,
# MAGIC     (v.departure_delay_min IS NOT NULL AND (v.departure_delay_min < -120 OR v.departure_delay_min > 1440))
# MAGIC       OR (v.arrival_delay_min IS NOT NULL AND (v.arrival_delay_min < -120 OR v.arrival_delay_min > 1440))
# MAGIC                                                                     AS delay_out_of_range
# MAGIC   FROM vra_deduplicated v
# MAGIC )
# MAGIC SELECT
# MAGIC   b.icao_airline,
# MAGIC   COALESCE(e.legal_name, concat('COMPANHIA NAO CADASTRADA (', b.icao_airline, ')'))
# MAGIC                                                                     AS airline_name,
# MAGIC   e.registry_origin                                                AS airline_registry,
# MAGIC   b.flight_number,
# MAGIC
# MAGIC   b.di_code,
# MAGIC   COALESCE(d.description, concat('Codigo nao catalogado (', b.di_code, ')'))
# MAGIC                                                                     AS di_description,
# MAGIC   b.line_type_code,
# MAGIC   COALESCE(t.description, concat('Codigo nao catalogado (', b.line_type_code, ')'))
# MAGIC                                                                     AS line_type_description,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN b.line_type_code IN ('N', 'C') THEN 'Domestico'
# MAGIC     WHEN b.line_type_code IN ('I', 'G') THEN 'Internacional'
# MAGIC     ELSE 'Nao classificado'
# MAGIC   END                                                               AS flight_scope,
# MAGIC
# MAGIC   b.icao_origin,
# MAGIC   b.icao_destination,
# MAGIC   concat(b.icao_origin, ' - ', b.icao_destination)                 AS route,
# MAGIC
# MAGIC   b.scheduled_departure,
# MAGIC   b.scheduled_departure_date,
# MAGIC   b.scheduled_departure_time,
# MAGIC   hour(b.scheduled_departure)                                      AS scheduled_departure_hour,
# MAGIC   CASE dayofweek(b.scheduled_departure_date)
# MAGIC     WHEN 1 THEN 'domingo'  WHEN 2 THEN 'segunda' WHEN 3 THEN 'terca'
# MAGIC     WHEN 4 THEN 'quarta'   WHEN 5 THEN 'quinta'  WHEN 6 THEN 'sexta'
# MAGIC     WHEN 7 THEN 'sabado'
# MAGIC   END                                                               AS day_of_week,
# MAGIC   date_trunc('MONTH', b.scheduled_departure_date)                  AS reference_month,
# MAGIC   b.actual_departure,
# MAGIC   b.scheduled_arrival,
# MAGIC   b.actual_arrival,
# MAGIC
# MAGIC   CASE WHEN b.delay_out_of_range THEN NULL ELSE b.departure_delay_min  END AS departure_delay_min,
# MAGIC   CASE WHEN b.delay_out_of_range THEN NULL ELSE b.arrival_delay_min    END AS arrival_delay_min,
# MAGIC   CASE WHEN b.delay_out_of_range THEN NULL ELSE b.minutes_recovered     END AS minutes_recovered,
# MAGIC   b.delay_out_of_range,
# MAGIC
# MAGIC   CASE WHEN b.delay_out_of_range OR b.departure_delay_min IS NULL THEN NULL
# MAGIC        ELSE b.departure_delay_min <= 15 END                        AS departure_punctual,
# MAGIC   CASE WHEN b.delay_out_of_range OR b.arrival_delay_min IS NULL THEN NULL
# MAGIC        ELSE b.arrival_delay_min <= 15 END                          AS arrival_punctual,
# MAGIC
# MAGIC   b.flight_status,
# MAGIC   (b.flight_status = 'CANCELADO')                                  AS flight_cancelled,
# MAGIC   (b.flight_status = 'REALIZADO')                                  AS flight_completed,
# MAGIC
# MAGIC   current_timestamp()                                             AS _processed_at
# MAGIC FROM base b
# MAGIC LEFT JOIN airline    e ON b.icao_airline    = e.icao
# MAGIC LEFT JOIN di         d ON b.di_code         = d.code
# MAGIC LEFT JOIN line_type  t ON b.line_type_code  = t.code