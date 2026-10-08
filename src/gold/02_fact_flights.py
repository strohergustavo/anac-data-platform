# Databricks notebook source
# DBTITLE 1,Gold — Flight Fact Table
# MAGIC %md
# MAGIC # Gold — Flight Fact Table
# MAGIC
# MAGIC Builds `<catalog>.gold.fact_flights` from `silver.vra` with exact deduplication (41 duplicate rows removed), airline name resolution, operation-code descriptions, domestic/international scope derivation, delay plausibility checks (±2 h to ±24 h), and the project's 15-minute punctuality metric. One row per flight step. This is where business rules live.

# COMMAND ----------

# MAGIC %run ../common/setup

# COMMAND ----------

# DBTITLE 1,Logging setup
logger = get_logger("gold.fact_flights")
_start = time.time()

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.fact_flights AS
WITH vra_deduplicated AS (
  SELECT * FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY icao_airline, flight_number, di_code, line_type_code,
                     icao_origin, icao_destination, scheduled_departure, actual_departure,
                     scheduled_arrival, actual_arrival, flight_status
        ORDER BY _ingerido_em
      ) AS _rn
    FROM {CATALOG}.silver.vra
  )
  WHERE _rn = 1
),
airline AS (
  SELECT icao, legal_name, registry_origin
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY icao
      ORDER BY CASE WHEN status = 'ATIVA' THEN 0 ELSE 1 END, legal_name
    ) AS rn
    FROM {CATALOG}.silver.airlines
    WHERE icao IS NOT NULL AND icao <> ''
  )
  WHERE rn = 1
),
di AS (
  SELECT code, description FROM {CATALOG}.silver.operation_codes WHERE domain = 'di_code'
),
line_type AS (
  SELECT code, description FROM {CATALOG}.silver.operation_codes WHERE domain = 'line_type_code'
),
base AS (
  SELECT
    v.*,
    (v.departure_delay_min IS NOT NULL AND (v.departure_delay_min < -120 OR v.departure_delay_min > 1440))
      OR (v.arrival_delay_min IS NOT NULL AND (v.arrival_delay_min < -120 OR v.arrival_delay_min > 1440))
                                                                    AS delay_out_of_range
  FROM vra_deduplicated v
)
SELECT
  b.icao_airline,
  COALESCE(e.legal_name, concat('COMPANHIA NAO CADASTRADA (', b.icao_airline, ')'))
                                                                    AS airline_name,
  e.registry_origin                                                AS airline_registry,
  b.flight_number,

  b.di_code,
  COALESCE(d.description, concat('Codigo nao catalogado (', b.di_code, ')'))
                                                                    AS di_description,
  b.line_type_code,
  COALESCE(t.description, concat('Codigo nao catalogado (', b.line_type_code, ')'))
                                                                    AS line_type_description,

  CASE
    WHEN b.line_type_code IN ('N', 'C') THEN 'Domestico'
    WHEN b.line_type_code IN ('I', 'G') THEN 'Internacional'
    ELSE 'Nao classificado'
  END                                                               AS flight_scope,

  b.icao_origin,
  b.icao_destination,
  concat(b.icao_origin, ' - ', b.icao_destination)                 AS route,

  b.scheduled_departure,
  b.scheduled_departure_date,
  b.scheduled_departure_time,
  hour(b.scheduled_departure)                                      AS scheduled_departure_hour,
  CASE dayofweek(b.scheduled_departure_date)
    WHEN 1 THEN 'domingo'  WHEN 2 THEN 'segunda' WHEN 3 THEN 'terca'
    WHEN 4 THEN 'quarta'   WHEN 5 THEN 'quinta'  WHEN 6 THEN 'sexta'
    WHEN 7 THEN 'sabado'
  END                                                               AS day_of_week,
  date_trunc('MONTH', b.scheduled_departure_date)                  AS reference_month,
  b.actual_departure,
  b.scheduled_arrival,
  b.actual_arrival,

  CASE WHEN b.delay_out_of_range THEN NULL ELSE b.departure_delay_min  END AS departure_delay_min,
  CASE WHEN b.delay_out_of_range THEN NULL ELSE b.arrival_delay_min    END AS arrival_delay_min,
  CASE WHEN b.delay_out_of_range THEN NULL ELSE b.minutes_recovered     END AS minutes_recovered,
  b.delay_out_of_range,

  CASE WHEN b.delay_out_of_range OR b.departure_delay_min IS NULL THEN NULL
       ELSE b.departure_delay_min <= 15 END                        AS departure_punctual,
  CASE WHEN b.delay_out_of_range OR b.arrival_delay_min IS NULL THEN NULL
       ELSE b.arrival_delay_min <= 15 END                          AS arrival_punctual,

  b.flight_status,
  (b.flight_status = 'CANCELADO')                                  AS flight_cancelled,
  (b.flight_status = 'REALIZADO')                                  AS flight_completed,

  current_timestamp()                                             AS _processed_at
FROM base b
LEFT JOIN airline    e ON b.icao_airline    = e.icao
LEFT JOIN di         d ON b.di_code         = d.code
LEFT JOIN line_type  t ON b.line_type_code  = t.code
""")

# COMMAND ----------

# DBTITLE 1,Log result
_rows = spark.table(f"{CATALOG}.gold.fact_flights").count()
logger.info("gold.fact_flights: %s rows in %.1fs", f"{_rows:,}", time.time() - _start)
