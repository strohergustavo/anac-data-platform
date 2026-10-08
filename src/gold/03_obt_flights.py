# Databricks notebook source
# DBTITLE 1,Gold — One Big Table
# MAGIC %md
# MAGIC # Gold — One Big Table
# MAGIC
# MAGIC Builds `<catalog>.gold.obt_flights` by denormalising `fact_flights` with `dim_airport` on both origin and destination. One row per flight step with all descriptive attributes resolved — designed for direct consumption by AI agents and BI tools without any JOIN. 39 columns covering airline, operation, origin, destination, route, time, metrics, and status.

# COMMAND ----------

# MAGIC %run ../common/setup

# COMMAND ----------

# DBTITLE 1,Logging setup
logger = get_logger("gold.obt_flights")
_start = time.time()

# COMMAND ----------
# DBTITLE 1,OBT SELECT
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.obt_flights AS
SELECT
  f.icao_airline,
  f.airline_name,
  f.flight_number,

  f.di_code,
  f.di_description,
  f.line_type_code,
  f.line_type_description,
  f.flight_scope,

  f.icao_origin,
  o.airport_name            AS origin_airport_name,
  o.airport_municipality    AS origin_municipality,
  o.airport_state           AS origin_state,
  o.airport_country         AS origin_country,

  f.icao_destination,
  d.airport_name            AS destination_airport_name,
  d.airport_municipality    AS destination_municipality,
  d.airport_state           AS destination_state,
  d.airport_country         AS destination_country,

  f.route                                                            AS route_icao,
  concat(coalesce(o.airport_municipality, f.icao_origin), ' - ',
         coalesce(d.airport_municipality, f.icao_destination))         AS route_municipalities,

  f.scheduled_departure,
  f.scheduled_departure_date,
  f.scheduled_departure_time,
  f.scheduled_departure_hour,
  f.day_of_week,
  f.reference_month,
  f.actual_departure,
  f.scheduled_arrival,
  f.actual_arrival,

  f.departure_delay_min,
  f.arrival_delay_min,
  f.minutes_recovered,
  f.delay_out_of_range,
  f.departure_punctual,
  f.arrival_punctual,

  f.flight_status,
  f.flight_completed,
  f.flight_cancelled,

  f._processed_at

FROM {CATALOG}.gold.fact_flights f
LEFT JOIN {CATALOG}.gold.dim_airport o ON f.icao_origin      = o.icao_airport
LEFT JOIN {CATALOG}.gold.dim_airport d ON f.icao_destination = d.icao_airport
""")

# COMMAND ----------

# DBTITLE 1,Log result
_rows = spark.table(f"{CATALOG}.gold.obt_flights").count()
logger.info("gold.obt_flights: %s rows in %.1fs", f"{_rows:,}", time.time() - _start)
