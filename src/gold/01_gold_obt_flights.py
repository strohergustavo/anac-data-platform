# Databricks notebook source
# DBTITLE 1,Gold — One Big Table
# MAGIC %md
# MAGIC # Gold — One Big Table
# MAGIC
# MAGIC Builds `airline_operations.gold.obt_flights` by denormalising `fact_flights` with `dim_airport` on both origin and destination. One row per flight step with all descriptive attributes resolved — designed for direct consumption by AI agents and BI tools without any JOIN. 39 columns covering airline, operation, origin, destination, route, time, metrics, and status.

# COMMAND ----------

# DBTITLE 1,Logging setup
import logging, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.gold.obt")
_start = time.time()

# COMMAND ----------

# DBTITLE 1,Log result
_rows = spark.table("airline_operations.gold.obt_flights").count()
logger.info("gold.obt_flights: %s rows in %.1fs", f"{_rows:,}", time.time() - _start)

# COMMAND ----------

# DBTITLE 1,OBT SELECT
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE airline_operations.gold.obt_flights AS
# MAGIC SELECT
# MAGIC   f.icao_airline,
# MAGIC   f.airline_name,
# MAGIC   f.flight_number,
# MAGIC
# MAGIC   f.di_code,
# MAGIC   f.di_description,
# MAGIC   f.line_type_code,
# MAGIC   f.line_type_description,
# MAGIC   f.flight_scope,
# MAGIC
# MAGIC   f.icao_origin,
# MAGIC   o.airport_name            AS origin_airport_name,
# MAGIC   o.airport_municipality    AS origin_municipality,
# MAGIC   o.airport_state           AS origin_state,
# MAGIC   o.airport_country         AS origin_country,
# MAGIC
# MAGIC   f.icao_destination,
# MAGIC   d.airport_name            AS destination_airport_name,
# MAGIC   d.airport_municipality    AS destination_municipality,
# MAGIC   d.airport_state           AS destination_state,
# MAGIC   d.airport_country         AS destination_country,
# MAGIC
# MAGIC   f.route                                                            AS route_icao,
# MAGIC   concat(coalesce(o.airport_municipality, f.icao_origin), ' - ',
# MAGIC          coalesce(d.airport_municipality, f.icao_destination))         AS route_municipalities,
# MAGIC
# MAGIC   f.scheduled_departure,
# MAGIC   f.scheduled_departure_date,
# MAGIC   f.scheduled_departure_time,
# MAGIC   f.scheduled_departure_hour,
# MAGIC   f.day_of_week,
# MAGIC   f.reference_month,
# MAGIC   f.actual_departure,
# MAGIC   f.scheduled_arrival,
# MAGIC   f.actual_arrival,
# MAGIC
# MAGIC   f.departure_delay_min,
# MAGIC   f.arrival_delay_min,
# MAGIC   f.minutes_recovered,
# MAGIC   f.delay_out_of_range,
# MAGIC   f.departure_punctual,
# MAGIC   f.arrival_punctual,
# MAGIC
# MAGIC   f.flight_status,
# MAGIC   f.flight_completed,
# MAGIC   f.flight_cancelled,
# MAGIC
# MAGIC   f._processed_at
# MAGIC
# MAGIC FROM airline_operations.gold.fact_flights f
# MAGIC LEFT JOIN airline_operations.gold.dim_airport o ON f.icao_origin      = o.icao_airport
# MAGIC LEFT JOIN airline_operations.gold.dim_airport d ON f.icao_destination = d.icao_airport