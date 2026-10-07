import pytest
from fixtures import flight, register_bronze_vra, register_references
from notebook_sql import query_for

SILVER = query_for("src/silver/01_silver_mirror.py", "silver.vra")
FACT = query_for("src/gold/03_gold_fact_flights.py", "gold.fact_flights")


def build(spark, rows):
    register_bronze_vra(spark, rows)
    register_references(spark)
    spark.sql(SILVER).createOrReplaceTempView("silver_vra")
    return spark.sql(FACT)


def test_exact_duplicates_are_removed(spark):
    assert build(spark, [flight(), flight(), flight(number="9")]).count() == 2


@pytest.mark.parametrize("delay, punctual", [(0, True), (15, True), (16, False)])
def test_fifteen_minute_punctuality_rule(spark, delay, punctual):
    row = build(spark, [flight(actual_dep=f"2026-08-01 10:{delay:02d}:00")]).first()
    assert row.departure_punctual is punctual


def test_implausible_delays_are_nulled_not_dropped(spark):
    df = build(spark, [flight(actual_dep="2026-08-03 10:00:00")])
    row = df.first()
    assert df.count() == 1
    assert row.delay_out_of_range is True
    assert row.departure_delay_min is None
    assert row.departure_punctual is None


@pytest.mark.parametrize("line, scope", [("N", "Domestico"), ("I", "Internacional"), ("X", "Nao classificado")])
def test_flight_scope(spark, line, scope):
    assert build(spark, [flight(line=line)]).first().flight_scope == scope


def test_unknown_airline_gets_an_explicit_fallback_name(spark):
    assert build(spark, [flight(airline="ZZZ")]).first().airline_name == "COMPANHIA NAO CADASTRADA (ZZZ)"


def test_active_airline_record_wins(spark):
    assert build(spark, [flight(airline="AZU")]).first().airline_name == "AZUL LINHAS AEREAS"


def test_cancelled_flag(spark):
    row = build(spark, [flight(status="CANCELADO", actual_dep="null", actual_arr="null")]).first()
    assert row.flight_cancelled is True
    assert row.departure_punctual is None
