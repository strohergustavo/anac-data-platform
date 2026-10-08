from fixtures import flight, register_bronze_vra
from notebook_sql import query_for

SILVER = query_for("src/silver/01_mirror.py", "silver.vra")


def build(spark, rows):
    register_bronze_vra(spark, rows)
    return spark.sql(SILVER)


def test_silver_is_a_lossless_mirror(spark):
    rows = [flight(number=str(n)) for n in range(5)]
    assert build(spark, rows).count() == len(rows)


def test_literal_null_strings_become_real_nulls(spark):
    row = build(spark, [flight(actual_dep="null", actual_arr="null")]).first()
    assert row.actual_departure is None
    assert row.departure_delay_min is None
    assert row.minutes_recovered is None


def test_delay_arithmetic(spark):
    row = build(spark, [flight(
        sched_dep="2026-08-01 10:00:00", actual_dep="2026-08-01 10:20:00",
        sched_arr="2026-08-01 11:00:00", actual_arr="2026-08-01 11:05:00",
    )]).first()
    assert row.departure_delay_min == 20
    assert row.arrival_delay_min == 5
    assert row.minutes_recovered == 15


def test_not_applicable_justification_is_null(spark):
    assert build(spark, [flight(justification="N/A")]).first().justification_code is None
