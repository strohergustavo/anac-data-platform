from fixtures import flight, register_bronze_vra, register_references
from notebook_sql import query_for

SILVER = query_for("src/silver/01_silver_mirror.py", "silver.vra")
MARKED = query_for("src/silver/transformations/01_vra_marked.sql", "vra_marcado")
QUARANTINE = query_for("src/silver/transformations/03_vra_quarantine.sql", "vra_quarentena")


def build(spark, rows):
    register_bronze_vra(spark, rows)
    register_references(spark)
    spark.sql(SILVER).createOrReplaceTempView("silver_vra")
    spark.sql(MARKED).createOrReplaceTempView("vra_marcado")
    # In the pipeline vra_auditado only attaches warn-mode expectations to vra_marcado.
    spark.table("vra_marcado").createOrReplaceTempView("vra_auditado")
    return spark.sql(QUARANTINE)


def test_clean_flights_stay_out_of_quarantine(spark):
    assert build(spark, [flight()]).count() == 0


def test_every_broken_rule_is_listed(spark):
    row = build(spark, [flight(airline="ZZZ", sched_dep="null", sched_arr="null")]).first()
    assert row.motivos_quarentena == "horarios_previstos_presentes | empresa_no_cadastro_anac"


def test_foreign_destination_is_flagged(spark):
    row = build(spark, [flight(destination="KJFK")]).first()
    assert row.motivos_quarentena == "aeroporto_destino_no_cadastro_anac"
