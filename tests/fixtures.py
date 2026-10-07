"""Small, hand-built inputs that mirror the shape of the ANAC files and silver tables."""
from datetime import datetime

BRONZE_VRA_COLUMNS = [
    "ICAO Empresa Aérea", "Número Voo", "Código Autorização (DI)", "Código Tipo Linha",
    "ICAO Aeródromo Origem", "ICAO Aeródromo Destino", "Partida Prevista", "Partida Real",
    "Chegada Prevista", "Chegada Real", "Situação Voo", "Código Justificativa",
    "_arquivo_origem", "_ingerido_em",
]
INGESTED = datetime(2026, 9, 1, 3, 0)


def flight(airline="TAM", number="3000", line="N", origin="SBGR", destination="SBRJ",
           sched_dep="2026-08-01 10:00:00", actual_dep="2026-08-01 10:10:00",
           sched_arr="2026-08-01 11:00:00", actual_arr="2026-08-01 11:05:00",
           status="REALIZADO", justification="N/A"):
    return (airline, number, "0", line, origin, destination, sched_dep, actual_dep,
            sched_arr, actual_arr, status, justification, "VRA_2026_08.csv", INGESTED)


def register_bronze_vra(spark, rows):
    spark.createDataFrame(rows, BRONZE_VRA_COLUMNS).createOrReplaceTempView("bronze_vra")


def register_references(spark):
    spark.createDataFrame(
        [("TAM", "TAM LINHAS AEREAS", "national", "ATIVA"),
         ("AZU", "AZUL ANTIGA", "national", "INATIVA"),
         ("AZU", "AZUL LINHAS AEREAS", "national", "ATIVA")],
        ["icao", "legal_name", "registry_origin", "status"],
    ).createOrReplaceTempView("silver_airlines")
    spark.createDataFrame(
        [("0", "Regular", "di_code"), ("N", "Nacional", "line_type_code")],
        ["code", "description", "domain"],
    ).createOrReplaceTempView("silver_operation_codes")
    spark.createDataFrame(
        [("SBGR", "GUARULHOS"), ("SBRJ", "SANTOS DUMONT")],
        ["icao", "name"],
    ).createOrReplaceTempView("silver_aerodromes")
