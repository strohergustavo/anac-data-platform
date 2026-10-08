# Databricks notebook source
# DBTITLE 1,Bronze — VRA Ingestion
# MAGIC %md
# MAGIC # Bronze — VRA Ingestion
# MAGIC
# MAGIC Raw ingestion of ANAC's *Voo Regular Ativo* (VRA) dataset from CSV files in the Unity Catalog volume into `airline_operations.bronze.vra`. All columns are loaded as strings with no transformation; metadata columns track provenance and ingestion time. Full-refresh, idempotent load.

# COMMAND ----------

# DBTITLE 1,Config
from pyspark.sql import functions as F
import logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.bronze.vra")
_start = time.time()

VRA_CSV_PATH = "/Volumes/airline_operations/bronze/data/VRA/*.csv"
BRONZE_VRA_TABLE = "airline_operations.bronze.vra"

# COMMAND ----------

# DBTITLE 1,Read CSVs
raw_df = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)  # skip ANAC metadata header row
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")  # keep malformed rows, fill with null
    .load(VRA_CSV_PATH)
)

print("columns read from file:")
for c in raw_df.columns:
    print(f"  {c!r}")

# COMMAND ----------

# DBTITLE 1,Add audit columns
bronze_df = raw_df.withColumn(
    "_arquivo_origem", F.col("_metadata.file_name")
).withColumn(
    "_ingerido_em", F.current_timestamp()
)


# COMMAND ----------

# DBTITLE 1,Write to Delta
(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .option("delta.enableDeletionVectors", "true")
    .saveAsTable(BRONZE_VRA_TABLE)
)

print(f"{BRONZE_VRA_TABLE}: {spark.table(BRONZE_VRA_TABLE).count():,} linhas")


# COMMAND ----------

# DBTITLE 1,Table comment
spark.sql(f"""
    COMMENT ON TABLE {BRONZE_VRA_TABLE} IS
    'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses (ago/2025 a jul/2026).
     Dado bruto: todas as colunas string, nenhuma linha descartada.
     Carga full refresh idempotente a partir de /Volumes/airline_operations/bronze/data/VRA/.'
""")

# COMMAND ----------

# DBTITLE 1,Tags + column comments for bronze.vra
# --- Tags ---
spark.sql("""
    ALTER TABLE airline_operations.bronze.vra SET TAGS (
        'layer' = 'bronze', 'domain' = 'aviation', 'source' = 'ANAC-VRA', 'grain' = 'flight_step'
    )
""")
print("Tags applied to bronze.vra")

# --- Column comments (Portuguese for Genie Agent compatibility) ---
VRA_COLUMNS = {
    "ICAO Empresa Aérea":       "Codigo ICAO de tres letras da companhia que operou a etapa.",
    "Número Voo":              "Numero comercial do voo divulgado pela companhia.",
    "Código Autorização (DI)": "Codigo de autorizacao da etapa (DI) publicado pela ANAC.",
    "Código Tipo Linha":       "Codigo do tipo de linha: N e C domesticas, I e G internacionais.",
    "ICAO Aeródromo Origem":   "Codigo ICAO do aeroporto de partida.",
    "ICAO Aeródromo Destino":  "Codigo ICAO do aeroporto de chegada.",
    "Partida Prevista":        "Data e hora programadas para a partida, na hora local do aeroporto de origem.",
    "Partida Real":            "Data e hora em que a aeronave efetivamente partiu. Nulo em voo cancelado.",
    "Chegada Prevista":        "Data e hora programadas para a chegada, na hora local do aeroporto de destino.",
    "Chegada Real":            "Data e hora em que a aeronave efetivamente pousou. Nulo em voo cancelado.",
    "Situação Voo":            "Situacao informada pela companhia: REALIZADO ou CANCELADO.",
    "Código Justificativa":    "Motivo declarado do atraso. Vazio em toda a janela deste projeto (IAC 1504 revogada em abril de 2020).",
    "_arquivo_origem":         "Auditoria: nome do arquivo CSV mensal da ANAC de onde a linha veio.",
    "_ingerido_em":            "Auditoria: momento em que a linha entrou no bronze.",
}

for col, comment in VRA_COLUMNS.items():
    spark.sql(f"ALTER TABLE airline_operations.bronze.vra ALTER COLUMN `{col}` COMMENT '{comment}'")
print(f"{len(VRA_COLUMNS)} column comments applied to bronze.vra")

# COMMAND ----------

# DBTITLE 1,Preview: rows by source file
display(
    spark.sql(f"""
        SELECT _arquivo_origem, COUNT(*) AS linhas, MAX(_ingerido_em) AS ingerido_em
        FROM {BRONZE_VRA_TABLE}
        GROUP BY _arquivo_origem
        ORDER BY _arquivo_origem
    """)
)