# Databricks notebook source
# DBTITLE 1,Bronze — Reference Tables
# MAGIC %md
# MAGIC # Bronze — Reference Tables
# MAGIC
# MAGIC Ingests four ANAC reference datasets into `airline_operations.bronze`: public aerodromes, national airlines, foreign airlines, and a curated operation-code seed table. Each table preserves the source schema with minimal aliasing for readability.

# COMMAND ----------

# DBTITLE 1,Config
from pyspark.sql import functions as F
import logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.bronze.reference")
_start = time.time()

REFERENCE_DATA_PATH = "/Volumes/airline_operations/bronze/data/references"
no_quotes = chr(0)

# COMMAND ----------

# DBTITLE 1,Ingest aerodromes
aerodromes = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("encoding", "ISO-8859-1")  
    .option("quote", no_quotes)          
    .load(f"{REFERENCE_DATA_PATH}/AerodromosPublicos.csv")
)

aerodromes = aerodromes.select(
    F.col("`Código OACI`").alias("icao"),
    F.col("CIAD").alias("ciad"),
    F.col("Nome").alias("name"),
    F.col("`Município`").alias("municipality"),
    F.col("UF").alias("state"),
    F.col("`Município Servido`").alias("served_municipality"),
    F.col("`UF Servido`").alias("served_state"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude"),
    F.col("Altitude").alias("altitude"),
    F.col("`Situação`").alias("status"),
).withColumn("_ingerido_em", F.current_timestamp())

aerodromes.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("airline_operations.bronze.aerodromos")

print(f"bronze.aerodromos: {spark.table('airline_operations.bronze.aerodromos').count():,} rows")
display(spark.sql("SELECT icao, name, municipality, state FROM airline_operations.bronze.aerodromos WHERE icao IN ('SBRB','SBGR','SBSP','SBFZ')"))


# COMMAND ----------

# DBTITLE 1,Ingest airlines
def read_companies(file: str):
    """Read a company registry. No union, no enrichment: one table per file."""
    return (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("skipRows", 1)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .load(f"{REFERENCE_DATA_PATH}/{file}")
        .select(
            F.col("ICAO").alias("icao"),
            F.col("Estrangeira").alias("iata_code"),
            F.col("Razao").alias("legal_name"),
            F.col("Servico").alias("service"),
            F.col("Cidade").alias("city"),
            F.col("UF").alias("state"),
            F.col("Ativa").alias("status"),
        )
        .withColumn("_arquivo_origem", F.lit(file))
        .withColumn("_ingerido_em", F.current_timestamp())
    )


for file, table in [
    ("pda_empresas_aereas_nacionais.csv",    "airline_operations.bronze.national_airlines"),
    ("pda_empresas_aereas_estrangeiros.csv", "airline_operations.bronze.foreign_airlines"),
]:
    read_companies(file).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table)
    print(f"{table}: {spark.table(table).count():,} rows")

# COMMAND ----------

display(spark.sql("""
    SELECT 'national_airlines' AS table, COUNT(*) AS rows,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS with_icao
    FROM airline_operations.bronze.national_airlines
    UNION ALL
    SELECT 'foreign_airlines', COUNT(*),
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END)
    FROM airline_operations.bronze.foreign_airlines
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT icao, legal_name, service, state, status
    FROM airline_operations.bronze.national_airlines
    WHERE icao IN ('GLO','TAM','AZU','PAM')
    ORDER BY icao
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT icao, legal_name, service, status
    FROM airline_operations.bronze.foreign_airlines
    WHERE icao IN ('AAL','TAP','AVA','ARG')
    ORDER BY icao
"""))

# COMMAND ----------

CODES = [
    ("di_code", "0", "Etapa Regular"),
    ("di_code", "2", "Etapa Extra"),
    ("di_code", "3", "Etapa de Retorno"),
    ("di_code", "4", "Inclusão de Etapa"),
    ("di_code", "6", "Etapa Não Remunerada Sem Transporte de Objetos"),
    ("di_code", "7", "Etapa de Voo de Fretamento"),
    ("di_code", "9", "Etapa de Voo Charter"),
    ("di_code", "D", "Etapa de Voo Duplicada"),
    ("di_code", "E", "Etapa Não Remunerada Com Transporte de Objetos"),
    ("line_type_code", "N", "Doméstica Mista"),
    ("line_type_code", "C", "Doméstica Cargueira"),
    ("line_type_code", "I", "Internacional Mista"),
    ("line_type_code", "G", "Internacional Cargueira"),
]

codes = spark.createDataFrame(CODES, "domain string, code string, description string")
codes.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("airline_operations.bronze.operation_codes")

print(f"bronze.operation_codes: {spark.table('airline_operations.bronze.operation_codes').count()} rows")
display(spark.table("airline_operations.bronze.operation_codes"))


# COMMAND ----------

display(spark.sql("SHOW TABLES IN airline_operations.bronze"))


# COMMAND ----------

display(spark.sql("""
    SELECT version, timestamp, operation,
           operationMetrics.numOutputRows AS rows_written
    FROM (DESCRIBE HISTORY airline_operations.bronze.vra)
    ORDER BY version
"""))

# COMMAND ----------

# Show current row count; time-travel to version 0 is skipped when the
# Delta history has been vacuumed past the 168-hour retention window.
try:
    first_version = spark.sql("DESCRIBE HISTORY airline_operations.bronze.vra").select("version").first()[0]
    display(spark.sql(f"""
        SELECT 'first available (v{first_version})' AS version,
               COUNT(*) AS rows,
               MIN(_ingerido_em) AS ingested_at
        FROM airline_operations.bronze.vra VERSION AS OF {first_version}
        UNION ALL
        SELECT 'current version', COUNT(*), MIN(_ingerido_em)
        FROM airline_operations.bronze.vra
    """))
except Exception:
    display(spark.sql("""
        SELECT 'current version' AS version,
               COUNT(*) AS rows,
               MIN(_ingerido_em) AS ingested_at
        FROM airline_operations.bronze.vra
    """))

# COMMAND ----------

for table, comment in [
    ("airline_operations.bronze.aerodromos",
     "Bronze - cadastro de aerodromos publicos da ANAC, como chegou. Chave: codigo ICAO (OACI). "
     "Cobre apenas aerodromos brasileiros - aeroportos estrangeiros do VRA nao estao aqui."),
    ("airline_operations.bronze.national_airlines",
     "Bronze - cadastro de empresas aereas NACIONAIS da ANAC, como chegou. Chave: codigo ICAO. "
     "Nao unir com empresas_estrangeiras nesta camada: a uniao e feita na silver."),
    ("airline_operations.bronze.foreign_airlines",
     "Bronze - cadastro de empresas aereas ESTRANGEIRAS autorizadas a operar no Brasil, como chegou. "
     "Chave: codigo ICAO. Cadastro separado do nacional na origem, mantido separado no bronze."),
    ("airline_operations.bronze.operation_codes",
     "Bronze - seed table curada a partir da pagina de descricao de variaveis da ANAC. "
     "Traduz codigo_di e codigo_tipo_linha para descricao em portugues."),
]:
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")

print("comments applied")

# COMMAND ----------

# --- Tags for all bronze reference tables ---
BRONZE_TAGS = {
    "airline_operations.bronze.aerodromos":       {"layer": "bronze", "domain": "aviation", "source": "ANAC-Aerodromos", "grain": "aerodrome"},
    "airline_operations.bronze.national_airlines": {"layer": "bronze", "domain": "aviation", "source": "ANAC-Operador-Aereo", "grain": "company"},
    "airline_operations.bronze.foreign_airlines":  {"layer": "bronze", "domain": "aviation", "source": "ANAC-Operador-Aereo", "grain": "company"},
    "airline_operations.bronze.operation_codes":   {"layer": "bronze", "domain": "aviation", "source": "ANAC-seed", "grain": "code"},
}
for table, tags in BRONZE_TAGS.items():
    pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")
    print(f"Tags applied to {table}")

# --- Column comments (Portuguese for Genie Agent compatibility) ---
COMMENTS = {
    "airline_operations.bronze.aerodromos": {
        "icao": "Codigo ICAO (OACI) do aerodromo. Chave da tabela.",
        "ciad": "Codigo de identificacao do aerodromo no cadastro da ANAC.",
        "name": "Nome do aerodromo como publicado pela ANAC.",
        "municipality": "Municipio onde o aerodromo esta fisicamente localizado.",
        "state": "Sigla da unidade federativa onde o aerodromo fica.",
        "served_municipality": "Municipio principal atendido pelo aerodromo.",
        "served_state": "Sigla da UF do municipio servido.",
        "latitude": "Latitude do aerodromo em graus, minutos e segundos.",
        "longitude": "Longitude do aerodromo em graus, minutos e segundos.",
        "altitude": "Altitude do aerodromo em metros (virgula decimal na origem).",
        "status": "Situacao do aerodromo no cadastro da ANAC.",
        "_ingerido_em": "Auditoria: momento da ingestao no bronze.",
    },
    "airline_operations.bronze.national_airlines": {
        "icao": "Codigo ICAO de tres letras da empresa. Vazio para operadores sem codigo.",
        "iata_code": "Sigla de duas letras da empresa no padrao IATA, como publicada pela ANAC.",
        "legal_name": "Razao social da empresa aerea.",
        "service": "Tipo de servico autorizado pela ANAC: transporte regular, nao regular, etc.",
        "city": "Municipio da sede ou do representante legal no Brasil.",
        "state": "Sigla da unidade federativa da sede.",
        "status": "Situacao do registro na ANAC: ATIVA ou nao.",
        "_arquivo_origem": "Auditoria: arquivo CSV de origem.",
        "_ingerido_em": "Auditoria: momento da ingestao no bronze.",
    },
    "airline_operations.bronze.foreign_airlines": {
        "icao": "Codigo ICAO de tres letras da empresa estrangeira.",
        "iata_code": "Sigla de duas letras da empresa no padrao IATA.",
        "legal_name": "Razao social da empresa aerea estrangeira.",
        "service": "Tipo de servico autorizado pela ANAC.",
        "city": "Cidade do representante legal no Brasil.",
        "state": "Sigla da unidade federativa do representante.",
        "status": "Situacao do registro na ANAC: ATIVA ou nao.",
        "_arquivo_origem": "Auditoria: arquivo CSV de origem.",
        "_ingerido_em": "Auditoria: momento da ingestao no bronze.",
    },
    "airline_operations.bronze.operation_codes": {
        "domain": "A qual coluna do VRA este codigo pertence: di_code ou line_type_code.",
        "code": "O codigo como aparece no VRA.",
        "description": "Descricao oficial do codigo, curada da pagina de descricao de variaveis da ANAC.",
    },
}

for table, col_map in COMMENTS.items():
    for col, comment in col_map.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {col} COMMENT '{comment}'")
    print(f"{len(col_map)} column comments applied to {table}")