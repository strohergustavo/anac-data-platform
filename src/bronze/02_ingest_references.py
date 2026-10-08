# Databricks notebook source
# DBTITLE 1,Bronze — Reference Tables
# MAGIC %md
# MAGIC # Bronze — Reference Tables
# MAGIC
# MAGIC Ingests four ANAC reference datasets into `<catalog>.bronze`: public aerodromes, national airlines, foreign airlines, and a curated operation-code seed table. Each table preserves the source schema with minimal aliasing for readability.

# COMMAND ----------

# MAGIC %run ../common/setup

# COMMAND ----------

# DBTITLE 1,Config
from pyspark.sql import functions as F

logger = get_logger("bronze.references")
_start = time.time()

REFERENCE_DATA_PATH = f"/Volumes/{CATALOG}/bronze/data/references"
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
).saveAsTable(f"{CATALOG}.bronze.aerodromos")

logger.info("bronze.aerodromos: %s rows", f"{spark.table(f'{CATALOG}.bronze.aerodromos').count():,}")


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
    ("pda_empresas_aereas_nacionais.csv",    f"{CATALOG}.bronze.national_airlines"),
    ("pda_empresas_aereas_estrangeiros.csv", f"{CATALOG}.bronze.foreign_airlines"),
]:
    read_companies(file).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table)
    logger.info(f"{table}: {spark.table(table).count():,} rows")

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
).saveAsTable(f"{CATALOG}.bronze.operation_codes")

logger.info("bronze.operation_codes: %s rows", spark.table(f"{CATALOG}.bronze.operation_codes").count())


# COMMAND ----------

for table, comment in [
    (f"{CATALOG}.bronze.aerodromos",
     "Bronze - cadastro de aerodromos publicos da ANAC, como chegou. Chave: codigo ICAO (OACI). "
     "Cobre apenas aerodromos brasileiros - aeroportos estrangeiros do VRA nao estao aqui."),
    (f"{CATALOG}.bronze.national_airlines",
     "Bronze - cadastro de empresas aereas NACIONAIS da ANAC, como chegou. Chave: codigo ICAO. "
     "Nao unir com empresas_estrangeiras nesta camada: a uniao e feita na silver."),
    (f"{CATALOG}.bronze.foreign_airlines",
     "Bronze - cadastro de empresas aereas ESTRANGEIRAS autorizadas a operar no Brasil, como chegou. "
     "Chave: codigo ICAO. Cadastro separado do nacional na origem, mantido separado no bronze."),
    (f"{CATALOG}.bronze.operation_codes",
     "Bronze - seed table curada a partir da pagina de descricao de variaveis da ANAC. "
     "Traduz codigo_di e codigo_tipo_linha para descricao em portugues."),
]:
    comment_table(table, comment)

logger.info("comments applied")

# COMMAND ----------

# --- Tags for all bronze reference tables ---
BRONZE_TAGS = {
    f"{CATALOG}.bronze.aerodromos":       {"layer": "bronze", "domain": "aviation", "source": "ANAC-Aerodromos", "grain": "aerodrome"},
    f"{CATALOG}.bronze.national_airlines": {"layer": "bronze", "domain": "aviation", "source": "ANAC-Operador-Aereo", "grain": "company"},
    f"{CATALOG}.bronze.foreign_airlines":  {"layer": "bronze", "domain": "aviation", "source": "ANAC-Operador-Aereo", "grain": "company"},
    f"{CATALOG}.bronze.operation_codes":   {"layer": "bronze", "domain": "aviation", "source": "ANAC-seed", "grain": "code"},
}
for table, tags in BRONZE_TAGS.items():
    tag_table(table, tags)
    logger.info(f"Tags applied to {table}")

# --- Column comments (Portuguese for Genie Agent compatibility) ---
COMMENTS = {
    f"{CATALOG}.bronze.aerodromos": {
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
    f"{CATALOG}.bronze.national_airlines": {
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
    f"{CATALOG}.bronze.foreign_airlines": {
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
    f"{CATALOG}.bronze.operation_codes": {
        "domain": "A qual coluna do VRA este codigo pertence: di_code ou line_type_code.",
        "code": "O codigo como aparece no VRA.",
        "description": "Descricao oficial do codigo, curada da pagina de descricao de variaveis da ANAC.",
    },
}

for table, col_map in COMMENTS.items():
    comment_columns(table, col_map)
    logger.info(f"{len(col_map)} column comments applied to {table}")

# COMMAND ----------

# DBTITLE 1,Log result
logger.info("bronze reference tables completed in %.1fs", time.time() - _start)
