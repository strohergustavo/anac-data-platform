# Databricks notebook source
# DBTITLE 1,Silver — Governed Mirror
# MAGIC %md
# MAGIC # Silver — Governed Mirror
# MAGIC
# MAGIC Transforms bronze tables into typed, documented silver tables under `<catalog>.silver`: `vra` (flight facts with typed timestamps and delay arithmetic), `airlines` (unified national + foreign registry), `aerodromes` (airport reference), and `operation_codes` (DI and line-type descriptions). Silver preserves the bronze row count — no filtering, no business rules. All tables receive column comments and UC tags.

# COMMAND ----------

# MAGIC %run ../common/setup

# COMMAND ----------

logger = get_logger("silver.mirror")
_start = time.time()

# COMMAND ----------

# DBTITLE 1,Silver VRA mirror
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.vra AS
WITH typed AS (
  SELECT
    `ICAO Empresa Aérea`        AS icao_airline,
    `Número Voo`                AS flight_number,
    `Código Autorização (DI)`   AS di_code,
    `Código Tipo Linha`         AS line_type_code,
    `ICAO Aeródromo Origem`     AS icao_origin,
    `ICAO Aeródromo Destino`    AS icao_destination,
    try_cast(nullif(`Partida Prevista`, 'null') AS TIMESTAMP) AS scheduled_departure,
    try_cast(nullif(`Partida Real`,     'null') AS TIMESTAMP) AS actual_departure,
    try_cast(nullif(`Chegada Prevista`, 'null') AS TIMESTAMP) AS scheduled_arrival,
    try_cast(nullif(`Chegada Real`,     'null') AS TIMESTAMP) AS actual_arrival,
    `Situação Voo`              AS flight_status,
    nullif(`Código Justificativa`, 'N/A')                     AS justification_code,
    _arquivo_origem,
    _ingerido_em
  FROM {CATALOG}.bronze.vra
)
SELECT
  icao_airline,
  flight_number,
  di_code,
  line_type_code,
  icao_origin,
  icao_destination,

  scheduled_departure,
  CAST(scheduled_departure AS DATE)         AS scheduled_departure_date,
  date_format(scheduled_departure, 'HH:mm') AS scheduled_departure_time,

  actual_departure,
  CAST(actual_departure AS DATE)         AS actual_departure_date,
  date_format(actual_departure, 'HH:mm') AS actual_departure_time,

  scheduled_arrival,
  CAST(scheduled_arrival AS DATE)         AS scheduled_arrival_date,
  date_format(scheduled_arrival, 'HH:mm') AS scheduled_arrival_time,

  actual_arrival,
  CAST(actual_arrival AS DATE)         AS actual_arrival_date,
  date_format(actual_arrival, 'HH:mm') AS actual_arrival_time,

  flight_status,
  justification_code,

  CAST(timestampdiff(MINUTE, scheduled_departure, actual_departure) AS INT) AS departure_delay_min,
  CAST(timestampdiff(MINUTE, scheduled_arrival,   actual_arrival)   AS INT) AS arrival_delay_min,
  CAST(
    timestampdiff(MINUTE, scheduled_departure, actual_departure)
    - timestampdiff(MINUTE, scheduled_arrival, actual_arrival)
  AS INT) AS minutes_recovered,

  _arquivo_origem,
  _ingerido_em,
  current_timestamp()                                AS _transformed_at
FROM typed
""")

logger.info("silver.vra created")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.airlines AS
SELECT
  icao,
  iata_code,
  legal_name,
  service,
  city,
  state,
  status,
  'national'      AS registry_origin,
  _arquivo_origem,
  _ingerido_em,
  current_timestamp() AS _transformed_at
FROM {CATALOG}.bronze.national_airlines
UNION ALL
SELECT
  icao,
  iata_code,
  legal_name,
  service,
  city,
  state,
  status,
  'foreign'   AS registry_origin,
  _arquivo_origem,
  _ingerido_em,
  current_timestamp() AS _transformed_at
FROM {CATALOG}.bronze.foreign_airlines
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.aerodromes AS
SELECT
  icao,
  ciad,
  name,
  municipality,
  state                                          AS state_name,
  served_municipality,
  served_state                                    AS served_state_name,
  latitude                                      AS latitude_dms,
  longitude                                     AS longitude_dms,
  try_cast(replace(altitude, ',', '.') AS DOUBLE) AS altitude_m,
  status,
  _ingerido_em,
  current_timestamp()                           AS _transformed_at
FROM {CATALOG}.bronze.aerodromos
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.operation_codes AS
SELECT
  domain,
  code,
  description,
  current_timestamp() AS _transformed_at
FROM {CATALOG}.bronze.operation_codes
""")

# COMMAND ----------

COMMENTS_VRA = {
    "icao_airline":            "Codigo ICAO de tres letras da empresa aerea que operou a etapa. Chave para silver.airlines.",
    "flight_number":           "Numero do voo divulgado pela companhia. Identificador comercial, nao numerico: pode ter zero a esquerda e se repete entre datas.",
    "di_code":                  "Codigo de autorizacao (DI) da etapa: distingue etapa regular, extra, de retorno, charter. Descricao em silver.operation_codes (dominio di_code).",
    "line_type_code":           "Codigo do tipo de linha: N e C domesticas, I e G internacionais. Descricao em silver.operation_codes (dominio line_type_code).",
    "icao_origin":              "Codigo ICAO do aerodromo de onde a etapa partiu. Chave para silver.aerodromes - aeroportos estrangeiros nao constam no cadastro da ANAC.",
    "icao_destination":         "Codigo ICAO do aerodromo onde a etapa pousou. Mesma observacao de cobertura da origem.",
    "scheduled_departure":      "Horario de partida programado pela companhia, na hora local do aeroporto de origem.",
    "scheduled_departure_date": "Data da partida programada, separada para facilitar analise por dia.",
    "scheduled_departure_time": "Hora e minuto da partida programada (HH:mm), separada para analise por faixa horaria.",
    "actual_departure":         "Horario em que a aeronave efetivamente saiu. Nulo em voo cancelado, que nao chegou a partir.",
    "actual_departure_date":    "Data da partida efetiva.",
    "actual_departure_time":    "Hora e minuto da partida efetiva (HH:mm).",
    "scheduled_arrival":       "Horario de chegada programado, na hora local do aeroporto de destino.",
    "scheduled_arrival_date":  "Data da chegada programada.",
    "scheduled_arrival_time":  "Hora e minuto da chegada programada (HH:mm).",
    "actual_arrival":           "Horario em que a aeronave efetivamente pousou. Nulo em voo cancelado.",
    "actual_arrival_date":      "Data da chegada efetiva.",
    "actual_arrival_time":      "Hora e minuto da chegada efetiva (HH:mm).",
    "flight_status":            "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao.",
    "justification_code":       "Motivo declarado do atraso. Deixou de ser exigido pela ANAC em abril de 2020 com a revogacao da IAC 1504: vem vazio em toda a janela deste projeto.",
    "departure_delay_min":      "Minutos entre a partida programada e a partida efetiva. Positivo e atraso, negativo e antecipacao. Aritmetica pura: nao aplica limiar de pontualidade.",
    "arrival_delay_min":        "Minutos entre a chegada programada e a chegada efetiva. Positivo e atraso, negativo e antecipacao.",
    "minutes_recovered":        "Minutos que a etapa recuperou em voo: atraso de partida menos atraso de chegada. Positivo significa que chegou menos atrasada do que saiu.",
    "_arquivo_origem":          "Auditoria: nome do arquivo CSV mensal da ANAC de onde a linha veio.",
    "_ingerido_em":             "Auditoria: momento em que a linha entrou no bronze.",
    "_transformed_at":           "Auditoria: momento em que a silver foi reconstruida a partir do bronze.",
}

comment_columns(f"{CATALOG}.silver.vra", COMMENTS_VRA)

logger.info(f"{len(COMMENTS_VRA)} columns commented in silver.vra")

# COMMAND ----------

COMMENTS_AIRLINES = {
    "icao":            "Codigo ICAO de tres letras da empresa. Vazio para operadores sem codigo (aviacao agricola, taxi aereo, aeroclube).",
    "iata_code":       "Sigla de duas letras da empresa no padrao IATA, como publicada pela ANAC.",
    "legal_name":      "Razao social da empresa aerea. E o nome que aparece para quem consome o produto final.",
    "service":         "Tipo de servico autorizado pela ANAC: transporte regular, nao regular, aeroagricola, taxi aereo.",
    "city":             "Municipio da sede ou do representante legal no Brasil.",
    "state":            "Sigla da unidade federativa da sede.",
    "status":           "Situacao do registro na ANAC: ATIVA ou nao. Registro inativo permanece na tabela porque a empresa pode ter voado no periodo analisado.",
    "registry_origin":  "De qual dos dois cadastros da ANAC este registro veio: nacional ou estrangeira. E a coluna que preserva a fronteira entre as duas fontes depois da uniao.",
    "_arquivo_origem":  "Auditoria: arquivo CSV de origem.",
    "_ingerido_em":     "Auditoria: momento da ingestao no bronze.",
    "_transformed_at":  "Auditoria: momento da construcao da silver.",
}

COMMENTS_AERODROMES = {
    "icao":              "Codigo ICAO (OACI) do aerodromo. Chave de ligacao com origem e destino do VRA.",
    "ciad":              "Codigo de identificacao do aerodromo no cadastro da ANAC.",
    "name":              "Nome do aerodromo como publicado pela ANAC.",
    "municipality":      "Municipio onde o aerodromo esta fisicamente localizado.",
    "state_name":        "Nome da unidade federativa POR EXTENSO (Acre, Sao Paulo), nao a sigla: e assim que a ANAC publica.",
    "served_municipality":"Municipio principal atendido pelo aerodromo, que pode ser diferente do municipio onde ele fica.",
    "served_state_name": "Nome por extenso da UF do municipio servido.",
    "latitude_dms":      "Latitude em graus, minutos e segundos, como publicada pela ANAC.",
    "longitude_dms":     "Longitude em graus, minutos e segundos, como publicada pela ANAC.",
    "altitude_m":        "Altitude do aerodromo em metros. Na origem vem com virgula decimal.",
    "status":            "Situacao do aerodromo no cadastro da ANAC.",
    "_ingerido_em":      "Auditoria: momento da ingestao no bronze.",
    "_transformed_at":  "Auditoria: momento da construcao da silver.",
}

COMMENTS_CODES = {
    "domain":           "A qual coluna do VRA este codigo pertence: di_code ou line_type_code.",
    "code":             "O codigo como aparece no VRA.",
    "description":       "Descricao oficial do codigo, curada da pagina de descricao de variaveis da ANAC.",
    "_transformed_at": "Auditoria: momento da construcao da silver.",
}

for table, mapping in [
    (f"{CATALOG}.silver.airlines",        COMMENTS_AIRLINES),
    (f"{CATALOG}.silver.aerodromes",      COMMENTS_AERODROMES),
    (f"{CATALOG}.silver.operation_codes", COMMENTS_CODES),
]:
    comment_columns(table, mapping)
    logger.info(f"{len(mapping)} columns commented in {table}")

# COMMAND ----------

TABLES = {
    f"{CATALOG}.silver.vra": (
        "Silver - espelho governado de bronze.vra. Mesmo grao (uma linha por etapa de voo) e "
        "MESMA contagem de linhas do bronze: sem filtro, sem agregacao e sem regra de negocio. "
        "Traz tipagem, data e hora separadas e as tres metricas de aritmetica pura de atraso. "
        "Pontualidade, escopo e exclusoes ficam na gold.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-VRA", "grain": "flight_step"},
    ),
    f"{CATALOG}.silver.airlines": (
        "Silver - cadastro unificado de empresas aereas: uniao dos dois cadastros do bronze "
        "(nacionais e estrangeiras) com a coluna registry_origin preservando a fonte de cada registro. "
        "Contagem igual a soma exata das duas tabelas de origem.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-Operador-Aereo", "grain": "company"},
    ),
    f"{CATALOG}.silver.aerodromes": (
        "Silver - espelho governado do cadastro de aerodromos publicos da ANAC. Cobre apenas "
        "aerodromos brasileiros: aeroportos estrangeiros do VRA nao constam aqui, e isso e "
        "propriedade da fonte, nao defeito.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-Aerodromos", "grain": "aerodrome"},
    ),
    f"{CATALOG}.silver.operation_codes": (
        "Silver - espelho da seed table de codigos de operacao (DI e tipo de linha) com as "
        "descricoes oficiais da ANAC.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-seed", "grain": "code"},
    ),
}

for table, (comment, tags) in TABLES.items():
    comment_table(table, comment)
    tag_table(table, tags)
    logger.info(f"{table}: comment + {len(tags)} tags")

# COMMAND ----------

# DBTITLE 1,Log result
logger.info("silver mirror completed in %.1fs", time.time() - _start)
