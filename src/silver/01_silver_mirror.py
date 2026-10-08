# Databricks notebook source
# DBTITLE 1,Silver — Governed Mirror
# MAGIC %md
# MAGIC # Silver — Governed Mirror
# MAGIC
# MAGIC Transforms bronze tables into typed, documented silver tables under `airline_operations.silver`: `vra` (flight facts with typed timestamps and delay arithmetic), `airlines` (unified national + foreign registry), `aerodromes` (airport reference), and `operation_codes` (DI and line-type descriptions). Silver preserves the bronze row count — no filtering, no business rules. All tables receive column comments and UC tags.

# COMMAND ----------

display(spark.sql("""
    SELECT
      COUNT(*)                                                              AS total_rows,
      SUM(CASE WHEN `Partida Real`     IS NULL THEN 1 ELSE 0 END)           AS actual_departure_truly_null,
      SUM(CASE WHEN `Partida Real`     = 'null' THEN 1 ELSE 0 END)          AS actual_departure_string_null,
      SUM(CASE WHEN `Partida Prevista` = 'null' THEN 1 ELSE 0 END)          AS scheduled_departure_string_null,
      SUM(CASE WHEN `Partida Prevista` LIKE '%.%' THEN 1 ELSE 0 END)        AS with_second_fraction
    FROM airline_operations.bronze.vra
"""))

# COMMAND ----------

# DBTITLE 1,Silver VRA mirror
spark.sql("CREATE SCHEMA IF NOT EXISTS airline_operations.silver")

spark.sql("""
CREATE OR REPLACE TABLE airline_operations.silver.vra AS
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
  FROM airline_operations.bronze.vra
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

print("silver.vra created")


# COMMAND ----------

display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM airline_operations.bronze.vra) AS bronze_vra,
      (SELECT COUNT(*) FROM airline_operations.silver.vra) AS silver_vra,
      (SELECT COUNT(*) FROM airline_operations.bronze.vra)
        - (SELECT COUNT(*) FROM airline_operations.silver.vra) AS difference
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT
      COUNT(scheduled_departure)  AS scheduled_departure_ok,
      COUNT(actual_departure)      AS actual_departure_ok,
      COUNT(scheduled_arrival)     AS scheduled_arrival_ok,
      COUNT(actual_arrival)        AS actual_arrival_ok,
      COUNT(departure_delay_min)   AS departure_delay_ok,
      COUNT(minutes_recovered)     AS minutes_recovered_ok
    FROM airline_operations.silver.vra
"""))


# COMMAND ----------

display(spark.sql("""
    SELECT icao_airline, flight_number, icao_origin, icao_destination,
           scheduled_departure, scheduled_departure_date, scheduled_departure_time,
           departure_delay_min, arrival_delay_min, minutes_recovered, flight_status
    FROM airline_operations.silver.vra
    ORDER BY scheduled_departure
    LIMIT 5
"""))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE airline_operations.silver.airlines AS
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
FROM airline_operations.bronze.national_airlines
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
FROM airline_operations.bronze.foreign_airlines
""")

display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM airline_operations.bronze.national_airlines)    AS bronze_national,
      (SELECT COUNT(*) FROM airline_operations.bronze.foreign_airlines) AS bronze_foreign,
      (SELECT COUNT(*) FROM airline_operations.bronze.national_airlines)
        + (SELECT COUNT(*) FROM airline_operations.bronze.foreign_airlines) AS expected_sum,
      (SELECT COUNT(*) FROM airline_operations.silver.airlines)              AS silver_airlines
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT registry_origin,
           COUNT(*) AS rows,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS with_icao
    FROM airline_operations.silver.airlines
    GROUP BY registry_origin
    ORDER BY registry_origin
"""))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE airline_operations.silver.aerodromes AS
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
FROM airline_operations.bronze.aerodromos
""")

spark.sql("""
CREATE OR REPLACE TABLE airline_operations.silver.operation_codes AS
SELECT
  domain,
  code,
  description,
  current_timestamp() AS _transformed_at
FROM airline_operations.bronze.operation_codes
""")

display(spark.sql("""
    SELECT 'aerodromes' AS table,
           (SELECT COUNT(*) FROM airline_operations.bronze.aerodromos) AS bronze,
           (SELECT COUNT(*) FROM airline_operations.silver.aerodromes) AS silver
    UNION ALL
    SELECT 'operation_codes',
           (SELECT COUNT(*) FROM airline_operations.bronze.operation_codes),
           (SELECT COUNT(*) FROM airline_operations.silver.operation_codes)
"""))

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

for column, comment in COMMENTS_VRA.items():
    spark.sql(f"ALTER TABLE airline_operations.silver.vra ALTER COLUMN {column} COMMENT '{comment}'")

print(f"{len(COMMENTS_VRA)} columns commented in silver.vra")

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
    ("airline_operations.silver.airlines",        COMMENTS_AIRLINES),
    ("airline_operations.silver.aerodromes",      COMMENTS_AERODROMES),
    ("airline_operations.silver.operation_codes", COMMENTS_CODES),
]:
    for column, comment in mapping.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{comment}'")
    print(f"{len(mapping)} columns commented in {table}")

# COMMAND ----------

TABLES = {
    "airline_operations.silver.vra": (
        "Silver - espelho governado de bronze.vra. Mesmo grao (uma linha por etapa de voo) e "
        "MESMA contagem de linhas do bronze: sem filtro, sem agregacao e sem regra de negocio. "
        "Traz tipagem, data e hora separadas e as tres metricas de aritmetica pura de atraso. "
        "Pontualidade, escopo e exclusoes ficam na gold.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-VRA", "grain": "flight_step"},
    ),
    "airline_operations.silver.airlines": (
        "Silver - cadastro unificado de empresas aereas: uniao dos dois cadastros do bronze "
        "(nacionais e estrangeiras) com a coluna registry_origin preservando a fonte de cada registro. "
        "Contagem igual a soma exata das duas tabelas de origem.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-Operador-Aereo", "grain": "company"},
    ),
    "airline_operations.silver.aerodromes": (
        "Silver - espelho governado do cadastro de aerodromos publicos da ANAC. Cobre apenas "
        "aerodromos brasileiros: aeroportos estrangeiros do VRA nao constam aqui, e isso e "
        "propriedade da fonte, nao defeito.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-Aerodromos", "grain": "aerodrome"},
    ),
    "airline_operations.silver.operation_codes": (
        "Silver - espelho da seed table de codigos de operacao (DI e tipo de linha) com as "
        "descricoes oficiais da ANAC.",
        {"layer": "silver", "domain": "aviation", "source": "ANAC-seed", "grain": "code"},
    ),
}

for table, (comment, tags) in TABLES.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")
    print(f"{table}: comment + {len(tags)} tags")


# COMMAND ----------

display(spark.sql("""
    SELECT table_name,
           COUNT(*)                                                          AS columns,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END)  AS without_comment,
           ROUND(100.0 * SUM(CASE WHEN comment IS NOT NULL AND comment <> '' THEN 1 ELSE 0 END)
                 / COUNT(*), 1)                                              AS pct_documented
    FROM airline_operations.information_schema.columns
    WHERE table_schema = 'silver'
    GROUP BY table_name
    ORDER BY table_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT table_name, tag_name, tag_value
    FROM airline_operations.information_schema.table_tags
    WHERE schema_name = 'silver'
    ORDER BY table_name, tag_name
"""))