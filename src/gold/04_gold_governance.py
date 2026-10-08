# Databricks notebook source
# DBTITLE 1,Gold — Governance & Validation
# MAGIC %md
# MAGIC # Gold — Governance & Validation
# MAGIC
# MAGIC Applies column comments and UC tags to all gold tables (`obt_flights`, `fact_flights`, `dim_airport`), then runs validation queries: documentation coverage, tag audit, and table-lineage check. Use this notebook after rebuilding any gold table to verify metadata is complete.

# COMMAND ----------

import logging, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("anac.gold.governance")
_start = time.time()

display(spark.sql("""
    SELECT
      COUNT(*)                                                      AS recovered_some_minutes,
      SUM(CASE WHEN arrival_delay_min <= 0 THEN 1 ELSE 0 END)      AS arrived_early_or_on_time,
      SUM(CASE WHEN arrival_delay_min >  0 THEN 1 ELSE 0 END)      AS arrived_late_anyway,
      SUM(CASE WHEN arrival_delay_min > 15 THEN 1 ELSE 0 END)      AS arrived_late_over_15
    FROM airline_operations.gold.obt_flights
    WHERE minutes_recovered > 0
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT flight_status,
           COUNT(*)                                                AS flights,
           SUM(CASE WHEN departure_punctual IS NULL THEN 1 ELSE 0 END) AS departure_punctual_null
    FROM airline_operations.gold.obt_flights GROUP BY flight_status
"""))

# COMMAND ----------

COMMENTS_OBT = {
    # ---- airline ----
    "icao_airline":           "Codigo ICAO de tres letras da companhia que operou a etapa. Use airline_name para exibir; este codigo serve para filtro exato.",
    "airline_name":           "Razao social da companhia aerea. Quando o codigo nao existe no cadastro da ANAC, traz COMPANHIA NAO CADASTRADA seguida do codigo, em vez de vazio.",
    "flight_number":          "Numero comercial do voo divulgado pela companhia. Nao e identificador unico: o mesmo numero se repete todos os dias.",

    # ---- operation ----
    "di_code":                "Codigo de autorizacao da etapa (DI) publicado pela ANAC. O codigo 1 aparece no dado e nao consta na tabela oficial de descricoes.",
    "di_description":         "Tipo da etapa por extenso: regular, extra, de retorno, charter, fretamento. Quando o codigo nao esta catalogado pela ANAC, diz isso explicitamente.",
    "line_type_code":         "Codigo do tipo de linha da ANAC: N e C domesticas, I e G internacionais.",
    "line_type_description":  "Tipo de linha por extenso, combinando escopo e natureza da operacao: Domestica Mista, Internacional Cargueira, etc.",
    "flight_scope":           "Classificacao de negocio do voo em Domestico ou Internacional, derivada do tipo de linha. E a coluna certa para comparar os dois universos.",

    # ---- origin ----
    "icao_origin":            "Codigo ICAO do aeroporto de partida. Use origin_airport_name para exibir.",
    "origin_airport_name":    "Nome do aeroporto de partida. Aeroporto estrangeiro nao consta no cadastro da ANAC e aparece como AEROPORTO FORA DO CADASTRO ANAC seguido do codigo.",
    "origin_municipality":    "Municipio do aeroporto de partida. Vazio para aeroporto estrangeiro, que nao esta no cadastro brasileiro.",
    "origin_state":           "Unidade federativa do aeroporto de partida, escrita POR EXTENSO (Sao Paulo, Ceara), como a ANAC publica. Nao e a sigla.",
    "origin_country":         "Brasil ou Exterior, deduzido do prefixo do codigo ICAO. Serve para separar operacao domestica de internacional pelo lado do aeroporto.",

    # ---- destination ----
    "icao_destination":       "Codigo ICAO do aeroporto de chegada. Use destination_airport_name para exibir.",
    "destination_airport_name":"Nome do aeroporto de chegada. Mesma regra de fallback do aeroporto de origem.",
    "destination_municipality":"Municipio do aeroporto de chegada. Vazio para aeroporto estrangeiro.",
    "destination_state":      "Unidade federativa do aeroporto de chegada, por extenso.",
    "destination_country":    "Brasil ou Exterior para o aeroporto de chegada.",

    # ---- route ----
    "route_icao":             "Rota no formato ORIGEM - DESTINO usando codigos ICAO. E a chave estavel para agrupar por rota.",
    "route_municipalities":   "Rota no formato municipio de origem - municipio de destino, para leitura humana. Aeroporto estrangeiro aparece pelo codigo ICAO, porque nao tem municipio no cadastro.",

    # ---- time ----
    "scheduled_departure":    "Data e hora que a companhia programou para a partida, na hora local do aeroporto de origem.",
    "scheduled_departure_date":"Data programada da partida. Use para series diarias e para recortar periodo.",
    "scheduled_departure_time":"Hora e minuto programados da partida, no formato HH:mm, para leitura.",
    "scheduled_departure_hour":"Hora cheia programada da partida, de 0 a 23. E a coluna certa para analisar o efeito cascata do atraso ao longo do dia.",
    "day_of_week":            "Dia da semana da partida programada, por extenso e em minusculas (domingo a sabado).",
    "reference_month":        "Primeiro dia do mes da partida programada, para agregacao mensal. E nulo nos voos que nao tem horario previsto informado.",
    "actual_departure":       "Data e hora em que a aeronave efetivamente partiu. Nulo em voo cancelado.",
    "scheduled_arrival":      "Data e hora programadas para a chegada, na hora local do aeroporto de destino.",
    "actual_arrival":         "Data e hora em que a aeronave efetivamente pousou. Nulo em voo cancelado.",

    # ---- metrics ----
    "departure_delay_min":    "Atraso de partida em minutos: horario real menos programado. Negativo significa que saiu adiantado. Nulo quando o voo foi cancelado, quando nao ha horario programado, ou quando o valor esta fora da faixa plausivel.",
    "arrival_delay_min":      "Atraso de chegada em minutos: horario real menos programado. Negativo significa que pousou adiantado. Mesmas regras de nulo do atraso de partida.",
    "minutes_recovered":      "Minutos que a etapa recuperou no ar: atraso de partida menos atraso de chegada. Positivo significa que chegou MENOS ATRASADA do que saiu, e NAO que chegou no horario - um voo pode recuperar 20 minutos e ainda assim pousar atrasado. Negativo significa que perdeu ainda mais tempo depois de decolar.",
    "delay_out_of_range":     "Verdadeiro quando o atraso calculado estava fora da faixa plausivel (menos de -2h ou mais de 24h), sinal de erro de data na origem. A linha continua contando como voo, mas as tres metricas de atraso foram anuladas.",
    "departure_punctual":     "Verdadeiro quando a partida atrasou 15 minutos ou menos, criterio de pontualidade deste projeto. Falso significa atraso maior que 15 minutos. Nulo significa que NAO DA para avaliar - voo cancelado ou sem horario programado - e nunca deve ser contado como atraso.",
    "arrival_punctual":       "Verdadeiro quando a chegada atrasou 15 minutos ou menos. Mesma regra de nulo da pontualidade de partida.",

    # ---- status ----
    "flight_status":          "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao aconteceu.",
    "flight_completed":      "Verdadeiro quando a etapa foi realizada. Use como denominador de metricas operacionais.",
    "flight_cancelled":       "Verdadeiro quando a etapa foi cancelada. Voo cancelado NAO entra em nenhuma media de atraso, porque nao tem horario real; use esta coluna para taxa de cancelamento.",

    "_processed_at":          "Auditoria: momento em que esta linha foi construida na camada gold.",
}

for column, comment in COMMENTS_OBT.items():
    spark.sql(f"ALTER TABLE airline_operations.gold.obt_flights ALTER COLUMN {column} COMMENT '{comment}'")

logger.info(f"{len(COMMENTS_OBT)} columns commented in gold.obt_flights")


# COMMAND ----------

COMMENTS_DIM = {
    "icao_airport":      "Codigo ICAO do aeroporto. Chave da dimensao, serve tanto para origem quanto para destino do fato.",
    "airport_name":      "Nome do aeroporto. Traz fallback textual com o codigo quando o aeroporto nao esta no cadastro da ANAC.",
    "airport_municipality": "Municipio onde o aeroporto esta localizado. Vazio para aeroporto estrangeiro.",
    "airport_state":        "Unidade federativa por extenso, como a ANAC publica. Nao e a sigla.",
    "airport_country":      "Brasil ou Exterior, deduzido do prefixo ICAO. Regra de negocio criada na gold.",
    "in_anac_registry":    "Verdadeiro quando o aeroporto existe no cadastro de aerodromos publicos da ANAC. Falso e o esperado para aeroporto estrangeiro, e nao indica erro.",
    "_processed_at":      "Auditoria: momento da construcao da dimensao.",
}

COMMENTS_FACT = dict(COMMENTS_OBT)
COMMENTS_FACT["icao_origin"]      = "Codigo ICAO do aeroporto de partida. Chave para gold.dim_airport."
COMMENTS_FACT["icao_destination"] = "Codigo ICAO do aeroporto de chegada. Chave para gold.dim_airport."
COMMENTS_FACT["route"] = "Rota no formato ORIGEM - DESTINO usando codigos ICAO."
COMMENTS_FACT["airline_registry"] = "De qual cadastro da ANAC veio a companhia: nacional ou estrangeira. Nulo quando o codigo nao tem cadastro."
FACT_COLUMNS = [c for c in COMMENTS_FACT if c not in (
    "origin_airport_name", "origin_municipality", "origin_state", "origin_country",
    "destination_airport_name", "destination_municipality", "destination_state", "destination_country",
    "route_icao", "route_municipalities")]

for column in FACT_COLUMNS:
    spark.sql(f"ALTER TABLE airline_operations.gold.fact_flights ALTER COLUMN {column} COMMENT '{COMMENTS_FACT[column]}'")
logger.info(f"{len(FACT_COLUMNS)} columns commented in gold.fact_flights")

for column, comment in COMMENTS_DIM.items():
    spark.sql(f"ALTER TABLE airline_operations.gold.dim_airport ALTER COLUMN {column} COMMENT '{comment}'")
logger.info(f"{len(COMMENTS_DIM)} columns commented in gold.dim_airport")


# COMMAND ----------

GOLD_TABLES = {
    "airline_operations.gold.obt_flights": (
        "Gold - One Big Table de voos da ANAC, desnormalizada e desenhada para consumo por agente de IA. "
        "Uma linha por etapa de voo, com nomes ja resolvidos e metricas prontas: responde as perguntas de "
        "negocio do projeto sem nenhum JOIN. Criterio de pontualidade: 15 minutos. "
        "Voo cancelado nao tem metrica de atraso.",
        {"layer": "gold", "domain": "aviation", "consumption": "genie", "grain": "flight_step", "pattern": "obt"},
    ),
    "airline_operations.gold.fact_flights": (
        "Gold - fato de voos no grao de uma linha por etapa, com companhia e codigos de operacao como "
        "dimensoes degeneradas. E aqui que nascem as regras de negocio: pontualidade a 15 minutos, "
        "escopo domestico/internacional e as decisoes sobre a quarentena. "
        "Contagem = silver.vra menos 41 duplicatas exatas.",
        {"layer": "gold", "domain": "aviation", "consumption": "bi", "grain": "flight_step", "pattern": "fact"},
    ),
    "airline_operations.gold.dim_airport": (
        "Gold - dimensao de aeroporto, servindo origem e destino do fato. Construida a partir dos codigos "
        "presentes no fato e enriquecida pelo cadastro da ANAC, para cobrir 100 por cento do fato inclusive "
        "os aeroportos estrangeiros, que a ANAC nao cadastra.",
        {"layer": "gold", "domain": "aviation", "consumption": "bi", "grain": "airport", "pattern": "dimension"},
    ),
}

for table, (comment, tags) in GOLD_TABLES.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")
    logger.info(f"{table}: comment + {len(tags)} tags")

# COMMAND ----------

display(spark.sql("""
    SELECT table_schema, table_name,
           COUNT(*)                                                         AS columns,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) AS without_comment
    FROM airline_operations.information_schema.columns
    WHERE table_schema IN ('silver', 'gold')
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT table_name, tag_name, tag_value
    FROM airline_operations.information_schema.table_tags
    WHERE schema_name = 'gold'
    ORDER BY table_name, tag_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT
      COALESCE(nullif(source_table_full_name, ''), '(file in volume)') AS source,
      target_table_full_name                                             AS target
    FROM system.access.table_lineage
    WHERE target_table_full_name LIKE 'airline_operations.%'
      AND event_date >= current_date() - 7
    GROUP BY 1, 2
    ORDER BY target, source
"""))

# COMMAND ----------

# DBTITLE 1,Log result
logger.info("gold governance completed in %.1fs", time.time() - _start)
