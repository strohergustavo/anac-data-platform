CREATE OR REFRESH MATERIALIZED VIEW vra_quarentena
COMMENT 'Silver quarentena - espelho DIAGNOSTICO dos registros de silver.vra que
 reprovaram em alguma expectation do contrato de dados, com o motivo por registro.
 NAO e filtro: silver.vra permanece com a contagem original. A decisao de excluir
 ou nao cada categoria e de negocio e acontece na gold.'
AS
SELECT
  *,
  concat_ws(' | ',
    CASE WHEN scheduled_departure IS NULL OR scheduled_arrival IS NULL
         THEN 'horarios_previstos_presentes' END,
    CASE WHEN flight_status NOT IN ('REALIZADO', 'CANCELADO') OR flight_status IS NULL
         THEN 'situacao_voo_conhecida' END,
    CASE WHEN scheduled_departure IS NOT NULL AND scheduled_arrival IS NOT NULL
               AND scheduled_arrival <= scheduled_departure
         THEN 'chegada_prevista_depois_da_partida_prevista' END,
    CASE WHEN actual_departure IS NOT NULL AND actual_arrival IS NOT NULL
               AND actual_arrival <= actual_departure
         THEN 'chegada_real_depois_da_partida_real' END,
    CASE WHEN departure_delay_min IS NOT NULL
               AND (departure_delay_min < -120 OR departure_delay_min > 1440)
         THEN 'atraso_partida_plausivel' END,
    CASE WHEN arrival_delay_min IS NOT NULL
               AND (arrival_delay_min < -120 OR arrival_delay_min > 1440)
         THEN 'atraso_chegada_plausivel' END,
    CASE WHEN NOT empresa_no_cadastro THEN 'empresa_no_cadastro_anac' END,
    CASE WHEN NOT origem_no_cadastro THEN 'aeroporto_origem_no_cadastro_anac' END,
    CASE WHEN NOT destino_no_cadastro THEN 'aeroporto_destino_no_cadastro_anac' END
  ) AS motivos_quarentena,
  current_timestamp() AS _quarentenado_em
FROM vra_auditado
WHERE NOT (
      scheduled_departure IS NOT NULL AND scheduled_arrival IS NOT NULL
  AND flight_status IN ('REALIZADO', 'CANCELADO')
  AND (scheduled_departure IS NULL OR scheduled_arrival IS NULL OR scheduled_arrival > scheduled_departure)
  AND (actual_departure IS NULL OR actual_arrival IS NULL OR actual_arrival > actual_departure)
  AND (departure_delay_min IS NULL OR departure_delay_min BETWEEN -120 AND 1440)
  AND (arrival_delay_min IS NULL OR arrival_delay_min BETWEEN -120 AND 1440)
  AND empresa_no_cadastro
  AND origem_no_cadastro
  AND destino_no_cadastro
);