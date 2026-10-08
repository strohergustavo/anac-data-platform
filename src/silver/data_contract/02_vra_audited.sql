CREATE LIVE VIEW vra_auditado (

  CONSTRAINT horarios_previstos_presentes
    EXPECT (scheduled_departure IS NOT NULL AND scheduled_arrival IS NOT NULL),

  CONSTRAINT situacao_voo_conhecida
    EXPECT (flight_status IN ('REALIZADO', 'CANCELADO')),

  CONSTRAINT chegada_prevista_depois_da_partida_prevista
    EXPECT (scheduled_departure IS NULL OR scheduled_arrival IS NULL
            OR scheduled_arrival > scheduled_departure),

  CONSTRAINT chegada_real_depois_da_partida_real
    EXPECT (actual_departure IS NULL OR actual_arrival IS NULL
            OR actual_arrival > actual_departure),

  CONSTRAINT atraso_partida_plausivel
    EXPECT (departure_delay_min IS NULL
            OR departure_delay_min BETWEEN -120 AND 1440),

  CONSTRAINT atraso_chegada_plausivel
    EXPECT (arrival_delay_min IS NULL
            OR arrival_delay_min BETWEEN -120 AND 1440),

  CONSTRAINT empresa_no_cadastro_anac
    EXPECT (empresa_no_cadastro),

  CONSTRAINT aeroporto_origem_no_cadastro_anac
    EXPECT (origem_no_cadastro),

  CONSTRAINT aeroporto_destino_no_cadastro_anac
    EXPECT (destino_no_cadastro)
)
COMMENT 'Contrato de dados de silver.vra. Nove expectations, todas em modo warn:
 medem qualidade sem descartar linha. A silver segue com a contagem original.'
AS SELECT * FROM vra_marcado;