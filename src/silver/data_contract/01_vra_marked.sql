CREATE LIVE VIEW vra_marcado AS
WITH aerodromo AS (
  SELECT DISTINCT icao FROM ${catalog}.silver.aerodromes
  WHERE icao IS NOT NULL AND icao <> ''
),
empresa AS (
  SELECT DISTINCT icao FROM ${catalog}.silver.airlines
  WHERE icao IS NOT NULL AND icao <> ''
)
SELECT
  v.*,
  (ao.icao IS NOT NULL) AS origem_no_cadastro,
  (ad.icao IS NOT NULL) AS destino_no_cadastro,
  (em.icao IS NOT NULL) AS empresa_no_cadastro
FROM ${catalog}.silver.vra v
LEFT JOIN aerodromo ao ON v.icao_origin      = ao.icao
LEFT JOIN aerodromo ad ON v.icao_destination = ad.icao
LEFT JOIN empresa   em ON v.icao_airline      = em.icao;