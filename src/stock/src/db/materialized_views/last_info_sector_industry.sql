-- === MATERIALIZED VIEW: mv_last_info_sector_industry ===
CREATE MATERIALIZED VIEW public.mv_last_info_sector_industry AS
SELECT
    s.ticker_id,
    s.sector,
    s.industry,
    s.last_update
FROM (
    SELECT
        i.ticker_id,
        i.sector,
        i.industry,
        i.last_update,
        row_number() OVER (PARTITION BY i.ticker_id ORDER BY i.last_update DESC) AS rn
    FROM info_sector_industry_history AS i
) AS s
WHERE s.rn = 1
WITH DATA;
