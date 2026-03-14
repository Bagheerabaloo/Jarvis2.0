-- === MATERIALIZED VIEW: mv_last_info_general_stock ===
CREATE MATERIALIZED VIEW public.mv_last_info_general_stock AS
SELECT
    s.ticker_id,
    s.symbol,
    s.exchange,
    s.last_update
FROM (
    SELECT
        i.ticker_id,
        i.symbol,
        i.exchange,
        i.last_update,
        row_number() OVER (PARTITION BY i.symbol ORDER BY i.last_update DESC) AS rn
    FROM info_general_stock AS i
) AS s
WHERE s.rn = 1
WITH DATA;
