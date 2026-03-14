-- === MATERIALIZED VIEW: mv_pe ===
CREATE MATERIALIZED VIEW public.mv_pe AS
SELECT
    sub."Ticker",
    sub."Trailing PE",
    sub."Forward PE",
    sub.last_update,
    sub.date,
    sub.rn
FROM (
    SELECT
        t.symbol AS "Ticker",
        ts.trailing_pe AS "Trailing PE",
        ts.forward_pe AS "Forward PE",
        ts.last_update,
        date(ts.last_update) AS date,
        row_number() OVER (
            PARTITION BY t.symbol, date(ts.last_update)
            ORDER BY ts.last_update DESC
        ) AS rn
    FROM info_trading_session AS ts
    JOIN ticker AS t ON t.id = ts.ticker_id
) AS sub
WHERE sub.rn = 1
WITH DATA;
