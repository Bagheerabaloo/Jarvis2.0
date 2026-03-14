-- === MATERIALIZED VIEW: mv_last_info_trading_session ===
CREATE MATERIALIZED VIEW public.mv_last_info_trading_session AS
SELECT
    t.ticker_id,
    t.market_cap,
    t.current_price,
    t.last_update,
    t.two_hundred_day_average,
    t.fifty_two_week_high,
    t.fifty_two_week_low,
    t.trailing_pe,
    t.forward_pe
FROM (
    SELECT
        i.ticker_id,
        i.market_cap,
        i.current_price,
        i.last_update,
        i.two_hundred_day_average,
        i.fifty_two_week_high,
        i.fifty_two_week_low,
        i.trailing_pe,
        i.forward_pe,
        row_number() OVER (PARTITION BY i.ticker_id ORDER BY i.last_update DESC) AS rn
    FROM info_trading_session AS i
) AS t
WHERE t.rn = 1
WITH DATA;
