-- === MATERIALIZED VIEW: mv_ticker_overview ===
-- Join via ticker_id; no dependency on dot/dash or renames
CREATE MATERIALIZED VIEW public.mv_ticker_overview
AS
SELECT
    t.id                    AS ticker_id,
    t.symbol,
    t.company_name,
    t.last_update           AS ticker_last_update,
    igs.exchange,
    igs.last_update         AS info_general_last_update,
    its.market_cap,
    its.fifty_two_week_high AS "52_week_high",
    its.fifty_two_week_low  AS "52_week_low",
    its.two_hundred_day_average AS "200MA",
    its.current_price       AS price,
    its.last_update         AS info_trading_last_update,
    its.trailing_pe,
    its.forward_pe,
    rcd.close,
    rcd.last_update         AS candle_day_last_update,
    lisi.sector,
    lisi.industry,
    lisi.last_update        AS sector_industry_last_update,
    (sp.ticker_id IS NOT NULL) AS sp500
FROM ticker t
LEFT JOIN mv_last_info_general_stock   igs  ON igs.ticker_id = t.id
LEFT JOIN mv_last_info_trading_session its  ON its.ticker_id = t.id
LEFT JOIN mv_recent_candle_data_day    rcd  ON rcd.ticker_id = t.id
LEFT JOIN mv_sp_500_latest_date        sp   ON sp.ticker_id   = t.id
LEFT JOIN mv_last_info_sector_industry lisi ON lisi.ticker_id = t.id
WITH DATA;

-- (Optional) helpful indexes on the overview MV
CREATE INDEX IF NOT EXISTS mv_ticker_overview_tid_idx ON public.mv_ticker_overview (ticker_id);
CREATE INDEX IF NOT EXISTS mv_ticker_overview_sym_idx ON public.mv_ticker_overview (symbol);

-- === VIEW: v_ticker_overview ===
CREATE OR REPLACE VIEW public.v_ticker_overview AS
SELECT
    mvo.ticker_id,
    mvo.symbol,
    mvo.company_name,
    mvo.ticker_last_update,
    mvo.exchange,
    mvo.info_general_last_update,
    mvo.market_cap,
    mvo."52_week_high",
    mvo."52_week_low",
    mvo."200MA",
    mvo.price,
    mvo.info_trading_last_update,
    mvo.trailing_pe,
    mvo.forward_pe,
    mvo.close,
    mvo.candle_day_last_update,
    mvo.sector,
    mvo.industry,
    mvo.sector_industry_last_update,
    mvo.sp500
FROM public.mv_ticker_overview AS mvo;
