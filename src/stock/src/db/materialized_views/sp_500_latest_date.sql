-- === MATERIALIZED VIEW: mv_sp_500_latest_date ===
-- Build empty first so we can add indexes before loading
CREATE MATERIALIZED VIEW public.mv_sp_500_latest_date
AS
SELECT
    h.date,
    h.ticker_id,
    h.symbol_at_date AS ticker,     -- snapshot text for display
    h.ticker_yfinance,
    h.last_update
FROM public.sp_500_historical AS h
WHERE h.date = (SELECT MAX(date) FROM public.sp_500_historical)
WITH NO DATA;

-- Helpful indexes (we are not using CONCURRENTLY)
CREATE UNIQUE INDEX mv_sp_500_latest_date_pk
    ON public.mv_sp_500_latest_date (date, ticker_id);

CREATE INDEX mv_sp_500_latest_date_yf_idx
    ON public.mv_sp_500_latest_date (ticker_yfinance);

-- First load (non-concurrent)
REFRESH MATERIALIZED VIEW public.mv_sp_500_latest_date;

-- === VIEW: sp_500_latest_date_view ===
-- Keep the same view name for backward-compat, but point to the new MV
CREATE OR REPLACE VIEW public.v_sp_500_latest_date AS
SELECT
    date,
    ticker_id,
    ticker,
    ticker_yfinance,
    last_update
FROM public.mv_sp_500_latest_date;
