-- === VIEW: v_sp_500_latest_date ===
CREATE OR REPLACE VIEW public.v_sp_500_latest_date AS
SELECT
    date,
    ticker_id,
    ticker,
    ticker_yfinance,
    last_update
FROM public.mv_sp_500_latest_date;
