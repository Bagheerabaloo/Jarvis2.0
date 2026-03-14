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
