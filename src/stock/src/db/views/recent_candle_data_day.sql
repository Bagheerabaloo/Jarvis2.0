-- === VIEW: recent_candle_data_day_view ===
CREATE OR REPLACE VIEW public.recent_candle_data_day_view AS
SELECT
    mv_recent_candle_data_day.id,
    mv_recent_candle_data_day.ticker_id,
    mv_recent_candle_data_day.date,
    mv_recent_candle_data_day.open,
    mv_recent_candle_data_day.high,
    mv_recent_candle_data_day.low,
    mv_recent_candle_data_day.close,
    mv_recent_candle_data_day.adj_close,
    mv_recent_candle_data_day.volume,
    mv_recent_candle_data_day.last_update,
    mv_recent_candle_data_day.rn
FROM public.mv_recent_candle_data_day;
