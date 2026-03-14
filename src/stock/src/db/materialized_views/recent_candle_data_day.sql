-- === MATERIALIZED VIEW: mv_recent_candle_data_day ===
CREATE MATERIALIZED VIEW public.mv_recent_candle_data_day AS
WITH ranked_data AS (
    SELECT
        candle_data_day.id,
        candle_data_day.ticker_id,
        candle_data_day.date,
        candle_data_day.open,
        candle_data_day.high,
        candle_data_day.low,
        candle_data_day.close,
        candle_data_day.adj_close,
        candle_data_day.volume,
        candle_data_day.last_update,
        row_number() OVER (
            PARTITION BY candle_data_day.ticker_id
            ORDER BY candle_data_day.date DESC
        ) AS rn
    FROM candle_data_day
)
SELECT
    ranked_data.id,
    ranked_data.ticker_id,
    ranked_data.date,
    ranked_data.open,
    ranked_data.high,
    ranked_data.low,
    ranked_data.close,
    ranked_data.adj_close,
    ranked_data.volume,
    ranked_data.last_update,
    ranked_data.rn
FROM ranked_data
WHERE ranked_data.rn = 1
WITH DATA;
