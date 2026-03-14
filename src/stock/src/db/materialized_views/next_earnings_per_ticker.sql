-- === MATERIALIZED VIEW: mv_next_earnings_per_ticker ===
CREATE MATERIALIZED VIEW public.mv_next_earnings_per_ticker AS
WITH next_earnings AS (
    SELECT
        earnings_dates.ticker_id,
        earnings_dates.date,
        earnings_dates.last_update,
        earnings_dates.earnings_period,
        earnings_dates.eps_estimate,
        earnings_dates.reported_eps,
        earnings_dates.surprise_percent
    FROM earnings_dates
    WHERE earnings_dates.date >= CURRENT_DATE
),
ranked AS (
    SELECT
        next_earnings.ticker_id,
        next_earnings.date,
        next_earnings.last_update,
        next_earnings.earnings_period,
        next_earnings.eps_estimate,
        next_earnings.reported_eps,
        next_earnings.surprise_percent,
        row_number() OVER (
            PARTITION BY next_earnings.ticker_id
            ORDER BY next_earnings.date
        ) AS rn
    FROM next_earnings
)
SELECT
    ranked.ticker_id,
    ranked.date,
    ranked.last_update,
    ranked.earnings_period,
    ranked.eps_estimate,
    ranked.reported_eps,
    ranked.surprise_percent,
    ranked.rn
FROM ranked
WHERE ranked.rn = 1
WITH DATA;
