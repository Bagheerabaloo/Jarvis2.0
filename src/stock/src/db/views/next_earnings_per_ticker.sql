-- === VIEW: v_next_earnings_per_ticker ===
CREATE OR REPLACE VIEW public.v_next_earnings_per_ticker AS
SELECT
    mv_next_earnings_per_ticker.ticker_id,
    mv_next_earnings_per_ticker.date,
    mv_next_earnings_per_ticker.last_update,
    mv_next_earnings_per_ticker.earnings_period,
    mv_next_earnings_per_ticker.eps_estimate,
    mv_next_earnings_per_ticker.reported_eps,
    mv_next_earnings_per_ticker.surprise_percent,
    mv_next_earnings_per_ticker.rn
FROM public.mv_next_earnings_per_ticker;
