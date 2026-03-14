-- === MATERIALIZED VIEW: mv_monthly_net_insider_transactions ===
CREATE MATERIALIZED VIEW public.mv_monthly_net_insider_transactions AS
WITH monthly_totals AS (
    SELECT
        t.ticker_id,
        date_trunc('month', t.start_date::timestamp with time zone) AS month,
        sum(
            CASE
                WHEN t.state::text = 'Entry'::text
                     AND COALESCE(t.avg_price, 0::numeric) > 0::numeric
                THEN t.value
                ELSE 0::numeric
            END
        ) AS buy_value_usd,
        sum(
            CASE
                WHEN t.state::text = 'Exit'::text THEN t.value
                ELSE 0::numeric
            END
        ) AS sell_value_usd,
        sum(
            CASE
                WHEN t.state::text = 'Entry'::text
                     AND COALESCE(t.avg_price, 0::numeric) > 0::numeric
                THEN t.shares
                ELSE 0::bigint
            END
        ) AS buy_shares,
        sum(
            CASE
                WHEN t.state::text = 'Exit'::text THEN t.shares
                ELSE 0::bigint
            END
        ) AS sell_shares,
        count(*) FILTER (
            WHERE t.state::text = 'Entry'::text
              AND COALESCE(t.avg_price, 0::numeric) > 0::numeric
        ) AS buy_tx,
        count(*) FILTER (
            WHERE t.state::text = 'Exit'::text
        ) AS sell_tx
    FROM insider_transactions AS t
    WHERE t.value IS NOT NULL
    GROUP BY
        t.ticker_id,
        date_trunc('month', t.start_date::timestamp with time zone)
)
SELECT
    tk.symbol AS ticker,
    monthly_totals.month,
    monthly_totals.buy_value_usd,
    monthly_totals.sell_value_usd,
    monthly_totals.buy_value_usd - monthly_totals.sell_value_usd AS net_value_usd,
    monthly_totals.buy_shares,
    monthly_totals.sell_shares,
    monthly_totals.buy_shares - monthly_totals.sell_shares AS net_shares,
    monthly_totals.buy_tx,
    monthly_totals.sell_tx
FROM monthly_totals
JOIN ticker AS tk ON tk.id = monthly_totals.ticker_id
ORDER BY monthly_totals.month DESC, monthly_totals.buy_value_usd DESC
WITH DATA;
