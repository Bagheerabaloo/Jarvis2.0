-- === VIEW: v_monthly_net_insider_transactions ===
CREATE OR REPLACE VIEW public.v_monthly_net_insider_transactions AS
SELECT
    mv_monthly_net_insider_transactions.ticker,
    mv_monthly_net_insider_transactions.month,
    mv_monthly_net_insider_transactions.buy_value_usd,
    mv_monthly_net_insider_transactions.sell_value_usd,
    mv_monthly_net_insider_transactions.net_value_usd,
    mv_monthly_net_insider_transactions.buy_shares,
    mv_monthly_net_insider_transactions.sell_shares,
    mv_monthly_net_insider_transactions.net_shares,
    mv_monthly_net_insider_transactions.buy_tx,
    mv_monthly_net_insider_transactions.sell_tx
FROM public.mv_monthly_net_insider_transactions;
