-- GET ALL TICKERS WITH INFO--
WITH ranked_general AS (
    SELECT
        T.id,
        T.symbol,
        T.company_name,
        IGS.exchange,
        IGS.last_update,
        ROW_NUMBER() OVER (PARTITION BY T.symbol ORDER BY IGS.last_update DESC) AS rn
    FROM ticker T
    JOIN info_general_stock IGS
        ON T.id = IGS.ticker_id
),
ranked_trading AS (
    SELECT
        T.id,
        ITS.market_cap,
        ITS.trailing_pe,
        ITS.forward_pe,
        ITS.current_price,
        ITS.two_hundred_day_average,
        ITS.fifty_two_week_high,
        ITS.fifty_two_week_low,
        ROW_NUMBER() OVER (PARTITION BY T.symbol ORDER BY ITS.last_update DESC) AS rn
    FROM ticker T
    JOIN info_trading_session ITS
        ON T.id = ITS.ticker_id
)
SELECT G.id, G.symbol, G.company_name,
       G.exchange, G.last_update,
       T.market_cap
       ,T.trailing_pe, T.forward_pe
       ,T.current_price, T.two_hundred_day_average
       ,T.fifty_two_week_high, T.fifty_two_week_low
       ,(T.current_price / T.two_hundred_day_average - 1) * 100 AS price_to_200_day_avg
       ,(T.current_price / T.fifty_two_week_high - 1) * 100 AS price_to_52_week_high
       ,(T.current_price / T.fifty_two_week_low - 1) * 100 AS price_to_52_week_low
FROM ranked_general G
JOIN ranked_trading T
    ON G.id = T.id
WHERE G.rn = 1 AND T.rn = 1
    AND T.market_cap IS NOT NULL
--     AND (G.exchange = 'NMS' OR G.exchange = 'NGM' OR G.exchange = 'NCM') -- NASDAQ
--     AND G.exchange = ' NYQ' -- NYSE
--     AND G.exchange = 'ASE' -- NYSE MKT
    AND G.exchange != 'NCM' -- NOT 'NCM'
ORDER BY T.market_cap DESC;


-- TABLE DIMENSIONS --
SELECT
    schemaname AS schema_name,
    tablename AS table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) AS table_size,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename) - pg_relation_size(schemaname || '.' || tablename)) AS indexes_size
FROM
    pg_tables
WHERE
    schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY
    pg_total_relation_size(schemaname || '.' || tablename) DESC;


-- SP500 TICKERS --
SELECT SP.*, T.symbol, T.id
FROM sp_500_historical SP
LEFT JOIN ticker T ON SP.ticker_yfinance = T.symbol
WHERE date = (
    SELECT MAX(date)
    FROM sp_500_historical
)
--AND SP.ticker LIKE 'BRK%'
--AND T.id IS NULL
;

SELECT *
FROM ticker
WHERE symbol LIKE 'BRK%';

SELECT DISTINCT ticker, ticker_yfinance
FROM sp_500_historical
WHERE ticker in(
    SELECT DISTINCT ticker
    FROM sp_500_historical
    WHERE ticker LIKE '%.%'
    )
;

UPDATE sp_500_historical
SET ticker_yfinance = REPLACE(ticker, '.', '-');

COMMIT;


-- LAST TRADING SESSION
SELECT t.ticker_id,
       t.market_cap,
       t.current_price,
       t.last_update,
       t.two_hundred_day_average,
       t.fifty_two_week_high,
       t.fifty_two_week_low,
       t.trailing_pe,
       t.forward_pe
FROM (
    SELECT
        i.ticker_id,
        i.market_cap,
        i.current_price,
        i.last_update,
        i.two_hundred_day_average,
        i.fifty_two_week_high,
        i.fifty_two_week_low,
        i.trailing_pe,
        i.forward_pe,
        -- Usa ROW_NUMBER() o RANK() a seconda di come vuoi gestire i "pareggi"
        ROW_NUMBER() OVER (
            PARTITION BY i.ticker_id
            ORDER BY i.last_update DESC
        ) AS rn
    FROM info_trading_session AS i
) AS t
WHERE t.rn = 1;


-- LAST TICKER INFO
SELECT
    t.id                     AS ticker_id,
    t.symbol                 AS symbol,
    t.company_name           AS company_name,
    t.last_update            AS ticker_last_update,

    -- Data from last_info_general_stock
    igs.exchange             AS exchange,
    igs.last_update          AS info_general_last_update,

    -- Data from last_info_trading_session
    its.market_cap           AS market_cap,
    its.fifty_two_week_high  AS "52_week_high",
    its.fifty_two_week_low   AS "52_week_low",
    its.two_hundred_day_average AS "200MA",
    its.current_price        AS price,
    its.last_update          AS info_trading_last_update,
    its.trailing_pe          AS trailing_pe,
    its.forward_pe           AS forward_pe,

    -- Data from recent_candle_data_day_view
    rcd.close                AS close,
    rcd.last_update          AS candle_day_last_update,

    -- Check S&P500 membership (True/False)
    CASE
        WHEN sp.ticker_yfinance IS NOT NULL THEN TRUE
        ELSE FALSE
    END                     AS SP500

FROM ticker t

-- LEFT JOIN to last_info_general_stock by ticker_id
LEFT JOIN mv_last_info_general_stock igs
       ON igs.ticker_id = t.id

-- LEFT JOIN to last_info_trading_session by ticker_id
LEFT JOIN mv_last_info_trading_session its
       ON its.ticker_id = t.id

-- LEFT JOIN to recent_candle_data_day by ticker_id
LEFT JOIN mv_recent_candle_data_day rcd
       ON rcd.ticker_id = t.id

-- LEFT JOIN to sp_500_latest_date by symbol
LEFT JOIN sp_500_latest_date sp
       ON sp.ticker_yfinance = t.symbol;


-- Monthly BUY/SELL and NET dollar volume by ticker
CREATE materialized VIEW public.mv_monthly_net_insider_transactions AS
WITH m AS (
  SELECT
    t.ticker_id,
    DATE_TRUNC('month', t.start_date) AS month,
    SUM(CASE WHEN t.state='Entry' AND COALESCE(t.avg_price,0) > 0 THEN t.value ELSE 0 END) AS buy_value_usd,
    SUM(CASE WHEN t.state='Exit'                                             THEN t.value ELSE 0 END) AS sell_value_usd,
    SUM(CASE WHEN t.state='Entry' AND COALESCE(t.avg_price,0) > 0 THEN t.shares ELSE 0 END) AS buy_shares,
    SUM(CASE WHEN t.state='Exit'                                             THEN t.shares ELSE 0 END) AS sell_shares,
    COUNT(*) FILTER (WHERE t.state='Entry' AND COALESCE(t.avg_price,0) > 0) AS buy_tx,
    COUNT(*) FILTER (WHERE t.state='Exit')                                   AS sell_tx
  FROM insider_transactions t
  WHERE t.value IS NOT NULL
  GROUP BY t.ticker_id, DATE_TRUNC('month', t.start_date)
)
SELECT
  tk.symbol AS ticker,
  m.month,
  m.buy_value_usd,
  m.sell_value_usd,
  (m.buy_value_usd - m.sell_value_usd)      AS net_value_usd,
  m.buy_shares,
  m.sell_shares,
  (m.buy_shares - m.sell_shares)            AS net_shares,
  m.buy_tx,
  m.sell_tx
FROM m
JOIN ticker tk ON tk.id = m.ticker_id
ORDER BY month desc, buy_value_usd DESC;                 -- change to buy_value_usd DESC if you prefer


CREATE VIEW public.v_monthly_net_insider_transactions AS
SELECT *
FROM public.mv_monthly_net_insider_transactions;

DROP VIEW v_monthly_net_insider_transactions;