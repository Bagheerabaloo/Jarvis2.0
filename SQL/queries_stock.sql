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
