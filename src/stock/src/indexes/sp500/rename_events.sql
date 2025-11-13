BEGIN;

-- 1) Rename the canonical ticker row (ticker_id stays the same)
UPDATE ticker
SET symbol = 'PSKY',
    last_update = NOW()
WHERE UPPER(symbol) = 'PARA';

-- 2) Keep mirrored info in sync (optional but useful)
-- UPDATE info_general_stock
-- SET symbol = 'PSKY',
--     last_update = NOW()
-- WHERE ticker_id = (SELECT id FROM ticker WHERE symbol = 'PSKY');

-- 3) Update historical snapshots (what you use with yfinance)
UPDATE sp_500_historical
SET symbol_at_date  = 'PSKY',
    ticker_yfinance = 'PSKY',     -- adjust here if YF symbol differs
    last_update     = NOW()
WHERE ticker_id = (SELECT id FROM ticker WHERE symbol = 'PSKY')
  AND date >= DATE '2025-08-07';  -- <-- effective date

-- 4) Text snapshot for changes (optional)
UPDATE sp_500_changes
SET symbol_at_event = 'PSKY',
    last_update     = NOW()
WHERE ticker_id = (SELECT id FROM ticker WHERE symbol = 'PSKY')
  AND date >= DATE '2025-08-07';

-- 5) Refresh dependent MVs (non-concurrent)
REFRESH MATERIALIZED VIEW public.mv_sp_500_latest_date;
REFRESH MATERIALIZED VIEW public.mv_ticker_overview;

COMMIT;
