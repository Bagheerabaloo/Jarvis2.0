"""increase ticker symbol length

Revision ID: e1510d4281c2
Revises: 068e3ea07741
Create Date: 2026-03-14 23:40:47.518478

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e1510d4281c2'
down_revision: Union[str, None] = '068e3ea07741'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


DROP_DEPENDENT_VIEWS_SQL = """
DROP VIEW IF EXISTS public.v_ticker_overview;
DROP VIEW IF EXISTS public.v_pe;
DROP VIEW IF EXISTS public.v_monthly_net_insider_transactions;
"""


DROP_DEPENDENT_MATERIALIZED_VIEWS_SQL = """
DROP MATERIALIZED VIEW IF EXISTS public.mv_ticker_overview;
DROP MATERIALIZED VIEW IF EXISTS public.mv_pe;
DROP MATERIALIZED VIEW IF EXISTS public.mv_monthly_net_insider_transactions;
"""


CREATE_MV_MONTHLY_NET_INSIDER_TRANSACTIONS_SQL = """
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
"""


CREATE_V_MONTHLY_NET_INSIDER_TRANSACTIONS_SQL = """
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
"""


CREATE_MV_PE_SQL = """
CREATE MATERIALIZED VIEW public.mv_pe AS
SELECT
    sub."Ticker",
    sub."Trailing PE",
    sub."Forward PE",
    sub.last_update,
    sub.date,
    sub.rn
FROM (
    SELECT
        t.symbol AS "Ticker",
        ts.trailing_pe AS "Trailing PE",
        ts.forward_pe AS "Forward PE",
        ts.last_update,
        date(ts.last_update) AS date,
        row_number() OVER (
            PARTITION BY t.symbol, date(ts.last_update)
            ORDER BY ts.last_update DESC
        ) AS rn
    FROM info_trading_session AS ts
    JOIN ticker AS t ON t.id = ts.ticker_id
) AS sub
WHERE sub.rn = 1
WITH DATA;
"""


CREATE_V_PE_SQL = """
CREATE OR REPLACE VIEW public.v_pe AS
SELECT
    mv_pe."Ticker",
    mv_pe."Trailing PE",
    mv_pe."Forward PE",
    mv_pe.last_update,
    mv_pe.date,
    mv_pe.rn
FROM public.mv_pe;
"""


CREATE_MV_TICKER_OVERVIEW_SQL = """
CREATE MATERIALIZED VIEW public.mv_ticker_overview AS
SELECT
    t.id AS ticker_id,
    t.symbol,
    t.company_name,
    t.last_update AS ticker_last_update,
    igs.exchange,
    igs.last_update AS info_general_last_update,
    its.market_cap,
    its.fifty_two_week_high AS "52_week_high",
    its.fifty_two_week_low AS "52_week_low",
    its.two_hundred_day_average AS "200MA",
    its.current_price AS price,
    its.last_update AS info_trading_last_update,
    its.trailing_pe,
    its.forward_pe,
    rcd.close,
    rcd.last_update AS candle_day_last_update,
    lisi.sector,
    lisi.industry,
    lisi.last_update AS sector_industry_last_update,
    (sp.ticker_id IS NOT NULL) AS sp500
FROM ticker AS t
LEFT JOIN mv_last_info_general_stock AS igs ON igs.ticker_id = t.id
LEFT JOIN mv_last_info_trading_session AS its ON its.ticker_id = t.id
LEFT JOIN mv_recent_candle_data_day AS rcd ON rcd.ticker_id = t.id
LEFT JOIN mv_sp_500_latest_date AS sp ON sp.ticker_id = t.id
LEFT JOIN mv_last_info_sector_industry AS lisi ON lisi.ticker_id = t.id
WITH DATA;
"""


CREATE_MV_TICKER_OVERVIEW_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS mv_ticker_overview_tid_idx
    ON public.mv_ticker_overview (ticker_id);
CREATE INDEX IF NOT EXISTS mv_ticker_overview_sym_idx
    ON public.mv_ticker_overview (symbol);
"""


CREATE_V_TICKER_OVERVIEW_SQL = """
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
"""


def upgrade() -> None:
    op.execute(DROP_DEPENDENT_VIEWS_SQL)
    op.execute(DROP_DEPENDENT_MATERIALIZED_VIEWS_SQL)

    op.alter_column(
        "ticker",
        "symbol",
        existing_type=sa.String(length=10),
        type_=sa.String(length=32),
        existing_nullable=False,
    )

    op.execute(CREATE_MV_MONTHLY_NET_INSIDER_TRANSACTIONS_SQL)
    op.execute(CREATE_V_MONTHLY_NET_INSIDER_TRANSACTIONS_SQL)

    op.execute(CREATE_MV_PE_SQL)
    op.execute(CREATE_V_PE_SQL)

    op.execute(CREATE_MV_TICKER_OVERVIEW_SQL)
    op.execute(CREATE_MV_TICKER_OVERVIEW_INDEXES_SQL)
    op.execute(CREATE_V_TICKER_OVERVIEW_SQL)


def downgrade() -> None:
    op.execute(DROP_DEPENDENT_VIEWS_SQL)
    op.execute(DROP_DEPENDENT_MATERIALIZED_VIEWS_SQL)

    op.alter_column(
        "ticker",
        "symbol",
        existing_type=sa.String(length=32),
        type_=sa.String(length=10),
        existing_nullable=False,
    )

    op.execute(CREATE_MV_MONTHLY_NET_INSIDER_TRANSACTIONS_SQL)
    op.execute(CREATE_V_MONTHLY_NET_INSIDER_TRANSACTIONS_SQL)

    op.execute(CREATE_MV_PE_SQL)
    op.execute(CREATE_V_PE_SQL)

    op.execute(CREATE_MV_TICKER_OVERVIEW_SQL)
    op.execute(CREATE_MV_TICKER_OVERVIEW_INDEXES_SQL)
    op.execute(CREATE_V_TICKER_OVERVIEW_SQL)
