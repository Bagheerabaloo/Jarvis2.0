"""sp500: add ticker_id columns and backfill (phase 1)

Revision ID: 6bf194c98a69
Revises: 4f0b15b7553f
Create Date: 2025-10-19 18:11:09.864920

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6bf194c98a69'
down_revision: Union[str, None] = '4f0b15b7553f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Speed- and safety-friendly settings for this transaction only
    op.execute("SET LOCAL statement_timeout = '30min';")
    op.execute("SET LOCAL lock_timeout = '30s';")
    op.execute("SET LOCAL work_mem = '256MB';")

    # 1) Add new columns (nullable for now) + FKs
    op.add_column("sp_500_changes", sa.Column("ticker_id", sa.Integer(), nullable=True))
    op.add_column("sp_500_changes", sa.Column("symbol_at_event", sa.String(length=10), nullable=True))
    op.add_column("sp_500_historical", sa.Column("ticker_id", sa.Integer(), nullable=True))
    op.add_column("sp_500_historical", sa.Column("symbol_at_date", sa.String(length=10), nullable=True))

    op.create_foreign_key("fk_sp500_changes_ticker", "sp_500_changes", "ticker", ["ticker_id"], ["id"])
    op.create_foreign_key("fk_sp500_hist_ticker", "sp_500_historical", "ticker", ["ticker_id"], ["id"])

    # 2) Build a temp map once (UPPER + dot/hyphen variants) and backfill in one scan
    op.execute("""
        CREATE TEMP TABLE tmp_tickermap(sym_u TEXT PRIMARY KEY, tid INTEGER NOT NULL) ON COMMIT DROP;
        INSERT INTO tmp_tickermap(sym_u, tid)
        SELECT DISTINCT UPPER(symbol), id FROM ticker;
        INSERT INTO tmp_tickermap(sym_u, tid)
        SELECT DISTINCT UPPER(REPLACE(symbol, '.', '-')), id FROM ticker WHERE symbol LIKE '%.%'
        ON CONFLICT (sym_u) DO NOTHING;
        INSERT INTO tmp_tickermap(sym_u, tid)
        SELECT DISTINCT UPPER(REPLACE(symbol, '-', '.')), id FROM ticker WHERE symbol LIKE '%-%'
        ON CONFLICT (sym_u) DO NOTHING;
    """)

    op.execute("""
        UPDATE sp_500_changes c
           SET ticker_id = m.tid,
               symbol_at_event = COALESCE(c.symbol_at_event, c.ticker)
          FROM tmp_tickermap m
         WHERE c.ticker_id IS NULL
           AND UPPER(c.ticker) = m.sym_u;
    """)

    op.execute("""
        UPDATE sp_500_historical h
           SET ticker_id = m.tid,
               symbol_at_date = COALESCE(h.symbol_at_date, h.ticker)
          FROM tmp_tickermap m
         WHERE h.ticker_id IS NULL
           AND UPPER(h.ticker) = m.sym_u;
    """)

    # 3) Safety: enforce completeness then NOT NULL
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM sp_500_changes WHERE ticker_id IS NULL) THEN
            RAISE EXCEPTION 'sp_500_changes: NULL ticker_id remains after backfill';
        END IF;
        IF EXISTS (SELECT 1 FROM sp_500_historical WHERE ticker_id IS NULL) THEN
            RAISE EXCEPTION 'sp_500_historical: NULL ticker_id remains after backfill';
        END IF;
    END $$;
    """)
    op.alter_column("sp_500_changes", "ticker_id", nullable=False)
    op.alter_column("sp_500_historical", "ticker_id", nullable=False)

    # 4) Helpful indexes + uniques for the new key (PK switch is phase 2)
    op.create_index("ix_sp_500_changes_ticker_id_date", "sp_500_changes", ["ticker_id", "date"])
    op.create_index("ix_sp_500_historical_ticker_id_date", "sp_500_historical", ["ticker_id", "date"])
    op.create_unique_constraint("uix_sp_500_changes_tid_date", "sp_500_changes", ["ticker_id", "date"])
    op.create_unique_constraint("uix_sp_500_historical_tid_date", "sp_500_historical", ["ticker_id", "date"])


def downgrade():
    # Drop NOT NULL before removing constraints/columns
    op.alter_column("sp_500_historical", "ticker_id", nullable=True)
    op.alter_column("sp_500_changes", "ticker_id", nullable=True)

    # Drop uniques/indexes
    op.drop_constraint("uix_sp_500_historical_tid_date", "sp_500_historical", type_="unique")
    op.drop_constraint("uix_sp_500_changes_tid_date", "sp_500_changes", type_="unique")
    op.drop_index("ix_sp_500_historical_ticker_id_date", table_name="sp_500_historical")
    op.drop_index("ix_sp_500_changes_ticker_id_date", table_name="sp_500_changes")

    # Drop FKs
    op.drop_constraint("fk_sp500_hist_ticker", "sp_500_historical", type_="foreignkey")
    op.drop_constraint("fk_sp500_changes_ticker", "sp_500_changes", type_="foreignkey")

    # Drop added columns
    op.drop_column("sp_500_historical", "symbol_at_date")
    op.drop_column("sp_500_historical", "ticker_id")
    op.drop_column("sp_500_changes", "symbol_at_event")
    op.drop_column("sp_500_changes", "ticker_id")

