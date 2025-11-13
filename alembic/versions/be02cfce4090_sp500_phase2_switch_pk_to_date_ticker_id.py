"""sp500: phase2 - switch PK to (date, ticker_id)

Revision ID: be02cfce4090
Revises: 6bf194c98a69
Create Date: 2025-10-20 01:29:00.350357

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'be02cfce4090'
down_revision: Union[str, None] = '6bf194c98a69'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Safety: quick checks (should be fast)
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM sp_500_changes WHERE ticker_id IS NULL) THEN
            RAISE EXCEPTION 'sp_500_changes: NULL ticker_id before PK switch';
        END IF;
        IF EXISTS (SELECT date, ticker_id, COUNT(*) FROM sp_500_changes GROUP BY 1,2 HAVING COUNT(*) > 1) THEN
            RAISE EXCEPTION 'sp_500_changes: duplicates on (date,ticker_id)';
        END IF;

        IF EXISTS (SELECT 1 FROM sp_500_historical WHERE ticker_id IS NULL) THEN
            RAISE EXCEPTION 'sp_500_historical: NULL ticker_id before PK switch';
        END IF;
        IF EXISTS (SELECT date, ticker_id, COUNT(*) FROM sp_500_historical GROUP BY 1,2 HAVING COUNT(*) > 1) THEN
            RAISE EXCEPTION 'sp_500_historical: duplicates on (date,ticker_id)';
        END IF;
    END $$;
    """)

    # sp_500_changes: drop legacy unique (ticker,date) and PK (date,ticker)
    op.drop_constraint("uix_sp_500_changes_ticker_date", "sp_500_changes", type_="unique")
    op.drop_constraint("sp_500_changes_pkey", "sp_500_changes", type_="primary")

    # new PK on (date, ticker_id)
    op.create_primary_key("sp_500_changes_pkey", "sp_500_changes", ["date", "ticker_id"])

    # sp_500_historical: drop old PK (date,ticker) and create new PK (date,ticker_id)
    op.drop_constraint("sp_500_historical_pkey", "sp_500_historical", type_="primary")
    op.create_primary_key("sp_500_historical_pkey", "sp_500_historical", ["date", "ticker_id"])

    # Optional cleanup: uniques on (ticker_id,date) created in Phase 1 are now redundant
    # Uncomment if you want to drop them.
    # op.drop_constraint("uix_sp_500_changes_tid_date", "sp_500_changes", type_="unique")
    # op.drop_constraint("uix_sp_500_historical_tid_date", "sp_500_historical", type_="unique")


def downgrade():
    # Restore old PKs
    op.drop_constraint("sp_500_historical_pkey", "sp_500_historical", type_="primary")
    op.create_primary_key("sp_500_historical_pkey", "sp_500_historical", ["date", "ticker"])

    op.drop_constraint("sp_500_changes_pkey", "sp_500_changes", type_="primary")
    op.create_primary_key("sp_500_changes_pkey", "sp_500_changes", ["date", "ticker"])

    # Restore legacy unique on (ticker,date) for sp_500_changes
    op.create_unique_constraint("uix_sp_500_changes_ticker_date", "sp_500_changes", ["ticker", "date"])
