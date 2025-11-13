"""Change PK on slv_earnings_history to (ticker_id, quarter_date) and add indexes

Revision ID: 068e3ea07741
Revises: 0dcfdbd2f1c1
Create Date: 2025-10-24 11:28:43.410581

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '068e3ea07741'
down_revision: Union[str, None] = '0dcfdbd2f1c1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop eventuale indice vecchio se esiste
    op.execute('DROP INDEX IF EXISTS ix_slv_earnings_history_ticker_quarter;')

    # Droppa la PK esistente qualunque nome abbia (tabella vuota: è safe)
    op.execute("""
    DO $$
    DECLARE pk_name text;
    BEGIN
      SELECT conname INTO pk_name
      FROM pg_constraint
      WHERE conrelid = 'slv_earnings_history'::regclass
        AND contype = 'p';
      IF pk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE slv_earnings_history DROP CONSTRAINT %I', pk_name);
      END IF;
    END$$;
    """)

    # Crea la nuova PK su (ticker_id, quarter_date)
    op.create_primary_key(
        'slv_earnings_history_pkey',
        'slv_earnings_history',
        ['ticker_id', 'quarter_date']
    )

    # Indici di supporto
    op.create_index(
        'ix_slv_earn_hist_ticker_quarterdate',
        'slv_earnings_history',
        ['ticker_id', 'quarter_date'],
        unique=False
    )
    op.create_index(
        'ix_slv_earn_hist_ticker_year_quarter',
        'slv_earnings_history',
        ['ticker_id', 'year', 'quarter'],
        unique=False
    )


def downgrade() -> None:
    # Drop nuovi indici
    op.drop_index('ix_slv_earn_hist_ticker_year_quarter', table_name='slv_earnings_history')
    op.drop_index('ix_slv_earn_hist_ticker_quarterdate', table_name='slv_earnings_history')

    # Ripristina la PK precedente (quarter_date, last_update, ticker_id)
    op.drop_constraint('slv_earnings_history_pkey', 'slv_earnings_history', type_='primary')
    op.create_primary_key(
        'slv_earnings_history_pkey',
        'slv_earnings_history',
        ['quarter_date', 'last_update', 'ticker_id']
    )

    # (Opzionale) ripristina il vecchio indice se ti serve
    op.create_index(
        'ix_slv_earnings_history_ticker_quarter',
        'slv_earnings_history',
        ['ticker_id', 'quarter', 'year'],
        unique=False
    )
