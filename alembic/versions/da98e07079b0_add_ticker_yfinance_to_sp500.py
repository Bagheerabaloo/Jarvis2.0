"""Add ticker yfinance to SP500

Revision ID: da98e07079b0
Revises: 53f25229845d
Create Date: 2025-01-28 13:35:04.159299

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'da98e07079b0'
down_revision: Union[str, None] = '53f25229845d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('sp_500_historical', sa.Column('ticker_yfinance', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('sp_500_historical', 'ticker_yfinance')
