"""Change precision in InfoTradingSession

Revision ID: 5207cd484ef1
Revises: 92ab940d1d9a
Create Date: 2024-12-29 00:21:15.851130

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5207cd484ef1'
down_revision: Union[str, None] = '92ab940d1d9a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('info_trading_session', 'last_price',
               existing_type=sa.NUMERIC(precision=20, scale=15),
               type_=sa.Numeric(precision=21, scale=15),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'year_high',
               existing_type=sa.NUMERIC(precision=20, scale=15),
               type_=sa.Numeric(precision=21, scale=15),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'year_low',
               existing_type=sa.NUMERIC(precision=20, scale=15),
               type_=sa.Numeric(precision=21, scale=15),
               existing_nullable=True)


def downgrade() -> None:
    op.alter_column('info_trading_session', 'year_low',
               existing_type=sa.Numeric(precision=21, scale=15),
               type_=sa.NUMERIC(precision=20, scale=15),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'year_high',
               existing_type=sa.Numeric(precision=21, scale=15),
               type_=sa.NUMERIC(precision=20, scale=15),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'last_price',
               existing_type=sa.Numeric(precision=21, scale=15),
               type_=sa.NUMERIC(precision=20, scale=15),
               existing_nullable=True)