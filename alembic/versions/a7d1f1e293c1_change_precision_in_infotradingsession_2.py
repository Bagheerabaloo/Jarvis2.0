"""Change precision in InfoTradingSession 2

Revision ID: a7d1f1e293c1
Revises: 5207cd484ef1
Create Date: 2024-12-29 00:23:36.768837

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a7d1f1e293c1'
down_revision: Union[str, None] = '5207cd484ef1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('info_trading_session', 'fifty_day_average',
               existing_type=sa.NUMERIC(precision=11, scale=6),
               type_=sa.Numeric(precision=13, scale=6),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'two_hundred_day_average',
               existing_type=sa.NUMERIC(precision=10, scale=5),
               type_=sa.Numeric(precision=13, scale=5),
               existing_nullable=True)


def downgrade() -> None:
    op.alter_column('info_trading_session', 'two_hundred_day_average',
               existing_type=sa.Numeric(precision=13, scale=5),
               type_=sa.NUMERIC(precision=10, scale=5),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'fifty_day_average',
               existing_type=sa.Numeric(precision=13, scale=6),
               type_=sa.NUMERIC(precision=11, scale=6),
               existing_nullable=True)