"""Info Trading Session Column data change

Revision ID: 53f25229845d
Revises: a7d1f1e293c1
Create Date: 2025-01-17 18:46:35.856684

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '53f25229845d'
down_revision: Union[str, None] = 'a7d1f1e293c1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('info_trading_session', 'trailing_pe',
               existing_type=sa.NUMERIC(precision=11, scale=7),
               type_=sa.Numeric(precision=13, scale=7),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'forward_pe',
               existing_type=sa.NUMERIC(precision=11, scale=7),
               type_=sa.Numeric(precision=13, scale=7),
               existing_nullable=True)


def downgrade() -> None:
    op.alter_column('info_trading_session', 'trailing_pe',
               existing_type=sa.Numeric(precision=13, scale=7),
               type_=sa.NUMERIC(precision=11, scale=7),
               existing_nullable=True)
    op.alter_column('info_trading_session', 'forward_pe',
               existing_type=sa.Numeric(precision=13, scale=7),
               type_=sa.NUMERIC(precision=11, scale=7),
               existing_nullable=True)
