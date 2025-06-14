"""add candle_error to ticker

Revision ID: 6dfb32a19d26
Revises: d367cc8bfafe
Create Date: 2025-01-31 22:09:36.925329

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6dfb32a19d26'
down_revision: Union[str, None] = 'd367cc8bfafe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('ticker', sa.Column('yf_error', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('ticker', 'yf_error')
