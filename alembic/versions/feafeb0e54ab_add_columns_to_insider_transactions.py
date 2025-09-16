"""Add columns to insider transactions

Revision ID: feafeb0e54ab
Revises: c3d366c79960
Create Date: 2025-09-05 14:25:35.055065

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'feafeb0e54ab'
down_revision: Union[str, None] = 'c3d366c79960'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('insider_transactions', sa.Column('price', sa.String(length=64), nullable=True))
    op.add_column('insider_transactions', sa.Column('avg_price', sa.Numeric(precision=18, scale=6), nullable=True))
    op.add_column('insider_transactions', sa.Column('state', sa.String(length=24), nullable=True))


def downgrade() -> None:
    pass
