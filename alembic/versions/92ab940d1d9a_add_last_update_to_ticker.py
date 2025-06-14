"""Add last update to Ticker

Revision ID: 92ab940d1d9a
Revises: ee2435c58309
Create Date: 2024-12-28 23:45:11.971568

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '92ab940d1d9a'
down_revision: Union[str, None] = 'ee2435c58309'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('ticker', sa.Column('last_update', sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column('ticker', 'last_update')

