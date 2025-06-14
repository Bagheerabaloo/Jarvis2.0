"""Add failed download to ticker

Revision ID: d367cc8bfafe
Revises: da98e07079b0
Create Date: 2025-01-31 16:19:56.233195

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd367cc8bfafe'
down_revision: Union[str, None] = 'da98e07079b0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('ticker', sa.Column('failed_candle_download', sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column('ticker', 'failed_candle_download')
