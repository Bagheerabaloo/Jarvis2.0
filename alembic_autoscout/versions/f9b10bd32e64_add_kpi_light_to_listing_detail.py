"""add kpi_light to listing_detail

Revision ID: f9b10bd32e64
Revises: caac9ec4aff6
Create Date: 2025-09-24 11:28:38.035133

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f9b10bd32e64'
down_revision: Union[str, None] = 'caac9ec4aff6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "listing_detail",
        sa.Column("kpi_light", sa.Float(), nullable=True),
    )


def downgrade() -> None:
    op.add_column(
        "listing_detail",
        sa.Column("kpi_light", sa.Float(), nullable=True),
    )
