"""add seller_phone to listing_detail

Revision ID: 7ce10d785453
Revises: 85de28a445c8
Create Date: 2025-09-16 19:20:02.569351

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_seller_phone_listing_detail'
down_revision: Union[str, None] = '85de28a445c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "listing_detail",
        sa.Column("seller_phone", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    pass
