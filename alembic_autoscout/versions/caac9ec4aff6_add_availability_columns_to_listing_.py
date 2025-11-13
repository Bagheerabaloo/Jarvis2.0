"""Add availability columns to listing_summary

Revision ID: caac9ec4aff6
Revises: add_seller_phone_listing_detail
Create Date: 2025-09-22 00:05:53.009092

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'caac9ec4aff6'
down_revision: Union[str, None] = 'add_seller_phone_listing_detail'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "listing_summary",
        sa.Column("is_available", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )
    op.add_column(
        "listing_summary",
        sa.Column("last_availability_check_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "listing_summary",
        sa.Column("unavailable_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("listing_summary", "unavailable_at")
    op.drop_column("listing_summary", "last_availability_check_at")
    op.drop_column("listing_summary", "is_available")
