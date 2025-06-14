"""Update numerica precision

Revision ID: 10187f7ea684
Revises: 
Create Date: 2024-12-22 00:56:44.959122

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '10187f7ea684'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'info_trading_session', 'last_price',
        existing_type=sa.Numeric(19, 15),
        type_=sa.Numeric(20, 15),
        existing_nullable=True
    )
    op.alter_column(
        'info_trading_session', 'year_high',
        existing_type=sa.Numeric(19, 15),
        type_=sa.Numeric(20, 15),
        existing_nullable=True
    )
    op.alter_column(
        'info_trading_session', 'year_low',
        existing_type=sa.Numeric(19, 15),
        type_=sa.Numeric(20, 15),
        existing_nullable=True
    )


def downgrade() -> None:
    op.alter_column(
        'info_trading_session', 'last_price',
        existing_type=sa.Numeric(20, 15),
        type_=sa.Numeric(19, 15),
        existing_nullable=True
    )
    op.alter_column(
        'info_trading_session', 'year_high',
        existing_type=sa.Numeric(20, 15),
        type_=sa.Numeric(19, 15),
        existing_nullable=True
    )
    op.alter_column(
        'info_trading_session', 'year_low',
        existing_type=sa.Numeric(20, 15),
        type_=sa.Numeric(19, 15),
        existing_nullable=True
    )
