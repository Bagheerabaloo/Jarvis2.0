"""Info Cash and Financial Ratios

Revision ID: ee2435c58309
Revises: 10187f7ea684
Create Date: 2024-12-28 11:41:07.279423

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ee2435c58309'
down_revision: Union[str, None] = '10187f7ea684'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# gross_margins = Column(Numeric(11, 10))  # Gross margins: precision 6, scale 5
# ebitda_margins = Column(Numeric(11, 10))  # EBITDA margins: precision 6, scale 5
# operating_margins = Column(Numeric(11, 10))  # Operating margins: precision 6, scale 5


def upgrade() -> None:
    op.alter_column(
        'info_cash_and_financial_ratios', 'gross_margins',
        existing_type=sa.Numeric(11, 10),
        type_=sa.Numeric(15, 10),
        existing_nullable=True
    )
    op.alter_column(
        'info_cash_and_financial_ratios', 'ebitda_margins',
        existing_type=sa.Numeric(11, 10),
        type_=sa.Numeric(15, 10),
        existing_nullable=True
    )
    op.alter_column(
        'info_cash_and_financial_ratios', 'operating_margins',
        existing_type=sa.Numeric(11, 10),
        type_=sa.Numeric(15, 10),
        existing_nullable=True
    )


def downgrade() -> None:
    op.alter_column(
        'info_cash_and_financial_ratios', 'gross_margins',
        existing_type=sa.Numeric(15, 10),
        type_=sa.Numeric(11, 10),
        existing_nullable=True
    )
    op.alter_column(
        'info_cash_and_financial_ratios', 'ebitda_margins',
        existing_type=sa.Numeric(15, 10),
        type_=sa.Numeric(11, 10),
        existing_nullable=True
    )
    op.alter_column(
        'info_cash_and_financial_ratios', 'operating_margins',
        existing_type=sa.Numeric(15, 10),
        type_=sa.Numeric(11, 10),
        existing_nullable=True
    )
