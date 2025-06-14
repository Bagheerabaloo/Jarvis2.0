"""Change last_update from Date to DateTime

Revision ID: c3d366c79960
Revises: 6dfb32a19d26
Create Date: 2025-02-12 20:48:51.793133

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d366c79960'
down_revision: Union[str, None] = '6dfb32a19d26'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('info_sector_industry_history', 'last_update',
               existing_type=sa.DATE(),
               type_=sa.DateTime(),
               existing_nullable=False)


def downgrade() -> None:

    op.alter_column('info_sector_industry_history', 'last_update',
               existing_type=sa.DateTime(),
               type_=sa.DATE(),
               existing_nullable=False)
