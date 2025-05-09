"""add source system id column

Revision ID: e6e17f750d54
Revises: 38d705884365
Create Date: 2025-05-08 11:52:23.412006

"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e6e17f750d54"
down_revision: str | None = "38d705884365"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("source_system_id", sqlalchemy.Text),
        schema="personmatch",
    )


def downgrade() -> None:
    op.drop_column(table_name="person", column_name="source_system_id", schema="personmatch")
