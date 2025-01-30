"""change id type

Revision ID: d8bcc04760f3
Revises: b1e99530d2e3
Create Date: 2025-01-30 14:16:41.004115

"""
from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d8bcc04760f3"
down_revision: str | None = "b1e99530d2e3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        table_name="person",
        column_name="id",
        type_=sqlalchemy.Text,
        existing_type=sqlalchemy.Integer,
        schema="personmatch",
    )


def downgrade() -> None:
    op.alter_column(
        table_name="person",
        column_name="id",
        type_=sqlalchemy.Text,
        existing_type=sqlalchemy.Integer,
        schema="personmatch",
    )
