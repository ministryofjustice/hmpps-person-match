"""add source system id column
Revision ID: 9b60b9a50127
Revises: 38d705884365
Create Date: 2025-05-07 13:56:43.984089
"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9b60b9a50127"
down_revision: str | None = "38d705884365"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("source_system_id", sqlalchemy.Text),
        schema="personmatch",
    )
    op.drop_column(table_name="person", column_name="crn", schema="personmatch")
    op.drop_column(table_name="person", column_name="prison_number", schema="personmatch")
    op.create_unique_constraint(
        constraint_name="uq_source_system_id",
        table_name="person",
        schema="personmatch",
        columns=["source_system_id"],
    )


def downgrade() -> None:
    op.drop_constraint(constraint_name="uq_source_system_id", table_name="person", schema="personmatch")
    op.drop_column(table_name="person", column_name="source_system_id", schema="personmatch")
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("crn", sqlalchemy.Text),
        schema="personmatch",
    )
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("prison_number", sqlalchemy.Text),
        schema="personmatch",
    )
