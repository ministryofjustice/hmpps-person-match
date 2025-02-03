"""add match id column

Revision ID: 941c0190bb2f
Revises: d8bcc04760f3
Create Date: 2025-02-03 16:26:18.504471

"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "941c0190bb2f"
down_revision: str | None = "d8bcc04760f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("match_id", sqlalchemy.Text),
        schema="personmatch",
    )
    op.create_unique_constraint(
        constraint_name="uq_match_id",
        table_name="person",
        schema="personmatch",
        columns=["match_id"],
    )
    op.create_index(index_name="ik_match_id", table_name="person", schema="personmatch", columns=["match_id"])


def downgrade() -> None:
    op.drop_index(index_name="ik_match_id", table_name="person", schema="personmatch")
    op.drop_constraint(constraint_name="uq_match_id", table_name="person", schema="personmatch")
    op.drop_column(
        table_name="person",
        column_name="match_id",
        schema="personmatch",
    )
