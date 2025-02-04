"""id to sequential int

Revision ID: e15ba08dba11
Revises: 941c0190bb2f
Create Date: 2025-02-04 13:35:29.683008

"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e15ba08dba11"
down_revision: str | None = "941c0190bb2f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # need to convert id column to sequential int - can't cast, so drop + add
    op.drop_column(
        table_name="person",
        column_name="id",
        schema="personmatch",
    )
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column(
            "id",
            sqlalchemy.Integer,
            nullable=False,
            primary_key=True,
            autoincrement=True,
        ),
        schema="personmatch",
    )
    # primary key gives index automatically
    op.create_primary_key(
        constraint_name="person_pk",
        table_name="person",
        columns=["id"],
        schema="personmatch",
    )
    # we get an index for free with unique constraint, so don't duplicate
    op.drop_index(index_name="ik_match_id", table_name="person", schema="personmatch")


def downgrade() -> None:
    op.alter_column(
        table_name="person",
        column_name="id",
        type_=sqlalchemy.Text,
        existing_type=sqlalchemy.Integer,
        schema="personmatch",
    )
    op.create_index(index_name="ik_match_id", table_name="person", schema="personmatch", columns=["match_id"])
