"""Add override_marker and override_scopes to person table

Revision ID: b519345b7ca3
Revises: 3aa1563f9c5b
Create Date: 2025-08-26 14:07:42.161885

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b519345b7ca3"
down_revision: str | None = "3aa1563f9c5b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("person", sa.Column("override_marker", sa.VARCHAR(), nullable=True), schema="personmatch")
    op.add_column("person", sa.Column("override_scopes", sa.ARRAY(sa.VARCHAR()), nullable=True), schema="personmatch")

    op.create_index(
        "ix_personmatch_person_override_marker",
        "person",
        ["override_marker"],
        unique=False,
        schema="personmatch",
    )


def downgrade() -> None:
    op.drop_index("ix_personmatch_person_override_marker", table_name="person", schema="personmatch")
    op.drop_column("person", "override_scopes", schema="personmatch")
    op.drop_column("person", "override_marker", schema="personmatch")
