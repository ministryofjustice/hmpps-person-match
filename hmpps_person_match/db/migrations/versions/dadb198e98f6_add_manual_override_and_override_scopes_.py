"""Add manual_override and override_scopes to person table

Revision ID: dadb198e98f6
Revises: 3aa1563f9c5b
Create Date: 2025-08-19 10:53:39.419432

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "dadb198e98f6"
down_revision: str | None = "3aa1563f9c5b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("person", sa.Column("manual_override", sa.VARCHAR(), nullable=True), schema="personmatch")
    op.add_column("person", sa.Column("override_scopes", sa.ARRAY(sa.VARCHAR()), nullable=True), schema="personmatch")


def downgrade() -> None:
    op.drop_column("person", "override_scopes", schema="personmatch")
    op.drop_column("person", "manual_override", schema="personmatch")
