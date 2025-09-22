"""add override marker index

Revision ID: 8376154e5f99
Revises: b519345b7ca3
Create Date: 2025-09-08 15:23:22.913320

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8376154e5f99"
down_revision: str | None = "b519345b7ca3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        index_name="idx_override_marker",
        table_name="person",
        columns=["override_marker"],
        schema="personmatch",
    )


def downgrade() -> None:
    op.drop_index(index_name="idx_override_marker", table_name="person", schema="personmatch", if_exists=True)
