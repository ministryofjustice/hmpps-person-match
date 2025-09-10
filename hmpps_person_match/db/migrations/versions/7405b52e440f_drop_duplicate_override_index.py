"""drop duplicate override index

Revision ID: 7405b52e440f
Revises: 8376154e5f99
Create Date: 2025-09-10 12:23:22.192006

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7405b52e440f"
down_revision: str | None = "8376154e5f99"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index("ix_personmatch_person_override_marker", table_name="person", schema="personmatch")


def downgrade() -> None:
    op.create_index(
        "ix_personmatch_person_override_marker",
        "person",
        ["override_marker"],
        unique=False,
        schema="personmatch",
    )
