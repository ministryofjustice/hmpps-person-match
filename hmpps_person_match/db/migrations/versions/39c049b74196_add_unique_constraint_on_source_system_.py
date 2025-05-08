"""add unique constraint on source system id

Revision ID: 39c049b74196
Revises: e6e17f750d54
Create Date: 2025-05-08 14:06:34.286081

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "39c049b74196"
down_revision: str | None = "e6e17f750d54"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        constraint_name="uq_source_system_id",
        table_name="person",
        schema="personmatch",
        columns=["source_system_id"],
    )


def downgrade() -> None:
    op.drop_constraint(constraint_name="uq_source_system_id", table_name="person", schema="personmatch")
