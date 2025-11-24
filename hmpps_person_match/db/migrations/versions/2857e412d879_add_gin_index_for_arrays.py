"""add GIN index for arrays

Revision ID: 2857e412d879
Revises: afa82883199f
Create Date: 2025-11-20 11:34:22.781915

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2857e412d879"
down_revision: str | None = "afa82883199f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_postcode_arr_gin",
        columns=["postcode_arr"],
        table_name="person",
        schema="personmatch",
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("idx_postcode_arr_gin", table_name="person")
