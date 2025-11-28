"""add index on master defendant id

Revision ID: afa82883199f
Revises: 74ed0e9619a6
Create Date: 2025-11-17 10:53:01.661237

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "afa82883199f"
down_revision: str | None = "74ed0e9619a6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        index_name="idx_master_defendant_id",
        table_name="person",
        columns=["master_defendant_id"],
        schema="personmatch",
    )


def downgrade() -> None:
    op.drop_index(index_name="idx_master_defendant_id", table_name="person", schema="personmatch", if_exists=True)
