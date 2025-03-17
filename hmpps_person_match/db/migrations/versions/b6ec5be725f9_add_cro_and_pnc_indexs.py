"""add cro and pnc indexs

Revision ID: b6ec5be725f9
Revises: 566b6f5a11c5
Create Date: 2025-02-28 14:17:55.762193

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b6ec5be725f9"
down_revision: str | None = "566b6f5a11c5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(index_name="idx_pnc_single", table_name="person", schema="personmatch", columns=["pnc_single"])
    op.create_index(index_name="idx_cro_single", table_name="person", schema="personmatch", columns=["cro_single"])


def downgrade() -> None:
    op.drop_index(index_name="idx_pnc_single", table_name="person", schema="personmatch")
    op.drop_index(index_name="idx_cro_single", table_name="person", schema="personmatch")
