"""re-add matchId index

Revision ID: 3aa1563f9c5b
Revises: 9d94f60c7b20
Create Date: 2025-05-15 10:48:54.425000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3aa1563f9c5b"
down_revision: str | None = "9d94f60c7b20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(index_name="idx_match_id", table_name="person", schema="personmatch", columns=["match_id"])


def downgrade() -> None:
    op.drop_index(index_name="idx_match_id", table_name="person", schema="personmatch", if_exists=True)
