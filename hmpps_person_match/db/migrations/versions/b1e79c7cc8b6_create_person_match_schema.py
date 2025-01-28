"""create person match schema

Revision ID: b1e79c7cc8b6
Revises:
Create Date: 2025-01-23 10:56:05.952888

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b1e79c7cc8b6"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA personmatch")


def downgrade() -> None:
    op.execute("DROP SCHEMA personmatch CASCADE")
