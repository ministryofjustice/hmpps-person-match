"""make override index partial

Revision ID: 97ce3cfb153d
Revises: 7405b52e440f
Create Date: 2025-09-10 14:33:08.499070

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "97ce3cfb153d"
down_revision: str | None = "7405b52e440f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index(index_name="idx_override_marker", table_name="person", schema="personmatch", if_exists=True)
    op.execute(
        """
        CREATE INDEX idx_override_marker
        ON personmatch.person (override_marker)
        WHERE override_marker IS NOT NULL
        """,
    )

def downgrade() -> None:
    op.create_index(
        index_name="idx_override_marker", table_name="person", columns=["override_marker"], schema="personmatch",
    )
    op.execute(
        """
        DROP INDEX idx_override_marker
        """,
    )
