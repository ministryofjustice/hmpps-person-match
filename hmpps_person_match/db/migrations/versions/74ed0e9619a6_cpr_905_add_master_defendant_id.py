"""CPR-905-add-master-defendant-id

Revision ID: 74ed0e9619a6
Revises: 7405b52e440f
Create Date: 2025-09-24 14:30:33.147934

"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "74ed0e9619a6"
down_revision: str | None = "7405b52e440f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("master_defendant_id", sqlalchemy.Text),
        schema="personmatch",
    )


def downgrade() -> None:
    op.drop_column(
        table_name="person",
        column_name="master_defendant_id",
        schema="personmatch",
    )
