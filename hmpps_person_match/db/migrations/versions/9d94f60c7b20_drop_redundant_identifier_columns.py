"""drop redundant identifier columns

Revision ID: 9d94f60c7b20
Revises: 39c049b74196
Create Date: 2025-05-09 11:56:25.837283

"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9d94f60c7b20"
down_revision: str | None = "39c049b74196"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column(table_name="person", column_name="crn", schema="personmatch")
    op.drop_column(table_name="person", column_name="prison_number", schema="personmatch")

def downgrade() -> None:
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("crn", sqlalchemy.Text),
        schema="personmatch",
    )
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("prison_number", sqlalchemy.Text),
        schema="personmatch",
    )
