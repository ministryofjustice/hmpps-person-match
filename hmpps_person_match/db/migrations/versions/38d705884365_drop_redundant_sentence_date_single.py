"""drop redundant sentence date single

Revision ID: 38d705884365
Revises: 6b6108d399d9
Create Date: 2025-03-20 09:25:49.322058

"""

from collections.abc import Sequence

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "38d705884365"
down_revision: str | None = "6b6108d399d9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # this column has identical data to sentence_date_last, by definition
    op.drop_column("person", "sentence_date_single", schema="personmatch")


def downgrade() -> None:
    op.add_column(
        table_name="person",
        column=sqlalchemy.Column("sentence_date_single", sqlalchemy.Date),
        schema="personmatch",
    )
    op.execute(
        "UPDATE personmatch.person SET sentence_date_single = sentence_date_last;",
    )
