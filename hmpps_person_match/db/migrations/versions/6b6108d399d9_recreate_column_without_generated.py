"""recreate-column-without-generated

Revision ID: 6b6108d399d9
Revises: 1068529d125d
Create Date: 2025-03-14 11:30:51.568940

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6b6108d399d9"
down_revision: str | None = "1068529d125d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def drop_and_recreate_columns(column_name: str, col_type: str = "varchar") -> None:
    """
    Have to drop and recreate columns to remove 'GENERATED AS' functionality
    """
    op.execute(f"ALTER table personmatch.person DROP COLUMN {column_name}")
    op.execute(f"ALTER table personmatch.person ADD COLUMN {column_name} {col_type};")

def upgrade() -> None:
    drop_and_recreate_columns("postcode_first")
    drop_and_recreate_columns("postcode_second")
    drop_and_recreate_columns("postcode_last")
    drop_and_recreate_columns("postcode_outcode_first")
    drop_and_recreate_columns("postcode_outcode_last")
    drop_and_recreate_columns("date_of_birth_last", "date")
    drop_and_recreate_columns("forename_first")
    drop_and_recreate_columns("forename_last")
    drop_and_recreate_columns("last_name_first")
    drop_and_recreate_columns("last_name_last")
    drop_and_recreate_columns("sentence_date_first", "date")
    drop_and_recreate_columns("sentence_date_last", "date")


def downgrade() -> None:
    # TODO
    pass
