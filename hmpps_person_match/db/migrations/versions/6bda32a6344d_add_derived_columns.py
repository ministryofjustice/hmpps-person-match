"""add derived columns

Revision ID: 6bda32a6344d
Revises: b6ec5be725f9
Create Date: 2025-03-12 16:36:10.183024

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6bda32a6344d"
down_revision: str | None = "b6ec5be725f9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def create_generated_column_sql(column_name: str, expression: str, col_type: str = "varchar") -> str:
    sql = f"""
    ALTER table personmatch.person
    ADD COLUMN {column_name} {col_type}
    GENERATED ALWAYS AS (personmatch.person.{expression}) STORED;
    """
    return sql

def array_last_sql(array_name: str) -> str:
    return f"{array_name}[array_length({array_name}, 1)]"

def upgrade() -> None:
    op.execute(create_generated_column_sql("postcode_first", "postcode_arr[1]"))
    op.execute(create_generated_column_sql("postcode_second", "postcode_arr[2]"))
    op.execute(create_generated_column_sql("postcode_last", array_last_sql("postcode_arr")))
    op.execute(create_generated_column_sql("postcode_outcode_first", "postcode_outcode_arr[1]"))
    op.execute(create_generated_column_sql("postcode_outcode_last", array_last_sql("postcode_outcode_arr")))
    op.execute(create_generated_column_sql("date_of_birth_last", array_last_sql("date_of_birth_arr"), "date"))
    op.execute(create_generated_column_sql("forename_first", "forename_std_arr[1]"))
    op.execute(create_generated_column_sql("forename_last", array_last_sql("forename_std_arr")))
    op.execute(create_generated_column_sql("last_name_first", "last_name_std_arr[1]"))
    op.execute(create_generated_column_sql("last_name_last", array_last_sql("last_name_std_arr")))
    op.execute(create_generated_column_sql("sentence_date_first", "sentence_date_arr[1]", "date"))
    op.execute(create_generated_column_sql("sentence_date_last", array_last_sql("sentence_date_arr"), "date"))


def downgrade() -> None:
    op.drop_column("postcode_first")
    op.drop_column("postcode_second")
    op.drop_column("postcode_last")
    op.drop_column("postcode_outcode_first")
    op.drop_column("postcode_outcode_last")
    op.drop_column("date_of_birth_last")
    op.drop_column("forename_first")
    op.drop_column("forename_last")
    op.drop_column("last_name_first")
    op.drop_column("last_name_last")
    op.drop_column("sentence_date_first")
    op.drop_column("sentence_date_last")
