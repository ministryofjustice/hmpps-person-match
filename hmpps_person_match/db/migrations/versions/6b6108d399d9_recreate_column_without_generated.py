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

###########################
## UPGRADE
###########################
def drop_expression(column_name: str, col_type: str = "varchar") -> None:
    """
    Remove 'GENERATED AS' functionality
    """
    op.execute(f"ALTER table personmatch.person ALTER COLUMN {column_name} DROP EXPRESSION IF EXISTS;")

def upgrade() -> None:
    drop_expression("postcode_first")
    drop_expression("postcode_second")
    drop_expression("postcode_last")
    drop_expression("postcode_outcode_first")
    drop_expression("postcode_outcode_last")
    drop_expression("date_of_birth_last", "date")
    drop_expression("forename_first")
    drop_expression("forename_last")
    drop_expression("last_name_first")
    drop_expression("last_name_last")
    drop_expression("sentence_date_first", "date")
    drop_expression("sentence_date_last", "date")

###########################
## DOWNGRADE
###########################

def create_generated_column_sql(column_name: str, expression: str, col_type: str = "VARCHAR") -> str:
    sql = f"""
    ALTER TABLE personmatch.person
    ADD COLUMN {column_name} {col_type}
    GENERATED ALWAYS AS (personmatch.person.{expression}) STORED;
    """
    return sql

def array_last_sql(array_name: str) -> str:
    return f"{array_name}[array_length({array_name}, 1)]"

def downgrade() -> None:
    op.drop_column("person", column_name="postcode_first", schema="personmatch")
    op.execute(create_generated_column_sql("postcode_first", "postcode_arr[1]"))

    op.drop_column("person", column_name="postcode_second", schema="personmatch")
    op.execute(create_generated_column_sql("postcode_second", "postcode_arr[2]"))

    op.drop_column("person", column_name="postcode_last", schema="personmatch")
    op.execute(create_generated_column_sql("postcode_last", array_last_sql("postcode_arr")))

    op.drop_column("person", column_name="postcode_outcode_first", schema="personmatch")
    op.execute(create_generated_column_sql("postcode_outcode_first", "postcode_outcode_arr[1]"))

    op.drop_column("person", column_name="postcode_outcode_last", schema="personmatch")
    op.execute(create_generated_column_sql("postcode_outcode_last", array_last_sql("postcode_outcode_arr")))

    op.drop_column("person", column_name="date_of_birth_last", schema="personmatch")
    op.execute(create_generated_column_sql("date_of_birth_last", array_last_sql("date_of_birth_arr"), "date"))

    op.drop_column("person", column_name="forename_first", schema="personmatch")
    op.execute(create_generated_column_sql("forename_first", "forename_std_arr[1]"))

    op.drop_column("person", column_name="forename_last", schema="personmatch")
    op.execute(create_generated_column_sql("forename_last", array_last_sql("forename_std_arr")))

    op.drop_column("person", column_name="last_name_first", schema="personmatch")
    op.execute(create_generated_column_sql("last_name_first", "last_name_std_arr[1]"))

    op.drop_column("person", column_name="last_name_last", schema="personmatch")
    op.execute(create_generated_column_sql("last_name_last", array_last_sql("last_name_std_arr")))

    op.drop_column("person", column_name="sentence_date_first", schema="personmatch")
    op.execute(create_generated_column_sql("sentence_date_first", "sentence_date_arr[1]", "date"))

    op.drop_column("person", column_name="sentence_date_last", schema="personmatch")
    op.execute(create_generated_column_sql("sentence_date_last", array_last_sql("sentence_date_arr"), "date"))
