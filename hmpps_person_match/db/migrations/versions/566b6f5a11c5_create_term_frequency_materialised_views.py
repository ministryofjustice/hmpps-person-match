"""create term frequency materialised views

Revision ID: 566b6f5a11c5
Revises: e15ba08dba11
Create Date: 2025-02-17 11:37:46.719432

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "566b6f5a11c5"
down_revision: str | None = "e15ba08dba11"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def term_frequency_select_sql(column_name: str, from_table: str = "personmatch.person") -> str:
    return f"""
        SELECT
            {column_name},
            COUNT(*)::float8 / SUM(COUNT(*)) OVER () AS tf_{column_name}
        FROM {from_table}
        WHERE {column_name} IS NOT NULL
        GROUP BY {column_name}
    """  # noqa: S608


def term_frequency_sql_simple(column_name: str) -> str:
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS personmatch.term_frequencies_{column_name} AS (
            {term_frequency_select_sql(column_name)}
        );
        """  # noqa: S608


def term_frequency_sql_array(column_name: str) -> str:
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS personmatch.term_frequencies_{column_name} AS (
            WITH exploded_table AS (
                SELECT UNNEST({column_name}_arr) AS {column_name}
                FROM personmatch.person
            )
            {term_frequency_select_sql(column_name, "exploded_table")}
        );
        """  # noqa: S608


simple_tf_columns = (
    "name_1_std",
    "name_2_std",
    "last_name_std",
    "first_and_last_name_std",
    "date_of_birth",
    "cro_single",
    "pnc_single",
)


def upgrade() -> None:
    for column_name in simple_tf_columns:
        op.execute(term_frequency_sql_simple(column_name))
        op.create_index(
            index_name=f"ik_{column_name}",
            table_name=f"term_frequencies_{column_name}",
            schema="personmatch",
            columns=[column_name],
            unique=True,
        )
    column_name = "postcode"
    op.execute(term_frequency_sql_array(column_name))
    op.create_index(
        index_name=f"ik_{column_name}",
        table_name=f"term_frequencies_{column_name}",
        schema="personmatch",
        columns=[column_name],
        unique=True,
    )


def downgrade() -> None:
    for column_name in simple_tf_columns:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS personmatch.term_frequencies_{column_name}")
