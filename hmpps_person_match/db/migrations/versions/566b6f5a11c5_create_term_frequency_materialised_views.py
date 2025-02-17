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

def term_frequency_sql(column_name: str) -> str:
    return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS personmatch.term_frequencies_{column_name} AS (
            SELECT
                {column_name},
                cast(count(*) as float8) /
                    (select count({column_name}) as total from personmatch.person)
                    AS tf_{column_name}
            from personmatch.person
            where {column_name} is not null
            group by {column_name}
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
        op.execute(term_frequency_sql(column_name))
        op.create_index(
            index_name=f"ik_{column_name}",
            table_name=f"term_frequencies_{column_name}",
            schema="personmatch",
            columns=[column_name],
        )


def downgrade() -> None:
    for column_name in simple_tf_columns:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS personmatch.term_frequencies_{column_name}")
