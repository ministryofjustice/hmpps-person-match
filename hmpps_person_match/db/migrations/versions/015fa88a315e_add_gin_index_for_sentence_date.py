"""add_gin_index_for_sentence_date

Revision ID: 015fa88a315e
Revises: 2857e412d879
Create Date: 2025-11-28 08:16:28.983081

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "015fa88a315e"
down_revision: str | None = "2857e412d879"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_sentence_date_arr_gin",
        columns=["sentence_date_arr"],
        table_name="person",
        schema="personmatch",
        postgresql_using="gin",
    )
    # Use op.execute as the index name exceeds the 63-character limit enforced by op.drop_index
    op.execute("DROP INDEX personmatch.idx_substring_name_1_std__substring_last_name_std__sentence_date_last")
    op.drop_index("idx_sentence_date_first__date_of_birth")


def downgrade() -> None:
    op.drop_index("idx_sentence_date_arr_gin", table_name="person")
    op.create_index(
        index_name="idx_substring_name_1_std__substring_last_name_std__sentence_date_last",
        columns=[
            "SUBSTRING(name_1_std FROM 1 FOR 2)",
            "SUBSTRING(last_name_std FROM 1 FOR 2)",
            "sentence_date_last",
        ],
        table_name="person",
    )
    op.create_index(
        index_name="idx_sentence_date_first__date_of_birth",
        columns=["sentence_date_first", "date_of_birth"],
        table_name="person",
    )
