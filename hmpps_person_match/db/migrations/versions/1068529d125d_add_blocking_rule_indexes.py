"""add blocking rule indexes

Revision ID: 1068529d125d
Revises: 6bda32a6344d
Create Date: 2025-03-12 17:30:28.675830

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1068529d125d"
down_revision: str | None = "6bda32a6344d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def create_index(index_name: str, columns: list[str]) -> None:
    col_string = ", ".join(columns)
    op.execute(
        f"create index {index_name} on personmatch.person ({col_string});",
    )


def upgrade() -> None:
    create_index(
        index_name="idx_date_of_birth__postcode_first",
        columns=["date_of_birth", "postcode_first"],
    )
    create_index(
        index_name="idx_date_of_birth__postcode_outcode_first__substring_name_1_std",
        columns=[
            "date_of_birth",
            "postcode_outcode_first",
            "SUBSTRING(name_1_std FROM 1 FOR 2)",
        ],
    )
    create_index(
        index_name="idx_date_of_birth_last__postcode_outcode_last__substring_last_name_std",
        columns=[
            "date_of_birth_last",
            "postcode_outcode_last",
            "SUBSTRING(last_name_std FROM 1 FOR 2)",
        ],
    )
    create_index(
        index_name="idx_forename_first__last_name_first__postcode_first",
        columns=["forename_first", "last_name_first", "postcode_first"],
    )
    create_index(
        index_name="idx_date_of_birth__postcode_last",
        columns=["date_of_birth", "postcode_last"],
    )
    create_index(
        index_name="idx_date_of_birth__postcode_second",
        columns=["date_of_birth", "postcode_second"],
    )
    create_index(
        index_name="idx_sentence_date_first__date_of_birth",
        columns=["sentence_date_first", "date_of_birth"],
    )
    create_index(
        index_name="idx_forename_last__last_name_last__date_of_birth",
        columns=["forename_last", "last_name_last", "date_of_birth"],
    )
    create_index(
        index_name="idx_forename_first__last_name_last__date_of_birth",
        columns=["forename_first", "last_name_last", "date_of_birth"],
    )
    create_index(
        index_name="idx_first_and_last_name_std__name_2_std",
        columns=["first_and_last_name_std", "name_2_std"],
    )
    create_index(
        index_name="idx_substring_name_1_std__substring_last_name_std__date_of_birth",
        columns=[
            "SUBSTRING(name_1_std FROM 1 FOR 2)",
            "SUBSTRING(last_name_std FROM 1 FOR 2)",
            "date_of_birth",
        ],
    )
    create_index(
        index_name="idx_substring_name_1_std__substring_last_name_std__postcode_first",
        columns=[
            "SUBSTRING(name_1_std FROM 1 FOR 2)",
            "SUBSTRING(last_name_std FROM 1 FOR 2)",
            "postcode_first",
        ],
    )
    create_index(
        index_name="idx_substring_name_1_std__substring_last_name_std__postcode_last",
        columns=[
            "SUBSTRING(name_1_std FROM 1 FOR 2)",
            "SUBSTRING(last_name_std FROM 1 FOR 2)",
            "postcode_last",
        ],
    )
    create_index(
        index_name="idx_substring_name_1_std__substring_last_name_std__sentence_date_last",
        columns=[
            "SUBSTRING(name_1_std FROM 1 FOR 2)",
            "SUBSTRING(last_name_std FROM 1 FOR 2)",
            "sentence_date_last",
        ],
    )
    create_index(
        index_name="idx_last_name_std__name_1_std__date_of_birth",
        columns=[
            "last_name_std",
            "name_1_std",
            "date_of_birth",
        ],
    )


def downgrade() -> None:
    op.drop_index("idx_date_of_birth__postcode_first")
    op.drop_index("idx_date_of_birth__postcode_outcode_first__substring_name_1_std")
    op.drop_index("idx_date_of_birth_last__postcode_outcode_last__substring_last_name_std")
    op.drop_index("idx_forename_first__last_name_first__postcode_first")
    op.drop_index("idx_date_of_birth__postcode_last")
    op.drop_index("idx_date_of_birth__postcode_second")
    op.drop_index("idx_sentence_date_first__date_of_birth")
    op.drop_index("idx_forename_last__last_name_last__date_of_birth")
    op.drop_index("idx_forename_first__last_name_last__date_of_birth")
    op.drop_index("idx_first_and_last_name_std__name_2_std")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__date_of_birth")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__postcode_first")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__postcode_last")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__sentence_date_last")
    op.drop_index("idx_last_name_std__name_1_std__date_of_birth")
