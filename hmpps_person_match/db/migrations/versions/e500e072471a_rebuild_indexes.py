"""rebuild-indexes

Revision ID: e500e072471a
Revises: 6b6108d399d9
Create Date: 2025-03-18 11:00:24.426419

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e500e072471a"
down_revision: str | None = "6b6108d399d9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def create_index(index_name: str, columns: list[str]) -> None:
    col_string = ", ".join(columns)
    op.execute(
        f"create index {index_name} on personmatch.person ({col_string});",
    )

def upgrade() -> None:
    op.drop_index("idx_date_of_birth__postcode_first", schema="personmatch")
    op.drop_index("idx_date_of_birth__postcode_outcode_first__substring_name_1_std", schema="personmatch")
    op.drop_index("idx_date_of_birth_last__postcode_outcode_last__substring_last_n", schema="personmatch")
    op.drop_index("idx_forename_first__last_name_first__postcode_first", schema="personmatch")
    op.drop_index("idx_date_of_birth__postcode_last", schema="personmatch")
    op.drop_index("idx_date_of_birth__postcode_second", schema="personmatch")
    op.drop_index("idx_sentence_date_first__date_of_birth", schema="personmatch")
    op.drop_index("idx_forename_last__last_name_last__date_of_birth", schema="personmatch")
    op.drop_index("idx_forename_first__last_name_last__date_of_birth", schema="personmatch")
    op.drop_index("idx_first_and_last_name_std__name_2_std", schema="personmatch")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__date_of_birt", schema="personmatch")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__postcode_fir", schema="personmatch")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__postcode_las", schema="personmatch")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std__sentence_dat", schema="personmatch")
    op.drop_index("idx_last_name_std__name_1_std__date_of_birth", schema="personmatch")

    create_index("idx_person__date_of_birth", ["date_of_birth"])
    create_index("idx_person__postcode_first", ["postcode_first"])
    create_index("idx_person__postcode_outcode_first", ["postcode_outcode_first"])
    create_index("idx_person__substring_name_1", ["SUBSTRING(name_1_std FROM 1 FOR 2)"])
    create_index("idx_person__date_of_birth_last", ["date_of_birth_last"])
    create_index("idx_person__postcode_outcode_last", ["postcode_outcode_last"])
    create_index("idx_person__last_name", ["SUBSTRING(last_name_std FROM 1 FOR 2)"])
    create_index("idx_person__forename_first", ["forename_first"])
    create_index("idx_person__last_name_first", ["last_name_first"])
    create_index("idx_person__postcode_last", ["postcode_last"])
    create_index("idx_person__postcode_second", ["postcode_second"])
    create_index("idx_person__sentence_date_first", ["sentence_date_first"])
    create_index("idx_person__last_name_last", ["last_name_last"])
    create_index("idx_person__first_and_last_name_std", ["first_and_last_name_std"])
    create_index("idx_person__name_1_std", ["name_1_std"])
    create_index("idx_person__name_2_std", ["name_2_std"])
    create_index("idx_person__sentence_date_last", ["sentence_date_last"])
    create_index("idx_person__last_name_std", ["last_name_std"])

def downgrade() -> None:
    op.drop_index("idx_person__date_of_birth", schema="personmatch")
    op.drop_index("idx_person__postcode_first", schema="personmatch")
    op.drop_index("idx_person__postcode_outcode_first", schema="personmatch")
    op.drop_index("idx_person__substring_name_1", schema="personmatch")
    op.drop_index("idx_person__postcode_outcode_last", schema="personmatch")
    op.drop_index("idx_person__last_name", schema="personmatch")
    op.drop_index("idx_person__forename_first", schema="personmatch")
    op.drop_index("idx_person__last_name_first", schema="personmatch")
    op.drop_index("idx_person__postcode_last", schema="personmatch")
    op.drop_index("idx_person__postcode_second", schema="personmatch")
    op.drop_index("idx_person__sentence_date_first", schema="personmatch")
    op.drop_index("idx_person__last_name_last", schema="personmatch")
    op.drop_index("idx_person__first_and_last_name_std", schema="personmatch")
    op.drop_index("idx_person__name_1_std", schema="personmatch")
    op.drop_index("idx_person__name_2_std", schema="personmatch")
    op.drop_index("idx_person__sentence_date_last", schema="personmatch")
    op.drop_index("idx_person__last_name_std", schema="personmatch")

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
