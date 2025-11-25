"""add GIN index for arrays

Revision ID: 2857e412d879
Revises: afa82883199f
Create Date: 2025-11-20 11:34:22.781915

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2857e412d879"
down_revision: str | None = "afa82883199f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_postcode_arr_gin",
        columns=["postcode_arr"],
        table_name="person",
        schema="personmatch",
        postgresql_using="gin",
    )

    # Create indices to support the non-postcode parts of the new blocking rules
    op.create_index(
        "idx_date_of_birth",
        columns=["date_of_birth"],
        table_name="person",
        schema="personmatch",
    )
    op.create_index(
        "idx_forename_first__last_name_first",
        columns=["forename_first", "last_name_first"],
        table_name="person",
        schema="personmatch",
    )
    op.execute(
        """
        create index idx_substring_name_1_std__substring_last_name_std
        on personmatch.person (
            SUBSTRING(name_1_std FROM 1 FOR 2),
            SUBSTRING(last_name_std FROM 1 FOR 2)
        );
        """,
    )

    op.drop_index(
        "idx_date_of_birth__postcode_first",
        table_name="person",
        schema="personmatch",
    )
    op.drop_index(
        "idx_forename_first__last_name_first__postcode_first",
        table_name="person",
        schema="personmatch",
    )
    op.drop_index(
        "idx_date_of_birth__postcode_last",
        table_name="person",
        schema="personmatch",
    )
    op.drop_index(
        "idx_substring_name_1_std__substring_last_name_std__postcode_first",
        table_name="person",
        schema="personmatch",
    )
    op.drop_index(
        "idx_substring_name_1_std__substring_last_name_std__postcode_last",
        table_name="person",
        schema="personmatch",
    )


def downgrade() -> None:
    op.execute(
        "create index idx_date_of_birth__postcode_first on personmatch.person (date_of_birth, postcode_first);",
    )
    op.execute(
        """
        create index idx_forename_first__last_name_first__postcode_first
        on personmatch.person (forename_first, last_name_first, postcode_first);
        """,
    )
    op.execute(
        "create index idx_date_of_birth__postcode_last on personmatch.person (date_of_birth, postcode_last);",
    )
    op.execute(
        """
        create index idx_substring_name_1_std__substring_last_name_std__postcode_first
        on personmatch.person (
            SUBSTRING(name_1_std FROM 1 FOR 2),
            SUBSTRING(last_name_std FROM 1 FOR 2),
            postcode_first
        );
        """,
    )
    op.execute(
        """
        create index idx_substring_name_1_std__substring_last_name_std__postcode_last
        on personmatch.person (
            SUBSTRING(name_1_std FROM 1 FOR 2),
            SUBSTRING(last_name_std FROM 1 FOR 2),
            postcode_last
        );
        """,
    )

    op.drop_index("idx_date_of_birth", table_name="person", schema="personmatch")
    op.drop_index("idx_forename_first__last_name_first", table_name="person", schema="personmatch")
    op.drop_index("idx_substring_name_1_std__substring_last_name_std", table_name="person", schema="personmatch")

    op.drop_index("idx_postcode_arr_gin", table_name="person", schema="personmatch")
