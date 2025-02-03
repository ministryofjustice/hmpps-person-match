"""create_person

Revision ID: b1e99530d2e3
Revises: b1e79c7cc8b6
Create Date: 2025-01-28 11:38:26.870913

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b1e99530d2e3"
down_revision: str | None = "b1e79c7cc8b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""CREATE TABLE IF NOT EXISTS personmatch.person (
    id INT PRIMARY KEY,

    name_1_std TEXT,
    name_2_std TEXT,
    name_3_std TEXT,
    last_name_std TEXT,
    first_and_last_name_std TEXT,
    forename_std_arr TEXT [],
    last_name_std_arr TEXT [],

    sentence_date_single DATE,
    sentence_date_arr DATE [],

    date_of_birth DATE,
    date_of_birth_arr DATE [],

    postcode_arr TEXT [],
    postcode_outcode_arr TEXT [],

    cro_single TEXT,
    pnc_single TEXT,
    crn TEXT,
    prison_number TEXT,

    source_system TEXT

);""")


def downgrade() -> None:
    op.execute("DROP TABLE IF NOT EXISTS personmatch.person")
