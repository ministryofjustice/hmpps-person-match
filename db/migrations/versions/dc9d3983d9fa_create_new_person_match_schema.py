"""create new person-match schema

Revision ID: dc9d3983d9fa
Revises:
Create Date: 2025-01-21 15:01:35.910613

"""
from collections.abc import Sequence
from typing import Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "dc9d3983d9fa"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA personmatch")


def downgrade() -> None:
    # Drop the schema (WARNING: This will delete all objects in the schema!)
    op.execute("DROP SCHEMA personmatch CASCADE")
