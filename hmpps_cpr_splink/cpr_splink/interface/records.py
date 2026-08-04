from collections.abc import Mapping
from typing import Any

from sqlalchemy import RowMapping

type CleanedRecord = Mapping[str, Any]
type ScoringCandidateRecord = RowMapping | Mapping[str, Any]
