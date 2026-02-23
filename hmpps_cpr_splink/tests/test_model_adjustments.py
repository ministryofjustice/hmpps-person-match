"""
Tests for specific model adjustments made in model_2026_02_23_1e09:

  1. DOB comparison: the levenshtein level now requires the year to be identical
     (old: levenshtein on the full date string; new: same year AND levenshtein on month-day).

  2. ID-comparison ELSE level: m=1/256, u=1.0 gives an exact match weight of -8.
     (old model had m≈0.056, u≈1.0, giving ≈-4 weight.)
"""

import math

import duckdb
import pytest
from splink import DuckDBAPI
from splink.internals.realtime import compare_records

from hmpps_cpr_splink.cpr_splink.interface.score import insert_data_into_duckdb
from hmpps_cpr_splink.cpr_splink.model.model import MODEL_PATH
from hmpps_cpr_splink.cpr_splink.model_cleaning import CLEANED_TABLE_SCHEMA

# ---------------------------------------------------------------------------
# Build the full schema from canonical sources (mirrors score.py)
# ---------------------------------------------------------------------------

_TF_COLUMNS = [
    "name_1_std",
    "name_2_std",
    "last_name_std",
    "first_and_last_name_std",
    "date_of_birth",
    "cro_single",
    "pnc_single",
]
_TF_SCHEMA = [(f"tf_{c}", "FLOAT") for c in _TF_COLUMNS]
_POSTCODE_TF_SCHEMA = [("postcode_arr_repacked", "VARCHAR[]"), ("postcode_freq_arr", "FLOAT[]")]
_FULL_SCHEMA = CLEANED_TABLE_SCHEMA + _TF_SCHEMA + _POSTCODE_TF_SCHEMA

# The base record has everything populated as null
_BASE = {col: ([] if dtype.endswith(("[]", "]")) else None) for col, dtype in _FULL_SCHEMA}


def _compare(left: dict, right: dict) -> dict:
    """Insert a record pair via the production insert path and return the scored row."""
    con = duckdb.connect(":memory:")
    table_name = insert_data_into_duckdb(con, [left, right], "records")
    con.execute(f"CREATE TABLE records_left  AS SELECT * FROM {table_name} WHERE id = 1")
    con.execute(f"CREATE TABLE records_right AS SELECT * FROM {table_name} WHERE id = 2")
    db_api = DuckDBAPI(connection=con)
    rows = compare_records("records_left", "records_right", settings=MODEL_PATH, db_api=db_api).as_record_dict()
    return rows[0]


def test_model_adjustments_id_mismatch_weight() -> None:
    """ID-comparison ELSE level should give a match weight of exactly -8."""
    row = _compare(
        {**_BASE, "id": 1, "pnc_single": "XX/1234A", "cro_single": "1234/56A"},
        {**_BASE, "id": 2, "pnc_single": "YY/9999B", "cro_single": "9999/99B"},
    )
    assert math.log2(row["bf_id_comparison"]) == pytest.approx(-8.0)


def test_id_one_different_other_null_gives_minus_8() -> None:
    """When one of PNC/CRO differs and the other has a value on one side but NULL
    on the other, no higher level matches so the ELSE level fires → -8."""
    row = _compare(
        {**_BASE, "id": 1, "pnc_single": "XX/1234A", "cro_single": "1234/56A"},
        {**_BASE, "id": 2, "pnc_single": "YY/9999B", "cro_single": None},
    )
    assert math.log2(row["bf_id_comparison"]) == pytest.approx(-8.0)


def test_id_one_different_other_same_gives_positive_weight() -> None:
    """When PNC matches but CRO differs, the PNC-match level fires and the
    match weight should be strongly positive (PNC match is very informative)."""
    row = _compare(
        {**_BASE, "id": 1, "pnc_single": "XX/1234A", "cro_single": "1234/56A"},
        {**_BASE, "id": 2, "pnc_single": "XX/1234A", "cro_single": "9999/99B"},
    )
    assert math.log2(row["bf_id_comparison"]) > 0


def test_model_adjustments_dob_year_diff_not_levenshtein() -> None:
    """A one-year difference in DOB must NOT match the levenshtein comparison level.

    '1990-01-14' vs '1991-01-14' has levenshtein distance 1 on the full string,
    so the old condition (levenshtein on full date <= 1) would have produced
    gamma_date_of_birth_arr = 2 (levenshtein level).

    The new condition additionally requires the year to be identical, so this
    pair must fall through to the 5-year absolute-difference level: gamma = 1.
    """
    row = _compare(
        {**_BASE, "id": 1, "date_of_birth": "1990-09-14", "date_of_birth_arr": ["1990-09-14"]},
        {**_BASE, "id": 2, "date_of_birth": "1991-09-14", "date_of_birth_arr": ["1991-09-14"]},
    )
    # Must NOT be the levenshtein level (gamma=2)
    assert row["gamma_date_of_birth_arr"] != 2, "pair should not match levenshtein level – year differs"
    # Must be the 5-year abs level (gamma=1)
    assert row["gamma_date_of_birth_arr"] == 1, (
        f"expected 5-year abs level (gamma=1), got gamma={row['gamma_date_of_birth_arr']}"
    )


def test_dob_day_diff_hits_levenshtein_level() -> None:
    """A one-day difference (same year+month) should match the levenshtein level
    (gamma=2): same year, and levenshtein('01-01', '01-02') = 1."""
    row = _compare(
        {**_BASE, "id": 1, "date_of_birth": "1990-01-01", "date_of_birth_arr": ["1990-01-01"]},
        {**_BASE, "id": 2, "date_of_birth": "1990-01-02", "date_of_birth_arr": ["1990-01-02"]},
    )
    assert row["gamma_date_of_birth_arr"] == 2, (
        f"expected levenshtein level (gamma=2), got gamma={row['gamma_date_of_birth_arr']}"
    )


def test_dob_month_diff_hits_levenshtein_level() -> None:
    """A one-month difference (same year+day) should match the levenshtein level
    (gamma=2): same year, and levenshtein('01-14', '02-14') = 1."""
    row = _compare(
        {**_BASE, "id": 1, "date_of_birth": "1990-01-14", "date_of_birth_arr": ["1990-01-14"]},
        {**_BASE, "id": 2, "date_of_birth": "1990-02-14", "date_of_birth_arr": ["1990-02-14"]},
    )
    assert row["gamma_date_of_birth_arr"] == 2, (
        f"expected levenshtein level (gamma=2), got gamma={row['gamma_date_of_birth_arr']}"
    )
