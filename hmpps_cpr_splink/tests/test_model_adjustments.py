"""
Tests for specific model adjustments made in model_2026_02_23_1e09:

  1. DOB comparison: the levenshtein level now requires the year to be identical
     (old: levenshtein on the full date string; new: same year AND levenshtein on month-day).
     A one-year difference (e.g. 1990-01-01 vs 1991-01-01) was previously captured by
     the levenshtein level (gamma=2) but must now fall through to the 5-year abs level
     (gamma=1).

  2. ID-comparison ELSE level: m=1/256, u=1.0 gives an exact match weight of -8.
     (old model had m≈0.056, u≈1.0, giving ≈-4 weight.)
"""

import math
import json

import duckdb
import pytest
from splink import DuckDBAPI
from splink.internals.realtime import compare_records

from hmpps_cpr_splink.cpr_splink.model.model import MODEL_PATH

# ---------------------------------------------------------------------------
# Shared schema / base record (mirrors try_model.py)
# ---------------------------------------------------------------------------

_SCHEMA = {
    "id": "INTEGER",
    "match_id": "VARCHAR",
    "source_system": "VARCHAR",
    "cro_single": "VARCHAR",
    "pnc_single": "VARCHAR",
    "date_of_birth": "DATE",
    "date_of_birth_arr": "DATE[]",
    "sentence_date_single": "DATE",
    "sentence_date_arr": "DATE[]",
    "postcode_arr_with_freq": "STRUCT(value VARCHAR, rel_freq DOUBLE)[]",
    "postcode_arr": "VARCHAR[]",
    "postcode_outcode_arr": "VARCHAR[]",
    "forename_std_arr": "VARCHAR[]",
    "last_name_std_arr": "VARCHAR[]",
    "name_1_std": "VARCHAR",
    "name_2_std": "VARCHAR",
    "name_3_std": "VARCHAR",
    "last_name_std": "VARCHAR",
    "first_and_last_name_std": "VARCHAR",
    "override_marker": "VARCHAR",
    "override_scopes": "VARCHAR[]",
    "master_defendant_id": "VARCHAR",
    "tf_name_1_std": "DOUBLE",
    "tf_name_2_std": "DOUBLE",
    "tf_last_name_std": "DOUBLE",
    "tf_first_and_last_name_std": "DOUBLE",
    "tf_date_of_birth": "DOUBLE",
    "tf_pnc_single": "DOUBLE",
    "tf_cro_single": "DOUBLE",
}

_BASE = {col: ([] if dtype.endswith("]") else None) for col, dtype in _SCHEMA.items()}


def _compare(left: dict, right: dict) -> dict:
    """Insert a single record pair and return the compare_records result row."""
    con = duckdb.connect(":memory:")
    columns_ddl = ", ".join(f"{col} {dtype}" for col, dtype in _SCHEMA.items())
    con.execute(f"CREATE TABLE records ({columns_ddl})")  # noqa: S608
    placeholders = ", ".join(["?"] * len(_SCHEMA))
    insert_sql = f"INSERT INTO records VALUES ({placeholders})"  # noqa: S608
    for record in (left, right):
        values = [json.dumps(record[col]) if col == "postcode_arr_with_freq" else record[col] for col in _SCHEMA]
        con.execute(insert_sql, values)
    con.execute("CREATE TABLE records_left  AS SELECT * FROM records WHERE id = 1")
    con.execute("CREATE TABLE records_right AS SELECT * FROM records WHERE id = 2")
    db_api = DuckDBAPI(connection=con)
    rows = compare_records("records_left", "records_right", settings=MODEL_PATH, db_api=db_api).as_record_dict()
    return rows[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_model_adjustments_id_mismatch_weight():
    """ID-comparison ELSE level should give a match weight of exactly -8.

    The new model sets m = 1/256 and u = 1.0, so the Bayes factor is 2^-8.
    Records have distinct, non-matching PNC and CRO values so all higher
    comparison levels are skipped and the ELSE level fires.
    """
    row = _compare(
        {**_BASE, "id": 1, "pnc_single": "XX/1234A", "cro_single": "1234/56A"},
        {**_BASE, "id": 2, "pnc_single": "YY/9999B", "cro_single": "9999/99B"},
    )
    assert math.log2(row["bf_id_comparison"]) == pytest.approx(-8.0)


def test_model_adjustments_dob_year_diff_not_levenshtein():
    """A one-year difference in DOB must NOT match the levenshtein comparison level.

    '1990-01-01' vs '1991-01-01' has levenshtein distance 1 on the full string,
    so the old condition (levenshtein on full date <= 1) would have produced
    gamma_date_of_birth_arr = 2 (levenshtein level).

    The new condition additionally requires the year to be identical, so this
    pair must fall through to the 5-year absolute-difference level: gamma = 1.
    """
    row = _compare(
        {**_BASE, "id": 1, "date_of_birth": "1990-01-01", "date_of_birth_arr": ["1990-01-01"]},
        {**_BASE, "id": 2, "date_of_birth": "1991-01-01", "date_of_birth_arr": ["1991-01-01"]},
    )
    # Must NOT be the levenshtein level (gamma=2)
    assert row["gamma_date_of_birth_arr"] != 2, "pair should not match levenshtein level – year differs"
    # Must be the 5-year abs level (gamma=1)
    assert row["gamma_date_of_birth_arr"] == 1, (
        f"expected 5-year abs level (gamma=1), got gamma={row['gamma_date_of_birth_arr']}"
    )
