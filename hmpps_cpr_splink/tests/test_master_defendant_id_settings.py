import json
import math
from pathlib import Path

from hmpps_cpr_splink.cpr_splink.model.model import MODEL_PATH


def _find_comparison(settings: dict, output_column: str) -> dict:
    for comparison in settings["comparisons"]:
        if comparison["output_column_name"] == output_column:
            return comparison
    msg = f"comparison {output_column} not found"
    raise AssertionError(msg)


def test_master_defendant_id_weights_are_correct():
    settings = json.loads(Path(MODEL_PATH).read_text())

    master_defendant = _find_comparison(settings, "master_defendant_id")
    levels = master_defendant["comparison_levels"]

    null_level = next(level for level in levels if level.get("is_null_level"))
    assert "IS NULL" in null_level["sql_condition"].upper()

    equality_level = next(
        level
        for level in levels
        if "master_defendant_id_l = master_defendant_id_r" in level["sql_condition"]
    )
    m_prob = float(equality_level["m_probability"])
    u_prob = float(equality_level["u_probability"])
    weight_bits = math.log2(m_prob / u_prob)
    assert 19.8 <= weight_bits <= 20.2

    else_level = next(level for level in levels if level["sql_condition"].strip().upper() == "ELSE")
    assert float(else_level["m_probability"]) == 0.5
    assert float(else_level["u_probability"]) == 0.5
