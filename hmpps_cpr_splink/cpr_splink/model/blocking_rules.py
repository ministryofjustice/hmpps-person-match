from splink import block_on
from splink.blocking_rule_library import CustomRule

# If these are updated then make sure to make a corresponding update to the database indexes
# generated columns, as appropriate
blocking_rules_for_prediction_tight_for_candidate_search = [
    block_on("pnc_single"),
    block_on("cro_single"),
    block_on("date_of_birth", "postcode_first"),
    block_on("date_of_birth", "postcode_outcode_first", "substr(name_1_std, 1, 2)"),
    block_on(
        "date_of_birth_last",
        "postcode_outcode_last",
        "substr(last_name_std, 1, 2)",
    ),
    block_on("forename_first", "last_name_first", "postcode_first"),
    block_on("date_of_birth", "postcode_last"),
    CustomRule("l.date_of_birth = r.date_of_birth and l.postcode_first = r.postcode_second"),
    block_on("sentence_date_first", "date_of_birth"),
    block_on("forename_last", "last_name_last", "date_of_birth"),
    block_on("forename_first", "last_name_last", "date_of_birth"),
    block_on("first_and_last_name_std", "name_2_std"),
    block_on("substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "date_of_birth"),
    block_on("substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "postcode_first"),
    block_on("substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "postcode_last"),
    block_on(
        "substr(name_1_std, 1, 2)",
        "substr(last_name_std, 1, 2)",
        "sentence_date_last",
    ),
    CustomRule(
        "l.name_1_std = r.last_name_std and l.last_name_std = r.name_1_std and l.date_of_birth = r.date_of_birth",
    ),
    block_on("override_marker"),
    block_on("master_defendant_id"),
]
