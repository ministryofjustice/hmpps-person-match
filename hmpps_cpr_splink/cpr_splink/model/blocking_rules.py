from splink import block_on
from splink.blocking_rule_library import CustomRule

blocking_rules_for_prediction_tight_for_candidate_search = [
    block_on("pnc_single"),
    block_on("cro_single"),
    block_on("date_of_birth", "postcode_arr[1]"),
    block_on("date_of_birth", "postcode_outcode_arr[1]", "substr(name_1_std, 1, 2)"),
    block_on(
        "date_of_birth_arr[-1]",
        "postcode_outcode_arr[-1]",
        "substr(last_name_std, 1, 2)",
    ),
    block_on("forename_std_arr[1]", "last_name_std_arr[1]", "postcode_arr[1]"),
    block_on("date_of_birth", "postcode_arr[-1]"),
    CustomRule(
        "l.date_of_birth = r.date_of_birth and l.postcode_arr[1] = r.postcode_arr[2]"
    ),
    block_on("sentence_date_arr[1]", "date_of_birth"),
    block_on("forename_std_arr[-1]", "last_name_std_arr[-1]", "date_of_birth"),
    block_on("forename_std_arr[1]", "last_name_std_arr[-1]", "date_of_birth"),
    block_on("first_and_last_name_std", "name_2_std"),
    block_on(
        "substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "date_of_birth"
    ),
    block_on(
        "substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "postcode_arr[1]"
    ),
    block_on(
        "substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "postcode_arr[-1]"
    ),
    block_on(
        "substr(name_1_std, 1, 2)",
        "substr(last_name_std, 1, 2)",
        "sentence_date_arr[-1]",
    ),
    CustomRule(
        "l.name_1_std = r.last_name_std and l.last_name_std = r.name_1_std "
        "and l.date_of_birth = r.date_of_birth"
    ),
]

blocking_rules_for_prediction_loose_for_initial_uuid_creation = [
    block_on("pnc_single"),
    block_on("cro_single"),
    block_on(
        "date_of_birth_arr",
        "postcode_arr",
        arrays_to_explode=["date_of_birth_arr", "postcode_arr"],
    ),
    block_on(
        "sentence_date_arr",
        "postcode_arr",
        arrays_to_explode=["sentence_date_arr", "postcode_arr"],
    ),
    block_on(
        "date_of_birth_arr",
        "sentence_date_arr",
        arrays_to_explode=["date_of_birth_arr", "sentence_date_arr"],
    ),
    block_on(
        "first_and_last_name_std",
        "sentence_date_arr",
        arrays_to_explode=["sentence_date_arr"],
    ),
    block_on(
        "first_and_last_name_std",
        "date_of_birth_arr",
        arrays_to_explode=["date_of_birth_arr"],
    ),
    block_on(
        "first_and_last_name_std",
        "postcode_outcode_arr",
        arrays_to_explode=["postcode_outcode_arr"],
    ),
    block_on(
        "date_of_birth_arr",
        "substr(name_1_std,1,1)",
        "substr(last_name_std,1,1)",
        arrays_to_explode=["date_of_birth_arr"],
    ),
    block_on(
        "postcode_outcode_arr",
        "substr(name_1_std,1,2)",
        "substr(last_name_std,1,2)",
        arrays_to_explode=["postcode_outcode_arr"],
    ),
    block_on(
        "forename_std_arr",
        "last_name_std_arr",
        "date_of_birth_arr",
        arrays_to_explode=[
            "forename_std_arr",
            "last_name_std_arr",
            "date_of_birth_arr",
        ],
    ),
    block_on(
        "forename_std_arr",
        "last_name_std_arr",
        "postcode_outcode_arr",
        arrays_to_explode=[
            "forename_std_arr",
            "last_name_std_arr",
            "postcode_outcode_arr",
        ],
    ),
    CustomRule(
        "l.name_1_std = r.last_name_std and l.last_name_std = r.name_1_std "
        "and l.date_of_birth = r.date_of_birth"
    ),
    block_on("first_and_last_name_std", "name_2_std"),
    block_on("name_1_std", "date_of_birth"),
    block_on("last_name_std", "date_of_birth"),
    # These add very few matches so aren't used in the tighter rules
    CustomRule("substring(r.cro_single, 1, 6) = substring(l.pnc_single, 7, 6)"),
    CustomRule("substring(l.cro_single, 1, 6) = substring(r.pnc_single, 7, 6)"),
    block_on("substring(pnc_single, 1, 5)", "substring(pnc_single, 8, 6)"),
]
