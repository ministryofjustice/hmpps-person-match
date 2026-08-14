from typing import Literal

from splink import block_on
from splink.blocking_rule_library import And, CustomRule
from splink.internals.blocking import BlockingRule

POSTCODE_INTERSECT = CustomRule("l.postcode_arr && r.postcode_arr")
SENTENCE_DATE_INTERSECT = CustomRule("l.sentence_date_arr && r.sentence_date_arr")

# If these are updated then make sure to make a corresponding update to the database indexes
# generated columns, as appropriate
blocking_rules_tight = [
    block_on("pnc_single"),
    block_on("cro_single"),
    And(block_on("date_of_birth"), POSTCODE_INTERSECT),
    block_on("date_of_birth", "postcode_outcode_first", "substr(name_1_std, 1, 2)"),
    block_on(
        "date_of_birth_last",
        "postcode_outcode_last",
        "substr(last_name_std, 1, 2)",
    ),
    And(block_on("forename_first", "last_name_first"), POSTCODE_INTERSECT),
    And(block_on("date_of_birth"), SENTENCE_DATE_INTERSECT),
    block_on("forename_last", "last_name_last", "date_of_birth"),
    block_on("forename_first", "last_name_last", "date_of_birth"),
    block_on("first_and_last_name_std", "name_2_std"),
    block_on("substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)", "date_of_birth"),
    And(
        block_on("substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)"),
        POSTCODE_INTERSECT,
    ),
    And(
        block_on("substr(name_1_std, 1, 2)", "substr(last_name_std, 1, 2)"),
        SENTENCE_DATE_INTERSECT,
    ),
    CustomRule(
        "l.name_1_std = r.last_name_std and l.last_name_std = r.name_1_std and l.date_of_birth = r.date_of_birth",
    ),
    block_on("override_marker"),
    block_on("master_defendant_id"),
]

# for now we use the same set of rules, but we can extend these
# once we have investigated options
blocking_rules_looser = [
    *blocking_rules_tight,
]


# TODO: this doesn't work directly, as our indexing doesnt work, but enough for now
# Splink 4.0.7 should have requisite change
blocking_rules_tight_dialected = list(
    map(
        lambda brc: brc.get_blocking_rule("postgres"),
        blocking_rules_tight,
    ),
)
for n, br in enumerate(blocking_rules_tight_dialected):
    br.add_preceding_rules(blocking_rules_tight_dialected[:n])

blocking_rules_looser_dialected = list(
    map(
        lambda brc: brc.get_blocking_rule("postgres"),
        blocking_rules_looser,
    ),
)
for n, br in enumerate(blocking_rules_looser_dialected):
    br.add_preceding_rules(blocking_rules_looser_dialected[:n])

type BlockingRuleSetName = Literal["tight", "looser"]

blocking_rule_sets: dict[BlockingRuleSetName, list[BlockingRule]] = {
    "tight": blocking_rules_tight_dialected,
    "looser": blocking_rules_looser_dialected,
}
