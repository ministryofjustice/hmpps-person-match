from splink import SettingsCreator

from .blocking_rules import (
    blocking_rules_for_prediction_loose_for_initial_uuid_creation,
)
from .comparisons import (
    date_of_birth_comparison,
    ids_comparison,
    name_2_comparison,
    name_comparison,
    postcode_comparison,
    sentence_date_comparison,
)

settings = SettingsCreator(
    link_type="link_and_dedupe",
    unique_id_column_name="id",
    source_dataset_column_name="source_system",
    comparisons=[
        name_comparison,
        date_of_birth_comparison,
        postcode_comparison,
        name_2_comparison,
        ids_comparison,
        sentence_date_comparison,
    ],
    blocking_rules_to_generate_predictions=blocking_rules_for_prediction_loose_for_initial_uuid_creation,
    retain_intermediate_calculation_columns=True,
    retain_matching_columns=True,
    additional_columns_to_retain=["match_id"],
)
