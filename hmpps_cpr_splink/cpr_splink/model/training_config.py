from splink import DuckDBAPI, Linker, block_on

from hmpps_cpr_splink.cpr_splink.model.settings import settings


class ModelTraining:
    deterministic_rules = [
        block_on("name_1_std", "last_name_std", "date_of_birth"),
        block_on("name_1_std", "last_name_std", "postcode_arr[1]"),
        block_on("name_1_std", "last_name_std", "cro_single"),
        block_on("name_1_std", "last_name_std", "pnc_single"),
        block_on("date_of_birth", "postcode_arr[1]", "name_1_std"),
        block_on("date_of_birth", "postcode_arr[1]", "last_name_std"),
        block_on("sentence_date_arr[1]", "name_1_std", "last_name_std"),
        block_on("sentence_date_arr[-1]", "name_1_std", "last_name_std"),
    ]
    recall = 0.8

    def __init__(self, train_u_size: float = 1e7):
        self.train_u_size = train_u_size

    em_blocks = [
        block_on("pnc_single"),
        block_on("cro_single"),
        block_on("name_1_std", "last_name_std", "date_of_birth"),
        block_on("postcode_arr[1]", "sentence_date_arr[1]"),
    ]

    # TODO: need to align default location with where we (will) access it for scoring
    def train(
        self,
        cleaned_data_table_name: str,
        db_api: DuckDBAPI,
        file_name: str = "new_model.json",
    ) -> Linker:
        linker = Linker(cleaned_data_table_name, settings=settings, db_api=db_api)

        linker.training.estimate_probability_two_random_records_match(
            self.deterministic_rules,
            recall=self.recall,
        )
        linker.training.estimate_u_using_random_sampling(max_pairs=self.train_u_size)

        for blocking_rule in self.em_blocks:
            linker.training.estimate_parameters_using_expectation_maximisation(
                blocking_rule,
            )

        return linker
