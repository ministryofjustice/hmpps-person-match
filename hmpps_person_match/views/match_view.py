import json

import pandas as pd
import pyarrow as pa
from splink import DuckDBAPI, Linker

from hmpps_person_match.domain.events import Events
from hmpps_person_match.domain.splink_models import SplinkModels
from hmpps_person_match.utils import standardisation_functions
from hmpps_person_match.views.base_view import BaseView


class MatchView(BaseView):
    """
    Match View
    """

    ROUTE = "/match"

    SCHEMA = pa.schema(
        [
            pa.field("source_dataset", pa.string(), nullable=True),
            pa.field("unique_id", pa.string(), nullable=True),
            pa.field("pnc_number_std", pa.string(), nullable=True),
            pa.field("dob_std", pa.string(), nullable=True),
            pa.field("surname_std", pa.string(), nullable=True),
            pa.field("forename1_std", pa.string(), nullable=True),
            pa.field("forename2_std", pa.string(), nullable=True),
            pa.field("forename3_std", pa.string(), nullable=True),
            pa.field("forename4_std", pa.string(), nullable=True),
            pa.field("forename5_std", pa.string(), nullable=True),
        ],
    )

    def post(self):
        """
        POST request handler
        """
        try:
            self.logger.info(Events.MATCH_SCORE_REQUESTED)

            data = pd.DataFrame(json.loads(self.request.get_data().decode("utf-8")))

            data = standardisation_functions.standardise_pnc_number(data, pnc_col="pnc_number")
            data = standardisation_functions.standardise_dob(data, dob_col="dob")
            data = standardisation_functions.standardise_names(data, name_cols=["first_name", "surname"])
            data = standardisation_functions.fix_zero_length_strings(data)

            # If no source dataset provided assume it's in the same format we expect,
            # our algorithm does not need to know which record is which
            # so this is just a formality
            if len(data["source_dataset"]) == 0:
                data["source_dataset"] = pd.Series({"0": "libra", "1": "delius"})
                data["source_dataset"] = data["source_dataset"].astype("str")

            response = self.score(data)
            self.logger.info(
                Events.MATCH_SCORE_GENERATED,
                extra={"custom_dimensions": json.dumps(self.custom_dimensions_from(response))},
            )
            return response
        except Exception as e:
            self.logger.exception(f"{__name__}: Exception at match endpoint")  # noqa: G004
            return e.args[0], 500

    @staticmethod
    def custom_dimensions_from(response: dict):
        return {
            key: value["0"]
            for key, value in response.items()
            if key
            in [
                "unique_id_l",
                "unique_id_r",
                "pnc_number_std_l",
                "pnc_number_std_r",
                "source_dataset_l",
                "source_dataset_r",
                "match_probability",
            ]
        }

    def score(self, data):
        row_1 = data[data["source_dataset"] == data["source_dataset"].unique()[0]]
        row_2 = data[data["source_dataset"] == data["source_dataset"].unique()[1]]

        # Important to impose explicit schema - otherwise where a column contains only
        # None DuckDB will have no way of knowing it's a nullable string column
        row_arrow_1 = pa.Table.from_pandas(row_1, schema=self.SCHEMA, preserve_index=False)
        row_arrow_2 = pa.Table.from_pandas(row_2, schema=self.SCHEMA, preserve_index=False)

        db_api = DuckDBAPI(connection=self.duckdb_connection)

        path_to_settings = self.get_model_path(SplinkModels.MODEL)
        linker = Linker(
            [row_arrow_1, row_arrow_2],
            path_to_settings,
            db_api=db_api,
        )

        prediction = linker.inference.predict()

        json_output = prediction.as_pandas_dataframe().to_json()

        return json.loads(json_output)
