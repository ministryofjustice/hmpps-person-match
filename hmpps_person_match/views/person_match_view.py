import json
from http import HTTPStatus

import pyarrow as pa
from splink import DuckDBAPI, Linker
from werkzeug import exceptions

from hmpps_person_match.domain.events import Events
from hmpps_person_match.domain.roles import Roles
from hmpps_person_match.domain.splink_models import SplinkModels
from hmpps_person_match.models.person_match_model import PersonMatching
from hmpps_person_match.utils.auth import authorize
from hmpps_person_match.views.base_view import BaseView


class PersonMatchView(BaseView):
    """
    Match View
    """

    ROUTE = "/person/match"

    SCHEMA = pa.schema(
        [
            pa.field("unique_id", pa.string(), nullable=True),
            pa.field("source_dataset", pa.string(), nullable=False),
            pa.field("pnc", pa.string(), nullable=True),
            pa.field("dob", pa.string(), nullable=True),
            pa.field("lastname", pa.string(), nullable=True),
            pa.field("firstname1", pa.string(), nullable=True),
            pa.field("firstname2", pa.string(), nullable=True),
            pa.field("firstname3", pa.string(), nullable=True),
        ],
    )

    @authorize(required_roles=[Roles.ROLE_PERSON_MATCH])
    def post(self):
        """
        POST request handler
        """
        try:
            self.logger.info(Events.PERSON_MATCH_SCORE_REQUESTED)
            person_match_model = self.validate(model=PersonMatching)
            result = self._match(person_match_model)
            self.logger.info(
                Events.PERSON_MATCH_SCORE_GENERATED,
                extra={"custom_dimensions": json.dumps(result.get("match_probability"))},
            )
            return result
        except exceptions.BadRequest as e:
            return self.return_error(e.code, e.description)
        except Exception as e:
            self.logger.exception(f"{__name__}: Exception at match endpoint, error: {str(e)}")  # noqa: G004
            return self.return_error(HTTPStatus.INTERNAL_SERVER_ERROR, e.args[0])

    def _match(self, person_match_model: PersonMatching):
        """
        Match records
        """
        pmm_dict = person_match_model.model_dump()
        dataset_1 = pa.Table.from_pylist(pmm_dict["matching_to"], schema=self.SCHEMA)
        dataset_2 = pa.Table.from_pylist([pmm_dict["matching_from"]], schema=self.SCHEMA)

        db_api = DuckDBAPI(connection=self.duckdb_connection)

        path_to_settings = self.get_model_path(SplinkModels.PERSON_MATCH_MODEL)

        linker = Linker(
            [dataset_1, dataset_2],
            path_to_settings,
            db_api=db_api,
        )

        prediction = linker.inference.predict()

        json_output = prediction.as_pandas_dataframe().to_json()

        return json.loads(json_output)
