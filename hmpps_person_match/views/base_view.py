import os

import duckdb
import flask
from flask.views import MethodView
from pydantic import BaseModel, ValidationError
from werkzeug.exceptions import BadRequest

from hmpps_person_match.domain.splink_models import SplinkModels
from hmpps_person_match.models.error_response_model import ErrorResponse


class BaseView(MethodView):
    """
    Base View Class to initialise logger
    Include request as part of the object
    """

    def __init__(self, logger) -> None:
        super().__init__()
        self.duckdb_connection = duckdb.connect(database=":memory:")
        self.logger = logger
        self.request = flask.request

    def __del__(self):
        self.duckdb_connection.close()

    def validate(self, model: BaseModel):
        """
        validate incoming message
        """
        try:
            return model(**self.request.json)
        except ValidationError as err:
            raise BadRequest() from err

    def return_error(self, status: int, message: str) -> flask.Response:
        """
        Return flask response with error message
        """
        return flask.Response(
            status=status,
            mimetype="application/json",
            response=ErrorResponse(
                error_message=message,
                status_code=status,
            ).model_dump_json(),
        )

    @staticmethod
    def get_model_path(model: SplinkModels):
        """
        Get model path from environment variable
        """
        model_path = f"./hmpps_person_match/splink_models/{model.value}.json"
        if not os.path.exists(model_path):
            raise Exception(f"{model_path} does not exist.")
        return model_path
