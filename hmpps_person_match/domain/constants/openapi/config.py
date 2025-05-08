from hmpps_person_match.models.error_response import ErrorResponse


class OpenAPIConfig:
    """
    Confgiuration for OpenAPI
    """

    DOCS_URL = "/swagger-ui.html"
    APPLICATION_TITLE = "HMPPS Person Match"
    APPLICATION_SUMMARY = """
        An API wrapper around a model developed by the MoJ Analytical Platform
        for scoring the confidence of people matches across MoJ systems.
    """
    APPLICATION_DESCRIPTION = """
        This API is strictly for the internal use of the Core Person Record team.
        It is not intended for use by any other teams, systems, or external parties.
        Usage by other services is not supported and may result in unexpected behaviour or security risks.
        If you have any questions or require further information, please contact the Core Person Record team directly.
    """
    APPLICATION_VERSION = "0.1.0"
    DEFAULT_RESPONSES = {
        400: {
            "description": "Validation Error",
            "model": ErrorResponse,
        },
        401: {
            "description": "Unauthorized",
            "model": ErrorResponse,
        },
        403: {
            "description": "Forbidden",
            "model": ErrorResponse,
        },
    }
