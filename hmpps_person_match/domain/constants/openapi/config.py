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
