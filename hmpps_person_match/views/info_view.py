import os

from hmpps_person_match.views.base_view import BaseView


class InfoView(BaseView):
    """
    Info View
    """

    ROUTE = "/info"

    def get(self):
        """
        GET request handler
        """
        version = os.environ.get("APP_BUILD_NUMBER", "unknown")
        commit_id = os.environ.get("APP_GIT_REF", "unknown")
        branch = os.environ.get("APP_GIT_BRANCH", "unknown")
        return dict(
            version=version,
            commit_id=commit_id,
            branch=branch,
        )
