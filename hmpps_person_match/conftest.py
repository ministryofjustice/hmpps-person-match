import pytest

from hmpps_person_match.app import PersonMatchApplication


@pytest.fixture(scope="module")
def app():
    app = PersonMatchApplication().app
    # other setup can go here
    yield app
    # clean up / reset resources here
