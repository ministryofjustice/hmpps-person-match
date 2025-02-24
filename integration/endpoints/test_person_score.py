import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hmpps_person_match.routes.person.score.person_score import ROUTE
from integration.client import Client


class TestPersonScoreEndpoint:
    """
    Test person score
    """

    @staticmethod
    @pytest.fixture(autouse=True, scope="function")
    async def clean_db(db_connection: AsyncSession):
        """
        Before Each
        Delete all records from the database
        """
        await db_connection.execute(text("TRUNCATE TABLE personmatch.person"))
        await db_connection.commit()

    async def test_score_no_matching(self, call_endpoint, match_id):
        """
        Test person score handles no matching match id
        """
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_score_invalid_match_id(self, call_endpoint, match_id):
        """
        Test person score handles non uuid match_id
        """
        match_id = "invalid_!!id123"
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 404
        assert response.json() == {}

    async def test_score_does_not_return_self(self, call_endpoint, match_id, create_person_data):
        """
        Test person score doesn't return its own record as part of candidates
        """
        # Create person
        data = create_person_data(match_id)
        response = call_endpoint("post", "/person", json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        # Call score for person
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert response.json() == []

    async def test_score_returns_candidates(self, call_endpoint, match_id, create_person_data):
        """
        Test person cleaned and stored on person endpoint
        """
        # Create person to match and score
        data = create_person_data(match_id)
        response = call_endpoint("post", "/person", json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        # Create different person
        matching_person_id_1 = str(uuid.uuid4())
        data = create_person_data(matching_person_id_1)
        response = call_endpoint("post", "/person", json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        # Create different person
        matching_person_id_2 = str(uuid.uuid4())
        data = create_person_data(matching_person_id_2)
        response = call_endpoint("post", "/person", json=data, client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200

        # Call score for person
        response = call_endpoint("get", self._build_score_url(match_id), client=Client.HMPPS_PERSON_MATCH)
        assert response.status_code == 200
        assert len(response.json()) == 2
        assert {
            "candidate_match_id": matching_person_id_2,
            "candidate_match_probability": 1.0,
            "candidate_match_weight": 70.76272963361279,
        } in response.json()
        assert {
            "candidate_match_id": matching_person_id_1,
            "candidate_match_probability": 1.0,
            "candidate_match_weight": 70.76272963361279,
        } in response.json()

    @staticmethod
    def _build_score_url(match_id: str):
        return ROUTE.format(match_id=match_id)
