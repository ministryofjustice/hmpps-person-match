from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import UUID

import pytest

from hmpps_cpr_splink.cpr_splink.interface.search import search_candidates
from hmpps_person_match.models.person.person_score import PersonScore
from integration.mock_person import MockPerson


@pytest.mark.asyncio
async def test_search_candidates_returns_empty_when_only_primary_record_is_retrieved() -> None:
    person = MockPerson(matchId="caller-supplied-id")
    cleaned_person = {"match_id": "internal-id"}
    connection_pg = Mock()
    connection_duckdb = MagicMock()

    with (
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.uuid4",
            return_value=UUID(int=1),
        ) as mock_uuid4,
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.clean_person",
            return_value=cleaned_person,
        ) as mock_clean_person,
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.candidate_search_for_record",
            new=AsyncMock(return_value=[{"match_id": str(UUID(int=1))}]),
        ) as mock_candidate_search,
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.duckdb.connect",
            return_value=connection_duckdb,
        ),
        patch("hmpps_cpr_splink.cpr_splink.interface.search.score_candidates") as mock_score_candidates,
    ):
        result = await search_candidates(person, connection_pg)

    internal_match_id = str(UUID(int=1))
    mock_uuid4.assert_called_once_with()
    assert internal_match_id != person.match_id
    mock_clean_person.assert_called_once_with(person, internal_match_id)
    mock_candidate_search.assert_awaited_once_with(cleaned_person, connection_pg)
    mock_score_candidates.assert_not_called()
    connection_duckdb.__exit__.assert_called_once()
    assert result == []


@pytest.mark.asyncio
async def test_search_candidates_scores_retrieved_candidates_and_closes_duckdb() -> None:
    person = MockPerson(matchId="caller-supplied-id")
    internal_match_id = str(UUID(int=2))
    cleaned_person = {"match_id": internal_match_id}
    candidates = [{"match_id": internal_match_id}, {"match_id": "candidate-id"}]
    expected_scores = [
        PersonScore(
            candidate_match_id="candidate-id",
            candidate_match_probability=0.99,
            candidate_match_weight=20,
            candidate_should_join=True,
            candidate_should_fracture=False,
            candidate_is_possible_twin=False,
            unadjusted_match_weight=20,
        ),
    ]
    connection_pg = Mock()
    connection_duckdb = MagicMock()

    with (
        patch("hmpps_cpr_splink.cpr_splink.interface.search.uuid4", return_value=UUID(int=2)),
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.clean_person",
            return_value=cleaned_person,
        ),
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.candidate_search_for_record",
            new=AsyncMock(return_value=candidates),
        ),
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.duckdb.connect",
            return_value=connection_duckdb,
        ) as mock_connect,
        patch(
            "hmpps_cpr_splink.cpr_splink.interface.search.score_candidates",
            return_value=expected_scores,
        ) as mock_score_candidates,
    ):
        result = await search_candidates(person, connection_pg)

    mock_connect.assert_called_once_with(":memory:")
    mock_score_candidates.assert_called_once_with(
        connection_duckdb.__enter__.return_value,
        internal_match_id,
        candidates,
    )
    connection_duckdb.__exit__.assert_called_once()
    assert result == expected_scores
