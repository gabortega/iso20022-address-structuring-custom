from unittest.mock import patch, Mock

import pytest
from fastapi.testclient import TestClient

from rest.server.app import app


@pytest.fixture
def mock_pipeline():
    """Mock the pipeline for the duration of the tests."""
    with (
        patch("rest.server.app.RunServerConfig") as mock_config,
        patch("rest.server.app.AddressStructuringPipeline") as mock_cls,
    ):
        mock_config.return_value = Mock(batch_size=32, hostname="localhost", port=8000)
        mock_pipe = Mock()
        mock_cls.return_value = mock_pipe
        yield mock_pipe


@pytest.fixture
def client(mock_pipeline):
    with TestClient(app) as client:
        yield client


def _mock_pipeline_result(hash_id: str, country_matched: str, country_conf: float, country_resolved: str,
                          town_matched: str, town_conf: float, town_resolved: str, town_origin: str,
                          country_start: int = 0, country_end: int = 5, town_start: int = 10, town_end: int = 15):
    """Create a mock ResultPostProcessing with one pair of matches."""
    result = Mock()
    result.hash_id = hash_id

    # i_th_best_match helpers: return real values for i=0, None-tuple for i>=1
    def country_match(i, value_if_none=None):
        if i == 0:
            return country_resolved, country_conf, country_matched
        return value_if_none, value_if_none, value_if_none

    def town_match(i, value_if_none=None):
        if i == 0:
            return town_resolved, town_conf, town_matched
        return value_if_none, value_if_none, value_if_none

    result.i_th_best_match_country.side_effect = country_match
    result.i_th_best_match_town.side_effect = town_match

    # fuzzy match result: model_dump and indexing
    country_detail = {"start": country_start, "end": country_end, "flags": []}
    town_detail = {"start": town_start, "end": town_end, "flags": []}

    result.fuzzy_match_result.country_matches.model_dump.return_value = [country_detail]
    result.fuzzy_match_result.town_matches.model_dump.return_value = [town_detail]

    town_match_obj = Mock()
    town_match_obj.origin = town_origin
    result.fuzzy_match_result.town_matches.__getitem__ = lambda self, i: town_match_obj

    return result


def _mock_empty_pipeline_result(hash_id: str):
    """Create a mock ResultPostProcessing with no matches."""
    result = Mock()
    result.hash_id = hash_id

    result.i_th_best_match_country.side_effect = lambda i, value_if_none=None: (
        value_if_none, value_if_none, value_if_none
    )
    result.i_th_best_match_town.side_effect = lambda i, value_if_none=None: (
        value_if_none, value_if_none, value_if_none
    )

    return result


class TestProcessAddress:

    def test_single_address(self, client, mock_pipeline):
        """Test that a single address returns one result with the correct country and town match fields."""
        input_json = {
            "addressSamples": [{"text": "Bahnhofstrasse 1, Zurich, Switzerland", "hashId": "abc123"}],
            "numResults": 1,
        }

        mock_pipeline.run.return_value = [
            _mock_pipeline_result(
                hash_id=input_json["addressSamples"][0]["hashId"],
                country_matched="SWITZERLAND", country_conf=0.95, country_resolved="CH",
                town_matched="ZURICH", town_conf=0.90, town_resolved="ZURICH",
                town_origin="CH",
            )
        ]

        resp = client.post("/process-address", json=input_json)

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 1
        result = body["results"][0]
        assert result["hashId"] == "abc123"
        assert len(result["matches"]) == 1

        country = result["matches"][0]["countryMatch"]
        assert country["matched"] == "SWITZERLAND"
        assert country["confidenceScore"] == 0.95
        assert country["resolvedName"] == "CH"

        town = result["matches"][0]["townMatch"]
        assert town["matched"] == "ZURICH"
        assert town["confidenceScore"] == 0.90
        assert town["resolvedName"] == "ZURICH"
        assert town["inferredCountryCode"] == "CH"

    def test_multiple_addresses(self, client, mock_pipeline):
        """Test that when multiple addresses are processed, they are returned in the same order."""
        input_json = {
            "addressSamples": [
                {"text": "10 Rue de Rivoli, Paris, France", "hashId": "idx1"},
                {"text": "Alexanderplatz 1, Berlin, Germany", "hashId": "idx2"},
            ],
            "numResults": 1,
        }

        mock_pipeline.run.return_value = [
            _mock_pipeline_result(
                hash_id=input_json["addressSamples"][0]["hashId"],
                country_matched="FRANCE", country_conf=0.88, country_resolved="FR",
                town_matched="PARIS", town_conf=0.92, town_resolved="PARIS",
                town_origin="FR",
            ),
            _mock_pipeline_result(
                hash_id=input_json["addressSamples"][1]["hashId"],
                country_matched="GERMANY", country_conf=0.85, country_resolved="DE",
                town_matched="BERLIN", town_conf=0.87, town_resolved="BERLIN",
                town_origin="DE",
            ),
        ]

        resp = client.post("/process-address", json=input_json)

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 2
        assert body["results"][0]["hashId"] == "idx1"
        assert body["results"][1]["hashId"] == "idx2"

    def test_null_matches_skipped(self, client, mock_pipeline):
        """When both country and town are None for a given rank, that match is skipped."""
        input_json = {
            "addressSamples": [{"text": "some gibberish", "hashId": "idx1"}],
            "numResults": 2,
        }

        mock_pipeline.run.return_value = [
            _mock_empty_pipeline_result(input_json["addressSamples"][0]["hashId"])
        ]

        resp = client.post("/process-address", json=input_json)

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 1
        assert body["results"][0]["hashId"] == "idx1"
        assert body["results"][0]["matches"] == []

    def test_empty_address_list(self, client, mock_pipeline):
        """Empty address list returns an empty results list."""
        input_json = {
            "addressSamples": [],
            "numResults": 1,
        }

        mock_pipeline.run.return_value = []

        resp = client.post("/process-address", json=input_json)

        assert resp.status_code == 200
        assert resp.json()["results"] == []

    def test_default_num_results(self, client, mock_pipeline):
        """numResults defaults to 2 when omitted."""
        input_json = {
            "addressSamples": [{"text": "test address", "hashId": "idx1"}],
        }

        result = _mock_empty_pipeline_result(input_json["addressSamples"][0]["hashId"])
        mock_pipeline.run.return_value = [result]

        resp = client.post("/process-address", json=input_json)

        assert resp.status_code == 200
        # The pipeline result methods should have been called with i=0 and i=1
        assert result.i_th_best_match_country.call_count == 2
        assert result.i_th_best_match_town.call_count == 2

    def test_suggested_country_forwarded(self, client, mock_pipeline):
        """Verify suggestedCountry reaches the JsonReader (and thus the pipeline)."""
        mock_pipeline.run.return_value = []

        resp = client.post("/process-address", json={
            "addressSamples": [
                {"text": "123 Main St", "hashId": "idx1", "suggestedCountry": "US", "forceSuggestedCountry": True}
            ],
            "numResults": 1,
        })

        assert resp.status_code == 200
        # Verify pipeline.run was called; the reader is constructed from the request
        mock_pipeline.run.assert_called_once()
        reader = mock_pipeline.run.call_args[0][0]
        samples = list(reader.read())
        assert len(samples) == 1
        assert samples[0].suggested_country == "US"
        assert samples[0].force_suggested_country is True

    def test_response_contains_start_end_indices(self, client, mock_pipeline):
        """Match results include the correct startIndex and endIndex for country and town."""
        mock_pipeline.run.return_value = [
            _mock_pipeline_result(
                hash_id="idx1",
                country_matched="CH", country_conf=0.9, country_resolved="CH",
                town_matched="BERN", town_conf=0.8, town_resolved="BERN",
                town_origin="CH",
                country_start=20, country_end=22,
                town_start=10, town_end=14,
            )
        ]

        resp = client.post("/process-address", json={
            "addressSamples": [{"text": "Marktgasse 1 Bern Switzerland", "hashId": "idx1"}],
            "numResults": 1,
        })

        match = resp.json()["results"][0]["matches"][0]
        assert match["countryMatch"]["startIndex"] == 20
        assert match["countryMatch"]["endIndex"] == 22
        assert match["townMatch"]["startIndex"] == 10
        assert match["townMatch"]["endIndex"] == 14

    def test_invalid_request_body(self, client, mock_pipeline):
        """Malformed request body returns a 422 validation error."""
        resp = client.post("/process-address", json={"bad": "payload"})
        assert resp.status_code == 422
