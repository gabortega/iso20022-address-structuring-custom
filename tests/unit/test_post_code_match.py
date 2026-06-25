"""
Unit tests for the postcode matcher (``post_code_match``).

Real postcodes are extracted from the resource files via the data provider
(``load_postcode_data``) so the matcher is exercised against the same data it
uses in production. The country-specific regex structures mirror the constants
defined in ``runner_postcode_match``.
"""
from functools import partial, reduce

import pytest

from data_structuring.components.data_provider.post_code_provider import load_postcode_data
from data_structuring.components.post_code_matching.post_code_match import (
    PostcodeMatch,
    PostcodeMatchResult,
    find_postcode_town_matches,
)
from data_structuring.config import DatabaseConfig

# Postcode regex structures, kept in sync with runner_postcode_match.
AR_POSTCODE_STRUCTURE = "[0-9]{4}"
BR_POSTCODE_STRUCTURE = "[0-9]{3}"
CL_POSTCODE_STRUCTURE = "[0-9]{4}"
CN_POSTCODE_STRUCTURE = "[0-9]{2}"
IE_POSTCODE_STRUCTURE = (
    "(?:[a-zA-Z][0-9][a-zA-Z][0-9]|[a-zA-Z]{2}[0-9]{2}|[a-zA-Z][0-9][a-zA-Z]{2})"
)
MT_POSTCODE_STRUCTURE = "[0-9]{4}"


@pytest.fixture(scope="module")
def postcode_data():
    """Load all postcode dictionaries / regex lists from the resource files."""
    (
        full_dict, full_regex_list,
        ireland_dict, ireland_regex_list,
        malta_dict, malta_regex_list,
        chile_dict, chile_regex_list,
        argentina_dict, argentina_regex_list,
        brazil_dict, brazil_regex_list,
        china_dict, china_regex_list,
    ) = load_postcode_data(DatabaseConfig())
    return {
        "full": (full_dict, full_regex_list, ""),
        "IE": (ireland_dict, ireland_regex_list, IE_POSTCODE_STRUCTURE),
        "MT": (malta_dict, malta_regex_list, MT_POSTCODE_STRUCTURE),
        "CL": (chile_dict, chile_regex_list, CL_POSTCODE_STRUCTURE),
        "AR": (argentina_dict, argentina_regex_list, AR_POSTCODE_STRUCTURE),
        "BR": (brazil_dict, brazil_regex_list, BR_POSTCODE_STRUCTURE),
        "CN": (china_dict, china_regex_list, CN_POSTCODE_STRUCTURE),
    }


# (group key, postcode input text, dict key, expected town, expected origin)
# The dict key is the value stored in the postcode dictionary; for the
# country-specific dictionaries the remaining digits satisfy the structure regex.
KNOWN_MATCHES = [
    ("full", "19648", "19648", "C UBIVKA", "UA"),
    ("IE", "D18H5C8", "D18", "DUBLIN 18", "IE"),
    ("AR", "S23490000", "S2349", "SUARDI", "AR"),
    ("BR", "481700000", "48170", "AGUA FRIA", "BR"),
    ("CL", "5360000", "536", "PUYEHUE", "CL"),
    ("CN", "35080000", "3508", "MINQING COUNTY", "CN"),
    ("MT", "MRS0000", "MRS", "MARSA", "MT"),
]


class TestFindPostcodeTownMatches:

    @pytest.mark.parametrize(
        "group, text, dict_key, town, origin", KNOWN_MATCHES,
        ids=[m[0] for m in KNOWN_MATCHES],
    )
    def test_known_postcode_resolves_to_town(
        self, postcode_data, group, text, dict_key, town, origin
    ):
        """A real postcode resolves to its town from the resource data."""
        postcodes_dict, regex_list, structure = postcode_data[group]

        # Sanity: the key really exists in the loaded resource data.
        assert dict_key in postcodes_dict

        result = find_postcode_town_matches(postcodes_dict, regex_list, text, structure)

        possibilities = {(m.possibility, m.origin) for m in result}
        assert (town, origin) in possibilities

    def test_match_positions_point_to_postcode(self, postcode_data):
        """start/end/matched describe the postcode location within the text."""
        full_dict, full_regex_list, _ = postcode_data["full"]
        text = "STREET 19648 CITY"

        result = find_postcode_town_matches(full_dict, full_regex_list, text)

        assert len(result) > 0
        match = result[0]
        assert match.matched == "19648"
        assert text[match.start:match.end] == "19648"

    def test_no_match_returns_empty_result(self, postcode_data):
        """Text without any known postcode yields an empty result."""
        full_dict, full_regex_list, _ = postcode_data["full"]

        result = find_postcode_town_matches(full_dict, full_regex_list, "NOTAPOSTCODEXYZ")

        assert isinstance(result, PostcodeMatchResult)
        assert len(result) == 0

    def test_lowercase_postcode_is_not_matched(self, postcode_data):
        """Only [A-Z0-9] is preserved, so lowercase postcodes are filtered out."""
        ireland_dict, ireland_regex_list, structure = postcode_data["IE"]

        result = find_postcode_town_matches(
            ireland_dict, ireland_regex_list, "d18h5c8", structure
        )

        assert len(result) == 0

    def test_postcode_embedded_in_address(self, postcode_data):
        """A postcode surrounded by free text is still detected."""
        ireland_dict, ireland_regex_list, structure = postcode_data["IE"]

        result = find_postcode_town_matches(
            ireland_dict, ireland_regex_list, "EIRCODE D18H5C8 DUBLIN", structure
        )

        assert ("DUBLIN 18", "IE") in {(m.possibility, m.origin) for m in result}

    def test_empty_regex_list_yields_no_matches(self, postcode_data):
        """With no regexes to scan, nothing is matched even for a valid postcode."""
        full_dict, _, _ = postcode_data["full"]

        result = find_postcode_town_matches(full_dict, [], "19648")

        assert len(result) == 0

    def test_multiple_postcodes_in_one_address(self, postcode_data):
        """Two distinct postcodes in the same text both resolve to their towns."""
        full_dict, full_regex_list, _ = postcode_data["full"]

        # Sanity: both keys exist in the loaded resource data.
        assert "19648" in full_dict
        assert "603287" in full_dict

        text = "STREET 19648 AND 603287 END"
        result = find_postcode_town_matches(full_dict, full_regex_list, text)

        possibilities = {(m.possibility, m.origin) for m in result}
        assert ("C UBIVKA", "UA") in possibilities
        assert ("JURONG EAST STREET 21", "SG") in possibilities

        # Each postcode is detected in its own, non-overlapping region.
        first = next(m for m in result if m.matched == "19648")
        second = next(m for m in result if m.matched == "603287")
        assert first.end <= second.start

    def test_overlapping_matches_at_same_span(self, postcode_data):
        """Several Irish regexes match one eircode, all sharing the same span."""
        ireland_dict, ireland_regex_list, structure = postcode_data["IE"]

        result = find_postcode_town_matches(
            ireland_dict, ireland_regex_list, "D18H5C8", structure
        )

        assert len(result) > 1
        spans = {(m.start, m.end) for m in result}
        assert spans == {(0, 7)}
        assert all(m.matched == "D18H5C8" for m in result)

    def test_partially_overlapping_matches_are_all_returned(self, postcode_data):
        """Different regexes match overlapping substrings of the same postcode."""
        full_dict, full_regex_list, _ = postcode_data["full"]

        result = find_postcode_town_matches(full_dict, full_regex_list, "STREET 19648 CITY")

        spans = sorted({(m.start, m.end) for m in result})

        def overlaps(a, b):
            return a != b and a[0] < b[1] and b[0] < a[1]

        assert any(overlaps(a, b) for a in spans for b in spans)


# Real postcodes per country-specific dictionary: (input text, dict key, town).
# The input is the dictionary key (a prefix) followed by characters satisfying
# the country's postcode_regex_structure; the matcher extracts the key portion.
COUNTRY_POSTCODES = {
    "AR": [
        ("S23490000", "S2349", "SUARDI"),
        ("S22480000", "S2248", "ESTACION BERNARDO DE IRIGOYEN"),
    ],
    "BR": [
        ("48170000", "48170", "AGUA FRIA"),
        ("89198000", "89198", "RIO DO CAMPO"),
    ],
    "CL": [
        ("5360000", "536", "PUYEHUE"),
        ("4440000", "444", "LOS ANGELES"),
    ],
    "CN": [
        ("350800", "3508", "MINQING COUNTY"),
        ("030400", "0304", "QINGXU COUNTY"),
    ],
    "IE": [
        ("D18A1B2", "D18", "DUBLIN 18"),
        ("F28A1B2", "F28", "WESTPORT"),
    ],
    "MT": [
        ("MRS0000", "MRS", "MARSA"),
        ("BZN0000", "BZN", "BALZAN"),
    ],
}

# Flattened (country, input, key, town) for parametrization.
COUNTRY_CASES = [
    (country, text, key, town)
    for country, examples in COUNTRY_POSTCODES.items()
    for text, key, town in examples
]


class TestCountrySpecificDictionaries:
    """Matching against the special AR/BR/CL/CN/IE/MT dictionaries.

    These dictionaries differ from the ``full`` one: their keys are postcode
    *prefixes* and matching requires the country's ``postcode_regex_structure``
    suffix to be present.
    """

    @pytest.mark.parametrize(
        "country, text, key, town", COUNTRY_CASES,
        ids=[f"{c}-{k}" for c, _, k, _ in COUNTRY_CASES],
    )
    def test_structured_postcode_resolves_to_town(
        self, postcode_data, country, text, key, town
    ):
        """A structured postcode resolves to its town with the country origin."""
        postcodes_dict, regex_list, structure = postcode_data[country]

        # Sanity: the prefix key exists in the loaded resource data.
        assert key in postcodes_dict

        result = find_postcode_town_matches(postcodes_dict, regex_list, text, structure)

        assert len(result) > 0
        assert (town, country) in {(m.possibility, m.origin) for m in result}
        # Every match from a country dictionary carries that country's origin.
        assert all(m.origin == country for m in result)

    @pytest.mark.parametrize(
        "country, text, key, town", COUNTRY_CASES,
        ids=[f"{c}-{k}" for c, _, k, _ in COUNTRY_CASES],
    )
    def test_structure_suffix_is_required(
        self, postcode_data, country, text, key, town
    ):
        """The bare prefix key, without the structure suffix, does not match."""
        postcodes_dict, regex_list, structure = postcode_data[country]

        result = find_postcode_town_matches(postcodes_dict, regex_list, key, structure)

        assert len(result) == 0

    @pytest.mark.parametrize("country", list(COUNTRY_POSTCODES))
    def test_postcode_is_isolated_to_its_dictionary(self, postcode_data, country):
        """A country's postcode does not match against any other country's dict."""
        text = COUNTRY_POSTCODES[country][0][0]

        for other in COUNTRY_POSTCODES:
            if other == country:
                continue
            other_dict, other_regex_list, other_structure = postcode_data[other]
            result = find_postcode_town_matches(
                other_dict, other_regex_list, text, other_structure
            )
            assert all(m.possibility != COUNTRY_POSTCODES[country][0][2] for m in result), (
                f"{country} postcode {text!r} leaked into {other} dictionary"
            )


def _fingerprint(result):
    """Order-independent fingerprint of a postcode match result."""
    return frozenset(
        (m.start, m.end, m.matched, m.possibility, m.origin) for m in result
    )


class TestNoContaminationBetweenSamples:
    """Processing several text samples must not let one run affect another.

    The matcher is stateless, but the runner reuses ``partial``-bound matchers
    and ``reduce``-merges their results across a whole batch of samples, so these
    tests exercise the same reuse pattern over multiple samples.
    """

    # (text sample, expected town or None when nothing should match)
    SAMPLES = [
        ("STREET 19648 CITY", "C UBIVKA"),
        ("NO POSTCODE HERE AT ALL", None),
        ("AREA 603287 ZONE", "JURONG EAST STREET 21"),
        ("ANOTHER PLAIN LINE", None),
    ]

    def test_each_sample_result_independent_of_predecessors(self, postcode_data):
        """Running a sample after others yields the same result as in isolation."""
        full_dict, full_regex_list, _ = postcode_data["full"]
        texts = [t for t, _ in self.SAMPLES]

        # Reference: every sample matched on its own.
        reference = {
            t: _fingerprint(find_postcode_town_matches(full_dict, full_regex_list, t))
            for t in texts
        }

        # Same matcher applied across the batch, including reversed order and
        # repeats, must reproduce each sample's isolated result exactly.
        for order in (texts, list(reversed(texts)), texts + texts):
            for t in order:
                result = find_postcode_town_matches(full_dict, full_regex_list, t)
                assert _fingerprint(result) == reference[t]

    def test_repeated_calls_do_not_accumulate(self, postcode_data):
        """Calling the matcher repeatedly never grows or shrinks the result."""
        full_dict, full_regex_list, _ = postcode_data["full"]

        first = _fingerprint(
            find_postcode_town_matches(full_dict, full_regex_list, "STREET 19648 CITY")
        )
        for _ in range(5):
            again = _fingerprint(
                find_postcode_town_matches(full_dict, full_regex_list, "STREET 19648 CITY")
            )
            assert again == first

    def test_match_then_empty_sample_returns_empty(self, postcode_data):
        """A non-matching sample after a matching one carries nothing over."""
        full_dict, full_regex_list, _ = postcode_data["full"]

        find_postcode_town_matches(full_dict, full_regex_list, "STREET 19648 CITY")
        result = find_postcode_town_matches(full_dict, full_regex_list, "NO POSTCODE HERE")

        assert len(result) == 0

    def test_runner_style_batch_has_no_cross_sample_leakage(self, postcode_data):
        """Reused partial matchers + reduce-merge stay isolated per sample.

        Mirrors ``RunnerPostcodeMatch.match``: matchers are bound once and reused
        for every sample, and per-sample results are merged across dictionaries.
        """
        # Build reused, partial-bound matchers across several dictionaries.
        matchers = []
        for group in ("full", "AR", "IE", "MT"):
            postcodes_dict, regex_list, structure = postcode_data[group]
            matchers.append(
                partial(
                    find_postcode_town_matches,
                    postcodes_dict=postcodes_dict,
                    regex_list=regex_list,
                    postcode_regex_structure=structure,
                )
            )

        def process(text):
            return reduce(
                PostcodeMatchResult.merge, [m(text=text) for m in matchers]
            )

        samples = [
            "STREET 19648 CITY",          # full -> C UBIVKA
            "EMPTY LINE",                  # nothing
            "CODE S23490000 HERE",         # AR  -> SUARDI
            "POST MRS0000 MALTA",          # MT  -> MARSA
        ]
        reference = {t: _fingerprint(process(t)) for t in samples}

        # Interleave and repeat the batch; each sample must stay identical.
        for t in samples + list(reversed(samples)) + samples:
            assert _fingerprint(process(t)) == reference[t]

        # Spot-check the batch actually produced the expected, distinct towns.
        assert ("C UBIVKA", "UA") in {
            (m.possibility, m.origin) for m in process(samples[0])
        }
        assert ("SUARDI", "AR") in {
            (m.possibility, m.origin) for m in process(samples[2])
        }
        assert ("MARSA", "MT") in {
            (m.possibility, m.origin) for m in process(samples[3])
        }
        assert len(process(samples[1])) == 0


class TestPostcodeMatchResult:

    @staticmethod
    def _make_match(matched="19648", possibility="CITY", origin="XX"):
        return PostcodeMatch(
            start=0, end=len(matched), matched=matched,
            possibility=possibility, origin=origin,
        )

    def test_len_iter_and_getitem(self):
        match = self._make_match()
        result = PostcodeMatchResult([match])

        assert len(result) == 1
        assert result[0] is match
        assert list(result) == [match]

    def test_merge_concatenates_results(self):
        first = PostcodeMatchResult([self._make_match(possibility="A")])
        second = PostcodeMatchResult([self._make_match(possibility="B")])

        merged = PostcodeMatchResult.merge(first, second)

        assert len(merged) == 2
        assert [m.possibility for m in merged] == ["A", "B"]

    def test_merge_does_not_mutate_inputs(self):
        first = PostcodeMatchResult([self._make_match(possibility="A")])
        second = PostcodeMatchResult([self._make_match(possibility="B")])

        PostcodeMatchResult.merge(first, second)

        assert len(first) == 1
        assert len(second) == 1
