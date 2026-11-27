import unittest

from altdata.domain import Problem, normalized, registry, unpack, utc
from tests.helpers import ROOT


class Contracts(unittest.TestCase):
    def setUp(self):
        self.policy = unpack((ROOT / "config/registry.json").read_bytes())
        self.row = unpack((ROOT / "data/fixtures/macro/1.json").read_bytes())["observations"][0]

    def normalize(self):
        return normalized("macro", self.row, self.policy["sources"]["macro"])[1]

    def test_times_require_offsets_and_normalize_to_utc(self):
        self.assertEqual(utc("2025-03-01T00:30:00+01:00"), "2025-02-28T23:30:00.000000Z")
        for bad in ("2025-01-01", "2025-02-30T00:00:00Z", "2025-03-01T00:00:00+01:60", None):
            with self.subTest(bad=bad), self.assertRaises(Problem):
                utc(bad)

    def test_values_units_and_versions(self):
        self.assertEqual(self.normalize()["value"], "101.2")
        for key, bad in [("revision", True), ("revision", 0), ("unit", "USD"),
                         ("value", "NaN"), ("value", None), ("value", 101.2)]:
            with self.subTest(key=key, bad=bad):
                original = self.row[key]
                self.row[key] = bad
                with self.assertRaises(Problem):
                    self.normalize()
                self.row[key] = original

    def test_missing_value_keeps_reason(self):
        self.row.update(value=None, missing_reason="withheld")
        self.assertIsNone(self.normalize()["value"])
        self.assertEqual(self.normalize()["missing_reason"], "withheld")

    def test_registry_rejects_unsafe_source_ids(self):
        self.policy["sources"]["../outside"] = self.policy["sources"]["news"]
        with self.assertRaises(Problem):
            registry(self.policy)

    def test_malformed_json_rejected(self):
        for data in (b'{"a":1,"a":2}', b'{"value":NaN}', b'\xff', b'1e900'):
            with self.subTest(data=data), self.assertRaises(Problem):
                unpack(data)
