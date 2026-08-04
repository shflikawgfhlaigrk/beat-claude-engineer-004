"""Tests for the ingest-edge contract.

Run:  python3 -m unittest discover -s tests -v

These are the invariants the design's loss and duplication claims rest on. If
one of these fails, the "zero data loss" claim in ANSWER.md is void, which is
the reason they exist as executable assertions rather than prose.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from detect_anomalies import build_report  # noqa: E402
from pipeline.normalize import (  # noqa: E402
    ACCEPT, ACCEPT_FLAGGED, DUPLICATE, QUARANTINE, A_BOT_BURST, A_CLIENT_AGGREGATE,
    A_DUPLICATE_ID, A_FUTURE_TS, A_MALFORMED, A_MISSING_TENANT, A_NEGATIVE_LATENCY,
    A_PII_PAYLOAD, A_SCHEMA_DRIFT, normalize_batch, normalize_line,
)

FIXTURE = ROOT / "fixtures" / "event_sample.jsonl"
EXPECTED_SHA = "1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235"


def ids_with(report, anomaly):
    return sorted(v["event_id"] for v in report["verdicts"] if anomaly in v["anomalies"])


class TestFixtureIntegrity(unittest.TestCase):
    def test_checksum_pins_the_round(self):
        """Fixtures rotate between hiring rounds. If this fails, every event_id
        cited in ANSWER.md is suspect and the analysis must be re-run."""
        self.assertEqual(build_report(FIXTURE)["fixture_sha256"], EXPECTED_SHA)


class TestNoSilentLoss(unittest.TestCase):
    def test_every_line_produces_exactly_one_verdict(self):
        lines = [ln for ln in FIXTURE.read_text().splitlines() if ln.strip()]
        verdicts = normalize_batch(lines)
        self.assertEqual(len(verdicts), len(lines))
        self.assertEqual(sorted(v.line_no for v in verdicts), list(range(1, len(lines) + 1)))

    def test_malformed_line_is_quarantined_with_raw_bytes_retained(self):
        v = normalize_line(1, '{"event_id":"evt-x","tenant_id":"t-1"')
        self.assertEqual(v.verdict, QUARANTINE)
        self.assertIn(A_MALFORMED, v.anomalies)
        self.assertEqual(v.event_id, "evt-x")   # still traceable
        self.assertEqual(v.tenant_id, "t-1")    # still deletable per-tenant
        self.assertIsNotNone(v.raw)             # still recoverable

    def test_quarantine_rows_stay_addressable_for_gdpr(self):
        rep = build_report(FIXTURE)
        for v in rep["verdicts"]:
            if v["verdict"] == QUARANTINE:
                self.assertTrue(v["event_id"] or v["tenant_id"],
                                "a quarantine row with no id cannot be deleted on request")


class TestIdempotency(unittest.TestCase):
    def test_repeated_event_id_becomes_duplicate_not_second_event(self):
        rep = build_report(FIXTURE)
        self.assertEqual(ids_with(rep, A_DUPLICATE_ID), ["evt-0002"])
        self.assertEqual(rep["verdict_counts"].get(DUPLICATE), 1)

    def test_same_shape_different_id_is_not_deduped(self):
        """Two genuine rapid clicks must survive. Dedupe is keyed on the SDK's
        event_id only - never on (visitor, type, ts), which would erase them."""
        a = '{"event_id":"e1","tenant_id":"t","anonymous_id":"a","type":"click","ts":"2026-06-15T14:00:00.000Z","received_at":"2026-06-15T14:00:00.100Z","properties":{}}'
        b = a.replace('"e1"', '"e2"')
        out = normalize_batch([a, b])
        self.assertEqual([v.verdict for v in out], [ACCEPT, ACCEPT])

    def test_same_event_id_across_tenants_is_not_deduped(self):
        """event_id uniqueness is only guaranteed within a tenant."""
        a = '{"event_id":"e1","tenant_id":"t-1","anonymous_id":"a","type":"click","ts":"2026-06-15T14:00:00.000Z","received_at":"2026-06-15T14:00:00.100Z","properties":{}}'
        b = a.replace('"t-1"', '"t-2"')
        self.assertEqual([v.verdict for v in normalize_batch([a, b])], [ACCEPT, ACCEPT])


class TestSeededAnomalies(unittest.TestCase):
    """One assertion per seeded anomaly class in the 2026-07 fixture."""

    @classmethod
    def setUpClass(cls):
        cls.rep = build_report(FIXTURE)

    def test_malformed_json(self):
        self.assertEqual(ids_with(self.rep, A_MALFORMED), ["evt-0020"])

    def test_missing_tenant(self):
        self.assertEqual(ids_with(self.rep, A_MISSING_TENANT), ["evt-0011"])

    def test_schema_drift(self):
        self.assertEqual(ids_with(self.rep, A_SCHEMA_DRIFT), ["evt-0009"])

    def test_clock_skew_and_future_timestamps(self):
        self.assertEqual(ids_with(self.rep, A_NEGATIVE_LATENCY),
                         ["evt-0005", "evt-0006", "evt-0016"])
        self.assertEqual(ids_with(self.rep, A_FUTURE_TS), ["evt-0016"])

    def test_pii_in_payload(self):
        self.assertEqual(ids_with(self.rep, A_PII_PAYLOAD), ["evt-0007"])

    def test_bot_burst(self):
        self.assertEqual(ids_with(self.rep, A_BOT_BURST),
                         ["evt-0012", "evt-0013", "evt-0014", "evt-0015"])

    def test_client_computed_aggregate(self):
        self.assertEqual(ids_with(self.rep, A_CLIENT_AGGREGATE), ["evt-0019"])


class TestPiiPrecision(unittest.TestCase):
    def test_custom_event_name_is_not_a_person(self):
        """Regression: an earlier hint list matched the key `name` and flagged
        evt-0016 and evt-0019, whose `properties.name` is the custom event's
        name. A PII scanner that cries wolf gets switched off."""
        rep = build_report(FIXTURE)
        for eid in ("evt-0016", "evt-0019"):
            v = next(x for x in rep["verdicts"] if x["event_id"] == eid)
            self.assertNotIn(A_PII_PAYLOAD, v["anomalies"])

    def test_real_contact_details_are_caught(self):
        v = normalize_line(1, json.dumps({
            "event_id": "e", "tenant_id": "t", "anonymous_id": "a", "type": "custom",
            "ts": "2026-06-15T14:00:00Z", "received_at": "2026-06-15T14:00:00.1Z",
            "properties": {"name": "quote_requested", "contact_email": "x@y.com",
                           "phone": "+1-555-0142"}}))
        self.assertEqual(v.detail["pii_fields"], ["contact_email", "phone"])


class TestDeletionCascade(unittest.TestCase):
    def test_delete_request_reaches_pre_identity_events(self):
        """The seeded GDPR request names u-1077, but the subject's other event
        (evt-0006) carries no user_id. Deleting only by user_id leaves it."""
        c = build_report(FIXTURE)["cross_event"]["deletion_cascade"][0]
        self.assertEqual(c["subject_user_id"], "u-1077")
        self.assertIn("evt-0006", c["matched_only_by_anonymous_id"])


class TestIdentityStitching(unittest.TestCase):
    def test_pre_identify_events_are_listed_for_backfill(self):
        st = {s["anonymous_id"]: s for s in
              build_report(FIXTURE)["cross_event"]["retroactive_identity_stitch"]}
        self.assertEqual(st["anon-9f2"]["backfill_events"], ["evt-0001", "evt-0002"])
        self.assertEqual(st["anon-c81"]["backfill_events"], ["evt-0004", "evt-0005"])
        self.assertEqual(st["anon-3d0"]["backfill_events"], ["evt-0007"])

    def test_identity_can_arrive_with_no_identify_event_in_window(self):
        """anon-52d and anon-77a carry user_ids with no identify event in the
        sample: the stitch table cannot be rebuilt from one stream window."""
        oob = {s["anonymous_id"] for s in
               build_report(FIXTURE)["cross_event"]["identity_without_identify_event"]}
        self.assertEqual(oob, {"anon-52d", "anon-77a"})


class TestClientAggregatesAreNeverTrusted(unittest.TestCase):
    def test_claimed_count_contradicts_server_observation(self):
        c = build_report(FIXTURE)["cross_event"]["client_aggregate_vs_server_truth"][0]
        self.assertEqual(c["event_id"], "evt-0019")
        self.assertEqual(c["claimed"], {"count_today": 3})
        self.assertEqual(c["server_observable_pricing_views_in_sample"], 1)


if __name__ == "__main__":
    unittest.main()
