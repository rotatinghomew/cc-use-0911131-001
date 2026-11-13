from __future__ import annotations

import contextlib
import copy
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from twin_market_flow.warehouse.run_ledger import ConflictError, DomainError
from twin_market_flow.experiment_analysis import (
    ExperimentAnalysisStore,
    analyse,
    canonical_fingerprint,
    run_analysis,
    validate_input,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "fixtures/analysis/input.json"


def load_fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def minimal_input() -> dict:
    """A small household-level experiment with known arithmetic."""

    return {
        "experiment_id": "unit-experiment",
        "seed": 7,
        "as_of": "2026-07-25",
        "observation_start": "2026-06-30",
        "observation_end": "2026-07-20",
        "assignments": [
            {"household_id": "h1", "arm": "treatment", "assigned_at": "2026-06-30"},
            {"household_id": "h2", "arm": "treatment", "assigned_at": "2026-06-30"},
            {"household_id": "h3", "arm": "control", "assigned_at": "2026-06-30"},
            {"household_id": "h4", "arm": "control", "assigned_at": "2026-06-30"},
        ],
        "identities": [
            {"link_id": f"l{household}", "device_id": f"d{household}", "household_id": household,
             "effective_at": "2026-06-20", "known_at": "2026-06-21", "revision": 1, "revoked": False}
            for household in ("h1", "h2", "h3", "h4")
        ],
        "events": [
            order("o1", "h1", "d h1", 100.0),
            order("o2", "h2", "d h2", 60.0),
            order("o3", "h3", "d h3", 40.0),
            order("o4", "h4", "d h4", 30.0),
        ],
    }


def device_trap_input() -> dict:
    """Household totals rise with treatment while the per-device mean falls.

    This is the regression that shows why device rows cannot be treated as
    independent observations: the naive device contrast reverses the sign of
    the household-level result.
    """

    treatment_totals = [60.0, 75.0, 90.0, 105.0, 120.0, 135.0]
    control_totals = [20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
    assignments, identities, events = [], [], []
    for index, total in enumerate(treatment_totals):
        household = f"t{index}"
        assignments.append({"household_id": household, "arm": "treatment", "assigned_at": "2026-06-30"})
        for device_index in range(4):
            device = f"td{index}-{device_index}"
            identities.append({
                "link_id": f"l-{device}", "device_id": device, "household_id": household,
                "effective_at": "2026-06-20", "known_at": "2026-06-21", "revision": 1, "revoked": False,
            })
            events.append(order(f"to{index}-{device_index}", household, device, total / 4))
    for index, total in enumerate(control_totals):
        household = f"c{index}"
        device = f"cd{index}"
        assignments.append({"household_id": household, "arm": "control", "assigned_at": "2026-06-30"})
        identities.append({
            "link_id": f"l-{device}", "device_id": device, "household_id": household,
            "effective_at": "2026-06-20", "known_at": "2026-06-21", "revision": 1, "revoked": False,
        })
        events.append(order(f"co{index}", household, device, total))
    return {
        "experiment_id": "device-trap",
        "seed": 3,
        "as_of": "2026-07-25",
        "observation_start": "2026-06-30",
        "observation_end": "2026-07-20",
        "assignments": assignments,
        "identities": identities,
        "events": events,
    }


def order(order_id: str, household: str, device: str, amount: float, **overrides) -> dict:
    row = {
        "event_id": f"ev-{order_id}",
        "revision": 1,
        "household_id": household,
        "device_id": device.replace(" ", ""),
        "type": "order",
        "order_id": order_id,
        "market": "leather",
        "amount": amount,
        "occurred_at": "2026-07-05",
        "known_at": "2026-07-06",
    }
    row.update(overrides)
    return row


class AnalysisTests(unittest.TestCase):
    def analyse(self, document: dict) -> dict:
        return analyse(validate_input(copy.deepcopy(document)))

    def test_fixture_is_inconclusive_and_keeps_unexposed_in_denominator(self):
        result = self.analyse(load_fixture())
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        self.assertEqual(revenue["unit"], "currency_per_household")
        self.assertEqual(revenue["status"], "inconclusive")
        self.assertLess(revenue["interval"][0], 0.0)
        self.assertGreater(revenue["interval"][1], 0.0)
        self.assertEqual(result["population"]["assigned"]["total"], 80)
        self.assertEqual(result["population"]["effective_sample"]["total"], 80)

    def test_household_units_not_devices(self):
        document = load_fixture()
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        # Two devices per household must not double the denominator.
        self.assertEqual(revenue["sample_size"], {"treatment": 40, "control": 40})
        self.assertEqual(result["sources"]["visible_counts"]["orders"], 160)

    def test_duplicate_order_observation_counts_once(self):
        document = minimal_input()
        # The same order_id observed twice through two devices of one household.
        document["events"].append(
            order("o1", "h1", "d h1-second", 100.0, event_id="ev-o1-second",
                  known_at="2026-07-07")
        )
        document["identities"].append({
            "link_id": "lh1-second", "device_id": "dh1-second", "household_id": "h1",
            "effective_at": "2026-06-20", "known_at": "2026-06-21", "revision": 1, "revoked": False,
        })
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        # h1: 100, h2: 60 -> treatment mean 80; control (40+30)/2 = 35.
        self.assertAlmostEqual(revenue["estimate"], 45.0, places=6)
        self.assertEqual(result["sources"]["visible_counts"]["orders"], 4)
        duplicate_check = next(c for c in result["checks"] if c["key"] == "duplicate_observations")
        self.assertEqual(duplicate_check["observed"]["device_duplicate_observations_collapsed"], 1)

    def test_cross_market_orders_sum_within_household(self):
        document = minimal_input()
        document["events"].append(
            order("o1b", "h1", "dh1", 50.0, market="textile", occurred_at="2026-07-08")
        )
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        # h1: 100 + 50, h2: 60 -> treatment mean 105; control 35.
        self.assertAlmostEqual(revenue["estimate"], 70.0, places=6)
        cross_market = next(c for c in result["checks"] if c["key"] == "cross_market_repeat_purchase")
        self.assertEqual(cross_market["status"], "noted")

    def test_conflicting_duplicate_order_fails(self):
        document = minimal_input()
        document["events"].append(
            order("o1", "h1", "d h1", 999.0, event_id="ev-o1-second", market="textile")
        )
        with self.assertRaises(ConflictError):
            self.analyse(document)

    def test_late_refund_does_not_change_primary_result(self):
        document = load_fixture()
        baseline = self.analyse(document)
        baseline_revenue = next(item for item in baseline["effects"] if item["metric"] == "net_revenue")
        late = copy.deepcopy(document)
        late["events"].append({
            "event_id": "refund-late", "revision": 1, "type": "refund", "device_id": "d3",
            "order_id": "o3", "amount": 20.0, "occurred_at": "2026-07-10", "known_at": "2026-07-30",
        })
        result = self.analyse(late)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        self.assertAlmostEqual(revenue["estimate"], baseline_revenue["estimate"], places=6)
        self.assertEqual(result["population"]["effective_sample"]["total"], 80)
        self.assertEqual(result["sensitivity"]["late_refund"]["estimate"] is not None, True)
        check = next(c for c in result["checks"] if c["key"] == "late_refunds")
        self.assertEqual(check["status"], "noted")
        self.assertEqual(check["observed"]["count"], 1)

    def test_refund_known_at_cutoff_is_applied(self):
        document = minimal_input()
        document["events"].append({
            "event_id": "refund-1", "revision": 1, "type": "refund", "device_id": "dh1",
            "order_id": "o1", "amount": 20.0, "occurred_at": "2026-07-10", "known_at": "2026-07-11",
        })
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        # h1: 80, h2: 60 -> treatment mean 70; control 35.
        self.assertAlmostEqual(revenue["estimate"], 35.0, places=6)

    def test_revision_after_cutoff_does_not_rewrite_history(self):
        document = minimal_input()
        revoked_later = copy.deepcopy(document)
        revoked_later["identities"].append({
            "link_id": "lh1", "device_id": "dh1", "household_id": "h1",
            "effective_at": "2026-07-20", "known_at": "2026-07-26",
            "revision": 2, "revoked": True,
        })
        baseline = self.analyse(document)
        result = self.analyse(revoked_later)
        self.assertEqual(
            next(item for item in result["effects"] if item["metric"] == "net_revenue")["estimate"],
            next(item for item in baseline["effects"] if item["metric"] == "net_revenue")["estimate"],
        )
        audit = result["sources"]["audits"]["identities"]
        self.assertIn("lh1", audit["objects_with_later_revision_after_cutoff"])

    def test_late_revision_replaces_known_revision(self):
        document = minimal_input()
        corrected = copy.deepcopy(document)
        corrected["events"].append(
            order("o1", "h1", "dh1", 10.0, event_id="ev-o1", revision=2, known_at="2026-07-10")
        )
        result = self.analyse(corrected)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        # h1 uses revision 2: 10, h2: 60 -> treatment mean 35; control 35.
        self.assertAlmostEqual(revenue["estimate"], 0.0, places=6)

    def test_order_without_identity_is_excluded_not_zero(self):
        document = minimal_input()
        document["events"].append(
            order("o5", None, "d-unknown", 50.0, event_id="ev-o5")
        )
        del document["events"][-1]["household_id"]
        result = self.analyse(document)
        self.assertEqual(result["population"]["unresolved_events"]["total"], 1)
        self.assertEqual(result["population"]["effective_sample"]["total"], 4)

    def test_ambiguous_device_is_excluded(self):
        document = minimal_input()
        document["identities"].append({
            "link_id": "l-h4-second", "device_id": "dh1", "household_id": "h4",
            "effective_at": "2026-06-20", "known_at": "2026-06-21", "revision": 1, "revoked": False,
        })
        result = self.analyse(document)
        self.assertIn("ambiguous_identity_at_cutoff", result["population"]["excluded"]["by_reason"])
        self.assertLess(result["population"]["effective_sample"]["total"], 4)

    def test_too_few_households_is_insufficient_evidence(self):
        document = minimal_input()
        document["assignments"] = document["assignments"][:3]
        document["identities"] = [row for row in document["identities"] if row["household_id"] != "h4"]
        document["events"] = [row for row in document["events"] if row.get("household_id") != "h4"]
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        self.assertEqual(revenue["status"], "insufficient_evidence")

    def test_cross_arm_exposure_is_reported_not_dropped(self):
        document = load_fixture()
        result = self.analyse(document)
        self.assertEqual(result["population"]["contaminated"]["total"], 1)
        self.assertEqual(result["population"]["contaminated"]["by_reason"]["cross_arm_exposure"], 1)

    def test_permitted_input_reordering_is_statistically_equivalent(self):
        document = load_fixture()
        reordered = copy.deepcopy(document)
        reordered["assignments"].reverse()
        reordered["identities"].reverse()
        reordered["events"].reverse()
        first = self.analyse(document)
        second = self.analyse(reordered)
        self.assertEqual(
            [(item["metric"], item["estimate"], item["interval"], item["status"]) for item in first["effects"]],
            [(item["metric"], item["estimate"], item["interval"], item["status"]) for item in second["effects"]],
        )
        self.assertEqual(first["population"], second["population"])

    def test_identical_duplicate_event_row_is_deduped(self):
        document = minimal_input()
        document["events"].append(copy.deepcopy(document["events"][0]))
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        self.assertAlmostEqual(revenue["estimate"], 45.0, places=6)
        self.assertEqual(result["sources"]["input_counts"]["duplicate_event_rows"], 1)

    def test_identical_duplicate_assignment_row_is_deduped(self):
        document = minimal_input()
        document["assignments"].append(copy.deepcopy(document["assignments"][0]))
        result = self.analyse(document)
        self.assertEqual(result["population"]["assigned"]["total"], 4)
        self.assertEqual(result["sources"]["input_counts"]["duplicate_assignment_rows"], 1)

    def test_stale_declared_household_is_reconciled_and_reported(self):
        document = minimal_input()
        document["events"][0]["household_id"] = "h2"  # device dh1 belongs to h1
        result = self.analyse(document)
        # The event still counts as h1's purchase, and the reconciliation is visible.
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        self.assertAlmostEqual(revenue["estimate"], 45.0, places=6)
        check = next(c for c in result["checks"] if c["key"] == "identity_reconciliation")
        self.assertEqual(check["status"], "noted")
        self.assertEqual(check["observed"]["events"], 1)

    def test_future_effective_revision_does_not_erase_current_link(self):
        document = minimal_input()
        # Revision 2 is known before the cutoff but only takes effect later.
        document["identities"].append({
            "link_id": "lh1", "device_id": "dh1", "household_id": "h1",
            "effective_at": "2026-08-01", "known_at": "2026-07-10",
            "revision": 2, "revoked": True,
        })
        result = self.analyse(document)
        # The revision-1 link stays in force at the cutoff, so h1 remains.
        self.assertEqual(result["population"]["effective_sample"]["total"], 4)
        self.assertIn(
            "lh1", result["sources"]["audits"]["identities"]["objects_with_later_revision_after_cutoff"]
        )

    def test_device_level_contrast_can_reverse_the_household_result(self):
        document = device_trap_input()
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        # Household totals: treatment mean 97.5, control mean 32.5 -> +65.
        self.assertAlmostEqual(revenue["estimate"], 65.0, places=6)
        self.assertEqual(revenue["status"], "supported")
        device_check = next(c for c in result["checks"] if c["key"] == "device_level_contrast")
        self.assertLess(device_check["observed"]["estimate"], 0.0)
        self.assertEqual(device_check["status"], "diagnostic_mismatch")
        self.assertIn("independent", device_check["observed"]["warning"])

    def test_material_unresolved_outcome_makes_effect_insufficient(self):
        document = minimal_input()
        ghost = order("o-ghost", None, "dghost", 5000.0, event_id="ev-ghost")
        del ghost["household_id"]
        document["events"].append(ghost)
        result = self.analyse(document)
        revenue = next(item for item in result["effects"] if item["metric"] == "net_revenue")
        self.assertEqual(revenue["status"], "insufficient_evidence")
        self.assertEqual(revenue["status_reason"], "outcome_partially_unobserved")
        check = next(c for c in result["checks"] if c["key"] == "unresolved_events")
        self.assertEqual(check["status"], "review")

    def test_conflicting_assignment_fails(self):
        document = minimal_input()
        document["assignments"].append({"household_id": "h1", "arm": "control", "assigned_at": "2026-06-30"})
        with self.assertRaises(ConflictError):
            self.analyse(document)

    def test_missing_amount_fails(self):
        document = minimal_input()
        del document["events"][0]["amount"]
        with self.assertRaises(DomainError):
            self.analyse(document)

    def test_assignment_after_cutoff_fails(self):
        document = minimal_input()
        document["assignments"][0]["assigned_at"] = "2026-08-01"
        with self.assertRaises(ConflictError):
            self.analyse(document)

    def test_secondary_does_not_replace_primary(self):
        document = load_fixture()
        result = self.analyse(document)
        self.assertEqual(result["secondary"]["effects"], [])
        self.assertIn("cannot replace", result["secondary"]["note"])


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "report.json"
        self.database = Path(self.temp.name) / "analysis.sqlite3"

    def test_cli_emits_required_sections(self):
        from twin_market_flow.experiment_analysis import main

        with contextlib.redirect_stdout(io.StringIO()):
            exit_code = main([
                "--input", str(FIXTURE), "--database", str(self.database), "--output", str(self.output),
            ])
        self.assertEqual(exit_code, 0)
        report = json.loads(self.output.read_text(encoding="utf-8"))
        for key in ("analysis_id", "population", "effects", "checks", "sources"):
            self.assertIn(key, report)
        for key in ("assigned", "excluded", "contaminated"):
            self.assertIn(key, report["population"])
        for effect in report["effects"]:
            for key in ("metric", "estimate", "interval", "status", "unit"):
                self.assertIn(key, effect)

    def test_reordered_equivalent_input_shares_fingerprint_and_analysis(self):
        document = load_fixture()
        first = validate_input(copy.deepcopy(document))
        reversed_input = copy.deepcopy(document)
        reversed_input["assignments"].reverse()
        reversed_input["identities"].reverse()
        reversed_input["events"].reverse()
        second = validate_input(reversed_input)
        self.assertEqual(canonical_fingerprint(first), canonical_fingerprint(second))
        first_result = run_analysis(FIXTURE, self.database)
        path = Path(self.temp.name) / "reordered.json"
        path.write_text(json.dumps(reversed_input), encoding="utf-8")
        second_result = run_analysis(path, self.database)
        self.assertEqual(first_result["analysis_id"], second_result["analysis_id"])
        self.assertTrue(second_result["persistence"]["replayed"])


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "analysis.sqlite3"

    def test_persists_and_replays_by_fingerprint(self):
        first = run_analysis(FIXTURE, self.database)
        second = run_analysis(FIXTURE, self.database)
        self.assertEqual(first["analysis_id"], second["analysis_id"])
        self.assertEqual(first["version"], 1)
        self.assertTrue(second["persistence"]["replayed"])
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM experiment_analyses").fetchone()[0], 1
            )

    def test_correction_creates_traceable_new_version(self):
        first = run_analysis(FIXTURE, self.database)
        path = self.temp_extra()
        path.write_text(json.dumps(fix_refund_known_at()), encoding="utf-8")
        corrected = run_analysis(path, self.database)
        self.assertNotEqual(first["analysis_id"], corrected["analysis_id"])
        self.assertEqual(corrected["version"], 2)
        self.assertEqual(corrected["parent_analysis_id"], first["analysis_id"])
        store = ExperimentAnalysisStore(self.database)
        history = store.history("joint-coupon")
        self.assertEqual([row["version"] for row in history], [1, 2])
        # The first analysis is unchanged.
        with sqlite3.connect(self.database) as connection:
            payload = json.loads(connection.execute(
                "SELECT payload FROM experiment_analyses WHERE analysis_id=?", (first["analysis_id"],)
            ).fetchone()[0])
        self.assertEqual(payload["effects"], first["effects"])

    def test_correcting_a_late_refund_changes_new_version_but_not_old_one(self):
        original = run_analysis(FIXTURE, self.database)
        path = self.temp_extra()
        path.write_text(json.dumps(fix_refund_known_at()), encoding="utf-8")
        corrected = run_analysis(path, self.database)
        # The older version keeps the late refund out of net revenue.
        self.assertNotEqual(corrected["effects"][0]["estimate"], original["effects"][0]["estimate"])
        self.assertIsNone(corrected["sensitivity"]["late_refund"])
        with sqlite3.connect(self.database) as connection:
            stored = json.loads(connection.execute(
                "SELECT payload FROM experiment_analyses WHERE analysis_id=?",
                (original["analysis_id"],),
            ).fetchone()[0])
        self.assertEqual(stored["effects"], original["effects"])
        self.assertIsNotNone(stored["sensitivity"]["late_refund"])

    def test_conflicting_input_creates_no_report(self):
        from twin_market_flow.experiment_analysis import main

        bad = load_fixture()
        bad["assignments"].append({"household_id": "h000", "arm": "treatment", "assigned_at": "2026-06-30"})
        path = self.temp_extra()
        path.write_text(json.dumps(bad), encoding="utf-8")
        output = Path(self.temp.name) / "report.json"
        with contextlib.redirect_stderr(io.StringIO()):
            exit_code = main([
                "--input", str(path), "--database", str(self.database), "--output", str(output),
            ])
        self.assertNotEqual(exit_code, 0)
        self.assertFalse(output.exists())
        ExperimentAnalysisStore(self.database)  # the schema exists but holds no analysis
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM experiment_analyses").fetchone()[0], 0
            )
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM failures").fetchone()[0], 1)

    def temp_extra(self):
        handle = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        self.addCleanup(lambda: Path(handle.name).unlink(missing_ok=True))
        handle.close()
        return Path(handle.name)


def fix_refund_known_at() -> dict:
    document = load_fixture()
    for event in document["events"]:
        if event["event_id"] == "refund-late":
            event["known_at"] = "2026-07-24"
    return document


if __name__ == "__main__":
    unittest.main()
