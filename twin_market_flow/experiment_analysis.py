"""Point-in-time experiment analysis for the joint coupon campaign.

This module answers one question only: what did the original household-level
randomisation imply at the agreed observation cutoff?  It is not a traffic
page, and it does not treat device rows as independent people.

Design choices
--------------

* The randomisation unit, the analysis unit and the clustering unit are all
  the household.  The primary contrast is intent-to-treat on the assignment
  recorded in the registry, so unexposed households remain in the denominator.
* Information is reconstructed as of ``as_of``.  Event and identity revisions
  whose ``known_at`` is after the cutoff are not allowed to travel back in
  time.  A later revision replaces an earlier one only when both its
  ``known_at`` and its effective time were already available at the cutoff.
* Revoked links are withheld from the identity graph.  A revocation known
  after the cutoff is not applied retroactively.  A household without any
  active identity link at the cutoff cannot have its non-purchase observed,
  so it is excluded with a reason instead of silently treated as a zero.
* Refunds known after the cutoff are not subtracted from historical net
  revenue.  Their count and value are reported and quantified as a
  sensitivity check.
* Repeated observations of the same ``order_id`` (for example a household
  using two devices in two markets) are counted once.  Attribution conflicts
  fail the run rather than produce a plausible-looking report.
* Uncertainty is estimated at the household level.  Net revenue uses a Welch
  t interval; the binary conversion metric uses a Newcombe hybrid score
  interval, which stays usable when an arm has no variation.  A seeded
  permutation p-value is reported alongside.  When the data cannot support an
  estimate -- too few households, no within-arm variation, material
  unobserved outcomes, or too many exclusions -- the metric status is
  ``insufficient_evidence`` rather than a confident-looking number.

Analyses are stored append-only in the same SQLite database used by the
existing business workflow.  Recomputing with a corrected input creates a new
traceable version; already formed analyses are never mutated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import statistics
import sys
import tempfile
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from twin_market_flow.warehouse.run_ledger import (
    ConflictError,
    DomainError,
    SQLiteLedger,
    stable_json,
    utc_now,
)

VALID_ARMS = ("control", "treatment")
EVENT_TYPES = ("exposure", "order", "refund")
PRIMARY_METRICS = ("net_revenue", "converted")
METHOD = "household_itt_welch_v1"
PERMUTATION_DRAWS = 2000
MIN_ARM_HOUSEHOLDS = 2
SMALL_CELL_THRESHOLD = 10
INTERVAL_LEVEL = 0.95


# ---------------------------------------------------------------------------
# Basic parsing helpers
# ---------------------------------------------------------------------------


def _text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f"{where}: a non-empty string is required")
    return value.strip()


def _day(value: Any, where: str) -> date:
    text = _text(value, where)
    try:
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return date.fromisoformat(text[:10])
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise DomainError(f"{where}: invalid ISO 8601 date {text!r}") from exc


def _revision(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise DomainError(f"{where}: revision must be a positive integer")
    return value


def _amount(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DomainError(f"{where}: amount must be a finite number, not a missing value")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise DomainError(f"{where}: amount must be finite and non-negative")
    return result


def _flag(value: Any, where: str) -> bool:
    if value is None:
        return False
    if not isinstance(value, bool):
        raise DomainError(f"{where}: revoked must be a boolean")
    return value


def _seed(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainError("seed: an integer seed is required")
    return value


def _duplicate_pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConflictError(f"input JSON contains duplicate key {key!r}")
        result[key] = value
    return result


# ---------------------------------------------------------------------------
# Input validation and canonicalisation
# ---------------------------------------------------------------------------


def _canonical_assignments(raw: Any) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(raw, list) or not raw:
        raise DomainError("assignments: a non-empty list is required")
    rows: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for index, item in enumerate(raw):
        where = f"assignments[{index}]"
        if not isinstance(item, Mapping):
            raise DomainError(f"{where}: an object is required")
        household_id = _text(item.get("household_id"), where + ".household_id")
        arm = _text(item.get("arm"), where + ".arm")
        if arm not in VALID_ARMS:
            raise ConflictError(f"{where}.arm: {arm!r} is not one of {VALID_ARMS}")
        assigned_at = _day(item.get("assigned_at"), where + ".assigned_at")
        row = {
            "household_id": household_id,
            "arm": arm,
            "assigned_at": assigned_at.isoformat(),
        }
        previous = rows.get(household_id)
        if previous is not None:
            if previous != row:
                raise ConflictError(
                    f"assignments: household {household_id!r} has conflicting registrations"
                )
            duplicates += 1
            continue
        rows[household_id] = row
    ordered = [rows[key] for key in sorted(rows)]
    return ordered, duplicates


def _canonical_identities(raw: Any, households: set[str]) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(raw, list):
        raise DomainError("identities: a list is required")
    rows: list[dict[str, Any]] = []
    duplicates = 0
    seen: dict[tuple[str, int], dict[str, Any]] = {}
    for index, item in enumerate(raw):
        where = f"identities[{index}]"
        if not isinstance(item, Mapping):
            raise DomainError(f"{where}: an object is required")
        link_id = _text(item.get("link_id"), where + ".link_id")
        device_id = _text(item.get("device_id"), where + ".device_id")
        household_id = _text(item.get("household_id"), where + ".household_id")
        if household_id not in households:
            raise ConflictError(
                f"{where}.household_id: {household_id!r} is not a registered household"
            )
        revision = _revision(item.get("revision"), where + ".revision")
        effective_at = _day(item.get("effective_at"), where + ".effective_at")
        known_at = _day(item.get("known_at"), where + ".known_at")
        revoked = _flag(item.get("revoked"), where + ".revoked")
        row = {
            "link_id": link_id,
            "device_id": device_id,
            "household_id": household_id,
            "revision": revision,
            "effective_at": effective_at.isoformat(),
            "known_at": known_at.isoformat(),
            "revoked": revoked,
        }
        key = (link_id, revision)
        previous = seen.get(key)
        if previous is not None:
            if previous != row:
                raise ConflictError(
                    f"identities: link {link_id!r} revision {revision} has conflicting content"
                )
            duplicates += 1
            continue
        seen[key] = row
        rows.append(row)
    rows.sort(
        key=lambda row: (
            row["link_id"], row["revision"], row["known_at"],
            row["effective_at"], row["household_id"], row["device_id"],
        )
    )
    return rows, duplicates


def _canonical_events(raw: Any, households: set[str]) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(raw, list):
        raise DomainError("events: a list is required")
    rows: list[dict[str, Any]] = []
    duplicates = 0
    seen: dict[tuple[str, int], dict[str, Any]] = {}
    for index, item in enumerate(raw):
        where = f"events[{index}]"
        if not isinstance(item, Mapping):
            raise DomainError(f"{where}: an object is required")
        event_id = _text(item.get("event_id"), where + ".event_id")
        revision = _revision(item.get("revision"), where + ".revision")
        kind = _text(item.get("type"), where + ".type")
        if kind not in EVENT_TYPES:
            raise ConflictError(f"{where}.type: {kind!r} is not one of {EVENT_TYPES}")
        device_id = _text(item.get("device_id"), where + ".device_id")
        occurred_at = _day(item.get("occurred_at"), where + ".occurred_at")
        known_at = _day(item.get("known_at"), where + ".known_at")
        household_id = item.get("household_id")
        if household_id is not None:
            household_id = _text(household_id, where + ".household_id")
            if household_id not in households:
                raise ConflictError(
                    f"{where}.household_id: {household_id!r} is not a registered household"
                )
        market = item.get("market")
        if market is not None:
            market = _text(market, where + ".market")
        order_id = item.get("order_id")
        if kind in ("order", "refund"):
            order_id = _text(order_id, where + ".order_id")
        amount = None
        if kind in ("order", "refund"):
            amount = _amount(item.get("amount"), where + ".amount")
        campaign_arm = item.get("campaign_arm")
        if campaign_arm is not None:
            campaign_arm = _text(campaign_arm, where + ".campaign_arm")
            if campaign_arm not in VALID_ARMS:
                raise ConflictError(
                    f"{where}.campaign_arm: {campaign_arm!r} is not one of {VALID_ARMS}"
                )
        row = {
            "event_id": event_id,
            "revision": revision,
            "type": kind,
            "device_id": device_id,
            "household_id": household_id,
            "market": market,
            "order_id": order_id,
            "amount": amount,
            "campaign_arm": campaign_arm,
            "occurred_at": occurred_at.isoformat(),
            "known_at": known_at.isoformat(),
        }
        key = (event_id, revision)
        previous = seen.get(key)
        if previous is not None:
            if previous != row:
                raise ConflictError(
                    f"events: event {event_id!r} revision {revision} has conflicting content"
                )
            duplicates += 1
            continue
        seen[key] = row
        rows.append(row)
    rows.sort(
        key=lambda row: (
            row["event_id"], row["revision"], row["known_at"],
            row["occurred_at"], row["type"],
        )
    )
    return rows, duplicates


def validate_input(raw: Any) -> dict[str, Any]:
    """Validate and canonically order a raw experiment input document."""

    if not isinstance(raw, Mapping):
        raise DomainError("experiment input must be a JSON object")
    experiment_id = _text(raw.get("experiment_id"), "experiment_id")
    seed = _seed(raw.get("seed"))
    as_of = _day(raw.get("as_of"), "as_of")
    observation_start = _day(raw.get("observation_start"), "observation_start")
    observation_end = _day(raw.get("observation_end"), "observation_end")
    if observation_end < observation_start:
        raise ConflictError("observation_end must not precede observation_start")
    if as_of < observation_start:
        raise ConflictError("as_of must not precede observation_start")

    assignments, duplicate_assignments = _canonical_assignments(raw.get("assignments"))
    households = {row["household_id"] for row in assignments}
    identities, duplicate_identities = _canonical_identities(raw.get("identities", []), households)
    events, duplicate_events = _canonical_events(raw.get("events", []), households)

    for row in assignments:
        assigned_at = date.fromisoformat(row["assigned_at"])
        if assigned_at > as_of:
            raise ConflictError(
                f"assignments: household {row['household_id']!r} was registered after as_of"
            )

    return {
        "experiment_id": experiment_id,
        "seed": seed,
        "as_of": as_of.isoformat(),
        "observation_start": observation_start.isoformat(),
        "observation_end": observation_end.isoformat(),
        "assignments": assignments,
        "identities": identities,
        "events": events,
        "input_counts": {
            "assignments": len(assignments),
            "identities": len(identities),
            "events": len(events),
            "duplicate_assignment_rows": duplicate_assignments,
            "duplicate_identity_rows": duplicate_identities,
            "duplicate_event_rows": duplicate_events,
        },
    }


def canonical_fingerprint(document: Mapping[str, Any]) -> str:
    return hashlib.sha256(stable_json(document).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Point-in-time reconstruction
# ---------------------------------------------------------------------------


def _take_revisions(
    rows: Sequence[Mapping[str, Any]], as_of: date, label: str
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Keep only the latest revision that was already known at the cutoff.

    Revisions with ``known_at`` after ``as_of`` are invisible.  For revisions
    that are visible, the highest revision wins; a tie between two visible
    revisions with the same number is a conflict and fails the run.  An
    effective time after the cutoff means the row is not yet in force.
    """

    grouped: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        grouped[identifier_key(row)][int(row["revision"])] = dict(row)

    def effective_time(row: Mapping[str, Any]) -> date:
        stamp = row.get("effective_at") or row.get("occurred_at")
        if stamp is None:
            raise DomainError(f"{label}: an effective time is required")
        return date.fromisoformat(str(stamp)[:10])

    kept: list[dict[str, Any]] = []
    ignored_late: list[str] = []
    superseded: list[str] = []
    future_effective: list[str] = []
    resolved_after_cutoff: list[str] = []
    rows_revised_away = 0
    for identifier in sorted(grouped):
        revisions = grouped[identifier]
        visible = {
            number: row
            for number, row in revisions.items()
            if date.fromisoformat(row["known_at"]) <= as_of
        }
        if not visible:
            ignored_late.append(identifier)
            continue
        # A revision that was known at the cutoff but only takes effect later
        # does not rewrite the earlier state.  Fall back to the newest
        # revision that was both known and in force at the cutoff.
        in_force = {
            number: row for number, row in visible.items() if effective_time(row) <= as_of
        }
        if not in_force:
            future_effective.append(identifier)
            continue
        latest = max(in_force)
        chosen = in_force[latest]
        if max(visible) > latest or max(revisions) > latest:
            # A correction exists that the historical snapshot must not see.
            resolved_after_cutoff.append(identifier)
        lower_revisions = [number for number in revisions if number < latest]
        if lower_revisions:
            superseded.append(identifier)
            rows_revised_away += len(lower_revisions)
        kept.append(chosen)
    kept.sort(key=lambda row: (row["known_at"], identifier_key(row)))
    # Content that exists in the input but was only learned after the cutoff.
    # It must never alter the historical snapshot, but late refunds are still
    # reported so their size is visible instead of being silently dropped.
    late_rows: list[dict[str, Any]] = []
    for identifier in sorted(grouped):
        revisions = grouped[identifier]
        visible = {
            number: row
            for number, row in revisions.items()
            if date.fromisoformat(row["known_at"]) <= as_of
        }
        for number in sorted(revisions):
            if number not in visible:
                late_rows.append(dict(revisions[number]))
    audit = {
        "label": label,
        "rows_total": len(rows),
        "rows_revised_away": rows_revised_away,
        "objects_visible": len(kept),
        "objects_ignored_late": sorted(ignored_late),
        "objects_future_effective": sorted(future_effective),
        "objects_with_later_revision_after_cutoff": sorted(resolved_after_cutoff),
    }
    return kept, audit, late_rows


def identifier_key(row: Mapping[str, Any]) -> str:
    return str(row.get("event_id") or row.get("link_id") or "")


def _device_graph(identities: Sequence[Mapping[str, Any]]) -> tuple[dict[str, dict[str, date]], dict[str, Any]]:
    """Return the device->household links visible at the cutoff.

    A revoked link is withheld.  If a device is still linked to more than one
    household through links active at the cutoff, the identity cannot be
    resolved and the device is marked ambiguous instead of being resolved by
    row order.  Effective dates are retained so an event that happened before
    a link existed is not attributed retroactively.
    """

    candidates: dict[str, dict[str, date]] = defaultdict(dict)
    revoked_devices: set[str] = set()
    for row in identities:
        # ``_take_revisions`` already reduced each link_id to its latest
        # revision visible at the cutoff.  Revocation therefore applies to
        # that link only; it must not erase an independent, still-active link
        # that happens to share the device.
        device = str(row["device_id"])
        if bool(row["revoked"]):
            revoked_devices.add(device)
            continue
        household = str(row["household_id"])
        effective = date.fromisoformat(row["effective_at"])
        previous = candidates[device].get(household)
        if previous is None or effective < previous:
            candidates[device][household] = effective

    ambiguous: dict[str, list[str]] = {}
    for device in sorted(candidates):
        if len(candidates[device]) > 1:
            ambiguous[device] = sorted(candidates[device])
    audit = {
        "devices_linked": len(candidates),
        "devices_resolved": len(candidates) - len(ambiguous),
        "devices_ambiguous": ambiguous,
        "devices_revoked_at_cutoff": sorted(revoked_devices),
        "devices_known": len(set(candidates) | revoked_devices),
    }
    return dict(candidates), audit


def _resolve_household(
    event: Mapping[str, Any],
    device_links: Mapping[str, Mapping[str, date]],
    known_devices: set[str],
) -> tuple[str | None, str]:
    """Resolve an event to a household through links active when it happened.

    Identity is the link graph, not a convenience field carried by the event.
    A declared ``household_id`` is only a consistency check: when it disagrees
    with the graph, the input is contradictory.  If the device has no
    unambiguous active link at the event time, the event is unresolved rather
    than attributed to a household the cutoff snapshot cannot support.
    """

    device = str(event["device_id"])
    occurred = date.fromisoformat(str(event["occurred_at"])[:10])
    links = device_links.get(device)
    declared = event.get("household_id")
    if links is None:
        return None, "unlinked" if device not in known_devices else "identity_not_resolved_at_cutoff"
    effective_households = sorted(
        household for household, effective in links.items() if effective <= occurred
    )
    if len(effective_households) > 1:
        return None, "ambiguous_identity"
    if not effective_households:
        return None, "identity_not_effective_at_event_time"
    household = effective_households[0]
    if declared is not None and str(declared) != household:
        # The event was recorded under an older identity.  A corrected link
        # known at the cutoff is authoritative, but the change is surfaced as
        # a reconciliation instead of being silently applied or rejected.
        return household, "link_overrides_declared_household"
    return household, "link"


def _dedupe_orders(
    resolved: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Collapse repeated observations of the same order to one transaction.

    The ``order_id`` is the transaction key.  If two visible observations of
    the same order disagree on the amount, household or market, the input is
    contradictory and the run fails instead of quietly picking one.
    """

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in resolved:
        if event["type"] == "order":
            grouped[str(event["order_id"])].append(event)
    kept: list[dict[str, Any]] = []
    duplicates = 0
    for order_id in sorted(grouped):
        observations = sorted(
            grouped[order_id],
            key=lambda row: (row["known_at"], row["event_id"], row["revision"]),
        )
        first = observations[0]
        signature = (first["household_id"], round(first["amount"], 9))
        markets = {first["market"]}
        for other in observations[1:]:
            other_signature = (other["household_id"], round(other["amount"], 9))
            if signature != other_signature:
                raise ConflictError(
                    f"events: order {order_id!r} is observed with conflicting amount "
                    "or household at the cutoff"
                )
            markets.add(other["market"])
            duplicates += 1
        # When the same transaction is recorded by devices active in different
        # markets, keep a single transaction and note the market scope.  The
        # order is not counted twice just because it is visible twice.
        markets.discard(None)
        record = dict(first)
        record["markets"] = sorted(str(market) for market in markets)
        kept.append(record)
    kept.sort(key=lambda row: (row["occurred_at"], row["order_id"], row["event_id"]))
    cross_market_observations = sum(1 for row in kept if len(row["markets"]) > 1)
    return kept, {
        "orders_observed": sum(len(items) for items in grouped.values()),
        "orders_kept": len(kept),
        "device_duplicate_observations_collapsed": duplicates,
        "orders_seen_in_multiple_markets": cross_market_observations,
    }


@dataclass
class Snapshot:
    households: list[str]
    arm: dict[str, str]
    orders: list[dict[str, Any]]
    exposures: list[dict[str, Any]]
    refunds_late: list[dict[str, Any]]
    refunds_known: list[dict[str, Any]]
    identities: list[dict[str, Any]]
    device_links: dict[str, dict[str, date]]
    audits: dict[str, Any]
    unresolved_events: list[dict[str, Any]]
    excluded: dict[str, str]


def build_snapshot(document: Mapping[str, Any]) -> Snapshot:
    as_of = date.fromisoformat(document["as_of"])
    window_start = date.fromisoformat(document["observation_start"])
    window_end = date.fromisoformat(document["observation_end"])
    assignments = list(document["assignments"])
    arm = {row["household_id"]: row["arm"] for row in assignments}
    households = sorted(arm)

    identities, identity_audit, _ = _take_revisions(document["identities"], as_of, "identities")
    events, event_audit, late_events = _take_revisions(document["events"], as_of, "events")
    device_links, graph_audit = _device_graph(identities)
    known_devices = {str(row["device_id"]) for row in identities}

    resolved: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    reconciled: list[dict[str, Any]] = []
    for event in events:
        household, basis = _resolve_household(event, device_links, known_devices)
        if household is None:
            unresolved.append({**event, "resolution": basis})
            continue
        if household not in arm:
            raise ConflictError(
                f"events: event {event['event_id']!r} resolves to unregistered household {household!r}"
            )
        if basis == "link_overrides_declared_household":
            reconciled.append({
                "event_id": event["event_id"],
                "type": event["type"],
                "device_id": event["device_id"],
                "declared_household_id": event["household_id"],
                "resolved_household_id": household,
            })
        resolved.append({**event, "household_id": household, "resolution": basis})

    # Outcomes are measured inside the agreed observation window.  An event
    # that happened outside it is not evidence about the in-window effect and
    # is reported rather than silently counted or silently deleted.
    in_window: list[dict[str, Any]] = []
    out_of_window: list[dict[str, Any]] = []
    for event in resolved:
        occurred = date.fromisoformat(str(event["occurred_at"])[:10])
        if window_start <= occurred <= window_end:
            in_window.append(event)
        else:
            out_of_window.append(event)
    window_audit = {
        "start": document["observation_start"],
        "end": document["observation_end"],
        "events_in_window": len(in_window),
        "events_out_of_window": len(out_of_window),
        "out_of_window_by_type": dict(Counter(event["type"] for event in out_of_window)),
    }

    orders, order_audit = _dedupe_orders(in_window)
    exposures: list[dict[str, Any]] = []
    refunds_known: list[dict[str, Any]] = []
    refunds_late: list[dict[str, Any]] = []

    for event in in_window:
        kind = event["type"]
        if kind == "exposure":
            if event["campaign_arm"] is not None:
                registry_arm = arm[event["household_id"]]
                if event["campaign_arm"] != registry_arm:
                    # Exposed to the other arm: this is contamination, and the
                    # exposure cannot be used to build an as-treated subset.
                    event = {**event, "cross_arm_exposure": True}
                else:
                    event = {**event, "cross_arm_exposure": False}
            else:
                event = {**event, "cross_arm_exposure": False}
            exposures.append(event)
        elif kind == "refund":
            refunds_known.append(event)

    # Late refunds are taken from the input content that was not visible at
    # the cutoff.  They are never applied to primary net revenue; they are
    # reported and quantified separately.  If the same refund has several
    # revisions, the newest content is the one whose late arrival matters.
    late_refunds_by_event: dict[str, dict[str, Any]] = {}
    late_refunds_without_identity: list[str] = []
    for event in late_events:
        if event["type"] != "refund":
            continue
        links = device_links.get(str(event["device_id"]))
        household = None
        if links:
            occurred = date.fromisoformat(str(event["occurred_at"])[:10])
            effective_households = sorted(
                item for item, effective in links.items() if effective <= occurred
            )
            if len(effective_households) == 1:
                household = effective_households[0]
        if household is None:
            late_refunds_without_identity.append(str(event["event_id"]))
            continue
        declared = event.get("household_id")
        if declared is not None and str(declared) != household:
            reconciled.append({
                "event_id": event["event_id"],
                "type": "refund",
                "device_id": event["device_id"],
                "declared_household_id": declared,
                "resolved_household_id": household,
            })
        late_refunds_by_event[str(event["event_id"])] = {
            **event,
            "household_id": str(household),
            "resolution": "late_refund",
        }
    refunds_late = [
        late_refunds_by_event[identifier] for identifier in sorted(late_refunds_by_event)
    ]

    # Exclusions are only legitimate when the household's outcome could not
    # have been observed at the cutoff.  A household with no active identity
    # link has no device through which a purchase could be seen, so a zero
    # cannot be distinguished from an unseen purchase.  A household whose
    # only links are ambiguous is the same situation and is also excluded.
    unambiguous_households = {
        household for links in device_links.values()
        if len(links) == 1 for household in links
    }
    ambiguous_households = {
        household
        for households in graph_audit["devices_ambiguous"].values()
        for household in households
    }
    excluded: dict[str, str] = {}
    for household in households:
        if household in unambiguous_households:
            continue
        if household in ambiguous_households:
            excluded[household] = "ambiguous_identity_at_cutoff"
        else:
            excluded[household] = "no_active_identity_at_cutoff"

    return Snapshot(
        households=households,
        arm=arm,
        orders=orders,
        exposures=exposures,
        refunds_late=refunds_late,
        refunds_known=refunds_known,
        identities=identities,
        device_links=device_links,
        audits={
            "identities": identity_audit,
            "events": event_audit,
            "identity_graph": graph_audit,
            "orders": order_audit,
            "observation_window": window_audit,
            "identity_reconciliations": reconciled,
            "late_refunds_without_identity": sorted(late_refunds_without_identity),
        },
        unresolved_events=unresolved,
        excluded=excluded,
    )


# ---------------------------------------------------------------------------
# Estimators
# ---------------------------------------------------------------------------


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the regularised incomplete beta (Numerical Recipes)."""

    max_iterations, epsilon, tiny = 300, 3e-14, 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    h = d
    for iteration in range(1, max_iterations + 1):
        doubled = 2 * iteration
        numerator = iteration * (b - iteration) * x / ((qam + doubled) * (a + doubled))
        d = 1.0 + numerator * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + numerator / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        h *= d * c
        numerator = -(a + iteration) * (qab + iteration) * x / ((a + doubled) * (qap + doubled))
        d = 1.0 + numerator * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + numerator / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < epsilon:
            break
    return h


def _betai(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    term = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return term * _betacf(a, b, x) / a
    return 1.0 - term * _betacf(b, a, 1.0 - x) / b


def student_t_cdf(value: float, degrees_of_freedom: float) -> float:
    if degrees_of_freedom <= 0:
        raise DomainError("degrees of freedom must be positive")
    x = degrees_of_freedom / (degrees_of_freedom + value * value)
    tail = 0.5 * _betai(degrees_of_freedom / 2.0, 0.5, x)
    return 1.0 - tail if value > 0 else tail


def student_t_quantile(probability: float, degrees_of_freedom: float) -> float:
    low, high = -1e3, 1e3
    for _ in range(200):
        middle = (low + high) / 2.0
        if student_t_cdf(middle, degrees_of_freedom) < probability:
            low = middle
        else:
            high = middle
    return (low + high) / 2.0


def welch_difference(
    treatment: Sequence[float], control: Sequence[float], level: float = INTERVAL_LEVEL
) -> dict[str, Any]:
    """Welch difference of means with a t interval and a seeded permutation p."""

    n_treatment, n_control = len(treatment), len(control)
    if n_treatment < MIN_ARM_HOUSEHOLDS or n_control < MIN_ARM_HOUSEHOLDS:
        return {
            "estimate": None,
            "interval": None,
            "degrees_of_freedom": None,
            "standard_error": None,
            "p_value": None,
            "insufficient_reason": "fewer_than_two_households_in_an_arm",
        }
    mean_treatment = statistics.fmean(treatment)
    mean_control = statistics.fmean(control)
    estimate = mean_treatment - mean_control
    variance_treatment = statistics.variance(treatment)
    variance_control = statistics.variance(control)
    term_treatment = variance_treatment / n_treatment
    term_control = variance_control / n_control
    standard_error = math.sqrt(term_treatment + term_control)
    if not math.isfinite(standard_error) or standard_error <= 0.0:
        return {
            "estimate": round(estimate, 6),
            "interval": None,
            "degrees_of_freedom": None,
            "standard_error": 0.0,
            "p_value": None,
            "insufficient_reason": "no_variation_within_arms",
        }
    denominator = 0.0
    if n_treatment > 1:
        denominator += term_treatment ** 2 / (n_treatment - 1)
    if n_control > 1:
        denominator += term_control ** 2 / (n_control - 1)
    if denominator <= 0.0:
        return {
            "estimate": round(estimate, 6),
            "interval": None,
            "degrees_of_freedom": None,
            "standard_error": round(standard_error, 9),
            "p_value": None,
            "insufficient_reason": "undefined_degrees_of_freedom",
        }
    degrees_of_freedom = (term_treatment + term_control) ** 2 / denominator
    quantile = student_t_quantile(1.0 - (1.0 - level) / 2.0, degrees_of_freedom)
    interval = (estimate - quantile * standard_error, estimate + quantile * standard_error)
    return {
        "estimate": round(estimate, 6),
        "interval": [round(interval[0], 6), round(interval[1], 6)],
        "degrees_of_freedom": round(degrees_of_freedom, 6),
        "standard_error": round(standard_error, 6),
        "p_value": None,
        "insufficient_reason": None,
    }


def wilson_interval(successes: int, total: int, z: float) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 1.0
    proportion = successes / total
    denominator = 1.0 + z * z / total
    centre = (proportion + z * z / (2.0 * total)) / denominator
    half = z * math.sqrt(proportion * (1.0 - proportion) / total + z * z / (4.0 * total * total)) / denominator
    return max(0.0, centre - half), min(1.0, centre + half)


def newcombe_difference(
    treatment: Sequence[float], control: Sequence[float], level: float = INTERVAL_LEVEL
) -> dict[str, Any]:
    """Newcombe hybrid score interval for a difference in proportions.

    Unlike Welch on 0/1 values, this stays usable when an arm is all-zero or
    all-one, which happens when many assigned households never purchase.
    """

    n_treatment, n_control = len(treatment), len(control)
    if n_treatment < MIN_ARM_HOUSEHOLDS or n_control < MIN_ARM_HOUSEHOLDS:
        return {
            "estimate": None,
            "interval": None,
            "insufficient_reason": "fewer_than_two_households_in_an_arm",
            "standard_error": None,
            "degrees_of_freedom": None,
            "p_value": None,
        }
    z = student_t_quantile(1.0 - (1.0 - level) / 2.0, 1e7)
    successes_treatment = sum(1 for value in treatment if value > 0.5)
    successes_control = sum(1 for value in control if value > 0.5)
    p_treatment = successes_treatment / n_treatment
    p_control = successes_control / n_control
    estimate = p_treatment - p_control
    low_treatment, high_treatment = wilson_interval(successes_treatment, n_treatment, z)
    low_control, high_control = wilson_interval(successes_control, n_control, z)
    lower = estimate - math.sqrt(
        (p_treatment - low_treatment) ** 2 + (high_control - p_control) ** 2
    )
    upper = estimate + math.sqrt(
        (high_treatment - p_treatment) ** 2 + (p_control - low_control) ** 2
    )
    return {
        "estimate": round(estimate, 6),
        "interval": [round(max(-1.0, lower), 6), round(min(1.0, upper), 6)],
        "standard_error": None,
        "degrees_of_freedom": None,
        "insufficient_reason": None,
    }


def permutation_p_value(
    treatment: Sequence[float],
    control: Sequence[float],
    seed: int,
    metric: str,
    draws: int = PERMUTATION_DRAWS,
) -> tuple[float | None, str]:
    """Two-sided permutation p for a difference in means, seeded and order-free."""

    pooled = list(treatment) + list(control)
    n_treatment = len(treatment)
    observed = statistics.fmean(treatment) - statistics.fmean(control)
    if len(pooled) < 2 or len(set(pooled)) < 2:
        return None, "no_variation"
    generator = random.Random(f"{seed}:{metric}:{draws}")
    exceeded = 0
    for _ in range(draws):
        shuffled = pooled[:]
        generator.shuffle(shuffled)
        simulated = statistics.fmean(shuffled[:n_treatment]) - statistics.fmean(shuffled[n_treatment:])
        if abs(simulated) >= abs(observed) - 1e-12:
            exceeded += 1
    return round((exceeded + 1) / (draws + 1), 6), "seeded_permutation"


def _status(
    estimate: Mapping[str, Any],
    excluded_share: float,
    unobserved_share: float,
) -> tuple[str, str | None]:
    if estimate.get("insufficient_reason"):
        return "insufficient_evidence", str(estimate["insufficient_reason"])
    if excluded_share >= 0.20:
        return "insufficient_evidence", "excluded_share_at_or_above_20_percent"
    if unobserved_share >= 0.05:
        return "insufficient_evidence", "outcome_partially_unobserved"
    low, high = estimate["interval"]
    if low <= 0.0 <= high:
        return "inconclusive", "interval_includes_zero"
    return "supported", "interval_excludes_zero"


# ---------------------------------------------------------------------------
# Analysis assembly
# ---------------------------------------------------------------------------


def _household_totals(
    households: Iterable[str],
    orders: Sequence[Mapping[str, Any]],
    refunds: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, float], dict[str, float], dict[str, int], list[str], list[str]]:
    gross = {household: 0.0 for household in households}
    refund = {household: 0.0 for household in households}
    order_houses = {household: set() for household in households}
    orderless_refunds: list[str] = []
    over_refunds: list[str] = []
    visible_orders = {str(order["order_id"]): order for order in orders}
    for order in orders:
        household = str(order["household_id"])
        if household not in gross:
            continue
        gross[household] += float(order["amount"])
        order_houses[household].add(str(order["order_id"]))
    for item in refunds:
        order_id = str(item["order_id"])
        order = visible_orders.get(order_id)
        if order is None:
            orderless_refunds.append(order_id)
            continue
        household = str(order["household_id"])
        if household not in refund:
            continue
        refund[household] += float(item["amount"])
        if refund[household] > gross[household] + 1e-9:
            over_refunds.append(order_id)
    net = {household: gross[household] - refund[household] for household in gross}
    return gross, net, {h: len(ids) for h, ids in order_houses.items()}, sorted(set(orderless_refunds)), sorted(set(over_refunds))


def _metric_entry(
    metric: str,
    treatment: Sequence[float],
    control: Sequence[float],
    unit: str,
    seed: str,
    role: str,
    population: str,
    excluded_share: float,
    unobserved_share: float,
    note: str,
) -> dict[str, Any]:
    if metric == "converted":
        result = newcombe_difference(treatment, control)
        result["interval_method"] = "newcombe_hybrid_score"
    else:
        result = welch_difference(treatment, control)
        result["interval_method"] = "welch_t"
    if result.get("insufficient_reason") is None:
        p_value, p_method = permutation_p_value(treatment, control, hash_seed(seed), metric)
        result["p_value"] = p_value
        result["p_value_method"] = p_method
    status, rationale = _status(result, excluded_share, unobserved_share)
    return {
        "metric": metric,
        "estimate": result.get("estimate"),
        "interval": result.get("interval"),
        "status": status,
        "status_reason": rationale,
        "unit": unit,
        "role": role,
        "population": population,
        "contrast": "treatment_minus_control",
        "method": METHOD,
        "interval_level": INTERVAL_LEVEL,
        "standard_error": result.get("standard_error"),
        "degrees_of_freedom": result.get("degrees_of_freedom"),
        "p_value": result.get("p_value"),
        "p_value_method": result.get("p_value_method"),
        "interval_method": result.get("interval_method"),
        "sample_size": {"treatment": len(treatment), "control": len(control)},
        "note": note,
    }


def hash_seed(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _check(key: str, status: str, observed: Any, expected: Any, message: str) -> dict[str, Any]:
    return {
        "key": key,
        "status": status,
        "observed": observed,
        "expected": expected,
        "message": message,
    }


def _device_level_contrast(snapshot: Snapshot) -> dict[str, Any]:
    """Diagnostic only: the naive per-device contrast that must not be used.

    It is computed to show how much the same data moves when device rows are
    treated as independent observations, which is the error the primary
    analysis avoids.
    """

    totals: dict[str, list[float]] = {arm: [] for arm in VALID_ARMS}
    for order in snapshot.orders:
        household = str(order["household_id"])
        totals[snapshot.arm[household]].append(float(order["amount"]))
    if not totals["treatment"] or not totals["control"]:
        return {"available": False, "reason": "no_orders_in_an_arm"}
    treatment_mean = statistics.fmean(totals["treatment"])
    control_mean = statistics.fmean(totals["control"])
    pooled = totals["treatment"] + totals["control"]
    variance = statistics.variance(pooled) if len(pooled) > 1 else 0.0
    standard_error = math.sqrt(variance * (1 / len(totals["treatment"]) + 1 / len(totals["control"]))) if variance else 0.0
    return {
        "available": True,
        "unit": "device_order",
        "estimate": round(treatment_mean - control_mean, 6),
        "standard_error_naive": round(standard_error, 6),
        "order_rows": {"treatment": len(totals["treatment"]), "control": len(totals["control"])},
        "warning": "treats repeat orders from the same household as independent observations",
    }


def analyse(document: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = build_snapshot(document)
    seed = str(document["seed"])
    arm_sizes = Counter(snapshot.arm.values())

    # Contamination is behaviour that does not follow the randomised arm:
    # a household exposed to the other arm's creative, or an identity that
    # cannot be pinned to one household at the cutoff.  ITT keeps these
    # households in the denominator; they are reported, never dropped.
    contaminated: dict[str, list[str]] = defaultdict(list)
    for exposure in snapshot.exposures:
        if exposure.get("cross_arm_exposure"):
            contaminated["cross_arm_exposure"].append(str(exposure["household_id"]))
    for device, households in snapshot.audits["identity_graph"]["devices_ambiguous"].items():
        for household in households:
            contaminated["ambiguous_device_identity"].append(str(household))
    for household in contaminated:
        contaminated[household] = sorted(set(contaminated[household]))
    contaminated_households = sorted({h for items in contaminated.values() for h in items})

    excluded = dict(snapshot.excluded)

    analyzed = [h for h in snapshot.households if h not in excluded]
    analyzed_by_arm = {
        arm: sorted(h for h in analyzed if snapshot.arm[h] == arm) for arm in VALID_ARMS
    }
    excluded_share = 0.0 if not snapshot.households else len(excluded) / len(snapshot.households)

    gross, net, order_counts, orderless_refunds, over_refunds = _household_totals(
        analyzed, snapshot.orders, snapshot.refunds_known
    )
    converted = {h: 1.0 if order_counts.get(h, 0) > 0 else 0.0 for h in analyzed}

    # Orders that could not be tied to a household are a data-quality limit,
    # not a reason to delete the household.  Their value is measured; when it
    # is a material share of gross the effect is downgraded rather than
    # presented as if the outcome were fully observed.
    unresolved_orders = [
        event for event in snapshot.unresolved_events if event.get("type") == "order"
    ]
    unresolved_order_value = round(
        sum(float(event.get("amount") or 0.0) for event in unresolved_orders), 6
    )
    total_gross = sum(gross.values())
    unobserved_share = 0.0 if total_gross <= 0 else unresolved_order_value / (total_gross + unresolved_order_value)

    primary_values = {
        "net_revenue": {h: net[h] for h in analyzed},
        "converted": converted,
    }
    effects: list[dict[str, Any]] = []
    for metric in PRIMARY_METRICS:
        values = primary_values[metric]
        treatment = [values[h] for h in analyzed_by_arm["treatment"]]
        control = [values[h] for h in analyzed_by_arm["control"]]
        effects.append(
            _metric_entry(
                metric=metric,
                treatment=treatment,
                control=control,
                unit="currency_per_household" if metric == "net_revenue" else "share_of_households",
                seed=seed,
                role="primary",
                population="assigned",
                excluded_share=excluded_share,
                unobserved_share=unobserved_share,
                note=(
                    "Intent-to-treat on the original household assignment; unexposed households "
                    "remain in the denominator. Refunds known after as_of are excluded."
                ),
            )
        )

    # Secondary description only: exposed treatment households against
    # assigned control.  Exposure is sparse and not randomised again, so this
    # cannot replace the primary result.
    exposed_households = sorted(
        {
            str(exposure["household_id"])
            for exposure in snapshot.exposures
            if snapshot.arm[str(exposure["household_id"])] == "treatment"
            and not exposure.get("cross_arm_exposure")
            and str(exposure["household_id"]) in analyzed
        }
    )
    secondary_effects: list[dict[str, Any]] = []
    secondary_reason = None
    if len(exposed_households) < MIN_ARM_HOUSEHOLDS:
        secondary_reason = "fewer_than_two_exposed_households"
    else:
        exposed_values = {
            "net_revenue": [net[h] for h in exposed_households],
            "converted": [converted[h] for h in exposed_households],
        }
        control_values = {
            "net_revenue": [net[h] for h in analyzed_by_arm["control"]],
            "converted": [converted[h] for h in analyzed_by_arm["control"]],
        }
        for metric in PRIMARY_METRICS:
            entry = _metric_entry(
                metric=metric,
                treatment=exposed_values[metric],
                control=control_values[metric],
                unit="currency_per_household" if metric == "net_revenue" else "share_of_households",
                seed=seed + ":exposed",
                role="secondary",
                population="exposed_treatment",
                excluded_share=excluded_share,
                unobserved_share=unobserved_share,
                note=(
                    "Descriptive only. Exposure is not a fresh randomisation; selection into "
                    "exposure can differ from the assigned population."
                ),
            )
            secondary_effects.append(entry)

    # Sensitivity: what the net revenue contrast would look like if late
    # refunds were allowed to travel back in time.  Shown for information only.
    late_refund_total = round(sum(float(item["amount"]) for item in snapshot.refunds_late), 6)
    late_by_household: dict[str, float] = defaultdict(float)
    visible_orders = {str(order["order_id"]): order for order in snapshot.orders}
    late_without_order = 0
    for item in snapshot.refunds_late:
        order = visible_orders.get(str(item["order_id"]))
        if order is None:
            late_without_order += 1
            continue
        late_by_household[str(order["household_id"])] += float(item["amount"])
    sensitivity_effect = None
    if late_refund_total > 0:
        adjusted = {h: net[h] - late_by_household.get(h, 0.0) for h in analyzed}
        treatment = [adjusted[h] for h in analyzed_by_arm["treatment"]]
        control = [adjusted[h] for h in analyzed_by_arm["control"]]
        sensitivity_effect = _metric_entry(
            metric="net_revenue",
            treatment=treatment,
            control=control,
            unit="currency_per_household",
            seed=seed + ":late-refund",
            role="sensitivity",
            population="assigned",
            excluded_share=excluded_share,
            unobserved_share=unobserved_share,
            note="Counterfactual sensitivity that applies refunds known after as_of; not the primary result.",
        )

    # Population accounting.
    population = {
        "assigned": {
            "total": len(snapshot.households),
            "by_arm": {arm: arm_sizes.get(arm, 0) for arm in VALID_ARMS},
            "unit": "household",
        },
        "excluded": {
            "total": len(excluded),
            "share": round(excluded_share, 6),
            "by_reason": dict(sorted(Counter(excluded.values()).items())),
            "households": sorted(excluded),
            "policy": "Only households whose outcome cannot be observed at the cutoff are excluded.",
        },
        "contaminated": {
            "total": len(contaminated_households),
            "share": round(0.0 if not snapshot.households else len(contaminated_households) / len(snapshot.households), 6),
            "by_reason": {reason: len(items) for reason, items in sorted(contaminated.items())},
            "households": contaminated_households,
            "policy": "Contaminated households stay in the ITT denominator; they are reported, not dropped.",
        },
        "effective_sample": {
            "total": len(analyzed),
            "by_arm": {arm: len(analyzed_by_arm[arm]) for arm in VALID_ARMS},
            "unit": "household",
        },
        "unresolved_events": {
            "total": len(snapshot.unresolved_events),
            "by_reason": dict(sorted(Counter(item["resolution"] for item in snapshot.unresolved_events).items())),
        },
    }

    market_scope: dict[str, set[str]] = defaultdict(set)
    for order in snapshot.orders:
        market_scope[str(order["household_id"])].update(order.get("markets") or [])
    cross_market_households = sorted(
        household for household, markets in market_scope.items() if len(markets) > 1
    )
    exposure_counts = Counter(
        ("cross_arm" if item.get("cross_arm_exposure") else "same_arm")
        for item in snapshot.exposures
    )

    checks = [
        _check(
            "assignment_integrity",
            "passed",
            {"households": len(snapshot.households), "by_arm": dict(sorted(arm_sizes.items()))},
            "each household registered exactly once with a valid arm",
            "The original randomisation is taken from the registry, not from observed behaviour.",
        ),
        _check(
            "as_of_snapshot",
            "passed",
            {
                "as_of": document["as_of"],
                "identities_ignored_late": len(snapshot.audits["identities"]["objects_ignored_late"]),
                "events_ignored_late": len(snapshot.audits["events"]["objects_ignored_late"]),
                "objects_with_later_revision_after_cutoff": sorted(
                    set(snapshot.audits["identities"]["objects_with_later_revision_after_cutoff"])
                    | set(snapshot.audits["events"]["objects_with_later_revision_after_cutoff"])
                ),
            },
            "no post-cutoff information changes the historical snapshot",
            "Revisions and identities known after as_of are not applied retroactively.",
        ),
        _check(
            "duplicate_observations",
            "passed" if snapshot.audits["orders"]["device_duplicate_observations_collapsed"] == 0 else "collapsed",
            snapshot.audits["orders"],
            "each order_id contributes once per household",
            "Repeated device observations of the same order are collapsed to one transaction.",
        ),
        _check(
            "cross_market_repeat_purchase",
            "noted" if cross_market_households else "passed",
            {"households": len(cross_market_households)},
            "household-level analysis absorbs repeat purchases across markets",
            "Cross-market repeat orders are summed within the household, not counted as new people.",
        ),
        _check(
            "identity_reconciliation",
            "noted" if snapshot.audits["identity_reconciliations"] else "passed",
            {
                "events": len(snapshot.audits["identity_reconciliations"]),
                "by_type": dict(sorted(Counter(
                    item["type"] for item in snapshot.audits["identity_reconciliations"]
                ).items())),
                "examples": snapshot.audits["identity_reconciliations"][:5],
            },
            "events resolve to the household linked at the event time",
            "A corrected link known at the cutoff overrides a stale declared household and is reported.",
        ),
        _check(
            "identity_revocation",
            "noted" if snapshot.audits["identity_graph"]["devices_revoked_at_cutoff"] else "passed",
            {
                "devices_revoked_at_cutoff": len(snapshot.audits["identity_graph"]["devices_revoked_at_cutoff"]),
                "devices_ambiguous": len(snapshot.audits["identity_graph"]["devices_ambiguous"]),
            },
            "revoked links do not support identity at the cutoff",
            "Revoked links are withheld; unresolved devices are not guessed.",
        ),
        _check(
            "observation_window",
            "noted" if snapshot.audits["observation_window"]["events_out_of_window"] else "passed",
            snapshot.audits["observation_window"],
            "outcomes are measured between observation_start and observation_end",
            "Events outside the window do not contribute to the effect.",
        ),
        _check(
            "late_refunds",
            "noted" if snapshot.refunds_late else "passed",
            {
                "count": len(snapshot.refunds_late),
                "amount": late_refund_total,
                "share_of_gross": round(
                    late_refund_total / max(sum(float(o['amount']) for o in snapshot.orders), 1e-9), 6
                ),
                "without_visible_order": late_without_order,
            },
            "refunds known after as_of are excluded from the primary net revenue",
            "Late refunds are quantified and shown as a sensitivity, not silently applied or dropped.",
        ),
        _check(
            "refunds_applied",
            "passed" if not orderless_refunds else "review",
            {
                "refunds_known_at_cutoff": len(snapshot.refunds_known),
                "without_visible_order": orderless_refunds,
                "exceeding_gross": over_refunds,
            },
            "every applied refund matches a visible order",
            "A refund without a visible order cannot be allocated to a household and is reported.",
        ),
        _check(
            "unresolved_events",
            "review" if unresolved_orders else "passed",
            {
                "events": len(snapshot.unresolved_events),
                "orders": len(unresolved_orders),
                "unresolved_order_value": unresolved_order_value,
                "share_of_gross": round(unobserved_share, 6),
                "by_reason": population["unresolved_events"]["by_reason"],
            },
            "unresolved orders are below 5% of gross",
            "Orders without a cutoff identity are quantified; a material share downgrades the effect.",
        ),
        _check(
            "contamination_share",
            "noted" if contaminated_households else "passed",
            population["contaminated"],
            "contamination is measured and retained in the ITT denominator",
            "Cross-arm exposure and ambiguous identities dilute ITT but are not removed.",
        ),
        _check(
            "excluded_share",
            "review" if excluded_share >= 0.20 else ("noted" if excluded else "passed"),
            {"share": round(excluded_share, 6), "by_reason": population["excluded"]["by_reason"]},
            "under 20% of assigned households excluded",
            "Exclusions above the threshold make the estimate insufficient rather than precise-looking.",
        ),
        _check(
            "small_cells",
            "review"
            if min(len(analyzed_by_arm["treatment"]), len(analyzed_by_arm["control"])) < SMALL_CELL_THRESHOLD
            else "passed",
            {"by_arm": population["effective_sample"]["by_arm"], "threshold": SMALL_CELL_THRESHOLD},
            "at least ten households per arm",
            "Small arms widen the interval and are reported rather than hidden.",
        ),
        _check(
            "device_level_contrast",
            "diagnostic_mismatch" if _device_contrast_mismatch(snapshot, effects) else "diagnostic_consistent",
            _device_level_contrast(snapshot),
            "device-level rows are not independent observations",
            "Shown to expose the misleading per-device contrast that the primary analysis replaces.",
        ),
        _check(
            "deterministic_replay",
            "passed",
            {"seed": document["seed"], "canonical_order": "sorted by stable identifiers"},
            "equivalent input in a different order yields the same result",
            "Assignments, identities and events are canonically ordered before any computation.",
        ),
    ]

    sources = {
        "input_fingerprint": canonical_fingerprint(document),
        "experiment_id": document["experiment_id"],
        "as_of": document["as_of"],
        "observation_window": {
            "start": document["observation_start"],
            "end": document["observation_end"],
        },
        "seed": document["seed"],
        "method": METHOD,
        "interval_level": INTERVAL_LEVEL,
        "permutation_draws": PERMUTATION_DRAWS,
        "input_counts": dict(document["input_counts"]),
        "visible_counts": {
            "assignments": len(snapshot.households),
            "identities": len(snapshot.identities),
            "events_visible_at_cutoff": (
                len(snapshot.orders) + len(snapshot.exposures) + len(snapshot.refunds_known)
            ),
            "orders": len(snapshot.orders),
            "exposures": dict(sorted(exposure_counts.items())),
            "refunds_known_at_cutoff": len(snapshot.refunds_known),
            "refunds_known_after_cutoff": len(snapshot.refunds_late),
        },
        "audits": snapshot.audits,
        "data_sources": [
            "assignments",
            "identities",
            "events.exposure",
            "events.order",
            "events.refund",
        ],
    }

    return {
        "analysis_id": None,  # assigned by persistence
        "experiment_id": document["experiment_id"],
        "as_of": document["as_of"],
        "population": population,
        "effects": effects,
        "secondary": {
            "population": "exposed_treatment",
            "exposed_households": exposed_households,
            "status_reason": secondary_reason,
            "effects": secondary_effects,
            "note": "Secondary description only; it cannot replace the primary intent-to-treat result.",
        },
        "sensitivity": {
            "late_refund": sensitivity_effect,
            "note": "Counterfactual only. The primary result does not let late refunds change history.",
        },
        "checks": checks,
        "sources": sources,
    }


def _device_contrast_mismatch(snapshot: Snapshot, effects: Sequence[Mapping[str, Any]]) -> bool:
    device = _device_level_contrast(snapshot)
    if not device.get("available"):
        return False
    primary = next((item for item in effects if item["metric"] == "net_revenue"), None)
    if primary is None or primary.get("estimate") is None:
        return False
    primary_estimate = float(primary["estimate"])
    device_estimate = float(device["estimate"])
    if primary_estimate == 0 or device_estimate == 0:
        return False
    return (primary_estimate > 0) != (device_estimate > 0) or abs(device_estimate) > 2 * abs(primary_estimate)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class ExperimentAnalysisStore:
    """Append-only store for experiment analyses in the existing SQLite database."""

    def __init__(self, database: str | Path):
        self.ledger = SQLiteLedger(database)
        self._lock = threading.RLock()
        with self.ledger.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS experiment_analyses(
                    analysis_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL,
                    input_fingerprint TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    parent_analysis_id TEXT,
                    methodology TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(experiment_id, input_fingerprint)
                );
                CREATE INDEX IF NOT EXISTS experiment_analyses_experiment
                    ON experiment_analyses(experiment_id, version);
                """
            )

    def persist(self, document: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
        fingerprint = canonical_fingerprint(document)
        with self._lock, self.ledger.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT analysis_id, version, payload FROM experiment_analyses "
                "WHERE experiment_id=? AND input_fingerprint=?",
                (document["experiment_id"], fingerprint),
            ).fetchone()
            if existing is not None:
                stored = json.loads(existing["payload"])
                stored["persistence"] = {
                    "stored": True,
                    "replayed": True,
                    "version": int(existing["version"]),
                    "path": self.ledger.database,
                }
                return stored
            version = int(
                connection.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM experiment_analyses WHERE experiment_id=?",
                    (document["experiment_id"],),
                ).fetchone()[0]
            ) + 1
            parent = connection.execute(
                "SELECT analysis_id FROM experiment_analyses WHERE experiment_id=? "
                "ORDER BY version DESC LIMIT 1",
                (document["experiment_id"],),
            ).fetchone()
            analysis_id = "exp-" + hashlib.sha256(
                f"{document['experiment_id']}:{fingerprint}:{version}".encode("utf-8")
            ).hexdigest()[:20]
            stored = dict(result)
            stored["analysis_id"] = analysis_id
            stored["version"] = version
            stored["parent_analysis_id"] = parent["analysis_id"] if parent else None
            stored["persistence"] = {
                "stored": True,
                "replayed": False,
                "version": version,
                "path": self.ledger.database,
            }
            connection.execute(
                "INSERT INTO experiment_analyses(analysis_id,experiment_id,input_fingerprint,as_of,"
                "version,parent_analysis_id,methodology,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    analysis_id,
                    document["experiment_id"],
                    fingerprint,
                    document["as_of"],
                    version,
                    stored["parent_analysis_id"],
                    METHOD,
                    stable_json(stored),
                    utc_now(),
                ),
            )
            return stored

    def history(self, experiment_id: str) -> list[dict[str, Any]]:
        with self.ledger.connect() as connection:
            rows = connection.execute(
                "SELECT analysis_id, version, input_fingerprint, as_of, parent_analysis_id, created_at "
                "FROM experiment_analyses WHERE experiment_id=? ORDER BY version",
                (experiment_id,),
            ).fetchall()
        return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Public entry point and command line
# ---------------------------------------------------------------------------


def run_analysis(input_path: str | Path, database: str | Path) -> dict[str, Any]:
    path = Path(input_path)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_duplicate_pairs_hook)
    except json.JSONDecodeError as exc:
        raise DomainError(f"input is not valid JSON: {exc}") from exc
    document = validate_input(raw)
    result = analyse(document)
    return ExperimentAnalysisStore(database).persist(document, result)


def _write_atomic(path: str | Path, payload: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=str(destination.parent), suffix=".tmp")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
        os.replace(temporary, destination)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _public_payload(result: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "analysis_id", "experiment_id", "as_of", "version", "parent_analysis_id",
        "population", "effects", "secondary", "sensitivity", "checks", "sources",
        "persistence",
    )
    return {key: result[key] for key in keys if key in result}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="按实验约定截止点重建身份与交易，并输出家庭层面的联营券效果估计"
    )
    parser.add_argument("--input", required=True, help="experiment input JSON")
    parser.add_argument("--database", required=True, help="SQLite database shared with the business workflow")
    parser.add_argument("--output", required=True, help="write the analysis report here")
    arguments = parser.parse_args(argv)

    try:
        result = run_analysis(arguments.input, arguments.database)
    except (DomainError, ConflictError, OSError) as error:
        # A conflicting or incomplete input must fail visibly and must not
        # leave behind a report that looks successful.
        try:
            SQLiteLedger(arguments.database).fail(
                "experiment-analysis", "experiment_analysis", str(error),
                {"input": str(arguments.input)},
            )
        except Exception:
            pass
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 2

    _write_atomic(arguments.output, _public_payload(result))
    print(json.dumps(_public_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
