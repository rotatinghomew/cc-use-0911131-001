from __future__ import annotations
import hashlib
import json
import math
import sqlite3
import statistics
import threading
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

class DomainError(ValueError):
    pass

class ConflictError(DomainError):
    pass

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def fingerprint(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()

def get_path(value: Mapping[str, Any], path: str, default: Any = None) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return default
        current = current[part]
    return current

def number(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default

def sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]

@dataclass(slots=True)
class Finding:
    rule: str
    stage: str
    severity: str
    message: str
    observed: Any
    expected: Any

@dataclass(slots=True)
class RunState:
    run_id: str
    case_id: str
    payload: dict[str, Any]
    state: str = "received"
    revision: int = 0
    findings: list[Finding] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)

    def emit(self, stage: str, kind: str, **detail: Any) -> None:
        self.events.append({"event_id": uuid.uuid4().hex, "stage": stage, "kind": kind, "detail": detail, "at": utc_now()})

    def add_finding(self, policy: Mapping[str, Any], observed: Any, expected: Any) -> None:
        self.findings.append(Finding(str(policy["key"]), str(policy.get("stage", self.state)), str(policy["severity"]), str(policy["message"]), observed, expected))

    def set_metric(self, key: str, value: Any) -> None:
        self.metrics[key] = round(number(value), 6)

    def advance(self, stage: str) -> None:
        if not stage or stage == self.state:
            raise DomainError("invalid state transition")
        before = self.state
        self.state = stage
        self.revision += 1
        self.emit(stage, "state_advanced", before=before, after=stage, revision=self.revision)

def evaluate_policy(policy: Mapping[str, Any], payload: Mapping[str, Any], state: RunState) -> dict[str, Any]:
    source = str(policy["source"])
    value = get_path(payload, source)
    op = str(policy["op"])
    threshold = policy.get("threshold", 0)
    failed = False
    expected: Any = threshold
    if value is None or value == "":
        failed = bool(policy.get("required")) or op == "required"
        expected = "present"
    elif op == "min":
        failed = number(value) < number(threshold)
        expected = {"minimum": threshold}
    elif op == "max":
        failed = number(value) > number(threshold)
        expected = {"maximum": threshold}
    elif op == "enum":
        allowed = list(policy.get("allowed", []))
        failed = str(value) not in allowed
        expected = allowed
    elif op == "unique":
        items = [stable_json(item) for item in sequence(value)]
        failed = len(items) != len(set(items))
        expected = "no duplicates"
    elif op == "trend":
        values = [number(item) for item in sequence(value)]
        failed = len(values) > 2 and values[-1] < values[0] * 0.7
        expected = "end >= 70% of start"
    elif op == "ratio":
        denominator = max(number(get_path(payload, "total"), 1.0), 1.0)
        ratio = number(value) / denominator
        failed = ratio > max(number(threshold) / 100.0, 1.0)
        expected = {"ratio_ceiling": max(number(threshold) / 100.0, 1.0)}
        state.set_metric(str(policy["key"]) + "_ratio", ratio)
    elif op == "score":
        failed = number(value) < number(threshold)
        expected = {"score_floor": threshold}
    elif op == "contains":
        token = str(policy.get("token", "")).casefold()
        failed = token not in stable_json(value).casefold()
        expected = {"contains": token}
    elif op == "freshness":
        failed = number(get_path(payload, "metrics.delay_hours")) > max(number(threshold), 24.0)
        expected = {"hours": max(number(threshold), 24.0)}
    elif op == "distribution":
        values = [number(item) for item in sequence(value) if isinstance(item, (int, float))]
        if len(values) > 1:
            spread = statistics.pstdev(values)
            state.set_metric(str(policy["key"]) + "_spread", spread)
            failed = spread > max(number(threshold), 100.0)
        expected = {"spread_ceiling": max(number(threshold), 100.0)}
    elif op == "sla":
        hours = number(get_path(payload, "metrics.delay_hours"))
        failed = hours > max(number(threshold), 3.0)
        expected = {"hours": max(number(threshold), 3.0)}
    if failed:
        state.add_finding(policy, value, expected)
    return {"rule": policy["key"], "failed": failed, "observed": value, "expected": expected}

class SQLiteLedger:
    def __init__(self, database: str | Path):
        self.database = str(database)
        self._lock = threading.RLock()
        self._initialize()

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.database, timeout=20)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY, case_id TEXT NOT NULL, state TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT NOT NULL, findings TEXT NOT NULL, metrics TEXT NOT NULL, artifacts TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS runs_case_id ON runs(case_id);
                CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, stage TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS idempotency(request_key TEXT PRIMARY KEY, run_id TEXT NOT NULL, response TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS failures(failure_id TEXT PRIMARY KEY, case_id TEXT, stage TEXT NOT NULL, error TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            """)

    def save(self, state: RunState) -> None:
        with self._lock, self.connect() as connection:
            row = connection.execute("SELECT revision FROM runs WHERE run_id=?", (state.run_id,)).fetchone()
            if row is not None and int(row["revision"]) > state.revision:
                raise ConflictError("stored revision is newer")
            connection.execute("INSERT INTO runs(run_id,case_id,state,revision,payload,findings,metrics,artifacts,updated_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET state=excluded.state,revision=excluded.revision,payload=excluded.payload,findings=excluded.findings,metrics=excluded.metrics,artifacts=excluded.artifacts,updated_at=excluded.updated_at", (state.run_id, state.case_id, state.state, state.revision, stable_json(state.payload), stable_json([asdict(item) for item in state.findings]), stable_json(state.metrics), stable_json(state.artifacts), utc_now()))
            for event in state.events:
                connection.execute("INSERT OR IGNORE INTO events(event_id,run_id,stage,kind,detail,created_at) VALUES(?,?,?,?,?,?)", (event["event_id"], state.run_id, event["stage"], event["kind"], stable_json(event["detail"]), event["at"]))

    def remember(self, request_key: str, run_id: str, response: Mapping[str, Any]) -> None:
        with self.connect() as connection:
            connection.execute("INSERT OR IGNORE INTO idempotency(request_key,run_id,response,created_at) VALUES(?,?,?,?)", (request_key, run_id, stable_json(response), utc_now()))

    def recalled(self, request_key: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT response FROM idempotency WHERE request_key=?", (request_key,)).fetchone()
        return json.loads(row["response"]) if row else None

    def fail(self, case_id: str, stage: str, error: str, payload: Mapping[str, Any]) -> None:
        with self.connect() as connection:
            connection.execute("INSERT INTO failures VALUES(?,?,?,?,?,?)", (uuid.uuid4().hex, case_id, stage, error, stable_json(payload), utc_now()))

    def stats(self) -> dict[str, int]:
        with self.connect() as connection:
            return {name: int(connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in ("runs", "events", "idempotency", "failures")}
