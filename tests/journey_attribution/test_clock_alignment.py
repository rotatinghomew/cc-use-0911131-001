from __future__ import annotations
import copy
import tempfile
import unittest
from pathlib import Path
from twin_market_flow.service import ApplicationService

BASE = {
    "case_id": "base-case", "merchant_id": "merchant-001", "market": "combined", "region": "haining",
    "status": "active", "source_system": "local-fixture", "event_date": "2026-11-12",
    "amount": 128.5, "count": 8, "total": 16,
    "metrics": {"conversion": 0.5, "traffic": 180, "repeat_rate": 0.4, "delay_hours": 2},
    "attributes": {"brand_level": "independent", "risk_score": 78, "channel": "hybrid", "originality": 0.85, "capacity": 24, "cost": 12.0},
    "tags": ["synthetic", "offline", "客流会话"],
    "records": [{"id": "r1", "value": 80, "amount": 20, "status": "open", "group": "leather"}, {"id": "r2", "value": 96, "amount": 25, "status": "closed", "group": "textile"}],
    "timeline": [{"at": "2026-11-10", "start": 1, "end": 3}, {"at": "2026-11-11", "start": 4, "end": 8}],
    "links": [{"from": "leather", "to": "textile", "weight": 3}, {"from": "textile", "to": "buyer", "weight": 2}],
}

CASES = [
    {
        "name": "twin_market_flow-g1-c00",
        "patch": {"status": "new", "market": "leather", "count": 1, "total": 3,
                  "metrics": {"conversion": 0.0, "traffic": 80, "repeat_rate": 0.0, "delay_hours": 1},
                  "attributes": {"brand_level": "unbranded", "risk_score": 13, "channel": "offline", "originality": 0.0, "capacity": 10, "cost": 5.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c01",
        "patch": {"status": "active", "market": "textile", "count": 2, "total": 4,
                  "metrics": {"conversion": 0.1, "traffic": 81, "repeat_rate": 0.1, "delay_hours": 6},
                  "attributes": {"brand_level": "oem", "risk_score": 26, "channel": "online", "originality": 0.1, "capacity": 11, "cost": 5.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c02",
        "patch": {"status": "pending", "market": "combined", "count": 3, "total": 5,
                  "metrics": {"conversion": 0.2, "traffic": 82, "repeat_rate": 0.2, "delay_hours": 11},
                  "attributes": {"brand_level": "independent", "risk_score": 39, "channel": "hybrid", "originality": 0.2, "capacity": 12, "cost": 5.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c03",
        "patch": {"status": "review", "market": "leather", "count": 4, "total": 6,
                  "metrics": {"conversion": 0.3, "traffic": 83, "repeat_rate": 0.3, "delay_hours": 16},
                  "attributes": {"brand_level": "regional", "risk_score": 52, "channel": "offline", "originality": 0.3, "capacity": 13, "cost": 6.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c04",
        "patch": {"status": "closed", "market": "textile", "count": 5, "total": 7,
                  "metrics": {"conversion": 0.4, "traffic": 84, "repeat_rate": 0.4, "delay_hours": 21},
                  "attributes": {"brand_level": "unbranded", "risk_score": 65, "channel": "online", "originality": 0.4, "capacity": 14, "cost": 6.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c05",
        "patch": {"status": "new", "market": "combined", "count": 6, "total": 8,
                  "metrics": {"conversion": 0.5, "traffic": 85, "repeat_rate": 0.5, "delay_hours": 26},
                  "attributes": {"brand_level": "oem", "risk_score": 78, "channel": "hybrid", "originality": 0.5, "capacity": 15, "cost": 7.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c06",
        "patch": {"status": "active", "market": "leather", "count": 7, "total": 9,
                  "metrics": {"conversion": 0.6, "traffic": 86, "repeat_rate": 0.6, "delay_hours": 31},
                  "attributes": {"brand_level": "independent", "risk_score": 91, "channel": "offline", "originality": 0.6, "capacity": 16, "cost": 7.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c07",
        "patch": {"status": "pending", "market": "textile", "count": 8, "total": 10,
                  "metrics": {"conversion": 0.7, "traffic": 87, "repeat_rate": 0.0, "delay_hours": 36},
                  "attributes": {"brand_level": "regional", "risk_score": 3, "channel": "online", "originality": 0.7, "capacity": 17, "cost": 7.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c08",
        "patch": {"status": "review", "market": "combined", "count": 9, "total": 11,
                  "metrics": {"conversion": 0.8, "traffic": 88, "repeat_rate": 0.1, "delay_hours": 41},
                  "attributes": {"brand_level": "unbranded", "risk_score": 16, "channel": "hybrid", "originality": 0.8, "capacity": 18, "cost": 8.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c09",
        "patch": {"status": "closed", "market": "leather", "count": 10, "total": 12,
                  "metrics": {"conversion": 0.9, "traffic": 89, "repeat_rate": 0.2, "delay_hours": 46},
                  "attributes": {"brand_level": "oem", "risk_score": 29, "channel": "offline", "originality": 0.0, "capacity": 19, "cost": 8.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c10",
        "patch": {"status": "new", "market": "textile", "count": 11, "total": 13,
                  "metrics": {"conversion": 0.0, "traffic": 90, "repeat_rate": 0.3, "delay_hours": 51},
                  "attributes": {"brand_level": "independent", "risk_score": 42, "channel": "online", "originality": 0.1, "capacity": 20, "cost": 9.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c11",
        "patch": {"status": "active", "market": "combined", "count": 12, "total": 14,
                  "metrics": {"conversion": 0.1, "traffic": 91, "repeat_rate": 0.4, "delay_hours": 56},
                  "attributes": {"brand_level": "regional", "risk_score": 55, "channel": "hybrid", "originality": 0.2, "capacity": 21, "cost": 9.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c12",
        "patch": {"status": "pending", "market": "leather", "count": 13, "total": 15,
                  "metrics": {"conversion": 0.2, "traffic": 92, "repeat_rate": 0.5, "delay_hours": 61},
                  "attributes": {"brand_level": "unbranded", "risk_score": 68, "channel": "offline", "originality": 0.3, "capacity": 22, "cost": 9.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c13",
        "patch": {"status": "review", "market": "textile", "count": 14, "total": 16,
                  "metrics": {"conversion": 0.3, "traffic": 93, "repeat_rate": 0.6, "delay_hours": 66},
                  "attributes": {"brand_level": "oem", "risk_score": 81, "channel": "online", "originality": 0.4, "capacity": 23, "cost": 10.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c14",
        "patch": {"status": "closed", "market": "combined", "count": 15, "total": 17,
                  "metrics": {"conversion": 0.4, "traffic": 94, "repeat_rate": 0.0, "delay_hours": 71},
                  "attributes": {"brand_level": "independent", "risk_score": 94, "channel": "hybrid", "originality": 0.5, "capacity": 24, "cost": 10.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c15",
        "patch": {"status": "new", "market": "leather", "count": 16, "total": 18,
                  "metrics": {"conversion": 0.5, "traffic": 95, "repeat_rate": 0.1, "delay_hours": 76},
                  "attributes": {"brand_level": "regional", "risk_score": 6, "channel": "offline", "originality": 0.6, "capacity": 25, "cost": 11.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c16",
        "patch": {"status": "active", "market": "textile", "count": 17, "total": 19,
                  "metrics": {"conversion": 0.6, "traffic": 96, "repeat_rate": 0.2, "delay_hours": 81},
                  "attributes": {"brand_level": "unbranded", "risk_score": 19, "channel": "online", "originality": 0.7, "capacity": 26, "cost": 11.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c17",
        "patch": {"status": "pending", "market": "combined", "count": 1, "total": 20,
                  "metrics": {"conversion": 0.7, "traffic": 97, "repeat_rate": 0.3, "delay_hours": 86},
                  "attributes": {"brand_level": "oem", "risk_score": 32, "channel": "hybrid", "originality": 0.8, "capacity": 27, "cost": 11.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c18",
        "patch": {"status": "review", "market": "leather", "count": 2, "total": 21,
                  "metrics": {"conversion": 0.8, "traffic": 98, "repeat_rate": 0.4, "delay_hours": 91},
                  "attributes": {"brand_level": "independent", "risk_score": 45, "channel": "offline", "originality": 0.0, "capacity": 28, "cost": 12.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c19",
        "patch": {"status": "closed", "market": "textile", "count": 3, "total": 3,
                  "metrics": {"conversion": 0.9, "traffic": 99, "repeat_rate": 0.5, "delay_hours": 0},
                  "attributes": {"brand_level": "regional", "risk_score": 58, "channel": "online", "originality": 0.1, "capacity": 29, "cost": 12.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c20",
        "patch": {"status": "new", "market": "combined", "count": 4, "total": 4,
                  "metrics": {"conversion": 0.0, "traffic": 100, "repeat_rate": 0.6, "delay_hours": 5},
                  "attributes": {"brand_level": "unbranded", "risk_score": 71, "channel": "hybrid", "originality": 0.2, "capacity": 10, "cost": 13.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c21",
        "patch": {"status": "active", "market": "leather", "count": 5, "total": 5,
                  "metrics": {"conversion": 0.1, "traffic": 101, "repeat_rate": 0.0, "delay_hours": 10},
                  "attributes": {"brand_level": "oem", "risk_score": 84, "channel": "offline", "originality": 0.3, "capacity": 11, "cost": 13.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c22",
        "patch": {"status": "pending", "market": "textile", "count": 6, "total": 6,
                  "metrics": {"conversion": 0.2, "traffic": 102, "repeat_rate": 0.1, "delay_hours": 15},
                  "attributes": {"brand_level": "independent", "risk_score": 97, "channel": "online", "originality": 0.4, "capacity": 12, "cost": 13.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c23",
        "patch": {"status": "review", "market": "combined", "count": 7, "total": 7,
                  "metrics": {"conversion": 0.3, "traffic": 103, "repeat_rate": 0.2, "delay_hours": 20},
                  "attributes": {"brand_level": "regional", "risk_score": 9, "channel": "hybrid", "originality": 0.5, "capacity": 13, "cost": 14.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c24",
        "patch": {"status": "closed", "market": "leather", "count": 8, "total": 8,
                  "metrics": {"conversion": 0.4, "traffic": 104, "repeat_rate": 0.3, "delay_hours": 25},
                  "attributes": {"brand_level": "unbranded", "risk_score": 22, "channel": "offline", "originality": 0.6, "capacity": 14, "cost": 14.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c25",
        "patch": {"status": "new", "market": "textile", "count": 9, "total": 9,
                  "metrics": {"conversion": 0.5, "traffic": 105, "repeat_rate": 0.4, "delay_hours": 30},
                  "attributes": {"brand_level": "oem", "risk_score": 35, "channel": "online", "originality": 0.7, "capacity": 15, "cost": 15.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c26",
        "patch": {"status": "active", "market": "combined", "count": 10, "total": 10,
                  "metrics": {"conversion": 0.6, "traffic": 106, "repeat_rate": 0.5, "delay_hours": 35},
                  "attributes": {"brand_level": "independent", "risk_score": 48, "channel": "hybrid", "originality": 0.8, "capacity": 16, "cost": 15.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c27",
        "patch": {"status": "pending", "market": "leather", "count": 11, "total": 11,
                  "metrics": {"conversion": 0.7, "traffic": 107, "repeat_rate": 0.6, "delay_hours": 40},
                  "attributes": {"brand_level": "regional", "risk_score": 61, "channel": "offline", "originality": 0.0, "capacity": 17, "cost": 15.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c28",
        "patch": {"status": "review", "market": "textile", "count": 12, "total": 12,
                  "metrics": {"conversion": 0.8, "traffic": 108, "repeat_rate": 0.0, "delay_hours": 45},
                  "attributes": {"brand_level": "unbranded", "risk_score": 74, "channel": "online", "originality": 0.1, "capacity": 18, "cost": 16.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c29",
        "patch": {"status": "closed", "market": "combined", "count": 13, "total": 13,
                  "metrics": {"conversion": 0.9, "traffic": 109, "repeat_rate": 0.1, "delay_hours": 50},
                  "attributes": {"brand_level": "oem", "risk_score": 87, "channel": "hybrid", "originality": 0.2, "capacity": 19, "cost": 16.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c30",
        "patch": {"status": "new", "market": "leather", "count": 14, "total": 14,
                  "metrics": {"conversion": 0.0, "traffic": 110, "repeat_rate": 0.2, "delay_hours": 55},
                  "attributes": {"brand_level": "independent", "risk_score": 100, "channel": "offline", "originality": 0.3, "capacity": 20, "cost": 17.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c31",
        "patch": {"status": "active", "market": "textile", "count": 15, "total": 15,
                  "metrics": {"conversion": 0.1, "traffic": 111, "repeat_rate": 0.3, "delay_hours": 60},
                  "attributes": {"brand_level": "regional", "risk_score": 12, "channel": "online", "originality": 0.4, "capacity": 21, "cost": 17.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c32",
        "patch": {"status": "pending", "market": "combined", "count": 16, "total": 16,
                  "metrics": {"conversion": 0.2, "traffic": 112, "repeat_rate": 0.4, "delay_hours": 65},
                  "attributes": {"brand_level": "unbranded", "risk_score": 25, "channel": "hybrid", "originality": 0.5, "capacity": 22, "cost": 17.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c33",
        "patch": {"status": "review", "market": "leather", "count": 17, "total": 17,
                  "metrics": {"conversion": 0.3, "traffic": 113, "repeat_rate": 0.5, "delay_hours": 70},
                  "attributes": {"brand_level": "oem", "risk_score": 38, "channel": "offline", "originality": 0.6, "capacity": 23, "cost": 18.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c34",
        "patch": {"status": "closed", "market": "textile", "count": 1, "total": 18,
                  "metrics": {"conversion": 0.4, "traffic": 114, "repeat_rate": 0.6, "delay_hours": 75},
                  "attributes": {"brand_level": "independent", "risk_score": 51, "channel": "online", "originality": 0.7, "capacity": 24, "cost": 18.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c35",
        "patch": {"status": "new", "market": "combined", "count": 2, "total": 19,
                  "metrics": {"conversion": 0.5, "traffic": 115, "repeat_rate": 0.0, "delay_hours": 80},
                  "attributes": {"brand_level": "regional", "risk_score": 64, "channel": "hybrid", "originality": 0.8, "capacity": 25, "cost": 19.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c36",
        "patch": {"status": "active", "market": "leather", "count": 3, "total": 20,
                  "metrics": {"conversion": 0.6, "traffic": 116, "repeat_rate": 0.1, "delay_hours": 85},
                  "attributes": {"brand_level": "unbranded", "risk_score": 77, "channel": "offline", "originality": 0.0, "capacity": 26, "cost": 19.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c37",
        "patch": {"status": "pending", "market": "textile", "count": 4, "total": 21,
                  "metrics": {"conversion": 0.7, "traffic": 117, "repeat_rate": 0.2, "delay_hours": 90},
                  "attributes": {"brand_level": "oem", "risk_score": 90, "channel": "online", "originality": 0.1, "capacity": 27, "cost": 19.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c38",
        "patch": {"status": "review", "market": "combined", "count": 5, "total": 3,
                  "metrics": {"conversion": 0.8, "traffic": 118, "repeat_rate": 0.3, "delay_hours": 95},
                  "attributes": {"brand_level": "independent", "risk_score": 2, "channel": "hybrid", "originality": 0.2, "capacity": 28, "cost": 20.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c39",
        "patch": {"status": "closed", "market": "leather", "count": 6, "total": 4,
                  "metrics": {"conversion": 0.9, "traffic": 119, "repeat_rate": 0.4, "delay_hours": 4},
                  "attributes": {"brand_level": "regional", "risk_score": 15, "channel": "offline", "originality": 0.3, "capacity": 29, "cost": 20.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c40",
        "patch": {"status": "new", "market": "textile", "count": 7, "total": 5,
                  "metrics": {"conversion": 0.0, "traffic": 120, "repeat_rate": 0.5, "delay_hours": 9},
                  "attributes": {"brand_level": "unbranded", "risk_score": 28, "channel": "online", "originality": 0.4, "capacity": 10, "cost": 21.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c41",
        "patch": {"status": "active", "market": "combined", "count": 8, "total": 6,
                  "metrics": {"conversion": 0.1, "traffic": 121, "repeat_rate": 0.6, "delay_hours": 14},
                  "attributes": {"brand_level": "oem", "risk_score": 41, "channel": "hybrid", "originality": 0.5, "capacity": 11, "cost": 21.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c42",
        "patch": {"status": "pending", "market": "leather", "count": 9, "total": 7,
                  "metrics": {"conversion": 0.2, "traffic": 122, "repeat_rate": 0.0, "delay_hours": 19},
                  "attributes": {"brand_level": "independent", "risk_score": 54, "channel": "offline", "originality": 0.6, "capacity": 12, "cost": 21.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c43",
        "patch": {"status": "review", "market": "textile", "count": 10, "total": 8,
                  "metrics": {"conversion": 0.3, "traffic": 123, "repeat_rate": 0.1, "delay_hours": 24},
                  "attributes": {"brand_level": "regional", "risk_score": 67, "channel": "online", "originality": 0.7, "capacity": 13, "cost": 22.2}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c44",
        "patch": {"status": "closed", "market": "combined", "count": 11, "total": 9,
                  "metrics": {"conversion": 0.4, "traffic": 124, "repeat_rate": 0.2, "delay_hours": 29},
                  "attributes": {"brand_level": "unbranded", "risk_score": 80, "channel": "hybrid", "originality": 0.8, "capacity": 14, "cost": 22.6}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c45",
        "patch": {"status": "new", "market": "leather", "count": 12, "total": 10,
                  "metrics": {"conversion": 0.5, "traffic": 125, "repeat_rate": 0.3, "delay_hours": 34},
                  "attributes": {"brand_level": "oem", "risk_score": 93, "channel": "offline", "originality": 0.0, "capacity": 15, "cost": 23.0}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c46",
        "patch": {"status": "active", "market": "textile", "count": 13, "total": 11,
                  "metrics": {"conversion": 0.6, "traffic": 126, "repeat_rate": 0.4, "delay_hours": 39},
                  "attributes": {"brand_level": "independent", "risk_score": 5, "channel": "online", "originality": 0.1, "capacity": 16, "cost": 23.4}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
    {
        "name": "twin_market_flow-g1-c47",
        "patch": {"status": "pending", "market": "combined", "count": 14, "total": 12,
                  "metrics": {"conversion": 0.7, "traffic": 127, "repeat_rate": 0.5, "delay_hours": 44},
                  "attributes": {"brand_level": "regional", "risk_score": 18, "channel": "hybrid", "originality": 0.2, "capacity": 17, "cost": 23.8}},
        "expected_state": "completed",
        "expected_project": "twin_market_flow",
        "expected_artifacts": 7,
    },
]

class BusinessGroup1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.service = ApplicationService(Path(self.temp.name) / "data.sqlite3")

    def record(self, case):
        record = copy.deepcopy(BASE)
        record["case_id"] = case["name"]
        for key, value in case["patch"].items():
            record[key] = value
        return record

    def test_business_matrix(self):
        for case in CASES:
            with self.subTest(case=case["name"]):
                result = self.service.execute(self.record(case), request_key=case["name"])
                self.assertEqual(result["state"], case["expected_state"])
                self.assertEqual(result["project"], case["expected_project"])
                self.assertEqual(len(result["artifacts"]), case["expected_artifacts"])
                self.assertGreaterEqual(result["event_count"], case["expected_artifacts"] + 1)
                self.assertIn("runs", result["storage"])

    def test_idempotent_replay(self):
        case = CASES[0]
        first = self.service.execute(self.record(case), request_key="stable-key")
        second = self.service.execute(self.record(case), request_key="stable-key")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual(first["artifacts"], second["artifacts"])

    def test_project_specific_projection(self):
        result = self.service.execute(self.record(CASES[2]), request_key="projection-key")
        self.assertIn('journey_graph', result["artifacts"])
        self.assertIn('visitor_links', result["artifacts"])
        domain_values = result["artifacts"]['visitor_links']["domain_values"]
        self.assertIsInstance(domain_values, dict)
        self.assertTrue(domain_values)
        self.assertGreaterEqual(result["revision"], 7)

    def test_partial_batch_failure(self):
        valid = self.record(CASES[1])
        invalid = copy.deepcopy(valid)
        invalid.pop("case_id")
        result = self.service.execute_many([valid, invalid])
        self.assertEqual(result["accepted_count"], 1)
        self.assertEqual(result["failure_count"], 1)
        self.assertIn("case_id", result["failures"][0]["error"])

if __name__ == "__main__":
    unittest.main()
