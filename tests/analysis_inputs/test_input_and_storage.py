from __future__ import annotations

import importlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class AnalysisInputTests(unittest.TestCase):
    def test_local_input_is_structured_and_finite(self):
        raw = json.loads((ROOT / 'fixtures/analysis/input.json').read_text(encoding='utf-8'))
        self.assertIsInstance(raw, dict)
        self.assertGreaterEqual(len(raw), 4)
        json.dumps(raw, allow_nan=False)
        self.assertTrue(any(isinstance(value, list) and value for value in raw.values()))

    def test_existing_workflow_persists_to_local_database(self):
        module = importlib.import_module('twin_market_flow.service')
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'business.sqlite3'
            result = module.run_fixture(ROOT / 'fixtures/demo.json', database)
            self.assertIsInstance(result, dict)
            self.assertTrue(result)
            self.assertTrue(database.is_file())
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                self.assertGreater(connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
