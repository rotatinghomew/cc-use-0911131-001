from __future__ import annotations
import argparse
import json
from twin_market_flow.service import run_fixture

def main() -> int:
    parser = argparse.ArgumentParser(description='皮革家纺双市场客流联动归因引擎')
    parser.add_argument("fixture")
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    result = run_fixture(args.fixture, args.database)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
