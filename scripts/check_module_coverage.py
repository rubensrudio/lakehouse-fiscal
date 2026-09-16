from __future__ import annotations

import json
import sys
from pathlib import Path

MINIMUM = 95.0
MODULES = (
    "src/lakehouse_fiscal/silver/transforms.py",
    "src/lakehouse_fiscal/gold/scd2.py",
    "src/lakehouse_fiscal/quality/expectations.py",
)


def main() -> None:
    report = Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json")
    files = json.loads(report.read_text(encoding="utf-8"))["files"]
    failures: list[str] = []
    for module in MODULES:
        coverage = float(files[module]["summary"]["percent_covered"])
        print(f"{module}: {coverage:.2f}% (required: {MINIMUM:.0f}%)")
        if coverage < MINIMUM:
            failures.append(f"{module}={coverage:.2f}%")
    if failures:
        raise SystemExit("Critical-module coverage gate failed: " + ", ".join(failures))


if __name__ == "__main__":
    main()
