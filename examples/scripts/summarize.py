"""Make a small, reviewable policy report from normalized ticket data."""

import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path


def main(source: Path, policy_path: Path, output: Path) -> None:
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if set(policy) != {"max_p90_queue_minutes"} or type(policy["max_p90_queue_minutes"]) not in (int, float):
        raise ValueError("Expected one numeric max_p90_queue_minutes policy")
    limit = policy["max_p90_queue_minutes"]
    if not math.isfinite(limit) or limit < 0:
        raise ValueError("Invalid policy limit")
    groups = defaultdict(list)
    with source.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            groups[row["region"]].append(float(row["queue_minutes"]))
    result = []
    for region, values in sorted(groups.items()):
        values.sort()
        p90 = values[math.ceil(0.9 * len(values)) - 1]
        result.append({"region": region, "tickets": len(values), "p90_queue_minutes": p90,
                       "over_policy": p90 > limit})
    output.write_text(json.dumps({"policy_max_p90_queue_minutes": limit,
                                  "regions": result}, sort_keys=True, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]))
