"""Reproduce the three cache outcomes without altering the checked-in example."""

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from runweave import run, verify


HERE = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", type=Path, help="write compact evidence JSON")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="runweave-demo-") as directory:
        root = Path(directory)
        shutil.copytree(HERE / "examples", root / "examples")
        manifest = root / "examples" / "pipeline.json"
        state = root / ".runweave"
        cold = run(manifest, root, state)
        warm = run(manifest, root, state)
        policy = root / "examples" / "policy.json"
        policy.write_text('{"max_p90_queue_minutes": 45}\n', encoding="utf-8")
        changed = run(manifest, root, state)
        for record in (cold, warm, changed):
            if record["status"] != "success" or not verify(state, record["run_id"])["verified"]:
                raise RuntimeError("Demo pipeline or artifact verification failed")
        expected = [["executed", "executed"], ["cached", "cached"], ["cached", "executed"]]
        records = [cold, warm, changed]
        observed = [[step["status"] for step in record["tasks"]] for record in records]
        if observed != expected:
            raise RuntimeError(f"Unexpected cache behavior: {observed}")
        evidence = {
            "scenario": "Synthetic support-ticket pipeline; policy changes from 25 to 45 minutes",
            "rows": 12,
            "steps": [
                {"label": label,
                 "tasks": [{"name": step["name"], "status": step["status"],
                            "artifact_sha256": step["artifact_sha256"]} for step in record["tasks"]]}
                for label, record in zip(("first run", "identical rerun", "policy-only change"), records)
            ],
        }
        print(json.dumps(evidence, indent=2))
        if args.save:
            args.save.parent.mkdir(parents=True, exist_ok=True)
            args.save.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
