"""CLI for local pipeline execution and artifact verification."""

import argparse
import json
import sys
from pathlib import Path

from .core import PipelineError, run, verify


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local, content-addressed pipeline")
    sub = parser.add_subparsers(dest="action", required=True)
    go = sub.add_parser("run", help="execute a pipeline and print its run record")
    go.add_argument("manifest", type=Path)
    go.add_argument("--workspace", type=Path, default=Path.cwd())
    go.add_argument("--state", type=Path)
    check = sub.add_parser("verify", help="check stored output hashes")
    check.add_argument("run_id")
    check.add_argument("--state", type=Path, default=Path(".runweave"))
    args = parser.parse_args()
    try:
        if args.action == "run":
            workspace = args.workspace.resolve()
            state = args.state or workspace / ".runweave"
            record = run(args.manifest, workspace, state)
            print(json.dumps(record, indent=2))
            return 0 if record["status"] == "success" else 2
        result = verify(args.state, args.run_id)
        print(json.dumps(result, indent=2))
        return 0 if result["verified"] else 2
    except PipelineError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
