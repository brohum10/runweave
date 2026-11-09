import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from runweave import PipelineError, load_manifest, run, verify


SOURCE = Path(__file__).resolve().parents[1] / "examples"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(SOURCE, self.root / "examples")
        self.manifest = self.root / "examples" / "pipeline.json"
        self.state = self.root / ".runweave"

    def edit_manifest(self, change):
        doc = json.loads(self.manifest.read_text())
        change(doc)
        self.manifest.write_text(json.dumps(doc))

    def test_first_run_then_cache_hit(self):
        first = run(self.manifest, self.root, self.state)
        second = run(self.manifest, self.root, self.state)
        self.assertEqual(first["status"], "success")
        self.assertEqual([step["status"] for step in first["tasks"]], ["executed", "executed"])
        self.assertEqual([step["status"] for step in second["tasks"]], ["cached", "cached"])
        self.assertEqual([step["artifact_sha256"] for step in first["tasks"]],
                         [step["artifact_sha256"] for step in second["tasks"]])
        self.assertTrue(verify(self.state, first["run_id"])["verified"])

    def test_policy_change_only_reruns_summary(self):
        run(self.manifest, self.root, self.state)
        (self.root / "examples" / "policy.json").write_text('{"max_p90_queue_minutes": 40}')
        changed = run(self.manifest, self.root, self.state)
        self.assertEqual([step["status"] for step in changed["tasks"]], ["cached", "executed"])
        summary = json.loads((self.state / "objects" / changed["tasks"][1]["artifact_sha256"][:2] /
                              changed["tasks"][1]["artifact_sha256"]).read_text())
        self.assertEqual(summary["policy_max_p90_queue_minutes"], 40)

    def test_raw_change_reruns_both(self):
        run(self.manifest, self.root, self.state)
        path = self.root / "examples" / "tickets.csv"
        path.write_text(path.read_text().replace("T001,EU,email,23", "T001,EU,email,24"))
        changed = run(self.manifest, self.root, self.state)
        self.assertEqual([step["status"] for step in changed["tasks"]], ["executed", "executed"])

    def test_script_change_invalidates_its_task(self):
        run(self.manifest, self.root, self.state)
        path = self.root / "examples" / "scripts" / "summarize.py"
        path.write_text(path.read_text() + "\n# source revision\n")
        changed = run(self.manifest, self.root, self.state)
        self.assertEqual([step["status"] for step in changed["tasks"]], ["cached", "executed"])

    def test_corrupt_artifact_is_detected_and_rebuilt(self):
        first = run(self.manifest, self.root, self.state)
        digest = first["tasks"][0]["artifact_sha256"]
        (self.state / "objects" / digest[:2] / digest).write_text("tampered")
        self.assertEqual(verify(self.state, first["run_id"])["damaged"], ["normalize"])
        repaired = run(self.manifest, self.root, self.state)
        self.assertEqual(repaired["tasks"][0]["status"], "executed")
        self.assertTrue(verify(self.state, first["run_id"])["verified"])

    def test_failed_task_records_failure(self):
        (self.root / "examples" / "policy.json").write_text('{"max_p90_queue_minutes": -1}')
        result = run(self.manifest, self.root, self.state)
        self.assertEqual(result["status"], "failed")
        self.assertIn("summarize exited", result["error"])
        self.assertEqual(len(result["tasks"]), 1)
        self.assertTrue((self.state / "runs" / (result["run_id"] + ".json")).exists())
        self.assertFalse(verify(self.state, result["run_id"])["verified"])

    def test_missing_output_is_failure(self):
        path = self.root / "examples" / "scripts" / "normalize.py"
        path.write_text("pass\n")
        result = run(self.manifest, self.root, self.state)
        self.assertEqual(result["status"], "failed")
        self.assertIn("Missing file", result["error"])

    def test_topological_order_ignores_manifest_order(self):
        self.edit_manifest(lambda doc: doc["tasks"].reverse())
        self.assertEqual([task.name for task in load_manifest(self.manifest, self.root)],
                         ["normalize", "summarize"])

    def test_cycle_rejected(self):
        self.edit_manifest(lambda doc: doc["tasks"][0]["needs"].append("summarize"))
        with self.assertRaisesRegex(PipelineError, "cycle"):
            load_manifest(self.manifest, self.root)

    def test_missing_dependency_rejected(self):
        self.edit_manifest(lambda doc: doc["tasks"][1]["needs"].append("unknown"))
        with self.assertRaisesRegex(PipelineError, "missing"):
            load_manifest(self.manifest, self.root)

    def test_path_traversal_rejected(self):
        self.edit_manifest(lambda doc: doc["tasks"][0]["inputs"].append("../../private.csv"))
        with self.assertRaises(PipelineError):
            load_manifest(self.manifest, self.root)

    def test_symlink_escape_rejected(self):
        outside_temp = tempfile.TemporaryDirectory()
        self.addCleanup(outside_temp.cleanup)
        outside = Path(outside_temp.name) / "private.csv"
        outside.write_text("secret")
        (self.root / "examples" / "link.csv").symlink_to(outside)
        self.edit_manifest(lambda doc: doc["tasks"][0]["inputs"].append("examples/link.csv"))
        with self.assertRaisesRegex(PipelineError, "escapes workspace"):
            load_manifest(self.manifest, self.root)

    def test_undeclared_placeholder_rejected(self):
        self.edit_manifest(lambda doc: doc["tasks"][0]["args"].append("{dep:summarize}"))
        with self.assertRaisesRegex(PipelineError, "placeholder"):
            load_manifest(self.manifest, self.root)

    def test_missing_output_placeholder_rejected(self):
        self.edit_manifest(lambda doc: doc["tasks"][0]["args"].remove("{output}"))
        with self.assertRaisesRegex(PipelineError, "output"):
            load_manifest(self.manifest, self.root)

    def test_invalid_version_rejected(self):
        self.edit_manifest(lambda doc: doc.update(version=True))
        with self.assertRaises(PipelineError):
            load_manifest(self.manifest, self.root)

    def test_bad_verify_id_rejected(self):
        with self.assertRaisesRegex(PipelineError, "run ID"):
            verify(self.state, "../../bad")

    def test_cli_run_and_verify(self):
        cwd = Path(__file__).resolve().parents[1]
        command = [sys.executable, "-m", "runweave", "run", str(self.manifest),
                   "--workspace", str(self.root)]
        result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(result.stdout)
        check = subprocess.run([sys.executable, "-m", "runweave", "verify", record["run_id"],
                                "--state", str(self.state)], cwd=cwd, text=True, capture_output=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        self.assertTrue(json.loads(check.stdout)["verified"])

    def test_external_state_directory_rejected(self):
        with self.assertRaisesRegex(PipelineError, "State directory"):
            run(self.manifest, self.root, self.root.parent)


if __name__ == "__main__":
    unittest.main()
