"""A small, inspectable content-addressed pipeline runner."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 128 * 1024
ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,39}$")


class PipelineError(Exception):
    """Invalid pipeline, failed task, or damaged cache evidence."""


@dataclass(frozen=True)
class Task:
    name: str
    script: str
    inputs: tuple[str, ...]
    needs: tuple[str, ...]
    args: tuple[str, ...]
    timeout_seconds: int


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_limited(path: Path, limit: int = MAX_FILE_BYTES) -> bytes:
    if not path.is_file():
        raise PipelineError(f"Missing file: {path}")
    if path.stat().st_size > limit:
        raise PipelineError(f"File exceeds {limit} bytes: {path}")
    data = path.read_bytes()
    if len(data) > limit:
        raise PipelineError(f"File exceeds {limit} bytes: {path}")
    return data


def _relative_file(root: Path, name: str) -> Path:
    if not isinstance(name, str) or not name or Path(name).is_absolute():
        raise PipelineError(f"Expected a workspace-relative file path: {name!r}")
    if any(part in (".", "..", "") for part in Path(name).parts):
        raise PipelineError(f"Unsafe file path: {name!r}")
    path = (root / name).resolve()
    if not path.is_relative_to(root):
        raise PipelineError(f"File escapes workspace: {name!r}")
    if not path.is_file():
        raise PipelineError(f"Missing workspace file: {name!r}")
    return path


def load_manifest(path: Path, workspace: Path) -> tuple[Task, ...]:
    """Validate a strict task manifest and return stable topological order."""
    root = workspace.resolve()
    source = path.resolve()
    if not source.is_relative_to(root):
        raise PipelineError("Manifest must be inside the workspace")
    try:
        doc = json.loads(_read_limited(source, MAX_MANIFEST_BYTES))
    except (ValueError, UnicodeDecodeError) as exc:
        raise PipelineError(f"Invalid JSON manifest: {exc}") from exc
    if not isinstance(doc, dict) or set(doc) != {"version", "tasks"} or type(doc["version"]) is not int or doc["version"] != 1:
        raise PipelineError("Manifest needs exactly version=1 and tasks")
    raw_tasks = doc["tasks"]
    if not isinstance(raw_tasks, list) or not 1 <= len(raw_tasks) <= 100:
        raise PipelineError("tasks must contain 1 to 100 entries")
    tasks: dict[str, Task] = {}
    for raw in raw_tasks:
        if not isinstance(raw, dict) or set(raw) != {"name", "script", "inputs", "needs", "args", "timeout_seconds"}:
            raise PipelineError("Each task needs exactly name, script, inputs, needs, args, timeout_seconds")
        name = raw["name"]
        if not isinstance(name, str) or not ID_PATTERN.fullmatch(name) or name in tasks:
            raise PipelineError(f"Invalid or duplicate task name: {name!r}")
        script = raw["script"]
        _relative_file(root, script)
        if not script.endswith(".py"):
            raise PipelineError(f"Task {name}: script must be a .py file")
        inputs, needs, args = raw["inputs"], raw["needs"], raw["args"]
        if not isinstance(inputs, list) or len(inputs) > 100 or not all(isinstance(x, str) for x in inputs) or len(set(inputs)) != len(inputs):
            raise PipelineError(f"Task {name}: inputs must be a unique list of paths")
        for item in inputs:
            _relative_file(root, item)
        if not isinstance(needs, list) or len(needs) > 100 or not all(isinstance(x, str) for x in needs) or len(set(needs)) != len(needs):
            raise PipelineError(f"Task {name}: needs must be a unique list of task names")
        if not isinstance(args, list) or len(args) > 100 or not all(isinstance(x, str) and len(x) < 1024 for x in args):
            raise PipelineError(f"Task {name}: args must be a list of short strings")
        timeout = raw["timeout_seconds"]
        if type(timeout) is not int or not 1 <= timeout <= 300:
            raise PipelineError(f"Task {name}: timeout_seconds must be 1 to 300")
        allowed = {"{output}"} | {f"{{input:{item}}}" for item in inputs} | {f"{{dep:{item}}}" for item in needs}
        if "{output}" not in args:
            raise PipelineError(f"Task {name}: args must include {{output}}")
        for arg in args:
            if ("{" in arg or "}" in arg) and arg not in allowed:
                raise PipelineError(f"Task {name}: unknown or embedded placeholder {arg!r}")
        tasks[name] = Task(name, script, tuple(inputs), tuple(needs), tuple(args), timeout)
    for task in tasks.values():
        if task.name in task.needs or any(dep not in tasks for dep in task.needs):
            raise PipelineError(f"Task {task.name}: missing or self dependency")
    ordered: list[Task] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise PipelineError(f"Dependency cycle at {name}")
        if name in visited:
            return
        visiting.add(name)
        for dep in sorted(tasks[name].needs):
            visit(dep)
        visiting.remove(name)
        visited.add(name)
        ordered.append(tasks[name])

    for name in sorted(tasks):
        visit(name)
    return tuple(ordered)


def _atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temp = Path(handle.name)
        try:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temp.unlink(missing_ok=True)
            raise
    os.replace(temp, path)


def _object_path(state: Path, digest: str) -> Path:
    return state / "objects" / digest[:2] / digest


def _valid_object(state: Path, digest: str) -> bool:
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        return False
    path = _object_path(state, digest)
    try:
        return _digest(_read_limited(path)) == digest
    except PipelineError:
        return False


def _cache_lookup(state: Path, key: str) -> str | None:
    path = state / "cache" / f"{key}.json"
    if not path.exists():
        return None
    try:
        item = json.loads(_read_limited(path, 4096))
    except (PipelineError, ValueError, UnicodeDecodeError):
        return None
    digest = item.get("artifact_sha256") if isinstance(item, dict) and item.get("key") == key else None
    return digest if _valid_object(state, digest) else None


def run(manifest: Path, workspace: Path, state_dir: Path) -> dict[str, Any]:
    """Run one trusted local pipeline. Cache reuse assumes declared, deterministic inputs."""
    root = workspace.resolve()
    state = state_dir.resolve()
    if state == root or not state.is_relative_to(root):
        raise PipelineError("State directory must be a workspace subdirectory")
    tasks = load_manifest(manifest, root)
    state.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    report: dict[str, Any] = {
        "version": 1,
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "workspace_manifest": str(manifest.resolve().relative_to(root)),
        "manifest_sha256": _digest(_read_limited(manifest.resolve(), MAX_MANIFEST_BYTES)),
        "status": "running",
        "tasks": [],
    }
    outputs: dict[str, str] = {}
    try:
        for task in tasks:
            script_path = _relative_file(root, task.script)
            input_paths = {name: _relative_file(root, name) for name in task.inputs}
            input_hashes = {name: _digest(_read_limited(path)) for name, path in input_paths.items()}
            dep_hashes = {name: outputs[name] for name in task.needs}
            script_hash = _digest(_read_limited(script_path))
            key = _digest(_canonical({
                "version": 1,
                "name": task.name,
                "script_sha256": script_hash,
                "args": task.args,
                "timeout_seconds": task.timeout_seconds,
                "inputs": input_hashes,
                "needs": dep_hashes,
                "python": sys.version,
                "platform": sys.platform,
                "machine": platform.machine(),
            }))
            prior = _cache_lookup(state, key)
            step: dict[str, Any] = {"name": task.name, "key": key, "inputs": input_hashes,
                                    "dependencies": dep_hashes, "script": task.script}
            if prior:
                step.update(status="cached", artifact_sha256=prior, duration_ms=0)
                outputs[task.name] = prior
                report["tasks"].append(step)
                continue
            started = time.perf_counter()
            with tempfile.TemporaryDirectory(prefix="runweave-", dir=state) as scratch:
                output_path = Path(scratch) / "output"
                if _digest(_read_limited(script_path)) != script_hash:
                    raise PipelineError(f"Task {task.name}: script changed during run")
                snapshots: dict[str, Path] = {}
                for index, (name, path) in enumerate(input_paths.items()):
                    data = _read_limited(path)
                    if _digest(data) != input_hashes[name]:
                        raise PipelineError(f"Task {task.name}: input changed during run: {name}")
                    snapshot = Path(scratch) / f"input-{index}-{path.name}"
                    snapshot.write_bytes(data)
                    snapshots[name] = snapshot
                substitutions = {"{output}": str(output_path)}
                substitutions.update({f"{{input:{name}}}": str(path) for name, path in snapshots.items()})
                substitutions.update({f"{{dep:{name}}}": str(_object_path(state, digest)) for name, digest in dep_hashes.items()})
                command = [sys.executable, str(script_path)] + [substitutions.get(arg, arg) for arg in task.args]
                error_path = Path(scratch) / "stderr"
                with error_path.open("wb") as errors:
                    try:
                        result = subprocess.run(command, cwd=root, stdout=subprocess.DEVNULL,
                                                stderr=errors, timeout=task.timeout_seconds, check=False)
                    except subprocess.TimeoutExpired as exc:
                        raise PipelineError(f"Task {task.name} timed out after {task.timeout_seconds}s") from exc
                if result.returncode != 0:
                    with error_path.open("rb") as errors:
                        errors.seek(max(0, error_path.stat().st_size - 2000))
                        stderr = errors.read().decode("utf-8", errors="replace")
                    raise PipelineError(f"Task {task.name} exited {result.returncode}: {stderr.strip()}")
                artifact = _read_limited(output_path)
                digest = _digest(artifact)
                object_path = _object_path(state, digest)
                if not _valid_object(state, digest):
                    _atomic_bytes(object_path, artifact)
                _atomic_bytes(state / "cache" / f"{key}.json", _canonical({"key": key, "artifact_sha256": digest}))
                step.update(status="executed", artifact_sha256=digest,
                            duration_ms=round((time.perf_counter() - started) * 1000, 2))
                outputs[task.name] = digest
                report["tasks"].append(step)
        report["status"] = "success"
    except PipelineError as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    _atomic_bytes(state / "runs" / f"{run_id}.json", _canonical(report))
    return report


def verify(state_dir: Path, run_id: str) -> dict[str, Any]:
    """Check the bytes behind every completed task in a stored run record."""
    if not re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", run_id):
        raise PipelineError("Invalid run ID")
    state = state_dir.resolve()
    try:
        report = json.loads(_read_limited(state / "runs" / f"{run_id}.json", 1024 * 1024))
    except (ValueError, UnicodeDecodeError) as exc:
        raise PipelineError(f"Invalid run record: {exc}") from exc
    if not isinstance(report, dict) or report.get("run_id") != run_id or not isinstance(report.get("tasks"), list):
        raise PipelineError("Invalid run record structure")
    damaged = [task.get("name", "unknown") if isinstance(task, dict) else "unknown"
               for task in report["tasks"]
               if not isinstance(task, dict) or not _valid_object(state, task.get("artifact_sha256"))]
    return {"run_id": run_id, "run_status": report.get("status"), "checked": len(report["tasks"]),
            "damaged": damaged, "verified": not damaged and report.get("status") == "success"}
