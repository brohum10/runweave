# RunWeave

**A small pipeline runner that can show why a result was recomputed or reused.**

[![Tests](https://github.com/brohum10/runweave/actions/workflows/tests.yml/badge.svg)](https://github.com/brohum10/runweave/actions/workflows/tests.yml) · [Explore the live example](https://brohum10.github.io/runweave/) · [Download its evidence](docs/evidence.json)

RunWeave executes a directed acyclic graph of local Python tasks. It hashes each task's declared files, script, arguments, and upstream artifacts before running. Outputs live in a content-addressed store; a later run reuses them only when its cache key and stored bytes verify. Every run gets a JSON record with input hashes, dependency hashes, execution or cache status, output hash, and failure details. Python 3.12+; standard library only.

This is a **local developer tool and teaching project**, not a managed workflow service.

## See it work

From the repository root:

```sh
python demo.py
python -m unittest discover -s tests -v
```

The demo copies a **synthetic 12-ticket support dataset** to a temporary workspace, then runs the same two-step pipeline three times. The third run changes only the review policy from 25 to 45 minutes:

| Run | Normalize tickets | Summarize by region | Why |
| --- | --- | --- | --- |
| First | Executed | Executed | No cached outputs |
| Identical rerun | Cached | Cached | Same declared inputs and verified artifacts |
| Policy-only change | Cached | Executed | Only the summary's policy input changed |

[The checked-in evidence](docs/evidence.json) contains the task statuses and full SHA-256 artifact hashes. Regenerate it with `python demo.py --save docs/evidence.json`; the run timestamps are deliberately omitted from that compact file. This demonstrates cache invalidation on a small constructed fixture, **not** a production speedup or reliability claim.

## Run your own pipeline

`examples/pipeline.json` is a complete manifest. Each task names one Python script, zero or more workspace-relative files, upstream tasks, arguments, and a timeout. Arguments use exact placeholders:

- `{input:examples/tickets.csv}` → a temporary snapshot of that declared file for this task.
- `{dep:normalize}` → the verified content-addressed output of an upstream task.
- `{output}` → the path where the task must write its one output file.

```sh
python -m runweave run examples/pipeline.json --workspace .
python -m runweave verify RUN_ID --state .runweave
```

Replace `RUN_ID` with the `run_id` printed by the first command. The runner stores artifacts under `.runweave/objects`, cache mappings under `.runweave/cache`, and run records under `.runweave/runs`. It does not overwrite the source data. `run` exits 0 for a completed pipeline and 2 for a failed task or invalid manifest. `verify` exits 0 only when the run succeeded and every recorded artifact still matches its hash. The example's final JSON output can be read using the `summarize` task's `artifact_sha256` from the run record.

## How it is built

```text
declared files + script hash ─┐
task arguments + Python/OS ───┼─> cache key ──> verified stored output?
upstream output hashes ───────┘                   │ yes: reuse
                                                no: run task
                                                     │
                                        hash output, store atomically
                                                     │
                                         record task outcome in run JSON
```

The manifest is strictly validated before execution: task names are unique, dependencies exist, cycles are rejected, placeholders must refer to declared inputs or upstream tasks, paths must stay inside the workspace, and timeouts are bounded. A stable topological order makes the result independent of task order in the JSON file. Inputs are copied into temporary snapshots before task execution and checked against the hashes used for the cache key. Output objects and run records are written through temporary files and atomic renames. Cache hits reread and hash the stored artifact; a damaged object is rebuilt.

The **18 tests** cover cold and warm runs, narrow invalidation, changed scripts, corrupted outputs, failure records, missing outputs, dependency order/cycles, path traversal, symlink escapes, invalid manifests, and real CLI execution. GitHub Actions runs the suite and demo on Python 3.12 and 3.14.

## Honest boundaries

- **Trusted local scripts only.** Tasks are ordinary Python processes and are not sandboxed. Do not run a manifest from an untrusted source. Scripts can read undeclared files, the network, time, or environment; doing so breaks the cache's determinism assumption. Declare every file that affects output.
- The cache key includes the script file, declared inputs, upstream output hashes, arguments, Python version, platform, and CPU architecture. It does not capture imported local modules, installed packages, environment variables, external services, or OS libraries. Declare local modules as inputs; for other dependencies, use a clean environment or disable/rebuild the cache.
- A successful cached result proves only that the recorded bytes match their SHA-256 hash and the declared key is unchanged. It is not an authenticity signature or a proof of task correctness. A local user who can edit the state directory can alter both artifacts and records.
- One output file per task; 16 MB maximum per source file or artifact, 100 tasks maximum, 300-second task timeout. Runs are intended for a **single local process**; there is no cross-process lock, remote cache, scheduler, streaming execution, rollback of external side effects, or cloud deployment.
- On a failed run, completed tasks may remain in the cache and are individually reusable. The run record clearly says `failed`, and `verify` does not mark a failed run as successful.

RunWeave was developed with AI assistance. The synthetic fixture, code, and tests are included so the behavior can be checked directly. MIT licensed.
