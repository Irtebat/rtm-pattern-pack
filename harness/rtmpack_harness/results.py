"""Manifest and results file handling under the results directory."""

from __future__ import annotations

import json
import os
from pathlib import Path

from rtmpack_harness.contract import RESULT_FIELDS, HarnessError


def manifest_path(results_dir: str | Path, run_id: str) -> Path:
    return Path(results_dir) / f"{run_id}.manifest.json"


def results_path(results_dir: str | Path, run_id: str) -> Path:
    return Path(results_dir) / f"{run_id}.jsonl"


def expected_path(results_dir: str | Path, run_id: str, worker_id: int) -> Path:
    """Side file of the expected aggregates one load worker produced."""
    return Path(results_dir) / f"{run_id}.expected.w{worker_id:03d}.npz"


def load_expected(results_dir: str | Path, run_id: str):
    """Merge every worker's expected-aggregate side file for a run."""
    from rtmpack_harness.stats import ExpectedPairs

    paths = sorted(Path(results_dir).glob(f"{run_id}.expected.w*.npz"))
    if not paths:
        raise HarnessError(f"no expected-aggregate side files for run {run_id!r} in {results_dir}")
    return ExpectedPairs.merge(ExpectedPairs.load(p) for p in paths)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def write_manifest(results_dir: str | Path, manifest: dict) -> Path:
    path = manifest_path(results_dir, manifest["run_id"])
    _atomic_write(path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return path


def load_manifest(results_dir: str | Path, run_id: str) -> dict:
    path = manifest_path(results_dir, run_id)
    if not path.is_file():
        raise HarnessError(f"no manifest for run {run_id!r} at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_result(results_dir: str | Path, run_id: str, record: dict) -> Path:
    """Write one (run, engine) line. A rerun for the same engine replaces its line."""
    record = {"value_errors": 0, **record}
    missing = [f for f in RESULT_FIELDS if f not in record]
    if missing:
        raise HarnessError(f"result record is missing fields: {', '.join(missing)}")
    ordered = {f: record[f] for f in RESULT_FIELDS}
    path = results_path(results_dir, run_id)

    kept: list[str] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            if json.loads(line).get("engine") != ordered["engine"]:
                kept.append(line)
    kept.append(json.dumps(ordered))
    _atomic_write(path, "\n".join(kept) + "\n")
    return path


def load_results(results_dir: str | Path) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(Path(results_dir).glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows
