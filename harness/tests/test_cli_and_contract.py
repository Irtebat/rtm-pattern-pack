"""Contract helpers, parameter validation, results files and CLI help output."""

import json
import subprocess
import sys

import pytest

from rtmpack_harness.cli import build_parser, main
from rtmpack_harness.contract import (
    HarnessError,
    RunParams,
    input_topic,
    output_topic,
    topic_names,
)
from rtmpack_harness.results import load_results, write_result
from rtmpack_harness.secrets import fetch_msk_credentials


def test_topic_names_follow_contract():
    assert input_topic(" 01-Dedup ") == "rtmpack.01-dedup.input"
    assert output_topic("01-dedup", "rtm") == "rtmpack.01-dedup.out.rtm"
    assert output_topic("01-dedup", "flink") == "rtmpack.01-dedup.out.flink"
    assert topic_names("01-dedup") == {
        "input": "rtmpack.01-dedup.input",
        "out.rtm": "rtmpack.01-dedup.out.rtm",
        "out.flink": "rtmpack.01-dedup.out.flink",
    }


def test_run_params_defaults_match_contract():
    p = RunParams()
    assert (p.rate_eps, p.record_bytes, p.partitions, p.key_cardinality) == (10_000, 1024, 8, 100_000)
    assert (p.duration_s, p.warmup_s, p.dup_ratio, p.seed) == (1800, 300, 0.2, 42)
    assert (p.dup_delay_ms_min, p.dup_delay_ms_max) == (0, 30_000)
    p.validate()


@pytest.mark.parametrize(
    "override",
    [
        {"warmup_s": 1800},
        {"dup_delay_ms_max": 60_000},
        {"dup_ratio": 1.5},
        {"pattern": "D 01"},
        {"rate_eps": 0},
    ],
)
def test_run_params_validation_rejects_bad_values(override):
    with pytest.raises(HarnessError):
        RunParams(**override).validate()


def test_write_result_replaces_the_same_engine_and_keeps_others(tmp_path):
    base = {
        "run_id": "r1", "pattern": "01-dedup", "engine": "rtm", "engine_version": "a",
        "started_at": "x", "rate_eps": 1, "record_bytes": 1024, "partitions": 8,
        "compute_desc": "c", "vcpu": 8, "cost_per_hour_usd": 1.0, "out_records": 1,
        "distinct_out_keys": 1, "expected_distinct": 1, "missing": 0, "sink_duplicates": 0,
        "p50_ms": 1, "p95_ms": 1, "p99_ms": 1, "p999_ms": 1, "max_ms": 1, "throughput_eps": 1.0,
        "p50_ms_h1": 1, "p99_ms_h1": 1, "p50_ms_h2": 1, "p99_ms_h2": 1, "stable": True,
    }
    write_result(tmp_path, "r1", base)
    write_result(tmp_path, "r1", {**base, "engine": "flink", "engine_version": "b"})
    write_result(tmp_path, "r1", {**base, "engine_version": "rerun"})
    rows = load_results(tmp_path)
    assert len(rows) == 2
    by_engine = {r["engine"]: r for r in rows}
    assert by_engine["rtm"]["engine_version"] == "rerun"
    assert by_engine["flink"]["engine_version"] == "b"
    assert list(rows[0].keys())[0] == "run_id"


def test_write_result_rejects_incomplete_records(tmp_path):
    with pytest.raises(HarnessError):
        write_result(tmp_path, "r1", {"run_id": "r1"})


class _FakeSecrets:
    def __init__(self, payload):
        self.payload = payload

    def get_secret_value(self, SecretId):
        return {"SecretString": self.payload}


def test_fetch_msk_credentials_reads_username_and_password():
    creds = fetch_msk_credentials(client=_FakeSecrets(json.dumps({"username": "u", "password": "p"})))
    assert creds == ("u", "p")


def test_fetch_msk_credentials_rejects_incomplete_secret():
    with pytest.raises(HarnessError):
        fetch_msk_credentials(client=_FakeSecrets(json.dumps({"username": "u"})))


SUBCOMMANDS = [
    ["--help"],
    ["topics", "--help"],
    ["topics", "create", "--help"],
    ["topics", "delete", "--help"],
    ["loadgen", "--help"],
    ["collect", "--help"],
    ["report", "--help"],
]


@pytest.mark.parametrize("argv", SUBCOMMANDS, ids=lambda a: " ".join(a))
def test_help_exits_zero_for_every_subcommand(argv):
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(argv)
    assert exc.value.code == 0


def test_report_renders_markdown_for_an_empty_results_dir(tmp_path, capsys):
    assert main(["report", "--results-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert out.startswith("# RTM Pattern Pack results")
    assert "| Run |" in out


def test_module_entry_point_runs():
    proc = subprocess.run(
        [sys.executable, "-m", "rtmpack_harness", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "rtmpack" in proc.stdout


def test_topics_retention_sets_every_pattern_topic():
    from confluent_kafka.admin import AlterConfigOpType

    from rtmpack_harness import topics

    seen = []

    class _Done:
        def result(self, timeout=None):
            return None

    class _Admin:
        def incremental_alter_configs(self, resources, request_timeout=None):
            seen.extend(resources)
            return {r: _Done() for r in resources}

    out = topics.set_retention(_Admin(), "01-dedup", 7_200_000)
    names = set(topics.topic_names("01-dedup").values())
    assert set(out) == names and set(out.values()) == {"set"}
    for r in seen:
        (entry,) = r.incremental_configs
        assert entry.name == "retention.ms" and entry.value == "7200000"
        assert entry.incremental_operation == AlterConfigOpType.SET


def test_default_topic_retention_is_12_hours():
    from rtmpack_harness.topics import topic_config

    assert topic_config()["retention.ms"] == "43200000"
