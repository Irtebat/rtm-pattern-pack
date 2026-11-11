"""Command-line entry point: rtmpack topics|loadgen|collect|refdata|report."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Optional, Sequence

from rtmpack_harness import __version__
from rtmpack_harness.config import add_connection_args, resolve_connection
from rtmpack_harness.contract import (
    DEFAULT_PARTITIONS,
    ENGINES,
    HarnessError,
    RunParams,
    normalize_pattern,
    topic_names,
)


def _pattern_type(value: str) -> str:
    try:
        return normalize_pattern(value)
    except HarnessError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rtmpack",
        description="Benchmark harness for the RTM Pattern Pack (Spark Real-Time Mode vs Flink).",
    )
    parser.add_argument("--version", action="version", version=f"rtmpack {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)

    # topics
    topics = commands.add_parser("topics", help="create or delete the topics for a pattern")
    topic_actions = topics.add_subparsers(dest="action", metavar="ACTION", required=True)

    t_create = topic_actions.add_parser(
        "create",
        help="create every input topic and every output topic of the engines",
        description="Create rtmpack.<pattern>.input, rtmpack.<pattern>.out.rtm and "
        "rtmpack.<pattern>.out.flink with LogAppendTime, 43200000 ms (12 h) retention and replication factor 3. "
        "Patterns with several topics get a suffix on the base name: 08-join has .input.orders and "
        ".input.payments; 04-routing has .out.<engine>.payment, .order, .click, .login and .all.",
    )
    t_create.add_argument("--pattern", required=True, type=_pattern_type, help="pattern id, for example 01-dedup")
    t_create.add_argument(
        "--partitions", type=_positive_int, default=DEFAULT_PARTITIONS,
        help=f"partition count for every topic (default {DEFAULT_PARTITIONS})",
    )
    add_connection_args(t_create)

    t_delete = topic_actions.add_parser(
        "delete",
        help="delete the topics for a pattern",
        description="Delete every input and output topic of a pattern. Requires --yes.",
    )
    t_delete.add_argument("--pattern", required=True, type=_pattern_type, help="pattern id, for example 01-dedup")
    t_delete.add_argument("--yes", action="store_true", help="confirm the deletion")

    t_ret = topic_actions.add_parser(
        "retention",
        help="set the retention of every topic of a pattern",
        description="Set retention.ms on the input and output topics of a pattern. Shortening it "
        "frees broker storage: Kafka deletes closed segments older than the new value.",
    )
    t_ret.add_argument("--pattern", required=True, type=_pattern_type, help="pattern id, for example 01-dedup")
    t_ret.add_argument("--hours", required=True, type=float, help="new retention in hours (> 0)")
    add_connection_args(t_ret)
    add_connection_args(t_delete)

    # loadgen
    loadgen = commands.add_parser(
        "loadgen",
        help="generate rate-controlled input events with duplicates",
        description="Produce the input topic for one run and write results/<run_id>.manifest.json.",
    )
    loadgen.add_argument("--pattern", required=True, type=_pattern_type)
    defaults = RunParams()
    loadgen.add_argument("--rate-eps", type=_positive_int, default=None,
                         help="first sends per second across all workers (default "
                         f"{defaults.rate_eps}; 08-join counts orders and defaults to 5000)")
    loadgen.add_argument("--record-bytes", type=_positive_int, default=defaults.record_bytes,
                         help=f"serialized value size in bytes (default {defaults.record_bytes})")
    loadgen.add_argument("--partitions", type=_positive_int, default=DEFAULT_PARTITIONS,
                         help="partition count recorded in the manifest; must match the topics")
    loadgen.add_argument("--key-cardinality", type=_positive_int, default=defaults.key_cardinality)
    loadgen.add_argument("--duration-s", type=_positive_int, default=defaults.duration_s,
                         help=f"run length in seconds, warm-up included (default {defaults.duration_s})")
    loadgen.add_argument("--warmup-s", type=int, default=defaults.warmup_s,
                         help=f"seconds excluded from statistics (default {defaults.warmup_s})")
    loadgen.add_argument("--dup-ratio", type=float, default=None,
                         help="share of first sends resent as duplicates "
                         f"(default {defaults.dup_ratio} for 01-dedup, 0 for every other pattern)")
    loadgen.add_argument("--dup-delay-ms-min", type=int, default=defaults.dup_delay_ms_min)
    loadgen.add_argument("--dup-delay-ms-max", type=int, default=defaults.dup_delay_ms_max,
                         help="resend delay drawn uniformly from [min, max]; must be below 60000")
    loadgen.add_argument("--seed", type=int, default=defaults.seed)
    loadgen.add_argument("--workers", type=_positive_int, default=None,
                         help="worker processes (default: derived from the rate)")
    loadgen.add_argument("--run-id", default=None, help="run identifier (default: generated from time)")
    loadgen.add_argument("--startup-delay-s", type=float, default=5.0,
                         help="delay before the common start time, so all workers begin together")
    loadgen.add_argument("--results-dir", default="results", help="directory for manifests (default results)")
    add_connection_args(loadgen)

    # collect
    collect = commands.add_parser(
        "collect",
        help="read one engine's output for a run and write its results line",
        description="Read rtmpack.<pattern>.out.<engine> for the run window and append a JSON line "
        "to results/<run_id>.jsonl.",
    )
    collect.add_argument("--run-id", required=True)
    collect.add_argument("--engine", required=True, choices=ENGINES)
    collect.add_argument("--engine-version", required=True, help="engine version string, recorded as is")
    collect.add_argument("--compute-desc", required=True, help="compute description, for example 'DBR 18.1 classic 8 workers'")
    collect.add_argument("--vcpu", required=True, type=_positive_int, help="total vCPU of the engine")
    collect.add_argument("--cost-per-hour-usd", required=True, type=float, help="cost of the engine per hour")
    collect.add_argument("--grace-s", type=int, default=None,
                         help="seconds read past the run end (default: max duplicate delay plus 60)")
    collect.add_argument("--results-dir", default="results")
    add_connection_args(collect)

    # refdata
    refdata = commands.add_parser(
        "refdata",
        help="write the reference data file of a pattern (05-lookup: merchants.csv)",
        description="Write the deterministic reference table of a pattern, for example "
        "patterns/05-lookup/data/merchants.csv for 05-lookup.",
    )
    refdata.add_argument("--pattern", required=True, type=_pattern_type, help="pattern id, for example 05-lookup")
    refdata.add_argument("--out", required=True, help="output file path")

    # report
    report = commands.add_parser("report", help="aggregate results/*.jsonl into a markdown table")
    report.add_argument("--results-dir", default="results")
    report.add_argument("--pattern", type=_pattern_type, default=None, help="only this pattern")
    report.add_argument("--out", default=None, help="write to this file instead of stdout")

    return parser


def cmd_topics(args: argparse.Namespace) -> int:
    from confluent_kafka.admin import AdminClient

    from rtmpack_harness.topics import create_topics, delete_topics

    conn = resolve_connection(args)
    admin = AdminClient(conn.kafka_conf())
    names = topic_names(args.pattern)
    if args.action == "create":
        outcome = create_topics(admin, args.pattern, args.partitions)
        for role, name in names.items():
            print(f"{role:<10} {name:<32} {outcome.get(name, '?')}")
        return 0

    if args.action == "retention":
        from rtmpack_harness.topics import set_retention

        if args.hours <= 0:
            raise HarnessError("--hours must be > 0")
        outcome = set_retention(admin, args.pattern, int(args.hours * 3_600_000))
        for role, name in names.items():
            print(f"{role:<10} {name:<32} {outcome.get(name, '?')}")
        return 0

    if not args.yes:
        raise HarnessError("topic deletion is destructive; pass --yes to confirm")
    outcome = delete_topics(admin, args.pattern)
    for role, name in names.items():
        print(f"{role:<10} {name:<32} {outcome.get(name, '?')}")
    return 0


def cmd_loadgen(args: argparse.Namespace) -> int:
    from rtmpack_harness.loadgen import run_loadgen
    from rtmpack_harness.patterns import get_pattern

    rules = get_pattern(args.pattern)
    params = RunParams(
        pattern=args.pattern,
        rate_eps=args.rate_eps or rules.default_rate_eps or RunParams().rate_eps,
        record_bytes=args.record_bytes,
        partitions=args.partitions,
        key_cardinality=args.key_cardinality,
        duration_s=args.duration_s,
        warmup_s=args.warmup_s,
        dup_ratio=rules.default_dup_ratio if args.dup_ratio is None else args.dup_ratio,
        dup_delay_ms_min=args.dup_delay_ms_min,
        dup_delay_ms_max=args.dup_delay_ms_max,
        seed=args.seed,
    ).validate()
    conn = resolve_connection(args)
    manifest = run_loadgen(
        params,
        conn,
        results_dir=args.results_dir,
        run_id=args.run_id,
        workers=args.workers,
        startup_delay_s=args.startup_delay_s,
    )
    print(manifest["run_id"])
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    from rtmpack_harness.collect import collect_run

    conn = resolve_connection(args)
    record, diagnostics = collect_run(
        conn,
        results_dir=args.results_dir,
        run_id=args.run_id,
        engine=args.engine,
        engine_version=args.engine_version,
        compute_desc=args.compute_desc,
        vcpu=args.vcpu,
        cost_per_hour_usd=args.cost_per_hour_usd,
        grace_s=args.grace_s,
    )
    print(json.dumps(record))
    print(json.dumps({"diagnostics": diagnostics}), file=sys.stderr)
    return 0


def cmd_refdata(args: argparse.Namespace) -> int:
    from rtmpack_harness.patterns import get_pattern

    rules = get_pattern(args.pattern)
    if rules.write_refdata is None:
        raise HarnessError(f"pattern {rules.id} has no reference data")
    rules.write_refdata(args.out)
    print(args.out)
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    from rtmpack_harness.report import build_report

    text = build_report(args.results_dir, pattern=args.pattern)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(text)
    else:
        sys.stdout.write(text)
    return 0


HANDLERS = {
    "topics": cmd_topics,
    "loadgen": cmd_loadgen,
    "collect": cmd_collect,
    "refdata": cmd_refdata,
    "report": cmd_report,
}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return HANDLERS[args.command](args)
    except HarnessError as exc:
        print(f"rtmpack: error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
