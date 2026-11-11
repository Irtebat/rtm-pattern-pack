"""Constants and parameter rules fixed by docs/CONTRACT.md.

Change these values only together with the contract.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


class HarnessError(RuntimeError):
    """Raised for user-facing errors (bad arguments, missing topics, missing runs)."""


ENGINES = ("rtm", "flink")

# Contract section 1: topic settings.
TOPIC_TIMESTAMP_TYPE = "LogAppendTime"
# 12 hours of retention.
TOPIC_RETENTION_MS = 43_200_000
TOPIC_REPLICATION_FACTOR = 3
DEFAULT_PARTITIONS = 8

# Contract section 1: credentials and region.
SECRET_ID = "AmazonMSK_rtmpack"
DEFAULT_REGION = "us-west-2"

# The state horizon for 01-dedup is 60 s (patterns/01-dedup/README.md, Benchmark setup).
# It must exceed the maximum duplicate delay.
STATE_HORIZON_MS = 60_000

# Pattern ids are the folder names under patterns/, for example 01-dedup or 06-tumbling.
_PATTERN_RE = re.compile(r"[0-9]{2}-[a-z][a-z0-9]*(-[a-z0-9]+)*")

RESULT_FIELDS = (
    "run_id",
    "pattern",
    "engine",
    "engine_version",
    "started_at",
    "rate_eps",
    "record_bytes",
    "partitions",
    "compute_desc",
    "vcpu",
    "cost_per_hour_usd",
    "out_records",
    "distinct_out_keys",
    "expected_distinct",
    "missing",
    "sink_duplicates",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "p999_ms",
    "max_ms",
    "throughput_eps",
    # Split-half stability check (contract section 8).
    "p50_ms_h1",
    "p99_ms_h1",
    "p50_ms_h2",
    "p99_ms_h2",
    "stable",
    # Value check (contract section 9); 0 for patterns without one.
    "value_errors",
)


def normalize_pattern(value: str) -> str:
    """Return the lower-case pattern id, or raise HarnessError."""
    pattern = value.strip().lower()
    if not _PATTERN_RE.fullmatch(pattern):
        raise HarnessError(f"invalid pattern id {value!r}; expected a form like 01-dedup")
    return pattern


def check_engine(engine: str) -> str:
    if engine not in ENGINES:
        raise HarnessError(f"engine must be one of {', '.join(ENGINES)}; got {engine!r}")
    return engine


# Contract section 9: patterns with several topics append a suffix to the base.
INPUT_SUFFIXES: dict[str, tuple[str, ...]] = {"08-join": ("orders", "payments")}
ROUTING_TYPES = ("payment", "order", "click", "login")
OUTPUT_SUFFIXES: dict[str, tuple[str, ...]] = {"04-routing": ROUTING_TYPES + ("all",)}


def input_topic(pattern: str) -> str:
    """Base name of the input topic(s)."""
    return f"rtmpack.{normalize_pattern(pattern)}.input"


def output_topic(pattern: str, engine: str) -> str:
    """Base name of the output topic(s) of one engine."""
    return f"rtmpack.{normalize_pattern(pattern)}.out.{check_engine(engine)}"


def input_topics(pattern: str) -> list[str]:
    """Every input topic of a pattern (the base name when there is no suffix)."""
    base = input_topic(pattern)
    suffixes = INPUT_SUFFIXES.get(normalize_pattern(pattern))
    return [f"{base}.{s}" for s in suffixes] if suffixes else [base]


def output_topics(pattern: str, engine: str) -> list[str]:
    """Every output topic one engine writes for a pattern."""
    base = output_topic(pattern, engine)
    suffixes = OUTPUT_SUFFIXES.get(normalize_pattern(pattern))
    return [f"{base}.{s}" for s in suffixes] if suffixes else [base]


def topic_suffix(topic: str, base: str) -> str | None:
    """The suffix of `topic` below `base`, or None when the topic is the base itself."""
    if topic == base:
        return None
    return topic[len(base) + 1:] if topic.startswith(base + ".") else None


def topic_names(pattern: str) -> dict[str, str]:
    """All input topics plus all output topics per engine, keyed by role."""
    pid = normalize_pattern(pattern)
    names: dict[str, str] = {}
    in_base = input_topic(pid)
    for topic in input_topics(pid):
        suffix = topic_suffix(topic, in_base)
        names["input" if suffix is None else f"input.{suffix}"] = topic
    for engine in ENGINES:
        out_base = output_topic(pid, engine)
        for topic in output_topics(pid, engine):
            suffix = topic_suffix(topic, out_base)
            names[f"out.{engine}" if suffix is None else f"out.{engine}.{suffix}"] = topic
    return names


@dataclass(frozen=True)
class RunParams:
    """Run parameters from contract section 5, with the contract defaults."""

    pattern: str = "01-dedup"
    rate_eps: int = 10_000
    record_bytes: int = 1024
    partitions: int = DEFAULT_PARTITIONS
    key_cardinality: int = 100_000
    duration_s: int = 1800
    warmup_s: int = 300
    dup_ratio: float = 0.2
    dup_delay_ms_min: int = 0
    dup_delay_ms_max: int = 30_000
    seed: int = 42

    def validate(self) -> "RunParams":
        normalize_pattern(self.pattern)
        if self.rate_eps <= 0:
            raise HarnessError("rate_eps must be positive")
        if self.record_bytes < 160:
            raise HarnessError("record_bytes must be at least 160 to fit the event envelope")
        if self.partitions < 1:
            raise HarnessError("partitions must be at least 1")
        if self.key_cardinality < 1:
            raise HarnessError("key_cardinality must be at least 1")
        if self.duration_s <= 0:
            raise HarnessError("duration_s must be positive")
        if not 0 <= self.warmup_s < self.duration_s:
            raise HarnessError("warmup_s must be >= 0 and smaller than duration_s")
        if not 0.0 <= self.dup_ratio <= 1.0:
            raise HarnessError("dup_ratio must be between 0 and 1")
        if not 0 <= self.dup_delay_ms_min <= self.dup_delay_ms_max:
            raise HarnessError("dup_delay_ms must satisfy 0 <= min <= max")
        if self.dup_delay_ms_max >= STATE_HORIZON_MS:
            raise HarnessError(
                f"dup_delay_ms_max must be below the {STATE_HORIZON_MS} ms state horizon"
            )
        return self

    def as_dict(self) -> dict:
        return {
            "rate_eps": self.rate_eps,
            "record_bytes": self.record_bytes,
            "partitions": self.partitions,
            "key_cardinality": self.key_cardinality,
            "duration_s": self.duration_s,
            "warmup_s": self.warmup_s,
            "dup_ratio": self.dup_ratio,
            "dup_delay_ms_min": self.dup_delay_ms_min,
            "dup_delay_ms_max": self.dup_delay_ms_max,
            "seed": self.seed,
        }
