"""The `Pattern` record and helpers shared by several patterns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from rtmpack_harness.contract import HarnessError, RunParams

# Smallest record that fits the shared event plus a few pattern fields.
EXTENDED_MIN_RECORD_BYTES = 256
AMOUNT_MIN = 100
AMOUNT_MAX = 50_000


@dataclass(frozen=True)
class Pattern:
    """Everything the harness needs to know about one pattern.

    Required fields describe the input and the expected output. The optional
    fields are only needed by patterns that use the feature named.
    """

    # ---- required
    id: str
    description: str
    # make_generator(seed, worker_id, key_lo, key_hi, key_cardinality,
    #                dup_ratio, dup_delay_ms_min, dup_delay_ms_max)
    make_generator: Callable[..., Any]
    # encode(event, record_bytes) -> bytes of exactly record_bytes.
    encode: Callable[[Any, int], bytes]
    # output_keys(event) -> Kafka keys of the output records one input must produce.
    output_keys: Callable[[Any], list[bytes]]
    # Manifest count that the collector uses as expected_distinct.
    expected_count_field: str
    min_record_bytes: int

    # ---- optional: value check
    # check_value(topic_suffix, output_doc) -> True when the value is correct.
    check_value: Optional[Callable[[Optional[str], dict], bool]] = None

    # ---- optional: duplicates (resends of the same event)
    # as_duplicate(event) -> the resend. None: the pattern has no duplicates.
    as_duplicate: Optional[Callable[[Any], Any]] = None
    default_dup_ratio: float = 0.0

    # ---- optional: several topics
    # Topic suffixes, written as <base>.<suffix>. Empty: one input or output topic.
    input_suffixes: tuple[str, ...] = ()
    output_suffixes: tuple[str, ...] = ()
    # route(event) -> input topic suffix of an event (patterns with input_suffixes).
    route: Optional[Callable[[Any], Optional[str]]] = None
    # follow_up(event, send_ts_ms) -> a second record sent later, after the delay
    # returned by the generator's draw_duplicate_delay_ms (used instead of as_duplicate).
    follow_up: Optional[Callable[[Any, int], Any]] = None

    # ---- optional: windowed aggregates
    # "record": one expected output key per event. "aggregate": updates per (key, window).
    kind: str = "record"
    # windows(event_ts) -> window start times (aggregate patterns only).
    windows: Optional[Callable[[int], tuple[int, ...]]] = None

    # ---- optional: rate, manifest and reference data
    # Default for --rate-eps when the flag is not given (None: the contract default of 10000).
    default_rate_eps: Optional[int] = None
    # What rate_eps counts, recorded in the manifest.
    rate_unit: str = "events"
    # manifest_counts(totals) -> extra named entries for the manifest counts.
    manifest_counts: Optional[Callable[[dict], dict]] = None
    # write_refdata(path) writes the pattern's reference data file.
    write_refdata: Optional[Callable[[str], None]] = None

    @property
    def is_aggregate(self) -> bool:
        return self.kind == "aggregate"

    def outputs_per_event(self, event) -> int:
        """Number of output records one first-send event must produce."""
        return len(self.output_keys(event))

    def expected_distinct(self, manifest: dict) -> int:
        counts = manifest["counts"]
        if self.expected_count_field not in counts:
            raise HarnessError(
                f"manifest of run {manifest.get('run_id')!r} has no counts.{self.expected_count_field}"
            )
        return int(counts[self.expected_count_field])

    def validate(self, params: RunParams) -> None:
        if params.record_bytes < self.min_record_bytes:
            raise HarnessError(
                f"record_bytes must be at least {self.min_record_bytes} for pattern {self.id}"
            )
        if self.as_duplicate is None and params.dup_ratio != 0:
            raise HarnessError(f"pattern {self.id} has no duplicates; dup_ratio must be 0")


def event_id_key(event) -> list[bytes]:
    """One output record per input, keyed by the input's event_id."""
    return [event.event_id.encode("utf-8")]


def aggregate_key(key: str, window_start_ms: int) -> bytes:
    """Kafka key of one aggregate output: `<key>:<window_start_ms>`."""
    return f"{key}:{window_start_ms}".encode("utf-8")
