"""01-dedup: deduplication by event_id. The only pattern whose input has duplicates."""

from rtmpack_harness.events import DedupGenerator, as_duplicate, encode_value
from rtmpack_harness.patterns.base import Pattern, event_id_key

PATTERN = Pattern(
    id="01-dedup",
    description="deduplication by event_id",
    make_generator=DedupGenerator,
    encode=encode_value,
    output_keys=event_id_key,
    expected_count_field="unique_sent",
    min_record_bytes=160,
    as_duplicate=as_duplicate,
    default_dup_ratio=0.2,
)
