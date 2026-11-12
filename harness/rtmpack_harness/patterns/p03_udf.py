"""03-udf: score each event with a SHA-256 based rule; the collector recomputes the score."""

from __future__ import annotations

import hashlib
from typing import Optional

from rtmpack_harness.events import EventGenerator, encode_value
from rtmpack_harness.patterns.base import EXTENDED_MIN_RECORD_BYTES, Pattern, event_id_key


def udf_score(event_id: str, key: str) -> int:
    """03-udf rule (patterns/03-udf/README.md, Benchmark setup): first 4 bytes of SHA-256(event_id|key), big endian, mod 1000."""
    h = hashlib.sha256((event_id + "|" + key).encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % 1000


def udf_decision(score: int) -> str:
    return "flag" if score >= 950 else "review" if score >= 800 else "approve"


class UdfGenerator(EventGenerator):
    pattern_id = "03-udf"


def _udf_check(suffix: Optional[str], doc: dict) -> bool:
    try:
        score = udf_score(doc["event_id"], doc["key"])
        return doc["score"] == score and doc["decision"] == udf_decision(score)
    except (KeyError, TypeError):
        return False


PATTERN = Pattern(
    id="03-udf",
    description="scalar UDF scoring",
    make_generator=UdfGenerator,
    encode=encode_value,
    output_keys=event_id_key,
    expected_count_field="unique_sent",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_udf_check,
)
