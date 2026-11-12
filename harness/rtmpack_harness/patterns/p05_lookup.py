"""05-lookup: enrich each event from a 10,000 row merchant table that the harness also writes."""

from __future__ import annotations

import hashlib
import json
from typing import Optional

from rtmpack_harness.events import EventGenerator, event_encoder
from rtmpack_harness.patterns.base import EXTENDED_MIN_RECORD_BYTES, Pattern, event_id_key

MERCHANT_COUNT = 10_000
_RISK_TIERS = ("low", "medium", "high")
_CATEGORIES = ("grocery", "fuel", "travel", "electronics", "dining", "apparel", "health", "digital")
_COUNTRIES = ("US", "GB", "DE", "FR", "IN", "JP", "BR", "CA", "AU", "SG")
MERCHANTS_HEADER = "merchant_id,risk_tier,category,country"


def merchant_row(i: int) -> tuple[str, str, str, str]:
    """05-lookup rule (patterns/05-lookup/README.md, Benchmark setup): (merchant_id, risk_tier, category, country) of merchant i."""
    h = int.from_bytes(hashlib.sha256(f"merchant|{i}".encode("utf-8")).digest()[:4], "big")
    return (f"m-{i:05d}", _RISK_TIERS[h % 3], _CATEGORIES[(h >> 8) % 8], _COUNTRIES[(h >> 16) % 10])


_MERCHANT_TABLE: dict[str, tuple[str, str, str]] = {}


def merchant_table() -> dict[str, tuple[str, str, str]]:
    """merchant_id -> (risk_tier, category, country), built once per process."""
    if not _MERCHANT_TABLE:
        for i in range(MERCHANT_COUNT):
            mid, tier, cat, country = merchant_row(i)
            _MERCHANT_TABLE[mid] = (tier, cat, country)
    return _MERCHANT_TABLE


def write_merchants_csv(path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        handle.write(MERCHANTS_HEADER + "\n")
        for i in range(MERCHANT_COUNT):
            handle.write(",".join(merchant_row(i)) + "\n")


class LookupGenerator(EventGenerator):
    pattern_id = "05-lookup"

    def _extra(self) -> dict:
        return {"merchant_id": f"m-{self._rng.randrange(MERCHANT_COUNT):05d}"}


def _lookup_check(suffix: Optional[str], doc: dict) -> bool:
    row = merchant_table().get(doc.get("merchant_id"))
    if row is None:
        return False
    return (doc.get("risk_tier"), doc.get("category"), doc.get("country")) == row


PATTERN = Pattern(
    id="05-lookup",
    description="lookup join with a 10,000 row reference table",
    make_generator=LookupGenerator,
    encode=event_encoder(lambda e: ',"merchant_id":' + json.dumps(e.merchant_id)),
    output_keys=event_id_key,
    expected_count_field="unique_sent",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_lookup_check,
    write_refdata=write_merchants_csv,
)
