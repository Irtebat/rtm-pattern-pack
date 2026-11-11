"""Pattern registry.

Everything that differs between patterns lives here: how first-send events are
generated and encoded, how many output records an event should produce, what the
output Kafka key is, and which manifest count the collector compares against.
The loader and the collector look a pattern up by the id in the manifest or the
command line and otherwise stay pattern-agnostic.

To add a pattern, subclass a generator from events.py (or write one with the
same `next_unique` and `draw_duplicate_delay_ms` methods), write an encoder, and
register a `Pattern` in `PATTERNS` (see the README).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

from rtmpack_harness.contract import ROUTING_TYPES, HarnessError, RunParams, normalize_pattern
from rtmpack_harness.events import (
    DedupGenerator,
    EventGenerator,
    KeyedGenerator,
    as_duplicate,
    encode_value,
    event_encoder,
    pad_value,
)


@dataclass(frozen=True)
class Pattern:
    """Per-pattern rules. Callables take and return plain events and counts."""

    id: str
    description: str
    # make_generator(seed, worker_id, key_lo, key_hi, key_cardinality,
    #                dup_ratio, dup_delay_ms_min, dup_delay_ms_max)
    make_generator: Callable[..., Any]
    encode: Callable[[Any, int], bytes]
    # Expected Kafka keys of the output records one first-send event must produce.
    output_keys: Callable[[Any], list[bytes]]
    # Manifest count that the collector uses as expected_distinct.
    expected_count_field: str
    min_record_bytes: int
    # as_duplicate(event) -> the resend of an event. None: the pattern has no duplicates.
    as_duplicate: Optional[Callable[[Any], Any]] = None
    default_dup_ratio: float = 0.0
    # "record": one expected output key per event. "aggregate": updates per (key, window).
    kind: str = "record"
    # Default for --rate-eps when the flag is not given (None: the contract default of 10000).
    default_rate_eps: Optional[int] = None
    # What rate_eps counts, recorded in the manifest.
    rate_unit: str = "events"
    # route(event) -> topic suffix for patterns with several input topics (None: base topic).
    route: Optional[Callable[[Any], Optional[str]]] = None
    # follow_up(event, send_ts_ms) -> record sent later from the delayed-resend heap
    # (used instead of as_duplicate). The delay comes from draw_duplicate_delay_ms.
    follow_up: Optional[Callable[[Any, int], Any]] = None
    # windows(event_ts) -> window start times (aggregate patterns only).
    windows: Optional[Callable[[int], tuple[int, ...]]] = None
    # check_value(topic_suffix, output_doc) -> True when the value is correct.
    check_value: Optional[Callable[[Optional[str], dict], bool]] = None
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


# ---------------------------------------------------------------- 01-dedup

def _event_id_key(event) -> list[bytes]:
    return [event.event_id.encode("utf-8")]


DEDUP = Pattern(
    id="01-dedup",
    description="deduplication by event_id",
    make_generator=DedupGenerator,
    encode=encode_value,
    output_keys=_event_id_key,
    expected_count_field="unique_sent",
    min_record_bytes=160,
    as_duplicate=as_duplicate,
    default_dup_ratio=0.2,
)


# ---------------------------------------------------------------- 02-unnest

UNNEST_MIN_ITEMS = 1
UNNEST_MAX_ITEMS = 5
_UNNEST_SKU_COUNT = 10_000


@dataclass(frozen=True)
class Item:
    sku: str
    qty: int
    price: float


@dataclass(frozen=True)
class Order:
    order_id: str
    key: str
    event_ts: int
    items: tuple[Item, ...]
    is_dup: bool = False


def unnest_output_key(order_id: str, item_idx: int) -> bytes:
    """Kafka key of one unnested output record (patterns/02-unnest/README.md, Benchmark setup)."""
    return f"{order_id}:{item_idx}".encode("utf-8")


def _unnest_output_keys(order: Order) -> list[bytes]:
    return [unnest_output_key(order.order_id, i) for i in range(len(order.items))]


def encode_order(order: Order, record_bytes: int) -> bytes:
    """Serialize an order as compact JSON padded to exactly `record_bytes` bytes."""
    items = ",".join(
        '{"sku":' + json.dumps(i.sku) + f',"qty":{i.qty},"price":{json.dumps(i.price)}' + "}"
        for i in order.items
    )
    head = (
        '{"order_id":"' + order.order_id + '","key":' + json.dumps(order.key)
        + f',"event_ts":{order.event_ts},"items":[' + items + '],"payload":"'
    )
    return pad_value(head, record_bytes)


class OrderGenerator(KeyedGenerator):
    """Orders with 1 to 5 items. Draws, in fixed order: key index, order id, item count, items."""

    pattern_id = "02-unnest"
    key_prefix = "cust"

    def next_unique(self, event_ts: int) -> Order:
        rng = self._rng
        _, key = self._draw_key()
        order_id = self._draw_uuid()
        n_items = rng.randint(UNNEST_MIN_ITEMS, UNNEST_MAX_ITEMS)
        items = tuple(
            Item(
                sku=f"SKU-{rng.randrange(_UNNEST_SKU_COUNT):06d}",
                qty=rng.randint(1, 10),
                price=round(rng.uniform(1.0, 500.0), 2),
            )
            for _ in range(n_items)
        )
        return Order(order_id=order_id, key=key, event_ts=event_ts, items=items)


UNNEST = Pattern(
    id="02-unnest",
    description="array unnest, one output record per order item",
    make_generator=OrderGenerator,
    encode=encode_order,
    output_keys=_unnest_output_keys,
    expected_count_field="expected_outputs",
    min_record_bytes=400,
)


# ------------------------------------------------- shared by patterns 03 to 08

EXTENDED_MIN_RECORD_BYTES = 256
AMOUNT_MIN = 100
AMOUNT_MAX = 50_000
MERCHANT_COUNT = 10_000
ROUTING_WEIGHTS = (0.4, 0.3, 0.2, 0.1)  # payment, order, click, login


# ---------------------------------------------------------------- 03-udf

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


UDF = Pattern(
    id="03-udf",
    description="scalar UDF scoring",
    make_generator=UdfGenerator,
    encode=encode_value,
    output_keys=_event_id_key,
    expected_count_field="unique_sent",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_udf_check,
)


# ---------------------------------------------------------------- 04-routing

class RoutingGenerator(EventGenerator):
    pattern_id = "04-routing"

    def _extra(self) -> dict:
        u = self._rng.random()
        acc = 0.0
        for name, weight in zip(ROUTING_TYPES, ROUTING_WEIGHTS):
            acc += weight
            if u < acc:
                return {"type": name}
        return {"type": ROUTING_TYPES[-1]}


def _routing_output_keys(event) -> list[bytes]:
    # One record in the type topic and one in the all topic; both keyed by event_id.
    return [event.event_id.encode("utf-8")] * 2


def _routing_check(suffix: Optional[str], doc: dict) -> bool:
    if suffix is None or suffix == "all":
        return True
    return doc.get("type") == suffix


ROUTING = Pattern(
    id="04-routing",
    description="demux by type and fan-out to an all-events topic",
    make_generator=RoutingGenerator,
    encode=event_encoder(lambda e: ',"type":' + json.dumps(e.type)),
    output_keys=_routing_output_keys,
    expected_count_field="expected_outputs",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_routing_check,
)


# ---------------------------------------------------------------- 05-lookup

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


LOOKUP = Pattern(
    id="05-lookup",
    description="lookup join with a 10,000 row reference table",
    make_generator=LookupGenerator,
    encode=event_encoder(lambda e: ',"merchant_id":' + json.dumps(e.merchant_id)),
    output_keys=_event_id_key,
    expected_count_field="unique_sent",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_lookup_check,
    write_refdata=write_merchants_csv,
)


# ------------------------------------------------------------ 06-tumbling and 07-sliding

TUMBLE_MS = 60_000
SLIDE_MS = 20_000


def tumbling_windows(event_ts: int) -> tuple[int, ...]:
    """06-tumbling rule (patterns/06-tumbling/README.md, Benchmark setup): the 60 s tumbling window start, aligned to the epoch."""
    return (event_ts // TUMBLE_MS * TUMBLE_MS,)


def sliding_windows(event_ts: int) -> tuple[int, ...]:
    """07-sliding rule (patterns/07-sliding/README.md, Benchmark setup): starts of the three 60 s windows (20 s slide) holding event_ts."""
    last = event_ts // SLIDE_MS * SLIDE_MS
    return (last - 2 * SLIDE_MS, last - SLIDE_MS, last)


class AmountGenerator(EventGenerator):
    def _extra(self) -> dict:
        return {"amount": self._rng.randint(AMOUNT_MIN, AMOUNT_MAX)}


class TumblingGenerator(AmountGenerator):
    pattern_id = "06-tumbling"


class SlidingGenerator(AmountGenerator):
    pattern_id = "07-sliding"


def aggregate_key(key: str, window_start_ms: int) -> bytes:
    """Kafka key of one aggregate output: `<key>:<window_start_ms>`."""
    return f"{key}:{window_start_ms}".encode("utf-8")


def _aggregate_pattern(pid: str, description: str, generator, windows) -> Pattern:
    return Pattern(
        id=pid,
        description=description,
        make_generator=generator,
        encode=event_encoder(lambda e: f',"amount":{e.amount}'),
        output_keys=lambda event: [aggregate_key(event.key, w) for w in windows(event.event_ts)],
        expected_count_field="expected_pairs",
        min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
        kind="aggregate",
        windows=windows,
    )


TUMBLING = _aggregate_pattern("06-tumbling", "tumbling window count and sum per key", TumblingGenerator, tumbling_windows)
SLIDING = _aggregate_pattern("07-sliding", "sliding window count and sum per key", SlidingGenerator, sliding_windows)


# ---------------------------------------------------------------- 08-join

JOIN_ORDER_RATE_EPS = 5_000
JOIN_PAID_SHARE = 0.9
JOIN_PAY_DELAY_MAX_MS = 5_000
_PAY_METHODS = ("card", "wallet", "bank")


@dataclass(frozen=True)
class JOrder:
    order_id: str
    key: str
    event_ts: int
    amount: int
    # Delay of the payment in ms, or None when the order is never paid.
    pay_delay_ms: Optional[int] = None


@dataclass(frozen=True)
class JPayment:
    payment_id: str
    order_id: str
    key: str  # the order's key, used as the Kafka key; not part of the JSON value
    event_ts: int
    method: str


def join_payment_id(order_id: str) -> str:
    """The payment id of an order: a UUID derived from the order id (08-join value check, patterns/08-join/README.md, Benchmark setup)."""
    h = hashlib.sha256(("payment|" + order_id).encode("utf-8")).digest()
    return str(uuid.UUID(bytes=h[:16], version=4))


def join_payment(order: JOrder, send_ts_ms: int) -> JPayment:
    h = hashlib.sha256(("payment|" + order.order_id).encode("utf-8")).digest()
    return JPayment(
        payment_id=join_payment_id(order.order_id), order_id=order.order_id, key=order.key,
        # The payment must not precede its order. The order's event_ts is rounded to the
        # millisecond, so the payment's event_ts is clamped to at least the order's event_ts.
        event_ts=max(int(send_ts_ms), int(order.event_ts)), method=_PAY_METHODS[h[16] % 3],
    )


def encode_join_order(order: JOrder, record_bytes: int) -> bytes:
    head = (
        '{"order_id":"' + order.order_id + '","key":' + json.dumps(order.key)
        + f',"event_ts":{order.event_ts},"amount":{order.amount},"payload":"'
    )
    return pad_value(head, record_bytes)


def encode_join_payment(payment: JPayment, record_bytes: int) -> bytes:
    head = (
        '{"payment_id":"' + payment.payment_id + '","order_id":"' + payment.order_id
        + f'","event_ts":{payment.event_ts},"method":"{payment.method}","payload":"'
    )
    return pad_value(head, record_bytes)


def _join_encode(event, record_bytes: int) -> bytes:
    if isinstance(event, JPayment):
        return encode_join_payment(event, record_bytes)
    return encode_join_order(event, record_bytes)


class JoinGenerator(KeyedGenerator):
    """Orders for the join pattern; each is paid with probability 0.9 after 0 to 5000 ms.

    Draws, in fixed order: key index, order id, amount, paid, delay.
    `draw_duplicate_delay_ms` returns the payment delay of the order just
    generated (None when unpaid), so the load worker's resend heap sends the
    payment. The payment content is derived from the order, so it needs no state.
    """

    pattern_id = "08-join"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._last_delay: Optional[int] = None

    def next_unique(self, event_ts: int) -> JOrder:
        rng = self._rng
        _, key = self._draw_key()
        order_id = self._draw_uuid()
        amount = rng.randint(AMOUNT_MIN, AMOUNT_MAX)
        paid = rng.random() < JOIN_PAID_SHARE
        delay = rng.randint(0, JOIN_PAY_DELAY_MAX_MS)  # always drawn, so the stream stays aligned
        self._last_delay = delay if paid else None
        return JOrder(order_id=order_id, key=key, event_ts=event_ts, amount=amount, pay_delay_ms=self._last_delay)

    def draw_duplicate_delay_ms(self) -> Optional[int]:
        return self._last_delay


def _join_check(suffix: Optional[str], doc: dict) -> bool:
    order_id = doc.get("order_id")
    return isinstance(order_id, str) and doc.get("payment_id") == join_payment_id(order_id)


def _join_manifest_counts(totals: dict) -> dict:
    return {
        "orders_sent": totals["unique_sent"],
        "payments_sent": totals["follow_up_sent"],
        "expected_matches": totals["expected_outputs"],
    }


JOIN = Pattern(
    id="08-join",
    description="inner stream-stream join of orders and payments",
    make_generator=JoinGenerator,
    encode=_join_encode,
    output_keys=lambda order: [] if order.pay_delay_ms is None else [order.order_id.encode("utf-8")],
    expected_count_field="expected_matches",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    default_rate_eps=JOIN_ORDER_RATE_EPS,
    rate_unit="orders",
    route=lambda event: "payments" if isinstance(event, JPayment) else "orders",
    follow_up=join_payment,
    check_value=_join_check,
    manifest_counts=_join_manifest_counts,
)


PATTERNS: dict[str, Pattern] = {p.id: p for p in (DEDUP, UNNEST, UDF, ROUTING, LOOKUP, TUMBLING, SLIDING, JOIN)}


def get_pattern(pattern_id: str) -> Pattern:
    pid = normalize_pattern(pattern_id)
    try:
        return PATTERNS[pid]
    except KeyError:
        raise HarnessError(
            f"pattern {pid!r} is not supported by the harness; supported: {', '.join(sorted(PATTERNS))}"
        ) from None
