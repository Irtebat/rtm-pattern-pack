"""08-join: inner stream-stream join of orders and payments on two input topics.

Uses its own event shapes. Each order may get a payment, sent later through the
same delayed-resend path that 01-dedup uses for duplicates (`follow_up`).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Optional

from rtmpack_harness.events import KeyedGenerator, pad_value
from rtmpack_harness.patterns.base import AMOUNT_MAX, AMOUNT_MIN, EXTENDED_MIN_RECORD_BYTES, Pattern

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


PATTERN = Pattern(
    id="08-join",
    description="inner stream-stream join of orders and payments",
    make_generator=JoinGenerator,
    encode=_join_encode,
    output_keys=lambda order: [] if order.pay_delay_ms is None else [order.order_id.encode("utf-8")],
    expected_count_field="expected_matches",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    default_rate_eps=JOIN_ORDER_RATE_EPS,
    rate_unit="orders",
    input_suffixes=("orders", "payments"),
    route=lambda event: "payments" if isinstance(event, JPayment) else "orders",
    follow_up=join_payment,
    check_value=_join_check,
    manifest_counts=_join_manifest_counts,
)
