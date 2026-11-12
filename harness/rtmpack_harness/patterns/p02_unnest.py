"""02-unnest: orders with 1 to 5 items; one output record per item. Uses its own event shape."""

from __future__ import annotations

import json
from dataclasses import dataclass

from rtmpack_harness.events import KeyedGenerator, pad_value
from rtmpack_harness.patterns.base import Pattern

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


PATTERN = Pattern(
    id="02-unnest",
    description="array unnest, one output record per order item",
    make_generator=OrderGenerator,
    encode=encode_order,
    output_keys=_unnest_output_keys,
    expected_count_field="expected_outputs",
    min_record_bytes=400,
)
