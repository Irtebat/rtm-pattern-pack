"""Pattern registry.

Each pattern is one module in this folder, named pNN_<name>.py, that defines
`PATTERN = Pattern(...)`. Modules are found automatically, so adding a pattern
means adding one file (start from _template.py). `base.py` holds the Pattern
record and helpers shared by several patterns.

The loader and the collector look a pattern up by id with get_pattern and
otherwise stay pattern-agnostic.
"""

from __future__ import annotations

import importlib
import pkgutil

from rtmpack_harness.contract import HarnessError, normalize_pattern
from rtmpack_harness.patterns.base import Pattern, aggregate_key, event_id_key


def _discover() -> dict[str, Pattern]:
    found: dict[str, Pattern] = {}
    for info in pkgutil.iter_modules(__path__):
        if info.name.startswith("_") or info.name in ("base", "windows"):
            continue
        module = importlib.import_module(f"{__name__}.{info.name}")
        pattern = getattr(module, "PATTERN", None)
        if pattern is None:
            continue
        if pattern.id in found:
            raise HarnessError(f"pattern id {pattern.id!r} is defined twice (in {info.name})")
        found[pattern.id] = pattern
    return dict(sorted(found.items()))


PATTERNS: dict[str, Pattern] = _discover()


def get_pattern(pattern_id: str) -> Pattern:
    pid = normalize_pattern(pattern_id)
    try:
        return PATTERNS[pid]
    except KeyError:
        raise HarnessError(
            f"pattern {pid!r} is not supported by the harness; supported: {', '.join(PATTERNS)}"
        ) from None


# Names used by tests and tools, kept importable from rtmpack_harness.patterns.
from rtmpack_harness.patterns.p01_dedup import PATTERN as DEDUP  # noqa: E402
from rtmpack_harness.patterns.p02_unnest import (  # noqa: E402
    PATTERN as UNNEST, Item, Order, OrderGenerator, encode_order, unnest_output_key,
    UNNEST_MAX_ITEMS, UNNEST_MIN_ITEMS,
)
from rtmpack_harness.patterns.p03_udf import PATTERN as UDF, udf_decision, udf_score  # noqa: E402
from rtmpack_harness.patterns.p04_routing import PATTERN as ROUTING, ROUTING_TYPES, ROUTING_WEIGHTS  # noqa: E402
from rtmpack_harness.patterns.p05_lookup import (  # noqa: E402
    PATTERN as LOOKUP, MERCHANT_COUNT, MERCHANTS_HEADER, merchant_row, merchant_table, write_merchants_csv,
)
from rtmpack_harness.patterns.p06_tumbling import PATTERN as TUMBLING, TUMBLE_MS, tumbling_windows  # noqa: E402
from rtmpack_harness.patterns.p07_sliding import PATTERN as SLIDING, SLIDE_MS, sliding_windows  # noqa: E402
from rtmpack_harness.patterns.p08_join import (  # noqa: E402
    PATTERN as JOIN, JOIN_ORDER_RATE_EPS, JOIN_PAID_SHARE, JOIN_PAY_DELAY_MAX_MS, JOrder, JPayment,
    join_payment, join_payment_id,
)
from rtmpack_harness.patterns.base import AMOUNT_MAX, AMOUNT_MIN, EXTENDED_MIN_RECORD_BYTES  # noqa: E402
