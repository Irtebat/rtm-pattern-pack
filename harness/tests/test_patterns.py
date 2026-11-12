"""Pattern registry, generators, encoders, side files and per-pattern correctness rules (01-dedup to 08-join)."""

import hashlib
import json
from collections import Counter

import numpy as np
import pytest

from rtmpack_harness.cli import main
from rtmpack_harness.contract import (
    HarnessError,
    RESULT_FIELDS,
    RunParams,
    input_topics,
    output_topics,
    topic_names,
)
from rtmpack_harness.events import Event
from rtmpack_harness.ingest import OutputIngest, make_accumulator
from rtmpack_harness.patterns import (
    DEDUP,
    JOIN,
    LOOKUP,
    PATTERNS,
    ROUTING,
    TUMBLING,
    UDF,
    UNNEST,
    OrderGenerator,
    encode_order,
    get_pattern,
    join_payment_id,
    merchant_row,
    sliding_windows,
    tumbling_windows,
    udf_decision,
    udf_score,
    unnest_output_key,
)
from rtmpack_harness.results import expected_path, load_expected
from rtmpack_harness.stats import AggregateAccumulator, ExpectedBuilder, ExpectedPairs, OutputAccumulator, pair_digest

from helpers import summarize_output

from test_loadgen import _run, _spec


def _orders(n, seed=42, worker_id=0, card=1000, lo=0, hi=1000):
    gen = OrderGenerator(seed, worker_id, lo, hi, card)
    return [gen.next_unique(1_700_000_000_000 + i) for i in range(n)]


def test_registry_lookup():
    assert get_pattern("01-DEDUP") is DEDUP
    assert get_pattern("02-unnest") is UNNEST
    assert set(PATTERNS) == {"01-dedup", "02-unnest", "03-udf", "04-routing", "05-lookup", "06-tumbling", "07-sliding", "08-join"}
    with pytest.raises(HarnessError, match="not supported"):
        get_pattern("99-none")
    with pytest.raises(HarnessError, match="invalid pattern id"):
        get_pattern("dedup")


def test_unnest_same_seed_gives_identical_orders_and_other_seed_differs():
    assert _orders(300) == _orders(300)
    assert _orders(300, seed=1) != _orders(300, seed=2)
    assert _orders(50, worker_id=0) != _orders(50, worker_id=1)


def test_unnest_item_counts_stay_in_one_to_five_and_cover_all_values():
    counts = Counter(len(o.items) for o in _orders(5000))
    assert set(counts) == {1, 2, 3, 4, 5}
    for n in range(1, 6):  # uniform: each value near 20 percent
        assert 800 < counts[n] < 1200


def test_unnest_items_have_valid_fields():
    for order in _orders(200):
        assert order.key.startswith("cust-")
        for item in order.items:
            assert item.sku.startswith("SKU-")
            assert 1 <= item.qty <= 10
            assert 1.0 <= item.price <= 500.0


@pytest.mark.parametrize("size", [400, 1024, 4096])
def test_unnest_value_is_exactly_record_bytes_and_matches_the_contract_schema(size):
    for order in _orders(300):
        raw = encode_order(order, size)
        assert len(raw) == size
        doc = json.loads(raw)
        assert list(doc) == ["order_id", "key", "event_ts", "items", "payload"]
        assert doc["order_id"] == order.order_id
        assert doc["event_ts"] == order.event_ts
        assert [set(i) for i in doc["items"]] == [{"sku", "qty", "price"}] * len(order.items)
        assert [i["sku"] for i in doc["items"]] == [i.sku for i in order.items]


def test_unnest_rejects_record_bytes_too_small_for_the_envelope():
    biggest = max(_orders(500), key=lambda o: len(o.items))
    with pytest.raises(HarnessError):
        encode_order(biggest, 100)


def test_unnest_has_no_duplicates():
    gen = OrderGenerator(42, 0, 0, 10, 10)
    gen.next_unique(1)
    assert gen.draw_duplicate_delay_ms() is None
    with pytest.raises(HarnessError):
        UNNEST.validate(RunParams(pattern="02-unnest", record_bytes=1024, dup_ratio=0.2))
    UNNEST.validate(RunParams(pattern="02-unnest", record_bytes=1024, dup_ratio=0.0))
    with pytest.raises(HarnessError):
        UNNEST.validate(RunParams(pattern="02-unnest", record_bytes=300, dup_ratio=0.0))


def test_unnest_output_key_math():
    order = _orders(1)[0]
    assert unnest_output_key("abc", 0) == b"abc:0"
    assert UNNEST.output_keys(order) == [f"{order.order_id}:{i}".encode() for i in range(len(order.items))]
    assert UNNEST.outputs_per_event(order) == len(order.items)
    assert DEDUP.outputs_per_event(Event("e-1", "user-00001", 1, 0)) == 1


def test_unnest_worker_counts_orders_and_expected_outputs():
    spec = _spec(pattern="02-unnest", topic="rtmpack.02-unnest.input", record_bytes=1024, dup_ratio=0.0)
    worker, producer = _run(spec)
    docs = [json.loads(r["value"]) for r in producer.records]
    assert worker.stats.unique_sent == len(docs) == 400
    assert worker.stats.dup_sent == 0
    assert worker.stats.outputs_sent == sum(len(d["items"]) for d in docs)
    assert 400 <= worker.stats.outputs_sent <= 2000
    assert len({d["order_id"] for d in docs}) == 400
    assert all(len(r["value"]) == 1024 for r in producer.records)


def test_unnest_worker_is_deterministic():
    spec = _spec(pattern="02-unnest", record_bytes=1024, dup_ratio=0.0, seed=9)
    assert [r["value"] for r in _run(spec)[1].records] == [r["value"] for r in _run(spec)[1].records]


def test_expected_distinct_comes_from_the_pattern_count_field():
    manifest = {"run_id": "r", "counts": {"unique_sent": 100, "expected_outputs": 300}}
    assert DEDUP.expected_distinct(manifest) == 100
    assert UNNEST.expected_distinct(manifest) == 300
    with pytest.raises(HarnessError):
        UNNEST.expected_distinct({"run_id": "r", "counts": {"unique_sent": 100}})


def test_unnest_correctness_is_computed_on_output_keys():
    orders = _orders(40)
    expected = sum(len(o.items) for o in orders)
    records = []
    for n, order in enumerate(orders):
        for key in UNNEST.output_keys(order):
            records.append((key, 1_000 + n, 1_050 + n))
    out = summarize_output(records, 0, 10_000, expected, measured_s=10.0)
    assert out["out_records"] == expected
    assert out["distinct_out_keys"] == expected
    assert out["missing"] == 0 and out["sink_duplicates"] == 0
    assert out["throughput_eps"] == round(expected / 10.0, 2)
    # Drop one output and resend another: both are visible on the key level.
    records = records[1:] + [records[5]]
    out = summarize_output(records, 0, 10_000, expected, measured_s=10.0)
    assert out["missing"] == 1
    assert out["sink_duplicates"] == 1


def test_unnest_latency_is_per_output_record():
    acc = OutputAccumulator(0, 1_000)
    for idx, out_ts in enumerate([110, 120, 130]):  # one order with three items
        acc.offer(unnest_output_key("o1", idx), 100, out_ts)
    stats = acc.finish(3, 1.0)
    assert stats["out_records"] == 3
    assert stats["p50_ms"] == 20
    assert stats["max_ms"] == 30


def test_dedup_stream_is_unchanged():
    # Digest of the 01-dedup stream (keys and values) captured before the registry refactor.
    worker, producer = _run(_spec(seed=7, dup_ratio=0.3))
    digest = hashlib.sha256()
    for record in producer.records:
        digest.update(record["key"] + b"|" + record["value"] + b"\n")
    assert len(producer.records) == 519
    assert digest.hexdigest() == "1e8e65464c552a80c165be5479f3361d9e64622656a06b86e1baa97088d4c8d1"
    assert worker.stats.outputs_sent == worker.stats.unique_sent


def test_dedup_output_key_is_event_id():
    from rtmpack_harness.events import Event

    event = Event("0b0e5d0e-0000-4000-8000-0000000000a1", "user-00001", 1, 5, False)
    assert DEDUP.output_keys(event) == [event.event_id.encode()]


def test_report_has_split_half_columns(tmp_path):
    from rtmpack_harness.report import build_report
    from rtmpack_harness.results import write_result

    record = {
        "run_id": "r1", "pattern": "02-unnest", "engine": "rtm", "engine_version": "a",
        "started_at": "x", "rate_eps": 10000, "record_bytes": 1024, "partitions": 8,
        "compute_desc": "c", "vcpu": 8, "cost_per_hour_usd": 1.0, "out_records": 30000,
        "distinct_out_keys": 30000, "expected_distinct": 30000, "missing": 0, "sink_duplicates": 0,
        "p50_ms": 20, "p95_ms": 30, "p99_ms": 40, "p999_ms": 50, "max_ms": 60, "throughput_eps": 30000.0,
        "p50_ms_h1": 20, "p99_ms_h1": 41, "p50_ms_h2": 21, "p99_ms_h2": 39, "stable": True,
    }
    write_result(tmp_path, "r1", record)
    text = build_report(str(tmp_path))
    header = [line for line in text.splitlines() if line.startswith("| Run")][0]
    assert "p50 h1" in header and "p99 h2" in header and "Stable" in header
    assert "| yes |" in text


def test_join_payment_never_precedes_its_order():
    from rtmpack_harness.patterns import join_payment, get_pattern
    pattern = get_pattern("08-join")
    gen = pattern.make_generator(seed=1, worker_id=0, key_lo=0, key_hi=100, key_cardinality=100,
                                 dup_ratio=0.0, dup_delay_ms_min=0, dup_delay_ms_max=5000)
    order = gen.next_unique(1_000_001)
    assert join_payment(order, 1_000_000).event_ts == order.event_ts
    assert join_payment(order, 1_000_500).event_ts == 1_000_500


# ------------------------------------------------------------ patterns 03-udf to 08-join

BASE_TS = 1_700_000_000_000
NEW = ["03-udf", "04-routing", "05-lookup", "06-tumbling", "07-sliding", "08-join"]


def _events(pid, n=300, seed=42, worker_id=0, card=1000):
    gen = get_pattern(pid).make_generator(seed, worker_id, 0, card, card)
    return [gen.next_unique(BASE_TS + i) for i in range(n)]


# ------------------------------------------------------------ generators and encoders

@pytest.mark.parametrize("pid", NEW)
def test_generators_are_deterministic_and_seed_sensitive(pid):
    assert _events(pid) == _events(pid)
    assert _events(pid, seed=1) != _events(pid, seed=2)
    assert _events(pid, worker_id=0, n=50) != _events(pid, worker_id=1, n=50)


@pytest.mark.parametrize("pid", NEW)
def test_new_patterns_reject_duplicates_and_draw_none(pid):
    rules = get_pattern(pid)
    assert rules.default_dup_ratio == 0 and rules.as_duplicate is None
    with pytest.raises(HarnessError):
        rules.validate(RunParams(pattern=pid, dup_ratio=0.2))
    with pytest.raises(HarnessError):
        rules.validate(RunParams(pattern=pid, record_bytes=200, dup_ratio=0))


@pytest.mark.parametrize("pid", ["03-udf", "04-routing", "05-lookup", "06-tumbling", "07-sliding"])
@pytest.mark.parametrize("size", [256, 1024])
def test_event_encoders_give_exact_size_and_valid_json(pid, size):
    rules = get_pattern(pid)
    for event in _events(pid, 100):
        raw = rules.encode(event, size)
        assert len(raw) == size
        doc = json.loads(raw)
        assert doc["event_id"] == event.event_id and doc["key"] == event.key
        assert doc["seq"] == event.seq and doc["event_ts"] == event.event_ts and doc["is_dup"] is False
        assert list(doc)[-1] == "payload"
    with pytest.raises(HarnessError):
        rules.encode(_events(pid, 1)[0], 100)


def test_pattern_specific_fields_and_distributions():
    types = Counter(e.type for e in _events("04-routing", 20000))
    assert set(types) == {"payment", "order", "click", "login"}
    for name, share in (("payment", 0.4), ("order", 0.3), ("click", 0.2), ("login", 0.1)):
        assert abs(types[name] / 20000 - share) < 0.02
    assert json.loads(ROUTING.encode(_events("04-routing", 1)[0], 512))["type"] in types

    merchants = {e.merchant_id for e in _events("05-lookup", 30000)}
    assert len(merchants) > 9000 and all(m.startswith("m-") and len(m) == 7 for m in merchants)
    assert max(merchants) <= "m-09999"

    for pid in ("06-tumbling", "07-sliding"):
        amounts = [e.amount for e in _events(pid, 5000)]
        assert 100 <= min(amounts) and max(amounts) <= 50000
        assert abs(np.mean(amounts) - 25050) < 1000
        assert json.loads(get_pattern(pid).encode(_events(pid, 1)[0], 512))["amount"] == amounts[0]


def test_key_cardinality_comes_from_params():
    gen = TUMBLING.make_generator(42, 0, 0, 50, 50)
    assert {gen.next_unique(1).key for _ in range(2000)} == {f"user-{i:05d}" for i in range(50)}


def test_join_order_and_payment_encoding():
    gen = JOIN.make_generator(42, 0, 0, 1000, 1000)
    orders = [gen.next_unique(BASE_TS + i) for i in range(400)]
    paid = [o for o in orders if o.pay_delay_ms is not None]
    assert 0.8 < len(paid) / len(orders) < 0.97
    assert all(0 <= o.pay_delay_ms <= 5000 for o in paid)
    for order in orders[:50]:
        doc = json.loads(JOIN.encode(order, 1024))
        assert list(doc) == ["order_id", "key", "event_ts", "amount", "payload"]
        assert len(JOIN.encode(order, 1024)) == 1024
    payment = JOIN.follow_up(paid[0], BASE_TS + 123)
    raw = JOIN.encode(payment, 1024)
    doc = json.loads(raw)
    assert len(raw) == 1024
    assert list(doc) == ["payment_id", "order_id", "event_ts", "method", "payload"]
    assert doc["order_id"] == paid[0].order_id and doc["event_ts"] == BASE_TS + 123
    assert doc["method"] in {"card", "wallet", "bank"}
    assert doc["payment_id"] == join_payment_id(paid[0].order_id)
    assert payment.key == paid[0].key
    assert JOIN.route(paid[0]) == "orders" and JOIN.route(payment) == "payments"


# ------------------------------------------------------------------ topics

def test_topic_names_for_multi_topic_patterns():
    assert input_topics("08-join") == ["rtmpack.08-join.input.orders", "rtmpack.08-join.input.payments"]
    assert output_topics("04-routing", "rtm") == [
        f"rtmpack.04-routing.out.rtm.{s}" for s in ("payment", "order", "click", "login", "all")
    ]
    assert output_topics("03-udf", "flink") == ["rtmpack.03-udf.out.flink"]
    names = set(topic_names("04-routing").values())
    assert len(names) == 1 + 2 * 5 and "rtmpack.04-routing.input" in names
    names = set(topic_names("08-join").values())
    assert names == {"rtmpack.08-join.input.orders", "rtmpack.08-join.input.payments",
                     "rtmpack.08-join.out.rtm", "rtmpack.08-join.out.flink"}


# ------------------------------------------------------------ value checks

def test_udf_score_rule_and_check():
    h = hashlib.sha256(b"abc|user-1").digest()
    assert udf_score("abc", "user-1") == int.from_bytes(h[:4], "big") % 1000
    assert [udf_decision(x) for x in (0, 799, 800, 949, 950, 999)] == [
        "approve", "approve", "review", "review", "flag", "flag"]
    ev = _events("03-udf", 1)[0]
    score = udf_score(ev.event_id, ev.key)
    good = {"event_id": ev.event_id, "key": ev.key, "score": score, "decision": udf_decision(score)}
    assert UDF.check_value(None, good)
    assert not UDF.check_value(None, {**good, "score": (score + 1) % 1000})
    assert not UDF.check_value(None, {**good, "decision": "flag" if good["decision"] != "flag" else "approve"})
    assert not UDF.check_value(None, {"event_id": ev.event_id})


def test_lookup_table_is_deterministic_and_check_compares_attributes():
    assert merchant_row(0)[0] == "m-00000" and merchant_row(9999)[0] == "m-09999"
    h = int.from_bytes(hashlib.sha256(b"merchant|7").digest()[:4], "big")
    assert merchant_row(7)[1] == ["low", "medium", "high"][h % 3]
    mid, tier, cat, country = merchant_row(1234)
    doc = {"merchant_id": mid, "risk_tier": tier, "category": cat, "country": country}
    assert LOOKUP.check_value(None, doc)
    assert not LOOKUP.check_value(None, {**doc, "country": "XX"})
    assert not LOOKUP.check_value(None, {**doc, "risk_tier": "none"})
    assert not LOOKUP.check_value(None, {**doc, "merchant_id": "m-99999"})


def test_refdata_cli_writes_merchants_csv(tmp_path):
    out = tmp_path / "m.csv"
    assert main(["refdata", "--pattern", "05-lookup", "--out", str(out)]) == 0
    lines = out.read_text().splitlines()
    assert lines[0] == "merchant_id,risk_tier,category,country"
    assert len(lines) == 10_001 and lines[1] == ",".join(merchant_row(0))
    assert main(["refdata", "--pattern", "01-dedup", "--out", str(tmp_path / "x.csv")]) == 2


def test_join_value_check_matches_payment_id_of_the_order():
    assert JOIN.check_value(None, {"order_id": "o-1", "payment_id": join_payment_id("o-1")})
    assert not JOIN.check_value(None, {"order_id": "o-1", "payment_id": join_payment_id("o-2")})
    assert not JOIN.check_value(None, {"order_id": "o-1"})


# ------------------------------------------------------- 04-routing multi-topic counting

def _routing_ingest(multi=True):
    acc = make_accumulator(ROUTING, 0, 10_000, 0, 10_000)
    return acc, OutputIngest(ROUTING, acc, multi_topic=multi)


def test_routing_counts_on_topic_and_key_and_checks_type_by_suffix():
    base = "rtmpack.04-routing.out.rtm"
    acc, ingest = _routing_ingest()
    events = _events("04-routing", 50)
    for n, ev in enumerate(events):
        doc = {"event_id": ev.event_id, "type": ev.type, "in_ts": 1000 + n}
        key = ev.event_id.encode()
        assert ingest.offer(f"{base}.{ev.type}", ev.type, key, doc, 1050 + n)
        assert ingest.offer(f"{base}.all", "all", key, doc, 1060 + n)
    stats = ingest.finish({"counts": {"expected_outputs": 100}}, 10.0)
    assert stats["out_records"] == 100 and stats["distinct_out_keys"] == 100
    assert stats["missing"] == 0 and stats["sink_duplicates"] == 0 and stats["value_errors"] == 0

    # A resend into the same topic is a sink duplicate; the same key in another topic is not.
    ev = events[0]
    doc = {"event_id": ev.event_id, "type": ev.type, "in_ts": 1000}
    ingest.offer(f"{base}.{ev.type}", ev.type, ev.event_id.encode(), doc, 1070)
    # A wrongly routed record in a type topic is a value error; `.all` is never checked.
    other = "click" if ev.type != "click" else "login"
    ingest.offer(f"{base}.{other}", other, b"zzz", {**doc, "type": ev.type}, 1080)
    ingest.offer(f"{base}.all", "all", b"yyy", {**doc, "type": "anything"}, 1080)
    stats = ingest.finish({"counts": {"expected_outputs": 100}}, 10.0)
    assert stats["sink_duplicates"] == 1
    assert stats["value_errors"] == 1
    assert stats["missing"] == -2  # two extra distinct keys beyond the 100 expected


def test_malformed_records_are_rejected():
    acc, ingest = _routing_ingest()
    assert not ingest.offer("t", "all", b"k", {"event_id": "x"}, 5)
    assert not ingest.offer("t", "all", None, {"in_ts": 1}, 5)


def test_routing_worker_expects_two_outputs_per_event():
    worker, producer = _run(_spec(pattern="04-routing", topic="rtmpack.04-routing.input", record_bytes=512, dup_ratio=0.0))
    assert worker.stats.outputs_sent == 2 * worker.stats.unique_sent == 800
    assert {r["topic"] for r in producer.records} == {"rtmpack.04-routing.input"}


# ------------------------------------------------------------ 08-join worker and manifest counts

def _join_spec(**kw):
    return _spec(pattern="08-join", topic="rtmpack.08-join.input", record_bytes=512, dup_ratio=0.0,
                 rate_eps=400.0, start_at=1_000.0, end_at=1_004.0, **kw)


def test_join_worker_sends_orders_and_delayed_payments_to_two_topics():
    worker, producer = _run(_join_spec())
    by_topic = Counter(r["topic"] for r in producer.records)
    orders = [r for r in producer.records if r["topic"].endswith(".orders")]
    payments = [r for r in producer.records if r["topic"].endswith(".payments")]
    assert set(by_topic) == {"rtmpack.08-join.input.orders", "rtmpack.08-join.input.payments"}
    assert len(orders) == worker.stats.unique_sent == 1600
    assert len(payments) == worker.stats.follow_up_sent
    assert worker.stats.dup_sent == 0
    # All payments belong to a sent order, are on the order's key, and are about 90 percent of orders.
    order_docs = {json.loads(r["value"])["order_id"]: r for r in orders}
    for rec in payments:
        doc = json.loads(rec["value"])
        assert rec["key"] == order_docs[doc["order_id"]]["key"]
        assert doc["payment_id"] == join_payment_id(doc["order_id"])
        assert doc["event_ts"] >= json.loads(order_docs[doc["order_id"]]["value"])["event_ts"]
    assert 0.85 < len(payments) / len(orders) < 0.95
    assert worker.stats.outputs_sent == len(payments)  # every payment yields one match
    assert all(len(r["value"]) == 512 for r in producer.records)


def test_join_manifest_counts_cover_orders_payments_and_matches():
    totals = {"unique_sent": 1600, "follow_up_sent": 1440, "expected_outputs": 1440}
    counts = JOIN.manifest_counts(totals)
    assert counts == {"orders_sent": 1600, "payments_sent": 1440, "expected_matches": 1440}
    assert JOIN.expected_distinct({"counts": counts}) == 1440
    assert JOIN.default_rate_eps == 5000 and JOIN.rate_unit == "orders"


def test_loadgen_cli_help_shows_join_default():
    from rtmpack_harness.cli import build_parser

    text = build_parser().parse_args(["loadgen", "--pattern", "08-join"])
    assert text.rate_eps is None  # resolved from the pattern default (5000) in cmd_loadgen


# ------------------------------------------------------------------ windows

def test_window_functions():
    ts = 1_700_000_017_000
    assert tumbling_windows(ts) == (1_700_000_000_000 - 1_700_000_000_000 % 60000,)
    assert tumbling_windows(60_000) == (60_000,) and tumbling_windows(59_999) == (0,)
    for t in (0, 19_999, 20_000, 59_999, 60_000, 1_700_000_017_123):
        w = sliding_windows(t)
        assert len(w) == 3 and all(s <= t < s + 60_000 for s in w)
        assert w[1] - w[0] == w[2] - w[1] == 20_000


# ------------------------------------------------------------ expected side file

def _aggregate_run(pid, tmp_path, n_workers=2, rate=200.0, run_id="r1"):
    pairs = {}
    for wid in range(n_workers):
        spec = _spec(pattern=pid, topic=f"rtmpack.{pid}.input", worker_id=wid, n_workers=n_workers,
                     rate_eps=rate, record_bytes=300, dup_ratio=0.0, key_cardinality=20,
                     start_at=59_990.0, end_at=60_010.0,
                     expected_file=str(expected_path(tmp_path, run_id, wid)))
        worker, producer = _run(spec)
        for rec in producer.records:
            doc = json.loads(rec["value"])
            for w in get_pattern(pid).windows(doc["event_ts"]):
                c, s = pairs.get((doc["key"], w), (0, 0))
                pairs[(doc["key"], w)] = (c + 1, s + doc["amount"])
    return pairs


@pytest.mark.parametrize("pid,per_event", [("06-tumbling", 1), ("07-sliding", 3)])
def test_expected_side_file_round_trip_matches_independent_accumulation(pid, per_event, tmp_path):
    pairs = _aggregate_run(pid, tmp_path)
    assert len(list(tmp_path.glob("r1.expected.w*.npz"))) == 2
    loaded = load_expected(tmp_path, "r1")
    assert len(loaded) == len(pairs)
    by_digest = {pair_digest(k, w): v for (k, w), v in pairs.items()}
    for d, c, s in zip(loaded.digests, loaded.counts, loaded.sums):
        assert by_digest[int(d)] == (int(c), int(s))
    assert int(loaded.counts.sum()) == per_event * 4000  # 200 eps in total for 20 s
    assert list(loaded.digests) == sorted(loaded.digests)


def test_load_expected_without_files_raises(tmp_path):
    with pytest.raises(HarnessError):
        load_expected(tmp_path, "nope")


def test_expected_builder_reduces_across_flushes():
    b = ExpectedBuilder()
    b.FLUSH_AT = 7
    for i in range(1000):
        b.add(f"k{i % 5}", 60_000 * (i % 3), i)
    p = b.pairs()
    assert len(p) == 15 and int(p.counts.sum()) == 1000 and int(p.sums.sum()) == sum(range(1000))


def test_worker_stats_report_expected_pairs(tmp_path):
    worker, _ = _run(_spec(pattern="06-tumbling", topic="rtmpack.06-tumbling.input", record_bytes=300, dup_ratio=0.0,
                           key_cardinality=20, start_at=59_990.0, end_at=60_010.0))
    assert worker.stats.expected_pairs > 0  # no file requested, still counted


# ------------------------------------------------- aggregate collector logic

def _expected(entries):
    d = [pair_digest(k, w) for (k, w) in entries]
    return ExpectedPairs(np.array(d, dtype=np.uint64),
                         np.array([c for c, _ in entries.values()]),
                         np.array([s for _, s in entries.values()]))


def _update(key, w, count, total, in_ts=1000):
    return {"key": key, "window_start_ms": w, "window_end_ms": w + 60000, "count": count,
            "sum_amount": total, "in_ts": in_ts, "engine": "rtm"}


def _agg_ingest():
    acc = make_accumulator(TUMBLING, 0, 10_000, 0, 10_000)
    assert isinstance(acc, AggregateAccumulator)
    return OutputIngest(TUMBLING, acc)


def test_aggregate_collector_counts_pairs_missing_and_null_sink_duplicates():
    expected = _expected({("a", 0): (3, 300), ("b", 0): (2, 50), ("a", 60000): (1, 10)})
    ingest = _agg_ingest()
    # Pair (a, 0): three updates (running counts), final matches. Pair (b, 0): final correct.
    for n, (k, w, c, s) in enumerate([("a", 0, 1, 100), ("a", 0, 2, 220), ("a", 0, 3, 300), ("b", 0, 2, 50)]):
        assert ingest.offer("t", None, f"{k}:{w}".encode(), _update(k, w, c, s, 1000 + n), 1040 + n)
    stats = ingest.finish({}, 10.0, expected)
    assert stats["out_records"] == 4
    assert stats["distinct_out_keys"] == 2
    assert stats["expected_distinct"] == 3
    assert stats["missing"] == 1  # (a, 60000) never seen
    assert stats["sink_duplicates"] is None
    assert stats["value_errors"] == 0
    assert stats["p50_ms"] == 40


def test_aggregate_final_update_is_the_one_with_the_highest_count():
    expected = _expected({("a", 0): (3, 300)})
    ingest = _agg_ingest()
    # The final update arrives first, a stale lower count after it: still correct.
    for c, s in [(3, 300), (1, 100), (2, 220)]:
        ingest.offer("t", None, b"a:0", _update("a", 0, c, s), 1040)
    assert ingest.finish({}, 10.0, expected)["value_errors"] == 0


def test_aggregate_value_errors_for_wrong_count_sum_and_unexpected_pairs():
    expected = _expected({("a", 0): (3, 300), ("b", 0): (2, 50), ("c", 0): (1, 5)})
    ingest = _agg_ingest()
    ingest.offer("t", None, b"a:0", _update("a", 0, 2, 220), 1040)   # count short
    ingest.offer("t", None, b"b:0", _update("b", 0, 2, 51), 1040)    # sum off
    ingest.offer("t", None, b"c:0", _update("c", 0, 1, 5), 1040)     # correct
    ingest.offer("t", None, b"z:0", _update("z", 0, 1, 5), 1040)     # not expected
    stats = ingest.finish({}, 10.0, expected)
    assert stats["value_errors"] == 3
    assert stats["missing"] == 0 and stats["distinct_out_keys"] == 4


def test_aggregate_ignores_updates_outside_the_correctness_window():
    expected = _expected({("a", 0): (1, 5)})
    acc = make_accumulator(TUMBLING, 2000, 8000, 1000, 9000)
    ingest = OutputIngest(TUMBLING, acc)
    ingest.offer("t", None, b"a:0", _update("a", 0, 1, 5, in_ts=500), 600)    # before the run
    ingest.offer("t", None, b"a:0", _update("a", 0, 1, 5, in_ts=1500), 1550)  # in run, in warm-up
    stats = ingest.finish({}, 6.0, expected)
    assert stats["out_records"] == 1 and stats["missing"] == 0 and stats["excluded_records"] == 1
    assert stats["throughput_eps"] == 0.0


def test_aggregate_accumulator_handles_many_updates_across_compactions():
    n_pairs = 3000
    entries = {(f"k{i}", 0): (4, 4 * i) for i in range(n_pairs)}
    expected = _expected(entries)
    acc = AggregateAccumulator(0, 10_000)
    acc.FLUSH_AT = 500
    acc.COMPACT_MIN = 1000
    for step in range(1, 5):
        for i in range(n_pairs):
            acc.offer(f"k{i}:0".encode(), 100, 130, count=step, sum_amount=step * i)
    stats = acc.finish(expected, 10.0)
    assert stats["out_records"] == 4 * n_pairs and stats["distinct_out_keys"] == n_pairs
    assert stats["missing"] == 0 and stats["value_errors"] == 0 and stats["p99_ms"] == 30


def test_malformed_aggregate_update_is_counted_by_the_caller():
    ingest = _agg_ingest()
    assert not ingest.offer("t", None, b"a:0", {"key": "a", "in_ts": 5}, 6)


# --------------------------------------------------------------- results

def test_value_errors_is_the_last_result_field_and_defaults_to_zero(tmp_path):
    assert RESULT_FIELDS[-1] == "value_errors"
    from rtmpack_harness.results import load_results, write_result
    from helpers import summarize_output

    stats = summarize_output([(b"k", 5, 9)], 0, 100, 1, 1.0)
    assert stats["value_errors"] == 0
    record = {f: 0 for f in RESULT_FIELDS if f != "value_errors"}
    record.update({"engine": "rtm", "sink_duplicates": None})
    write_result(tmp_path, "r1", record)
    row = load_results(tmp_path)[0]
    assert row["value_errors"] == 0 and row["sink_duplicates"] is None


def test_cli_help_works():
    with pytest.raises(SystemExit) as exc:
        main(["loadgen", "--help"])
    assert exc.value.code == 0
    with pytest.raises(SystemExit) as exc:
        main(["refdata", "--help"])
    assert exc.value.code == 0


# ------------------------------------------------------------ registry and template

def test_registry_finds_one_module_per_pattern():
    from rtmpack_harness.patterns import PATTERNS
    assert list(PATTERNS) == sorted(PATTERNS)
    assert "NN-name" not in PATTERNS  # _template.py is skipped


def test_template_pattern_is_valid():
    from rtmpack_harness.patterns._template import PATTERN
    gen = PATTERN.make_generator(seed=1, worker_id=0, key_lo=0, key_hi=100, key_cardinality=100,
                                 dup_ratio=0.0, dup_delay_ms_min=0, dup_delay_ms_max=0)
    event = gen.next_unique(1_700_000_000_000)
    value = PATTERN.encode(event, 1024)
    assert len(value) == 1024
    assert PATTERN.outputs_per_event(event) == 1


def test_topic_suffixes_come_from_the_pattern():
    from rtmpack_harness.contract import input_topics, output_topics
    assert input_topics("08-join") == ["rtmpack.08-join.input.orders", "rtmpack.08-join.input.payments"]
    assert len(output_topics("04-routing", "rtm")) == 5
    assert input_topics("01-dedup") == ["rtmpack.01-dedup.input"]
