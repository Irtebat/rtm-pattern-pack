"""Aggregate results/*.jsonl into a markdown table."""

from __future__ import annotations

from typing import Optional

from rtmpack_harness.results import load_results

COLUMNS = (
    ("run_id", "Run"),
    ("pattern", "Pattern"),
    ("engine", "Engine"),
    ("engine_version", "Version"),
    ("rate_eps", "Rate (eps)"),
    ("partitions", "Partitions"),
    ("compute_desc", "Compute"),
    ("vcpu", "vCPU"),
    ("cost_per_hour_usd", "USD/h"),
    ("throughput_eps", "Out (eps)"),
    ("out_records", "Out records"),
    ("distinct_out_keys", "Distinct"),
    ("expected_distinct", "Expected"),
    ("missing", "Missing"),
    ("sink_duplicates", "Sink dups"),
    ("p50_ms", "p50 ms"),
    ("p95_ms", "p95 ms"),
    ("p99_ms", "p99 ms"),
    ("p999_ms", "p99.9 ms"),
    ("max_ms", "Max ms"),
    ("p50_ms_h1", "p50 h1"),
    ("p99_ms_h1", "p99 h1"),
    ("p50_ms_h2", "p50 h2"),
    ("p99_ms_h2", "p99 h2"),
    ("stable", "Stable"),
    ("value_errors", "Value errors"),
    ("usd_per_m_events", "USD per 1M out"),
)


def _usd_per_million(row: dict) -> Optional[float]:
    rate = row.get("throughput_eps") or 0
    if rate <= 0 or row.get("cost_per_hour_usd") is None:
        return None
    return row["cost_per_hour_usd"] / (rate * 3600) * 1_000_000


def _cell(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:,.2f}" if abs(value) < 100 else f"{value:,.0f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value).replace("|", "/")


def build_report(results_dir: str, pattern: Optional[str] = None) -> str:
    rows = load_results(results_dir)
    if pattern:
        rows = [r for r in rows if r.get("pattern") == pattern.lower()]
    rows.sort(key=lambda r: (r["pattern"], r["rate_eps"], r["run_id"], r["engine"]))
    for row in rows:
        row["usd_per_m_events"] = _usd_per_million(row)

    header = "| " + " | ".join(label for _, label in COLUMNS) + " |"
    divider = "|" + "|".join("---" for _ in COLUMNS) + "|"
    lines = [
        "# RTM Pattern Pack results",
        "",
        "Latency is output append time minus input append time (LogAppendTime, contract section 4). "
        "Percentiles cover records whose input timestamp falls after the warm-up cutoff. "
        "h1 and h2 are the first and second half of that window; Stable is yes when p50 and p99 "
        "each differ by at most 15 percent between the halves.",
        "",
        header,
        divider,
    ]
    for row in rows:
        lines.append("| " + " | ".join(_cell(row.get(key)) for key, _ in COLUMNS) + " |")
    if not rows:
        lines.append("")
        lines.append("No results found.")
    return "\n".join(lines) + "\n"
