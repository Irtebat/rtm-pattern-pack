"""Plots per-second latency timelines written by latency_timeline.py --csv.

Draws one panel per engine with p50, p99 and max per second on a log scale, and
marks every interval boundary (for example the RTM real-time batch interval)
on one engine's panel, measured from that engine's first spike, so periodic pauses are easy to see.

Usage (locally, needs matplotlib):
  python tools/plot_timeline.py --csv rtm=results/<run>.timeline.rtm.csv \
      --csv flink=results/<run>.timeline.flink.csv --out results/<run>.timeline.png \
      [--title "02-unnest tuned run"] [--interval-s 300]
"""
from __future__ import annotations

import argparse
import csv

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def load(path: str) -> dict[str, list[int]]:
    cols: dict[str, list[int]] = {"second": [], "p50_ms": [], "p99_ms": [], "max_ms": []}
    with open(path) as fh:
        for row in csv.DictReader(fh):
            for k in cols:
                cols[k].append(int(row[k]))
    return cols


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="append", required=True, help="engine=path, repeatable")
    parser.add_argument("--out", required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--interval-s", type=int, default=0, help="Mark boundaries at this period")
    parser.add_argument("--interval-engine", default="rtm", help="Panel that gets the boundary marks")
    parser.add_argument("--spike-ms", type=int, default=250)
    args = parser.parse_args()

    series = [(spec.split("=", 1)[0], load(spec.split("=", 1)[1])) for spec in args.csv]
    fig, axes = plt.subplots(len(series), 1, figsize=(12, 3.2 * len(series)), sharex=True, sharey=True)
    if len(series) == 1:
        axes = [axes]
    for ax, (engine, d) in zip(axes, series):
        ax.plot(d["second"], d["max_ms"], color="#c0392b", lw=0.8, label="max")
        ax.plot(d["second"], d["p99_ms"], color="#2c3e50", lw=0.8, label="p99")
        ax.plot(d["second"], d["p50_ms"], color="#7f8c8d", lw=0.8, label="p50")
        ax.axhline(args.spike_ms, color="#999999", lw=0.6, ls=":")
        if args.interval_s and engine == args.interval_engine:
            spikes = [s for s, m in zip(d["second"], d["max_ms"]) if m > args.spike_ms]
            if spikes:
                t = spikes[0]
                while t <= d["second"][-1]:
                    ax.axvline(t, color="#e67e22", lw=0.6, ls="--", alpha=0.6)
                    t += args.interval_s
        ax.set_yscale("log")
        ax.set_ylabel(f"{engine} latency (ms)")
        ax.grid(True, which="major", alpha=0.25)
        ax.legend(loc="upper right", fontsize=8, ncol=3)
    axes[-1].set_xlabel("seconds after warm-up cutoff")
    if args.title:
        axes[0].set_title(args.title)
    fig.tight_layout()
    fig.savefig(args.out, dpi=130)


if __name__ == "__main__":
    main()
