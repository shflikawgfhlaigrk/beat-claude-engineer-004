#!/usr/bin/env python3
"""Single-core throughput benchmark for the ingest-edge contract.

The point is not that Python is the production language. The point is to put a
measured floor under the sizing arithmetic in the design doc instead of an
asserted one: if the *slowest plausible* implementation of the normalize step
already clears the required per-core rate with headroom, the sizing is safe,
and if it does not, the design has a problem worth knowing about now.

    python3 bench_ingest.py --events 200000 --repeat 3

Reports events/sec/core, p50/p95/p99 per-event normalize latency, and the
implied core count for the brief's average and 10x-spike rates.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline.normalize import normalize_batch, normalize_line  # noqa: E402

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "event_sample.jsonl"

# [Observed] from the brief: 50,000,000 events/day.
EVENTS_PER_DAY = 50_000_000
SPIKE_MULTIPLIER = 10  # [Observed] brief: "10x traffic spikes"


def synth(n: int):
    """Repeat the fixture's real line shapes up to n lines, rewriting ids so the
    idempotency map does real work instead of hitting one hot key."""
    base = [ln for ln in FIXTURE.read_text().splitlines() if ln.strip()]
    out = []
    i = 0
    while len(out) < n:
        for ln in base:
            out.append(ln.replace("evt-00", "evt-{}-".format(i)))
            if len(out) >= n:
                break
        i += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=200_000)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--batch", type=int, default=500, help="events per normalize_batch call")
    ap.add_argument("--json", dest="json_out")
    args = ap.parse_args()

    lines = synth(args.events)
    batches = [lines[i:i + args.batch] for i in range(0, len(lines), args.batch)]

    runs = []
    for r in range(args.repeat):
        t0 = time.perf_counter()
        total = 0
        for b in batches:
            total += len(normalize_batch(b))
        elapsed = time.perf_counter() - t0
        runs.append(args.events / elapsed)
        print("run {}: {} events in {:.3f}s -> {:,.0f} events/sec/core (verdicts={})"
              .format(r + 1, args.events, elapsed, args.events / elapsed, total))

    # Per-event latency distribution, measured on normalize_line alone so the
    # number is not smeared by batch-level work.
    sample = lines[:20_000]
    lat = []
    for ln in sample:
        t = time.perf_counter()
        normalize_line(1, ln)
        lat.append((time.perf_counter() - t) * 1e6)
    lat.sort()

    def pct(p):
        return lat[min(len(lat) - 1, int(len(lat) * p))]

    rate = statistics.median(runs)
    avg_eps = EVENTS_PER_DAY / 86400.0
    spike_eps = avg_eps * SPIKE_MULTIPLIER

    result = {
        "python": platform.python_version(),
        "platform": "{} {}".format(platform.system(), platform.machine()),
        "events_per_run": args.events,
        "batch_size": args.batch,
        "runs_events_per_sec": [round(x) for x in runs],
        "median_events_per_sec_per_core": round(rate),
        "normalize_line_latency_us": {"p50": round(pct(0.50), 1), "p95": round(pct(0.95), 1),
                                      "p99": round(pct(0.99), 1), "n": len(lat)},
        "brief_avg_events_per_sec": round(avg_eps, 1),
        "brief_spike_events_per_sec": round(spike_eps, 1),
        "cores_for_avg": round(avg_eps / rate, 3),
        "cores_for_10x_spike": round(spike_eps / rate, 3),
    }

    print("\n[Benchmarked] median {:,.0f} events/sec/core on {} / Python {}"
          .format(rate, result["platform"], result["python"]))
    print("[Benchmarked] normalize_line p50={}us p95={}us p99={}us"
          .format(result["normalize_line_latency_us"]["p50"],
                  result["normalize_line_latency_us"]["p95"],
                  result["normalize_line_latency_us"]["p99"]))
    print("[Observed]    brief average load  = {:,.0f} events/sec".format(avg_eps))
    print("[Estimated]   cores to hold average = {:.2f}".format(result["cores_for_avg"]))
    print("[Estimated]   cores to hold 10x spike = {:.2f}".format(result["cores_for_10x_spike"]))
    print("\nCaveat: measured on one laptop core in CPython, cold OS page cache, no "
          "network, no serialization to the stream. Treat as a floor for CPU cost of "
          "the transform only, not as end-to-end pipeline throughput.")

    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(result, indent=2) + "\n")
        print("wrote {}".format(args.json_out))


if __name__ == "__main__":
    main()
