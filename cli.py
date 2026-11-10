#!/usr/bin/env python3
"""
cronstorm CLI — Command-line schedule collision analyzer & deterministic de-aligner.

Usage:
  python3 cli.py analyze manifests/fintech_prod.cron --pool-cap 120 --dealign fnv_hash --emit-patch
  python3 cli.py manifests/fintech_prod.cron --pool-cap 120 --dealign fnv_hash --emit-patch
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
from typing import List

from engine import (
    ScheduledJob,
    compute_impulse_matrix,
    fnv1a_32,
    optimize_manifest,
    simulate_retry_storm,
)


def parse_manifest_file(path: Path) -> List[ScheduledJob]:
    jobs: List[ScheduledJob] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 6:
            raise ValueError(f"{path}:{line_no}: expected 5 cron fields + service_name")
        cron_expr = " ".join(parts[:5])
        name = parts[5]
        conns = int(parts[6]) if len(parts) >= 7 else 20
        dur = int(parts[7]) if len(parts) >= 8 else 3
        jobs.append(ScheduledJob(name=name, cron=cron_expr, conns=conns, duration_min=dur))
    return jobs


def fmt_hhmm(minute: int) -> str:
    return f"{minute // 60:02d}:{minute % 60:02d}"


def main(argv: List[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] == "analyze":
        raw_args = raw_args[1:]

    parser = argparse.ArgumentParser(
        prog="cronstorm",
        description="Distributed Cron Collision Analyzer & Metastable Retry Physics Engine",
    )
    parser.add_argument("manifest", type=Path, help="Path to crontab schedule manifest")
    parser.add_argument("--pool-cap", type=int, default=120, help="Database connection pool cap")
    parser.add_argument(
        "--dealign",
        choices=["none", "fnv_hash", "coprime", "valley_greedy"],
        default="fnv_hash",
        help="Deterministic schedule phase-offset strategy",
    )
    parser.add_argument("--emit-patch", action="store_true", help="Print unified git diff patch")
    args = parser.parse_args(raw_args)

    jobs = parse_manifest_file(args.manifest)
    base_tl, _ = compute_impulse_matrix(jobs, use_orig=True)
    base_peak = max(base_tl)
    base_peak_min = base_tl.index(base_peak)
    base_over = sum(1 for c in base_tl if c > args.pool_cap)

    if args.dealign != "none":
        optimize_manifest(jobs, strategy=args.dealign)

    curr_tl, _ = compute_impulse_matrix(jobs, use_orig=False)
    curr_peak = max(curr_tl)
    curr_peak_min = curr_tl.index(curr_peak)
    curr_over = sum(1 for c in curr_tl if c > args.pool_cap)

    print("======================================================================")
    print(f"CRONSTORM TELEMETRY REPORT // MANIFEST: {args.manifest.name}")
    print("======================================================================")
    print(f"Workloads Analyzed       : {len(jobs)} recurring services")
    print(f"DB Connection Pool Cap   : {args.pool_cap} connections")
    print(f"Baseline Peak Demand     : {base_peak} conns @ {fmt_hhmm(base_peak_min)} UTC ({base_over} overload mins/24h)")
    if args.dealign != "none":
        reduction = ((base_peak - curr_peak) / max(1, base_peak)) * 100.0
        print(
            f"De-Aligned Peak ({args.dealign}) : {curr_peak} conns @ {fmt_hhmm(curr_peak_min)} UTC "
            f"(-{reduction:.1f}% peak, {curr_over} overload mins/24h)"
        )

    print("\n--- RETRY-STORM METASTABLE BENCHMARK (18s Downstream Brownout) ---")
    for pol in ("none", "equal", "full", "decorrelated"):
        res = simulate_retry_storm(policy=pol, pool_cap=args.pool_cap)
        print(
            f"  Policy={pol.ljust(12)} | Peak={int(res['peak_rps']):4d} RPS | "
            f"Amp={res['amplification']:.2f}x | Recovery={int(res['recovery_sec']):2d}s | "
            f"Success={res['success_pct']:5.1f}%"
        )

    if args.emit_patch and args.dealign != "none":
        print("\n--- UNIFIED SCHEDULE DE-ALIGNMENT PATCH ---")
        print(f"--- a/{args.manifest}")
        print(f"+++ b/{args.manifest}")
        for j in jobs:
            if j.cron != j.orig_cron:
                print(f"-{j.orig_cron:<18} {j.name} {j.conns} {j.duration_min}")
                print(
                    f"+{j.cron:<18} {j.name} {j.conns} {j.duration_min} "
                    f"# fnv1a=0x{fnv1a_32(j.name):08x}"
                )
            else:
                print(f" {j.cron:<18} {j.name} {j.conns} {j.duration_min}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
