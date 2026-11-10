#!/usr/bin/env python3
"""
cronstorm.engine — Deterministic Distributed Schedule Collision & Retry Physics Engine.

Zero-dependency Python 3 core engine that parses 5-field cron expressions and
Kubernetes CronJob manifests, computes 1,440-minute concurrency impulse matrices,
deterministically de-aligns colliding schedules via FNV-1a service-name hashing or
valley-filling optimization, and simulates metastable retry storms with per-attempt
cohort tracking.
"""

from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class ScheduledJob:
    name: str
    cron: str
    conns: int = 20
    duration_min: int = 3
    orig_cron: Optional[str] = None

    def __post_init__(self) -> None:
        if self.orig_cron is None:
            self.orig_cron = self.cron


def fnv1a_32(text: str) -> int:
    """Computes a deterministic 32-bit FNV-1a hash digest."""
    h = 0x811C9DC5
    for ch in text.encode("utf-8"):
        h ^= ch
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def parse_cron_field(field_str: str, min_val: int, max_val: int) -> Set[int]:
    """Parses a single cron field (*, */n, a-b, a-b/n, or comma lists) into a Set of ints."""
    values: Set[int] = set()
    for part in field_str.strip().split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        range_part = part
        if "/" in part:
            range_part, step_str = part.split("/", 1)
            step = int(step_str)
            if step <= 0:
                raise ValueError(f"Non-positive cron step in '{field_str}'")

        if range_part == "*":
            start, end = min_val, max_val
        elif "-" in range_part:
            s_str, e_str = range_part.split("-", 1)
            start = max(min_val, min(max_val, int(s_str)))
            end = max(min_val, min(max_val, int(e_str)))
            if start > end:
                raise ValueError(f"Inverted cron range '{range_part}'")
        else:
            val = int(range_part)
            if val < min_val or val > max_val:
                raise ValueError(f"Cron value {val} out of bounds [{min_val}, {max_val}]")
            start = val
            end = max_val if "/" in part else val

        for v in range(start, end + 1, step):
            values.add(v)

    if not values:
        raise ValueError(f"Empty cron field '{field_str}'")
    return values


def expand_cron_day_minutes(cron_expr: str) -> List[int]:
    """Expands a 5-field cron expression into sorted firing minutes [0..1439] across 24h."""
    normalized = cron_expr.strip()
    macros = {
        "@hourly": "0 * * * *",
        "@daily": "0 0 * * *",
        "@midnight": "0 0 * * *",
    }
    normalized = macros.get(normalized, normalized)
    tokens = normalized.split()
    if len(tokens) < 5:
        raise ValueError(f"Expected 5-field cron expression, got '{cron_expr}'")

    minutes = parse_cron_field(tokens[0], 0, 59)
    hours = parse_cron_field(tokens[1], 0, 23)

    firing: List[int] = []
    for h in range(24):
        if h in hours:
            for m in range(60):
                if m in minutes:
                    firing.append(h * 60 + m)
    return firing


def compute_impulse_matrix(
    jobs: List[ScheduledJob], use_orig: bool = False
) -> Tuple[List[int], List[List[str]]]:
    """
    Computes the 1,440-minute connection load vector and active job list per minute.
    """
    timeline = [0] * 1440
    active_map: List[List[str]] = [[] for _ in range(1440)]

    for job in jobs:
        expr = (job.orig_cron or job.cron) if use_orig else job.cron
        starts = expand_cron_day_minutes(expr)
        dur = max(1, min(60, job.duration_min))
        conns = max(1, job.conns)
        for s in starts:
            for d in range(dur):
                m = (s + d) % 1440
                timeline[m] += conns
                active_map[m].append(job.name)

    return timeline, active_map


def dealign_job_schedule(
    job: ScheduledJob,
    index: int,
    strategy: str = "fnv_hash",
    running_timeline: Optional[List[int]] = None,
) -> str:
    """
    Rewrites a cron schedule to preserve exact cadence while shifting phase offset.
    Supported strategies: 'fnv_hash', 'coprime', 'valley_greedy'.
    """
    orig = (job.orig_cron or job.cron).strip()
    tokens = orig.split()
    if len(tokens) < 5:
        return orig

    min_f, hour_f, dom_f, mon_f, dow_f = tokens[:5]
    digest = fnv1a_32(f"{job.name}:{index}")

    if min_f.startswith("*/"):
        period = int(min_f.split("/", 1)[1])
        if 1 < period <= 30:
            if strategy == "fnv_hash":
                offset = (digest % (period - 1)) + 1
            elif strategy == "coprime":
                offset = ((index * 7 + 3) % (period - 1)) + 1
            elif strategy == "valley_greedy" and running_timeline is not None:
                best_offset = 0
                best_peak = float("inf")
                for cand in range(period):
                    cand_cron = f"{cand}-59/{period} {hour_f} {dom_f} {mon_f} {dow_f}"
                    starts = expand_cron_day_minutes(cand_cron)
                    peak = max(
                        running_timeline[(s + d) % 1440] + job.conns
                        for s in starts
                        for d in range(job.duration_min)
                    )
                    if peak < best_peak:
                        best_peak = peak
                        best_offset = cand
                offset = best_offset
            else:
                offset = (digest % (period - 1)) + 1
            return f"{offset}-59/{period} {hour_f} {dom_f} {mon_f} {dow_f}"

    if min_f.isdigit():
        if strategy == "fnv_hash":
            new_min = (digest % 53) + 3
        elif strategy == "coprime":
            new_min = ((index * 13 + 5) % 55) + 2
        elif strategy == "valley_greedy" and running_timeline is not None:
            best_min = int(min_f)
            best_peak = float("inf")
            for cand in range(1, 58):
                cand_cron = f"{cand} {hour_f} {dom_f} {mon_f} {dow_f}"
                starts = expand_cron_day_minutes(cand_cron)
                peak = max(
                    running_timeline[(s + d) % 1440] + job.conns
                    for s in starts
                    for d in range(job.duration_min)
                )
                if peak < best_peak:
                    best_peak = peak
                    best_min = cand
            new_min = best_min
        else:
            new_min = (digest % 53) + 3

        new_hour = hour_f
        if hour_f.startswith("*/"):
            h_period = int(hour_f.split("/", 1)[1])
            if 1 < h_period <= 12:
                h_offset = (digest >> 8) % h_period
                new_hour = f"{h_offset}-23/{h_period}"
        return f"{new_min} {new_hour} {dom_f} {mon_f} {dow_f}"

    return orig


def optimize_manifest(
    jobs: List[ScheduledJob], strategy: str = "fnv_hash"
) -> List[ScheduledJob]:
    """De-aligns a list of ScheduledJob items in descending weight order."""
    running = [0] * 1440
    indexed = list(enumerate(jobs))
    indexed.sort(key=lambda pair: pair[1].conns * pair[1].duration_min, reverse=True)

    for idx, job in indexed:
        job.cron = dealign_job_schedule(job, idx, strategy=strategy, running_timeline=running)
        for s in expand_cron_day_minutes(job.cron):
            for d in range(job.duration_min):
                running[(s + d) % 1440] += job.conns

    return jobs


def simulate_retry_storm(
    policy: str = "none",
    fault_duration_sec: int = 18,
    max_retries: int = 4,
    pool_cap: int = 120,
    peak_conns: int = 240,
    shedding_mode: str = "none",
    seed: int = 133742,
) -> Dict[str, float]:
    """
    Runs a 180-second discrete-event retry amplification simulation with strict
    per-attempt cohort tracking (attempt 0 = primary, 1..max_retries = retry waves).
    Policies: 'none', 'full', 'equal', 'decorrelated'.
    """
    state_rng = seed & 0xFFFFFFFF

    def rand_float() -> float:
        nonlocal state_rng
        state_rng = (state_rng + 0x6D2B79F5) & 0xFFFFFFFF
        t = ((state_rng ^ (state_rng >> 15)) * (1 | state_rng)) & 0xFFFFFFFF
        t = ((t + ((t ^ (t >> 7)) * (61 | t))) ^ t) & 0xFFFFFFFF
        return ((t ^ (t >> 14)) & 0xFFFFFFFF) / 4294967296.0

    total_sec = 180
    fault_start = 20
    fault_end = fault_start + fault_duration_sec
    server_cap = max(20, int(pool_cap * 0.85))
    base_rps = max(10, int(server_cap * 0.50))
    cron_burst_rps = max(20, int(peak_conns * 0.42))

    primary = [0] * total_sec
    retry_cohorts: List[List[int]] = [[0] * (max_retries + 1) for _ in range(total_sec)]
    dropped = [0] * total_sec

    for t in range(total_sec):
        burst = cron_burst_rps if 20 <= t <= 25 else 0
        primary[t] = base_rps + burst

    total_primary = sum(primary)
    total_retries = 0
    total_served = 0
    contention_penalty = 0.0

    for t in range(total_sec):
        inc_primary = primary[t]
        inc_retries_by_att = list(retry_cohorts[t])
        inc_retries_total = sum(inc_retries_by_att[1:])

        if shedding_mode == "token_bucket" and inc_retries_total > 0:
            allowed = math.ceil(inc_primary * 0.20)
            if inc_retries_total > allowed:
                keep_ratio = allowed / inc_retries_total
                for a in range(1, max_retries + 1):
                    inc_retries_by_att[a] = int(inc_retries_by_att[a] * keep_ratio)
                inc_retries_total = sum(inc_retries_by_att[1:])

        total_incoming = inc_primary + inc_retries_total

        if fault_start <= t < fault_end:
            eff_cap = max(5, int(server_cap * 0.12))
            contention_penalty = 0.0
        else:
            # Synchronized retry spikes (>1.65x server_cap) trigger severe lock contention
            if total_incoming > server_cap * 1.65 and shedding_mode == "none":
                contention_penalty = min(0.72, contention_penalty + 0.55)
            elif shedding_mode == "lifo_codel":
                contention_penalty = max(0.0, contention_penalty - 0.35)
            else:
                contention_penalty = max(0.0, contention_penalty - 0.25)
            eff_cap = max(15, int(server_cap * (1.0 - contention_penalty)))

        served = min(total_incoming, eff_cap)
        failed_total = max(0, total_incoming - served)
        dropped[t] = failed_total
        total_served += served

        if failed_total > 0 and total_incoming > 0:
            fail_ratio = failed_total / total_incoming
            for att in range(0, max_retries):
                cohort_size = inc_primary if att == 0 else inc_retries_by_att[att]
                failed_in_cohort = int(round(cohort_size * fail_ratio))
                if failed_in_cohort <= 0:
                    continue

                next_att = att + 1
                base_delay = 2 ** next_att

                for _ in range(failed_in_cohort):
                    if policy == "none":
                        delay = base_delay
                    elif policy == "equal":
                        half = max(2, base_delay)
                        delay = half + int(rand_float() * (base_delay * 2 + 1))
                    elif policy == "full":
                        delay = 1 + int(rand_float() * (base_delay * 4))
                    else:
                        delay = 2 + int(rand_float() * (base_delay * 5))

                    target = t + delay
                    if target < total_sec:
                        retry_cohorts[target][next_att] += 1
                        total_retries += 1

    recovery_sec = 0
    for t in range(fault_end, total_sec):
        tot_t = primary[t] + sum(retry_cohorts[t][1:])
        if dropped[t] > base_rps * 0.05 or tot_t > server_cap * 1.05:
            recovery_sec = t - fault_end + 1

    peak_load = max(primary[i] + sum(retry_cohorts[i][1:]) for i in range(total_sec))
    amplification = (total_primary + total_retries) / max(1, total_primary)
    return {
        "peak_rps": float(peak_load),
        "amplification": round(amplification, 3),
        "recovery_sec": float(recovery_sec),
        "success_pct": round(min(100.0, (total_served / max(1, total_primary)) * 100.0), 2),
    }
