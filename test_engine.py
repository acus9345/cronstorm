#!/usr/bin/env python3
"""Unit test suite for cronstorm engine & CLI."""

import unittest
from pathlib import Path
from engine import (
    ScheduledJob,
    compute_impulse_matrix,
    expand_cron_day_minutes,
    fnv1a_32,
    optimize_manifest,
    simulate_retry_storm,
)


class TestCronstormEngine(unittest.TestCase):
    def test_fnv1a_determinism(self) -> None:
        self.assertEqual(fnv1a_32("ledger-reconcile"), fnv1a_32("ledger-reconcile"))
        self.assertNotEqual(fnv1a_32("ledger-reconcile"), fnv1a_32("stripe-replay"))

    def test_cron_expansion_cadence_preservation(self) -> None:
        orig_mins = expand_cron_day_minutes("*/15 * * * *")
        shifted_mins = expand_cron_day_minutes("7-59/15 * * * *")
        self.assertEqual(len(orig_mins), 96)
        self.assertEqual(len(shifted_mins), 96)
        self.assertIn(0, orig_mins)
        self.assertNotIn(0, shifted_mins)
        self.assertIn(7, shifted_mins)

    def test_dealignment_reduces_peak_collision(self) -> None:
        jobs = [
            ScheduledJob("svc-a", "*/15 * * * *", conns=30, duration_min=3),
            ScheduledJob("svc-b", "*/15 * * * *", conns=30, duration_min=3),
            ScheduledJob("svc-c", "*/15 * * * *", conns=30, duration_min=3),
            ScheduledJob("svc-d", "0 * * * *", conns=40, duration_min=4),
            ScheduledJob("svc-e", "0 * * * *", conns=40, duration_min=4),
        ]
        base_tl, _ = compute_impulse_matrix(jobs, use_orig=True)
        self.assertEqual(max(base_tl), 170)

        optimize_manifest(jobs, strategy="valley_greedy")
        opt_tl, _ = compute_impulse_matrix(jobs, use_orig=False)
        self.assertLess(max(opt_tl), max(base_tl))

    def test_retry_storm_jitter_outperforms_naive(self) -> None:
        naive = simulate_retry_storm(policy="none", fault_duration_sec=20, max_retries=4)
        full_jitter = simulate_retry_storm(policy="full", fault_duration_sec=20, max_retries=4)
        self.assertLessEqual(full_jitter["peak_rps"], naive["peak_rps"])
        self.assertLessEqual(full_jitter["recovery_sec"], naive["recovery_sec"])


if __name__ == "__main__":
    unittest.main()
