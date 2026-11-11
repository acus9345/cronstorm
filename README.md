> [!IMPORTANT]
> **This repo is archived.** Use [cronphase](https://github.com/acus9345/cronphase) instead; it does the same job and is maintained.

# CRONSTORM (`cronstorm`)

**Deterministic Distributed Schedule Collision Analyzer, Auto-Jitter Patch Generator & Metastable Retry-Storm Physics Engine.**

Every engineering organization eventually suffers a self-inflicted `:00` minute outage: dozens of independent `crontab` entries, Kubernetes `CronJobs`, Celery beat tasks, and GitHub Actions workflows silently align at `:00`, `:15`, `:30`, and `00:00 UTC`. When their combined database connections exceed the connection pool cap, queries queue up and time out simultaneously—triggering synchronized client retry waves that lock the system into a metastable failure loop.

`cronstorm` solves both sides of this hazard in a zero-dependency package:
1. **1,440-Minute Impulse Convolution**: Parses standard 5-field cron expressions (including range-steps `7-59/15` and lists) alongside each job's connection weight and runtime duration to find exact collision timestamps.
2. **Deterministic Phase-Shift De-Alignment (`fnv_hash` / `coprime` / `valley_greedy`)**: Rewrites colliding schedules using stateless 32-bit FNV-1a hashes of the service name (e.g. `*/15 * * * *` -> `7-59/15 * * * *`) so every job keeps its exact execution frequency while peak concurrency drops by 55–75%.
3. **Discrete-Event Retry Storm Simulator**: Simulates how synchronized cron bursts interact with client backoff policies (`Naive Exponential`, `Equal Jitter`, `AWS Full Jitter`, `Decorrelated Jitter`) and server load shedding during a transient brownout.

## Quickstart (CLI)

```bash
# Run the unit test suite
python3 -m unittest test_engine.py -v

# Analyze a production schedule manifest and emit a unified git diff patch
python3 cli.py manifests/fintech_prod.cron --pool-cap 120 --dealign fnv_hash --emit-patch
```

## Interactive Oscilloscope Workbench (`index.html`)

Open `index.html` directly in any browser—no build step, bundler, or server required.
