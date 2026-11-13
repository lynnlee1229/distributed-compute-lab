# Distributed Compute Lab · validation

Baseline date: 2026-11-13 (Asia/Shanghai). This file accompanies the reproducible learning release. Publication evidence is recorded separately in the private workspace ops/distributed-compute-lab-release.json.

## Executed baseline

- Five real Spark applications, 240,000 rows each; Spark 3.5.8 / Py4J 0.10.9.9 / Python 3.11.2 / Zulu JDK 11.0.15, macOS arm64 on Apple M1 Pro, 8 cores / 16 GB.
- local[2,2]: two execution threads in a single JVM with Python workers, 1 GiB heap. Not a distributed executor cluster or multi-machine scaling test. Actual remote Shuffle reads are zero.
- Parallelism, aggregation, Join and resources: one warm-up for each variant; three measured repeats, alternating variant order. Recovery: three complete mechanism scenarios, no warm-up.
- Aggregation checks all 1,000 key totals against an independent Python oracle. First measured Shuffle bytes: group values 85,751; local combine 46,645. PySpark records are batched; counters are not source row counts.
- Join outputs: 240,000 rows, weight sum 384,001 in every variant. Largest partition: shuffle 197,242; broadcast 30,000; salted 78,149. Salting still has collisions, moves more bytes and does not improve the baseline median timing.
- Each recovery scenario records one failed attempt followed by a successful retry; cache transformation evaluations 8/0/8; checkpoint materializes and truncates lineage. No process kill or host-failure claim.
- Resource sorting validates row count, ID sum 28,799,880,000, payload length 30,720,000 and per-partition ordering. Real disk/memory Spill counters are nonzero. Spill is deliberately triggered by an internal 8,192-element threshold, not natural OOM pressure.
- Raw runtime artifacts stay local under ignored build/. Published reports are allowlisted TaskEnd metrics and cleaned plans; result hashes are in results/index.json.

## Validation entry points

```sh
python -m unittest discover -s tests -v
python scripts/build_site.py
python scripts/check_site.py
python scripts/run_all.py --smoke
```

Tests check semantic invariants, observed plans, task failure/retry identity, cache counters, checkpoint lineage, actual Spill, result hashes and the exporter's privacy/stage-attribution behavior. Optional smoke checks execute only when smoke evidence exists. The public CI runs all smoke applications and retains sanitized reports.

The bilingual static site contains six core chapters plus overview, evidence, reproduction and code pages. Source packaging is allowlisted, never a workspace snapshot. Complete source is downloadable and does not require the private homepage repository. References are primary papers and pinned official Spark documentation; no paid course material is copied.

Browser and extracted-kit acceptance details are added after the corresponding checks. Timing values are machine observations, not production capacity conclusions or statistically established speedups. Multi-host networking, driver/host failure, natural memory pressure, I/O layout and AQE performance are outside the measured baseline.

## Local acceptance completed

- Extracted the actual downloadable ZIP into a fresh project directory. Using the pinned installed runtime, ran all five smoke applications there; all succeeded. All eight evidence/export tests passed, including the generated smoke checks. Built and validated that extracted project's own 20 bilingual pages and allowlisted archive.
- Browser: report switching, warm-up visibility, Join output histogram, measured run selection, actual plan expansion and per-task expansion passed. Recovery view shows nine attempts including ExceptionFailure and successful retry.
- Cost model changes from 620 ms to 515 ms when slots change from 2 to 3 under its explicit illustrative assumptions. Read marking updates aria-pressed. EN→ZH preserves the current chapter.
- At 390 CSS px, Chinese course and evidence pages have document scrollWidth 390; table overflow remains in its container. No application warning/error was observed in the local browser console. Screenshots remain in private build/.
- The in-app browser's download-event capture timed out. The source archive was independently opened/extracted and executed; release verification additionally downloads the published ZIP and JSON over HTTP and checks their hashes. This is distinguished from a successful browser download-event callback.
