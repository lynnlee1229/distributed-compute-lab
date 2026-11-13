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

Browser and extracted-kit acceptance details are added after the corresponding checks. Timing values are machine observations, not production capacity conclusions or statistically established speedups. Multi-host networking, driver/host failure, natural memory pressure and I/O layout are outside the measured baseline. The 2026-11-14 addition below measures AQE coalescing separately; the original five applications still disable AQE.

## Local acceptance completed

- Extracted the actual downloadable ZIP into a fresh project directory. Using the pinned installed runtime, ran all five smoke applications there; all succeeded. All eight evidence/export tests passed, including the generated smoke checks. Built and validated that extracted project's own 20 bilingual pages and allowlisted archive.
- Browser: report switching, warm-up visibility, Join output histogram, measured run selection, actual plan expansion and per-task expansion passed. Recovery view shows nine attempts including ExceptionFailure and successful retry.
- Cost model changes from 620 ms to 515 ms when slots change from 2 to 3 under its explicit illustrative assumptions. Read marking updates aria-pressed. EN→ZH preserves the current chapter.
- At 390 CSS px, Chinese course and evidence pages have document scrollWidth 390; table overflow remains in its container. No application warning/error was observed in the local browser console. Screenshots remain in private build/.
- The in-app browser's download-event capture timed out. The source archive was independently opened/extracted and executed; release verification additionally downloads the published ZIP and JSON over HTTP and checks their hashes. This is distinguished from a successful browser download-event callback.

## Depth iteration · 2026-11-14 (Asia/Shanghai)

### Audit and chosen gap

The audit found four learning gaps: upstream links lacked a searchable structured reading map; AQE was explicitly unexecuted; old Join plans were captured before actions (strategy evidence, not final adaptive plans); the downloadable kit had no AQE action/plan/metrics reproduction contract. The selected implementation is post-shuffle coalescing, with exact pre/post-collect plans, independent correctness checks and real TaskEnd metrics. The five original report files and their experiments are unchanged. Executor-loss recovery and AQE skew Join were deliberately left unimplemented rather than implied by local tasks.

### Course and learning evidence

- `resources/catalog.json`: two selected university courses, Berkeley CS267 Applications of Parallel Computers (Spring 2016 archive) and MIT 6.5840 Distributed Systems (Spring 2026 published notes/specification); seven units map to exact lectures, reading locations or a study-only assignment specification and original runnable exercises. Metadata includes school, course number/name, term, prerequisites, access/enrollment distinctions, original URLs and 2026-11-14 verification dates.
- `resources/verification.json`: unauthenticated access checks of selected official URLs, with status and content type; the lecture/PDF contents were separately inspected. This does not assert video access or course enrollment. CS267 2025's official page specifies a Berkeley login for videos; the selected open archive is dated 2016. Reused Lecture 23 slides include older dates/numbering, disclosed in the catalogue.
- MIT's actual collaboration policy prohibits public solutions. The repository contains no MIT coordinator implementation, starter code or assignment answer. ZeRO (2019/2020) and Orca (2022) are published primary reading extensions about state sharding/communication and iteration scheduling/tail latency; training, GPU collectives and inference engines are not implemented or evaluated.
- Search works via `scripts/search_resources.py`, including Chinese terms and `--unit`, and through the pre-rendered bilingual course map. Readings are paired with predictions, invariants, concrete commands and interpretation, not only external link cards.

### New executed learning loop

Environment matches the original pinned local runtime: Spark 3.5.8 / Python 3.11.2 / JDK 11.0.15, macOS arm64, local[2,2], 1 GiB JVM heap. AQE gets a separate application; no forced Spill threshold is configured. Seed 42 is a deterministic modular offset, not a sampled population. The input uses eight partitions and the exchange initially has 32.

- `results/aqe.json`: 240,000 rows, one warm-up per variant, three alternating measured repetitions. AQE off and AQE on with coalescing disabled each retain 32 readers and 40 attempts (8+32). Coalescing enabled gives one reader and 9 attempts (8+1) in every repetition.
- Before collect, adaptive plans have `isFinalPlan=false`; afterward they have `isFinalPlan=true`. The same collected DataFrame's final plan contains `AQEShuffleRead coalesced`, SQL metric `numPartitions=1` and a `CoalescedPartitionSpec` covering [0,32). Actual TaskEnd stage counts agree. The original 32-bucket exchange remains; this is reader coalescing, not a rewrite of the original hash bucket count.
- Each measured variant writes/reads 109,878 Shuffle bytes. The task reduction does not establish byte or network savings. All remote reads remain zero.
- Median observed wall ms: AQE off 338.072 (282.766–366.075), AQE on without coalescing 318.730 (294.457–391.172), coalesced 234.418 (211.144–244.862). These include plan construction, execution, Python validation and plan/metric inspection; startup is excluded. Three repetitions are descriptive, not a confidence interval or established production speedup.
- `results/aqe-1-byte.json`: same 240,000-row input, one warm-up per variant and one measured repetition with only the advisory target changed to 1 byte. All three variants retain 32 readers/40 attempts. Enabling AQE and the rule does not guarantee merging; an advisory target is not a hard maximum and coalescing cannot split oversized existing buckets. This is a mechanism counterexample, not a timing study.
- The published full output comes from actual collect, not an authored expected fixture. Each action asserts equality of all 1,000 key counts/sums with an independent Python oracle and saves its output digest. `scripts/check_aqe.py` uses a second enumeration (per-key arithmetic progressions via a modular inverse) and checks final-plan flags, SQL reader specs, task-stage agreement and output hashes. Negative regression cases reject two wrong key sums even when global totals match, provisional plans, and inconsistent stage counts.
- Executed the documented modification: 23,017 rows, seed 73, 16 initial Shuffle partitions, one measured repeat. Uneven per-key counts pass the independent oracle; controls retain 16 readers and coalescing gives 1. Its sanitized report remains in the local acceptance assets. No hard-coded baseline answer is used by the executor.

### Reproduction and delivery checks

The validation entry points above remain applicable. New entry points are:

```sh
python scripts/run_aqe.py
python scripts/check_aqe.py build/aqe-results/aqe.json --expect coalesced
python scripts/run_aqe.py --advisory-bytes 1 --repeats 1 --out build/aqe-tiny-results
python scripts/check_aqe.py build/aqe-tiny-results/aqe.json --expect unchanged
python scripts/run_aqe.py --rows 23017 --seed 73 --shuffle-partitions 16 --repeats 1 --out build/aqe-changed-results
python scripts/check_aqe.py build/aqe-changed-results/aqe.json --expect coalesced
```

The source checks verify the five unchanged baseline semantics, final AQE evidence, counterexample, independent verifier rejection cases, source-map referential integrity, and preservation of earlier hashes after selective export. CI executes the five original smoke applications plus the new coalescing controls, tiny-target counterexample and changed-input exercise on Linux. Committed laptop results are distinct from CI evidence.

Acceptance additionally extracts the actual downloadable archive into an isolated temporary directory, executes the original smoke applications and documented AQE loop there with the pinned installed interpreter, then runs that extracted kit's own tests, build and privacy/link checks. Browser acceptance covers resource search, paired languages, selected-run final plans and SQL/stage metrics, the counterexample, and retained original Join/recovery views. Public release status, commit hashes, exact HTTP hashes and Pages results are recorded in the private workspace release receipt rather than copied from an earlier release.

Raw events, logs and scratch from the new runner are session-owned temporary files removed after export, on success or failure. Temporary extraction/preview assets are cleaned at completion; source, pinned dependencies, sanitized reports and necessary acceptance receipts are retained. No global cleanup, unrelated resources, private course data or paid APIs are involved. This addition does not change the original forced-Spill or task-exception boundaries.
