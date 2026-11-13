# 07 · Prove that the adaptive plan changed

AQE is a runtime optimizer, so a configuration flag is insufficient evidence. This original exercise executes the same aggregation three ways and checks the final physical plan of the exact DataFrame that was collected. It closes the earlier unmeasured coalescing extension; the five older baseline experiments remain unchanged.

## Before you start

Know keys, mergeable sums, Shuffle stages and task slots from Chapters 01–06. Read [Spark 3.5.8: Coalescing Post Shuffle Partitions](https://spark.apache.org/docs/3.5.8/sql-performance-tuning.html#coalescing-post-shuffle-partitions). For the conceptual task-granularity background, read CS267 Lecture 23 through the [course reading map](/distributed-compute-lab/en/courses/). CS267 is a parallel-computing course; the pinned Spark guide explains this implementation.

You will distinguish a provisional plan from a final one, compare SQL operator metrics with TaskEnd evidence, and design a counterexample where AQE is enabled but no partitions merge. Predict the three output partition counts before opening the saved report.

## Contract and controls

The source is a deterministic integer range, split into eight input partitions. With seed 42, each index i produces k=(31×i+42) mod 1000 and v=(17×i+42) mod 101−50. There is no random timing generator, external data, model API or pretrained fixture. Seed is an explicit offset of this modular generator, not a claim of random sampling.

The output is the count and sum for every key. A Python loop computes an oracle without consulting Spark. Every collected result must equal it in full. The published output is an actual collected result; each run also saves its full-output SHA-256. The independent verifier enumerates each key's arithmetic progression using the modular inverse of 31, a different enumeration from the runtime oracle. Matching a global count and sum alone is too weak: moving one unit between two key sums preserves both totals but must fail verification.

- aqe-off: adaptive.enabled=false; coalescing setting is present but inactive.
- aqe-on-no-coalesce: adaptive.enabled=true, coalescePartitions.enabled=false. This isolates the particular rule from the AQE umbrella.
- aqe-on-coalesce: both true, parallelismFirst=false, advisory target 1 MiB, minimum partition size 1 byte, initial Shuffle partitions 32.

These deliberately small teaching thresholds expose the mechanism on a laptop. They are not production recommendations. Automatic broadcasting, skew-join optimization and the local Shuffle reader are disabled; the query contains no Join. Each variant has one warm-up and three measured repeats, with order reversed on alternate repeats. Reversing is a partial order control, not complete randomization. No application or input cache is reused between separate script invocations.

## Execute and inspect

Activate the pinned environment from the reproduction guide, then run from the companion root:

```sh
python scripts/run_aqe.py
python scripts/check_aqe.py build/aqe-results/aqe.json --expect coalesced
```

The default writes sanitized evidence to build/aqe-results/ and preserves results/. The script uses local[2,2] and a 1 GiB heap, with a ten-minute subprocess deadline. It cleans its own temporary event logs, scratch and runtime messages on success or failure. A timeout terminates only its owned process group. A loopback port is required by Spark, even though the workload uses no remote service.

Read runs[].adaptive, runs[].stage_metrics, tasks and plans in the JSON or the [evidence viewer](/distributed-compute-lab/en/evidence/). For each action there is a before and after plan under that action's group name. AQE plans must change from isFinalPlan=false before collect to isFinalPlan=true afterward. Reading an unrelated count action's plan, or inspecting only the pre-action string, would not establish this.

In the committed 240,000-row run, both controls retain 32 output partitions and 40 task attempts (8 map + 32 reduce). Coalescing uses one output partition and 9 task attempts (8 + 1). Its final plan contains AQEShuffleRead coalesced; the actual reader's numPartitions is 1, with a CoalescedPartitionSpec spanning reducer indices [0,32). That is one reader covering 32 original hash buckets, not a claim that the original exchange was rewritten with one bucket. The upstream rule uses runtime map-output statistics. [Pinned rule source](https://github.com/apache/spark/blob/v3.5.8/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala)

The partial aggregation and original exchange remain. Compare Shuffle bytes across all three variants: merging readers can reduce scheduling work without reducing the bytes originally written. SQL partitionDataSize is a map-status estimate; it need not equal the exact TaskEnd byte counters. numCoalescedPartitions counts output specs that combine buckets, not the number of removed buckets. Remote read bytes remain zero on this single host.

## Run the counterexample

```sh
python scripts/run_aqe.py --advisory-bytes 1 --repeats 1 --out build/aqe-tiny-results
python scripts/check_aqe.py build/aqe-tiny-results/aqe.json --expect unchanged
```

Only the advisory target changes; the one measured repetition is a mechanism check, not a performance estimate. The saved counterexample retains 32 output partitions and 40 attempts even with AQE and coalescing enabled. Existing buckets exceed the tiny target; the coalescing rule cannot split them to satisfy a one-byte target. This refutes “AQE enabled means fewer tasks” and “advisory bytes is a hard limit.” [Saved real counterexample](/distributed-compute-lab/results/aqe-1-byte.json)

## Change an input, retain the contract

```sh
python scripts/run_aqe.py --rows 23017 --seed 73 --shuffle-partitions 16 --repeats 1 --out build/aqe-changed-results
python scripts/check_aqe.py build/aqe-changed-results/aqe.json --expect coalesced
```

Predict the full output's change and the input/reduce task counts. This non-multiple-of-1,000 input makes key counts uneven, so a hard-coded 240-per-key answer cannot pass. This exact modification was executed during acceptance. The seed changes data; it does not fix OS scheduling or JVM timing. Save your evidence outside the committed baseline unless intentionally producing a new release.

## Defend your conclusion

Compare every repetition, not just the fastest. The timings include query construction, collect, Python equality validation and plan/metric inspection; JVM startup is excluded. Coalescing saves tasks here, but may remove useful parallelism on a larger workload. This experiment measures scheduling/partition mechanisms on two local task threads, not network savings or production capacity. It provides no confidence interval or statistically established speedup.

AQE skew Join, Join strategy conversion, multiple actual executors, host failure and distributed training/inference remain unimplemented. The [AI reading bridge](/distributed-compute-lab/en/courses/#ai-bridge) connects sharding and tail latency as reasoning questions, without claiming this Spark run evaluates an LLM engine.

? Why is the no-coalescing AQE control necessary?

! It separates the umbrella optimizer from one rule. It can have a final adaptive plan while retaining 32 reducers, so the final-plan flag alone is not enough either.

? What would falsify the “32 to 1” mechanism conclusion?

! An unfinalized plan, missing coalesced reader, incompatible reader specs, different per-key output, or TaskEnd stage counts disagreeing with the observed partitioning. The verifier checks those signals together.

? Can the one-byte target produce one-byte tasks?

! No. It is advisory and this rule only combines whole existing buckets. Splitting skewed Join partitions is a different feature and was disabled here.

## Exit task

Submit a prediction, one coalesced run and one non-coalesced counterexample, exact configuration, full-output verification, before/final plans and task stages. Explain whether your timings support only a mechanism observation or a repeatable performance claim. A result with no measured speedup is acceptable when the evidence and limits are sound.
