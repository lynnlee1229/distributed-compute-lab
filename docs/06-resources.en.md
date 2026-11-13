# 06 · Diagnose resources, not just time

A slow job may be waiting for compute, memory, disk, network, a long tail of tasks or the driver. A useful diagnosis connects a symptom to a mechanism and then tests one change.

## Goals and prerequisites

Prerequisites: stages, Shuffle, Join skew and caching. You will separate execution from storage memory, interpret Spill and task distributions, and propose a bounded follow-up experiment. You will also distinguish measured features from optional extensions.

## A resource budget has several owners

Execution memory supports operations such as sorts and aggregations. Storage memory keeps cached blocks. Spark manages these areas together with eviction rules; they do not constitute the whole process. User objects, JVM overhead, Python workers and operating-system caches consume additional memory. A 1 GiB driver heap is not a 1 GiB cap on this application's total resident memory. The tuning guide explains Spark's memory model and serialization choices. [Spark tuning](https://spark.apache.org/docs/3.5.8/tuning.html)

When an external sorter writes runs to disk and later merges them, the operation can complete without retaining all records in its execution buffer. Spill is evidence of that path, not automatically an error. Memory bytes spilled estimate uncompressed in-memory data processed through Spill; disk bytes spilled represent a different encoding. Adding or comparing them as though they were identical resident-memory measurements is misleading.

:::resources

## A bounded way to observe Spill

This experiment sorts 240,000 rows with a 128-character payload inside each partition, comparing 2 and 16 partitions while retaining two task slots. It sets the internal `spark.shuffle.spill.numElementsForceSpillThreshold` to 8,192 for this application only. That deliberate threshold makes real sorter Spill visible without trying to exhaust host memory. It is a teaching control, not a production recommendation or a natural memory-pressure benchmark. Its implementation can be inspected in the pinned source. [Spark configuration source](https://github.com/apache/spark/blob/v3.5.8/core/src/main/scala/org/apache/spark/internal/config/package.scala) · [UnsafeExternalSorter](https://github.com/apache/spark/blob/v3.5.8/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java)

```python
q = source.repartition(partitions).sortWithinPartitions('sort_key')
# Consume every payload, validate per-partition order, count rows and sum ids.
bins = q.rdd.mapPartitions(resource_partition).collect()
```

This validates local sorting, not global order. Both variants preserve row count, ID sum 28,799,880,000 and total payload length 30,720,000. Their per-partition order is checked row by row. The measured first repeat spills 37,070,511 disk bytes with two partitions and 28,685,305 with sixteen. The timing spread is large: read all three repeats before attributing an improvement to a single resource.

## Run and diagnose

```sh
python scripts/run_all.py --only resources
```

First verify the result invariant. Then compare task counts, maximum task time, peak task execution memory, disk Spill and Shuffle bytes. More partitions make more tasks and change per-task working sets; the host still has two slots. Peak execution memory is a task metric, not full JVM RSS. In local mode remote Shuffle reads are zero, so this evidence cannot identify a real network bottleneck. Metric definitions come from the official monitoring guide. [Spark monitoring](https://spark.apache.org/docs/3.5.8/monitoring.html)

A diagnostic sequence is: locate the slow stage, inspect task spread, check bytes and Spill, inspect the plan, then vary one factor. A single long task suggests skew or a slow input; uniformly slow tasks suggest a shared resource or too much work per task. These are hypotheses, not diagnoses without evidence.

## Data layout and adaptive execution

Storage partitioning can prune irrelevant files; columnar formats can avoid reading unused columns; many tiny files can add metadata and scheduling work. Those effects require an I/O experiment, not this generated in-memory source. AQE can coalesce Shuffle partitions, change Join strategy and handle supported skew cases based on runtime information. The five original baseline experiments disable AQE. The new Chapter 07 executes post-shuffle coalescing with two controls and a one-byte counterexample; it checks the final plan and actual task metrics. AQE skew Join and Join-strategy conversion remain unmeasured. [Run the AQE walkthrough](/distributed-compute-lab/en/07-aqe/) [AQE reference](https://spark.apache.org/docs/3.5.8/sql-performance-tuning.html#adaptive-query-execution)

? Does nonzero Spill mean the heap was exhausted?

! No. Here the sorter is forced to spill by an element-count threshold. Other workloads may spill under memory pressure. Inspect configuration and mechanism before interpreting the counter.

? Can the fastest single run establish a production recommendation?

! No. Preserve repetitions and variation, control the workload, and repeat under the intended deployment. This Lab supports reasoning about mechanisms, not a capacity plan.

## Capstone

Choose one workload. Write its correctness contract and expected communication, predict its slowest stage, run three repetitions, then change one factor. Submit code, versions, input formula, plans, all timings and task metrics, plus a conclusion with an explicit failure condition. An acceptable result can be “no improvement”; explaining it is the learning goal.
