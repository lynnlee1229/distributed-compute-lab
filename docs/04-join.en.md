# 04 · Join strategies and skew

A Join brings matching records together. The best movement strategy depends on relation size, key distribution, output cardinality and memory capacity. The algorithm's name alone is not a diagnosis.

## Goals and prerequisites

Prerequisites: keyed partitioning, Shuffle and local aggregation. You will compare broadcast and sort-merge plans, inspect a hot partition, and evaluate salting as a tradeoff. Basic inner Join semantics are assumed: every matching pair contributes one output row.

## Decide which side travels

A broadcast hash Join builds a lookup structure from a sufficiently small side and makes it available where the large side is processed. It trades replication and build memory for avoiding a keyed redistribution of the large side. The serialized file size is not necessarily the in-memory hash table size. The small side must fit within the actual resource and Join-type constraints.

A shuffled sort-merge Join repartitions matching keys to compatible owners and sorts the inputs as needed. It can handle a larger build relation but pays for movement and sorting. A hash function distributes distinct keys; it cannot split one enormous key group merely by increasing the partition count. Spark's strategy hints and AQE behavior are documented here. [Spark SQL performance guide](https://spark.apache.org/docs/3.5.8/sql-performance-tuning.html)

:::join

## The controlled comparison

Our fact table has 240,000 rows; 80% use key 0. A 1,000-row dimension has exactly one row per key. The output must contain 240,000 rows with weight checksum 384,001. The experiment forces sort-merge, explicitly broadcasts the dimension, and tests a salted sort-merge alternative. AQE and automatic broadcasting are disabled so baseline plans stay interpretable.

```python
shuffle = fact.hint('merge').join(dim.hint('merge'), 'k')
broadcast = fact.join(F.broadcast(dim), 'k')
# Salt: one bucket per fact row; replicate dimension across all 8 buckets.
left = fact.withColumn('salt', (F.col('i') % 8).cast('int'))
right = dim.crossJoin(spark.range(8).select(F.col('id').cast('int').alias('salt')))
salted = left.hint('merge').join(right.hint('merge'), ['k', 'salt'])
```

Replicating the small side matches each fact row once because its salt has exactly one corresponding dimension copy. Replicating both sides indiscriminately would multiply matches. This teaching implementation salts every key, so it deliberately pays more replication than a production strategy targeting only known hot keys.

## Read the evidence before choosing

```sh
python scripts/run_all.py --only join
```

Inspect saved physical plans for `SortMergeJoin`, `BroadcastHashJoin` and `Exchange`. We consume Join output directly by partition, before a final aggregation could hide skew. The unsalted output's largest partition contains 197,242 rows. Broadcast retains eight 30,000-row fact partitions. Salting lowers the largest partition to 78,149, but hash collisions leave it uneven. In this run salting moves more bytes and its median wall time does not improve. A less skewed histogram is not automatically a faster job.

The fixture's unique dimension key makes row-count plus weight checksum a useful invariant, but checksums are not a universal proof for arbitrary Join rewrites. For your own small fixture, compare complete sorted output including duplicates and null cases. For outer Joins, reconsider unmatched rows before applying any salting rewrite.

## Practice and self-check

Change the hot-key fraction while holding total rows and dimension fixed. Predict task imbalance, output count and broadcast feasibility separately. In a separate optional study, enable AQE, save both initial and final executed plans, and inspect whether runtime thresholds actually trigger a rewrite. This release explains AQE but does not claim to have measured that extension.

? Does adding partitions split a single hot key automatically?

! Hash partitioning keeps equal keys together. More buckets may spread other keys while leaving the hot group intact.

? Why not broadcast every dimension?

! Replication, hash-table memory, transfer cost, Join support and timeouts bound the strategy. A hint expresses a preference, not unlimited memory or guaranteed applicability.

## Exit task

Write a decision using relation cardinalities, uniqueness, hot-key frequency, memory budget and actual plan nodes. Include a condition that would invalidate your choice. Next, ask what happens when a task needs to be repeated.
