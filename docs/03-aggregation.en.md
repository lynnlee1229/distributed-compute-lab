# 03 · Aggregate before moving data

A distributed sum does not need every original value at its destination. It needs enough information to reconstruct the correct sum. Choosing that information is an algorithm design decision.

## Goals and prerequisites

Prerequisite: Chapter 02, associative operations and integer sums. You will design a mergeable summary, explain local combining, and distinguish logical record counts from serialized and compressed Shuffle metrics.

## A summary must preserve the question

For values 2 and 4 on one partition and 9 on another, a local sum produces summaries 6 and 9; merging gives 15. This works because integer addition is associative and commutative. A local average is different: averaging 3 and 9 gives 6, while the global average is 5. Carry `(sum,count)` instead, merge both fields, and divide once at the end.

A useful summary has an identity, a merge operation and a clear invariant. For `(sum,count)`, identity is `(0,0)` and merge is componentwise addition. Exact median cannot generally be recovered from just local medians. Floating-point addition is not exactly associative, so reordered sums can differ slightly even when the distributed algorithm is semantically appropriate. Our integer fixture avoids that ambiguity.

:::aggregate

## Two implementations of the same sum

The experiment generates 240,000 `(i%1000,i%17)` pairs in eight partitions. It caches and materializes this common input outside the comparison. One variant groups values and then sums; another reduces with a local combiner. Both collect 1,000 key totals and must match an independent key-by-key oracle, not just a coincidentally equal grand total.

```python
grouped = pairs.groupByKey(8).mapValues(sum)
combined = pairs.reduceByKey(lambda a, b: a + b, 8)
assert sorted(grouped.collect()) == sorted(combined.collect())
```

Spark documents local merging for reduceByKey and recommends reduce/aggregate operations when a grouped workload only needs aggregation. [reduceByKey](https://spark.apache.org/docs/3.5.8/api/python/reference/api/pyspark.RDD.reduceByKey.html) · [groupByKey](https://spark.apache.org/docs/3.5.8/api/python/reference/api/pyspark.RDD.groupByKey.html)

Do not infer that groupByKey emits one physical Shuffle record per source row. PySpark can batch serialized objects and locally organize value lists. In this saved run both variants report 320 Shuffle records, although they carry different information. The first measured run wrote 85,751 bytes for grouped values and 46,645 bytes for local combining. These are event-log byte counters with compression, not a network packet capture. Remote-read bytes are zero in this local deployment.

## Run and interpret

```sh
python scripts/run_all.py --only aggregation
```

Inspect every repeat in Recorded evidence. Timing ranges overlap; reduced data movement is supported more directly than a broad claim about speed. Our small repetitive integer values compress well. Larger payloads, more keys or fewer duplicate keys per input partition can change the ratio. Local state also has a memory cost; combining is not free.

Change the key count from 1,000 to 10,000 in the generator and matching oracle. Predict how summaries per partition and bytes will change. Then implement mean using `(sum,count)`, with unequal partition sizes to catch the average-of-averages bug. Preserve raw input generation and both equality checks in your result report.

## Check your understanding

? Can any function be safely moved before a Shuffle?

! No. It must preserve the query's required information and obey the needed merge semantics. Order-sensitive string concatenation or an arbitrary local median cannot be treated as an integer sum.

? If compressed bytes are lower, has cross-machine network throughput improved?

! This experiment has no remote Shuffle reads. It shows less serialized Shuffle output under one encoding and dataset. Actual network throughput needs a separate multi-host measurement.

## Exit task

Specify a summary for minimum, mean and distinct count. Distinguish an exact set-based count from an approximate sketch, including memory and error contracts. Use that habit of asking “what must move?” in the Join chapter.
