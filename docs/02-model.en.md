# 02 · From records to a computation graph

A data-parallel model separates what a record means from where work executes. The central question is whether an output can be computed from one input partition or needs records owned by many partitions.

## Goals and prerequisites

Prerequisite: Chapter 01 and key/value pairs. You will trace a MapReduce computation, distinguish narrow from wide dependencies, and translate a Spark DAG into stages and tasks. Do not start by memorizing API names; first identify which records must meet.

## Grouping creates a communication boundary

Suppose one partition holds `(blue,2), (red,3)` and another holds `(blue,5), (red,1)`. Mapping each value to twice its value can happen independently. Producing one sum per color needs both partitions' contributions. A hash partitioner can assign each color to an owner, but equal keys still need to arrive at that owner.

MapReduce expresses this as mapping records to intermediate key/value pairs, grouping values by key, and reducing each group. The original paper is the primary source for that model and its runtime responsibilities. Our example and diagram are original teaching material. [Dean and Ghemawat, OSDI 2004](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/)

:::dag

In the standard RDD model, a narrow dependency lets each parent partition feed at most one child partition. Several parents may still be coalesced into one child. A one-to-one map can be pipelined with the next map. A wide dependency can require inputs from many parents: regrouping by a new key generally needs a shuffle. “Wide” describes the dependency, not how many bytes happen to move in one tiny example.

## Spark's execution vocabulary

A transformation builds lineage; an action requests a result. A job created for an action is split into stages at shuffle boundaries. Each stage computes partitions with tasks. A DAG is not a promise that every source-code line becomes a stage. Operators may be pipelined, SQL may rewrite a plan, and reused shuffle output can avoid repeating earlier work. The RDD guide documents lazy transformations and shuffle behavior. [Spark execution guide](https://spark.apache.org/docs/3.5.8/rdd-programming-guide.html#rdd-operations)

```python
pairs = sc.parallelize(range(rows), 8).map(lambda i: (i % 1000, i % 17))
by_key = pairs.reduceByKey(lambda a, b: a + b, 8)
print(by_key.toDebugString().decode())
result = by_key.collect()  # action; output is bounded to 1,000 keys
```

The driver's `collect` is safe here because output cardinality is explicitly bounded. Collecting an unbounded raw dataset would turn a distributed computation into a driver-memory bottleneck.

## Run and annotate a graph

```sh
python scripts/run_all.py --only aggregation
```

In the evidence page, expand the saved lineage and task records for a measured group. Identify the `ShuffledRDD`, the input tasks and the downstream tasks. Use stage IDs as runtime identifiers, not as a guaranteed numbering scheme across reruns. The evidence parser maps JobStart group IDs to Stage IDs and summarizes actual TaskEnd events.

Draw the graph for map → filter → keyed sum. Mark the boundary where records must change owners. Then predict what happens if a filter is placed before the shuffle: it may reduce the records moved, provided the filter preserves the intended query semantics.

## Check your understanding

? Does calling map start a Spark job?

! Normally it records a lazy transformation. An action such as collect or count requests execution; some higher-level APIs may perform planning or metadata work separately.

? Does repartitioning remove the need for every later shuffle?

! No. The next operator may need a different key, ordering or partition count. A useful existing partitioner can sometimes be preserved, but the physical plan is the evidence.

## Exit task

For every edge in your graph, state which parent partitions can contribute to one child. Explain why the keyed sum is logically global even when the final output contains only 1,000 records. Next, reduce the amount of information that must cross that edge.
