# 05 · Retry, recompute, persist

Failure tolerance begins with a question: what information survives when an intermediate result disappears? Retrying a task, reconstructing data and committing an external side effect are different problems.

## Goals and prerequisites

Prerequisites: lazy DAG execution and partitions. You will observe a real failed task attempt, distinguish cached reuse from lineage recomputation, and explain what a checkpoint does and does not protect. This chapter concerns finite batch computations; the existing Streaming Lab covers changing records and stateful streaming separately.

## Lineage is a recipe

If a deterministic partition is lost but its inputs and transformations remain available, the engine can reconstruct it. The RDD paper develops this recovery idea for coarse-grained transformations. It does not mean arbitrary mutable state or external services are automatically recoverable. [Zaharia et al., NSDI 2012](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia)

Caching keeps a computed result for reuse. It changes how often a recipe runs, but cached blocks can be evicted or lost. Persistence levels choose memory, disk and replication tradeoffs; calling persist is lazy. A durable checkpoint materializes data and permits a shorter dependency history, paying storage and I/O. Its durability depends on the underlying storage. Local disk checkpoints in this Lab survive a task retry but do not survive loss of the host.

:::recovery

## Three mechanisms, measured separately

The master is `local[2,2]`: two task threads, with up to two attempts per task. Our partition function raises a deliberate exception only for partition 0 on attempt 0. The retried attempt returns its partition index, attempt number and sum. No unrelated process is stopped, and no node failure is claimed.

```python
def flaky(partition, rows):
    context = TaskContext.get()
    if partition == 0 and context.attemptNumber() == 0:
        raise RuntimeError('LAB_INJECTED_FIRST_ATTEMPT_FAILURE')
    yield partition, context.attemptNumber(), sum(rows)
```

The second probe counts successful executions of a pure partition transformation. It materializes a cached RDD, uses it again, then explicitly unpersists it and repeats the action. In each of three independent scenarios the transformation executes 8, 0 and 8 times. The result stays 57,599,760,000. Unpersist is a controlled removal of cached data, not a simulation of a failed node. Accumulators are diagnostic here, not transactional application output.

The third probe requests an RDD checkpoint, runs an action to materialize it, checks `isCheckpointed`, and saves before/after lineage showing `ReliableCheckpointRDD`. An additional checkpoint job can appear in the same group. Spark's API describes when checkpointing takes effect. [RDD.checkpoint](https://spark.apache.org/docs/3.5.8/api/python/reference/api/pyspark.RDD.checkpoint.html)

## Run and interpret

```sh
python scripts/run_all.py --only recovery
```

Inspect the retry group's nine task attempts: eight succeed and one fails. Find the same partition with attempt numbers 0 and 1. Inspect the three cache evaluation traces and the checkpoint lineage. Wall times are retained, but there is no warm-up for these mechanism traces; do not interpret the first materialization versus reuse timing as an isolated performance experiment.

Change the injected failure condition to fail on every attempt in a separate run. Predict and verify job failure when the retry budget is exhausted. Keep this expected-negative run outside the published successful result set. Never attach non-idempotent external writes to this probe.

## Check your understanding

? Can a retried task send a payment exactly once merely because Spark retries it?

! No. Computation retry does not make arbitrary external side effects transactional. Use an appropriate sink protocol, idempotency key or transactional design, and define the failure window.

? Is cache a replacement for a durable checkpoint?

! No. Cache accelerates reuse and can be reconstructed from lineage. A checkpoint materializes a new recovery boundary; storage reliability and access from executors determine what failures it covers.

## Exit task

Draw the boundary between input durability, recomputable intermediate data and external output commits. Explain one failure that this local experiment covers and two it does not. For CDC, stream state and lakehouse snapshots, continue in the [Streaming Lab](https://lynnlee1229.github.io/stream-batch-lab/en/).
