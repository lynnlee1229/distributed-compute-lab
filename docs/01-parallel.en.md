# 01 · Why parallelize?

Parallel execution buys capacity only when useful work outweighs coordination. This chapter starts with a cost model, then challenges it with a real workload small enough for a laptop.

## Goals and prerequisites

You should be able to distinguish work, critical path, task count and available execution slots; explain why more partitions can make a job slower; and specify correctness before comparing speed. Prerequisites: loops, functions, integer arithmetic and elapsed time. You do not need prior Spark experience. Read Setup first if Python environments or JDKs are new.

## Work is not elapsed time

Let W be total useful computation and P the number of slots that can run tasks at once. Even with perfect division, elapsed compute time cannot beat W/P. Dependencies impose a second lower bound, the critical path D. A useful starting point is T ≥ max(W/P, D). Scheduling, serialization, transfers, skew and retries add costs; the formula is a bound, not a prediction.

For example, eight independent tasks costing 100 ms each have 800 ms of work. Two slots need at least four waves, or 400 ms. Eight tasks are not eight CPUs. If a serial 200 ms preparation step precedes them, the critical path and end-to-end latency include that step. Throughput counts completed units over an interval; latency measures one unit's delay. Increasing batch size may improve the former while hurting the latter.

If fraction s of a fixed problem is inherently serial, the idealized Amdahl speedup is at most 1/(s+(1-s)/P). This assumes fixed work and ignores overhead. It explains a ceiling, not a benchmark result. The calculator below adds a simple per-task cost to make the assumptions visible.

:::cost

## From the model to Spark

A partition describes a share of data. A stage schedules a task for each partition it must compute. Our local master has two execution threads; increasing partitions creates more scheduling waves. Python workers, the JVM and the operating system share one physical host. The Spark programming guide defines the driver and distributed collection abstraction; here we exercise its local implementation. [Spark RDD guide](https://spark.apache.org/docs/3.5.8/rdd-programming-guide.html)

The experiment sums `(i*31)%997` over 240,000 integers using 1, 8 and 64 partitions. Every variant must equal an independent Python sum. Input size and arithmetic stay fixed. This workload is intentionally cheap: scheduler and Python boundary costs can dominate, so it does not demonstrate that one partition is generally optimal.

```python
rdd = sc.parallelize(range(rows), partitions)
actual = rdd.map(lambda i: (i * 31) % 997).sum()
expected = sum((i * 31) % 997 for i in range(rows))
assert actual == expected
```

## Run, inspect, change

```sh
python scripts/run_all.py --only parallel
```

Open Recorded evidence → parallel. Separate the warm-up from the three measured repeats, compare the range as well as the median, and inspect task counts. If 64 partitions lose, first ask whether each task had enough useful work. A shorter wall time does not prove CPU efficiency; this release does not sample CPU utilization.

Change the arithmetic to a heavier pure function in a new working copy, keeping the same mathematical output across variants. Record the new formula, input size, repetitions and environment. Predict where task overhead stops dominating before you run it. Do not compare the changed workload against the original as if only parallelism changed.

## Check your understanding

? Sixteen partitions, two slots: can all sixteen execute simultaneously?

! No. Ignoring retries and resource changes, at most two tasks execute at once. Sixteen tasks require at least eight waves.

? Does a 2× speedup on this host predict 2× throughput on two machines?

! No. Separate machines introduce networking, storage placement, failures and different resource contention. A scaling study must vary machines under controlled load and state whether total work is fixed.

## Exit task

Write a hypothesis with one independent variable, one correctness invariant, one timing boundary and two possible explanations for a slower run. Carry the distinction between partition count and slot count into Chapter 02.
