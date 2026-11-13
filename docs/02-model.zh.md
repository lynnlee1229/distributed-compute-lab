# 02 · 从记录到计算图

数据并行模型把“每条记录的含义”和“在哪里执行”分开。核心问题是：一个输出能否只依赖单个输入分区，还是必须汇集多个分区持有的记录？

## 学习目标与前置知识

需要第一章以及键值对知识。本章要能追踪 MapReduce 计算、区分窄宽依赖，并把 Spark DAG 解释成 Stage 与 Task。先判断哪些记录必须相遇，再学习 API 名字。

## 分组形成通信边界

一个分区有 `(blue,2), (red,3)`，另一个有 `(blue,5), (red,1)`。每个值翻倍可以独立完成；每种颜色只输出一个总和，则需要两边的贡献。哈希分区器能决定键的归属，但仍要把相同键的数据送到对应位置。

MapReduce 把过程表达为：Map 产生中间键值对，按键分组，Reduce 合并每组。原论文是模型与运行时职责的原始资料；这里的例子和图是原创教学内容。[Dean 与 Ghemawat，OSDI 2004](https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/)

:::dag

在标准 RDD 模型中，窄依赖允许每个父分区至多供给一个子分区；多个父分区仍可合并到一个子分区。一对一的 map 可以与后续 map 串接。宽依赖可能需要许多父分区的数据；按新键重新归组通常需要 Shuffle。“宽”描述依赖结构，不是某次小实验实际传输了多少字节。

## Spark 的执行词汇

Transformation 建立 Lineage，Action 请求结果。Action 对应的 Job 在 Shuffle 边界拆成 Stage，每个 Stage 用 Task 计算分区。源码的一行不必对应一个 Stage：算子可能流水执行，SQL 可能重写计划，已有 Shuffle 输出也可能被复用。官方指南解释了惰性变换与 Shuffle。[Spark 执行指南](https://spark.apache.org/docs/3.5.8/rdd-programming-guide.html#rdd-operations)

```python
pairs = sc.parallelize(range(rows), 8).map(lambda i: (i % 1000, i % 17))
by_key = pairs.reduceByKey(lambda a, b: a + b, 8)
print(by_key.toDebugString().decode())
result = by_key.collect()  # 输出显式限制为 1,000 个键
```

这里 collect 的输出数量有界，因此适合带回 Driver。直接收集无界原始数据，会把分布式工作转成 Driver 内存瓶颈。

## 运行并标注计算图

```sh
python scripts/run_all.py --only aggregation
```

在证据页展开保存的 Lineage 和某个计时组的任务记录，找出 `ShuffledRDD`、上游任务和下游任务。Stage ID 只是本次运行的标识，重跑时不保证编号相同。导出器通过 JobStart 的分组 ID 找到 Stage ID，再汇总实际 TaskEnd 事件。

自己画出 map → filter → 按键求和的图，标出记录需要改变归属的边界。再预测把过滤放到 Shuffle 前的影响：只要语义不变，它可能减少需要移动的记录数。

## 自测

? 调用 map 就启动 Spark Job 吗？

! 通常只是记录惰性变换；collect、count 等 Action 才请求执行。一些高层 API 还可能单独做规划或元数据工作。

? 提前 repartition 就能消除后面所有 Shuffle 吗？

! 不能。后续算子可能要求另一种键、排序或分区数。已有分区器有时能被保留，但仍应查看物理执行计划。

## 过关任务

为图中每条边说明：一个子分区可能收到哪些父分区的贡献。解释为什么最终只有 1,000 条记录的按键求和仍是全局计算。下一章将减少跨边界传递的信息。
