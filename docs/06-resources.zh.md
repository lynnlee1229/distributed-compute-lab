# 06 · 诊断资源，不只看时间

慢任务可能等待计算、内存、磁盘、网络、长尾任务或 Driver。有效诊断应把现象连到机制，再通过一次受控修改验证。

## 学习目标与前置知识

需要 Stage、Shuffle、Join 倾斜和缓存知识。本章区分执行与存储内存，解释 Spill 和任务分布，并设计有边界的后续实验，同时区分已测特性与可选扩展。

## 内存预算有多个使用者

执行内存支持排序、聚合等操作；存储内存保存缓存块。Spark 结合驱逐规则管理这些区域，但它们不是整个进程。用户对象、JVM 开销、Python Worker 和操作系统缓存还要消耗其他内存。Driver 的 1 GiB 堆并不等于整个应用常驻内存上限为 1 GiB。官方调优指南介绍内存模型与序列化选择。[Spark 调优](https://spark.apache.org/docs/3.5.8/tuning.html)

外部排序器可将有序段写盘，再归并，使操作不必把全部记录留在执行缓冲区。Spill 证明走了这条路径，不自动意味着错误。Memory Bytes Spilled 估算经过溢写处理的未压缩内存数据，Disk Bytes Spilled 对应另一种编码；不能把两者直接相加，或当作同口径常驻内存。

:::resources

## 有边界地观察 Spill

实验在分区内部排序 240,000 行，每行含 128 字符负载，比较 2 与 16 分区，执行槽位始终为两个。只对这个应用设置内部参数 `spark.shuffle.spill.numElementsForceSpillThreshold=8192`，让真实排序器在元素阈值下溢写，而不尝试耗尽宿主内存。它是教学控制，不是生产调优建议，也不是自然内存压力基准。实现可在固定版本源码中检查。[配置源码](https://github.com/apache/spark/blob/v3.5.8/core/src/main/scala/org/apache/spark/internal/config/package.scala) · [UnsafeExternalSorter](https://github.com/apache/spark/blob/v3.5.8/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java)

```python
q = source.repartition(partitions).sortWithinPartitions('sort_key')
# 消费所有负载，验证各分区排序，统计行数和 ID 总和。
bins = q.rdd.mapPartitions(resource_partition).collect()
```

验证的是分区内排序，不是全局顺序。两方案都保留行数、ID 总和 28,799,880,000，以及负载总长度 30,720,000，并逐行检查分区内顺序。第一次计时运行中，两分区写出 37,070,511 字节 Spill，十六分区为 28,685,305。计时波动较大，归因前应查看三次重复。

## 运行与诊断

```sh
python scripts/run_all.py --only resources
```

先验证结果不变量，再比较任务数、最长任务、任务峰值执行内存、磁盘 Spill 和 Shuffle 字节。更多分区产生更多任务并改变单任务工作集，宿主仍只有两个槽位。Peak Execution Memory 是任务指标，不是完整 JVM RSS。本地远程 Shuffle 读取为零，因此不能据此识别真实网络瓶颈。指标定义见官方监控文档。[Spark 监控](https://spark.apache.org/docs/3.5.8/monitoring.html)

诊断顺序可以是：定位慢 Stage，检查任务分布、字节与 Spill，查看计划，再改变一个因素。一个长任务可能来自倾斜或慢输入；全部任务都慢可能来自共享资源或单任务工作过多。这些只是待验证假设。

## 数据布局与自适应执行

存储分区可裁剪无关文件，列式格式可跳过无用列，大量小文件会增加元数据与调度工作。这些影响需要 I/O 实验，本版的内存生成数据没有测量它们。AQE 可以根据运行时信息合并 Shuffle 分区、改变 Join 策略，或处理受支持的倾斜场景。五类原始基线关闭 AQE。新增第 07 单元实际执行 Shuffle 后分区合并，包含两个对照和一字节反例，并核验最终计划与实际任务指标。AQE skew Join 与 Join 策略转换仍未实测。[运行 AQE 实操](/distributed-compute-lab/zh/07-aqe/)[AQE 参考](https://spark.apache.org/docs/3.5.8/sql-performance-tuning.html#adaptive-query-execution)

? Spill 非零是否说明堆已耗尽？

! 不说明。这里由元素数量阈值触发。其他任务可能因内存压力溢写，应先检查配置和机制再解释计数器。

? 最快的一次运行能形成生产建议吗？

! 不能。需要保留重复与波动、控制负载，并在目标部署上重测。本 Lab 支持机制推理，不提供容量规划结论。

## 结课实验

选一个工作负载，写正确性约定和预期通信量，预测最慢 Stage，运行三次，再改变一个因素。提交代码、版本、输入公式、计划、全部计时与任务指标，以及包含失效条件的结论。“没有改善”也是有效结果，解释原因才是学习目标。
