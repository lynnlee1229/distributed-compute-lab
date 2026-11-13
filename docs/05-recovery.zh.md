# 05 · 重试、重算与持久化

容错首先要问：中间结果消失后，还有哪些信息存活？重试任务、重建数据、提交外部副作用，是三个不同的问题。

## 学习目标与前置知识

需要理解惰性 DAG 和分区。本章观察真实失败的任务尝试，区分缓存复用与 Lineage 重算，解释检查点的保障边界。本章处理有界批计算；已有 Streaming Lab 单独讲变化记录与有状态流计算。

## Lineage 是一份配方

若确定性分区丢失，但输入和变换仍可用，引擎可以重建它。RDD 论文从粗粒度变换出发发展这一恢复思想；它不意味着任意可变状态或外部服务都自动可恢复。[Zaharia 等，NSDI 2012](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia)

缓存保存已计算的结果，改变配方的执行次数，但缓存块也可能被驱逐或丢失。持久化级别在内存、磁盘与复制间选择；persist 调用本身是惰性的。可靠检查点物化数据、缩短依赖历史，代价是存储和 I/O。其可靠性取决于底层存储。本 Lab 的本地磁盘检查点可用于任务级恢复，不能抵抗整台主机丢失。

:::recovery

## 分别测量三个机制

Master 为 `local[2,2]`：两个任务线程，每个任务最多两次尝试。分区函数只在分区 0 的第 0 次尝试抛出受控异常；重试返回分区号、尝试次数与求和。实验不停止无关进程，也不声称测试了节点故障。

```python
def flaky(partition, rows):
    context = TaskContext.get()
    if partition == 0 and context.attemptNumber() == 0:
        raise RuntimeError('LAB_INJECTED_FIRST_ATTEMPT_FAILURE')
    yield partition, context.attemptNumber(), sum(rows)
```

第二个探针统计纯分区变换成功执行的次数：物化缓存、再次使用、显式 unpersist 后重跑。三个独立场景中，执行次数均为 8、0、8，逻辑结果始终为 57,599,760,000。Unpersist 是受控移除缓存，不是节点故障模拟。累加器只在这里用于诊断，不能作为事务性业务输出。

第三个探针请求 RDD 检查点，通过 Action 物化，检查 `isCheckpointed`，并保存前后 Lineage，其中出现 `ReliableCheckpointRDD`。同一分组可能包含额外的检查点 Job。API 说明了检查点何时生效。[RDD.checkpoint](https://spark.apache.org/docs/3.5.8/api/python/reference/api/pyspark.RDD.checkpoint.html)

## 运行与解释

```sh
python scripts/run_all.py --only recovery
```

每个 retry 组有九次任务尝试：八次成功、一次失败。找到同一分区的 attempt 0 和 1，再检查三组缓存计数及检查点 Lineage。保留的墙钟时间没有独立预热，因此不要把首次物化与复用的计时差当成隔离了其他因素的性能实验。

在单独运行中，把注入条件改为每次尝试都失败。预测并验证达到重试上限后 Job 失败。将这个预期失败用例与已发布的成功结果分开保存。不要为该探针接入不可幂等的外部写入。

## 自测

? Spark 会重试任务，因此任务中的付款能自动恰好执行一次吗？

! 不能。计算重试不让任意外部副作用自动获得事务语义。需要适当的 Sink 协议、幂等键或事务设计，并明确故障窗口。

? 缓存能替代可靠检查点吗？

! 不能。缓存用于复用，可从 Lineage 重建；检查点物化新的恢复边界。它能覆盖哪些故障，取决于存储可靠性和 Executor 的访问能力。

## 过关任务

画出输入持久性、可重算中间数据和外部输出提交的边界。说明本地实验覆盖的一种故障，以及不覆盖的两种故障。CDC、流状态和湖仓快照继续阅读 [Streaming Lab](https://lynnlee1229.github.io/stream-batch-lab/zh/)。
