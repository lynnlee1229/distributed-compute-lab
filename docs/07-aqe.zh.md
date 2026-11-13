# 07 · 证明自适应计划确实改变

AQE 是运行时优化器，单独一个配置开关不构成证据。本原创练习用三种配置执行同一聚合，检查真正 collect 的那个 DataFrame 的最终物理计划，补齐此前未实跑的分区合并扩展。五类旧基线保持原样。

## 开始之前

先掌握 01–06 的键、可合并求和、Shuffle Stage 和任务槽位，再读 [Spark 3.5.8：Coalescing Post Shuffle Partitions](https://spark.apache.org/docs/3.5.8/sql-performance-tuning.html#coalescing-post-shuffle-partitions)。任务粒度的概念背景见[课程阅读映射](/distributed-compute-lab/zh/courses/)中的 CS267 Lecture 23。CS267 是并行计算课程，具体 Spark 实现由固定版本官方指南解释。

学习目标是区分暂定和最终计划，把 SQL 算子指标与 TaskEnd 证据对照，并构造“开启 AQE 却不合并分区”的反例。查看结果前，先预测三个方案的输出分区数。

## 正确性约定与对照

输入是分成八个输入分区的确定性整数范围。seed=42 时，每个索引 i 产生 k=(31×i+42) mod 1000，v=(17×i+42) mod 101−50。没有随机耗时生成器、外部数据、模型 API 或预制引擎结果。seed 是模运算生成器的显式偏移，不代表随机抽样。

输出为每个键的 count 与 sum。Python 循环独立计算 oracle，不读取 Spark 的计划或结果；每次 collect 的完整输出都必须相等。公开的 output 是实际 collect 的结果，各次运行另外保存完整输出的 SHA-256。独立验收器利用 31 的模逆，逐键枚举等差索引序列，采用不同于运行时 oracle 的枚举方法。仅核对全局行数和总和太弱：把一个键的和加一、另一个减一，仍保持全局总和，却必须验收失败。

- aqe-off：adaptive.enabled=false；合并配置存在，但不生效。
- aqe-on-no-coalesce：adaptive.enabled=true，coalescePartitions.enabled=false；隔离 AQE 总开关与特定规则。
- aqe-on-coalesce：两者都开启，parallelismFirst=false，目标 1 MiB，最小分区 1 字节，初始 Shuffle 分区 32。

这些小阈值是为了在笔记本上观察机制，不是生产建议。关闭自动广播、skew join 优化与 local Shuffle reader；查询没有 Join。每方案预热一次，再计时三次，隔次反转执行顺序。反序只能部分控制顺序影响，不等于完整随机化。不同脚本调用不复用 Spark 应用或输入缓存。

## 执行与检查

按复现页激活固定环境，在配套项目根目录执行：

```sh
python scripts/run_aqe.py
python scripts/check_aqe.py build/aqe-results/aqe.json --expect coalesced
```

默认写入 build/aqe-results/，不覆盖 results/。使用 local[2,2]、1 GiB 堆与十分钟子进程期限。脚本在成功或失败后清理自己创建的临时事件、scratch 与运行日志；超时只终止自己的进程组。虽然没有远程服务，Spark 仍需绑定本机 loopback 端口。

在 JSON 或[实测查看器](/distributed-compute-lab/zh/evidence/)中检查 runs[].adaptive、runs[].stage_metrics、tasks 和 plans。每个动作都有以自己的 group 命名的 before/after 计划。AQE 计划在 collect 前应为 isFinalPlan=false，之后为 isFinalPlan=true。读取另一个 count 动作的计划，或只看执行前字符串，都不能证明这次查询的最终计划。

已提交的 240,000 行结果中，两个对照均保留 32 个输出分区、40 次任务尝试（8 map + 32 reduce）。开启合并后为 1 个输出分区、9 次任务尝试（8+1）。最终计划出现 AQEShuffleRead coalesced；真实 reader 的 numPartitions 为 1，CoalescedPartitionSpec 覆盖 reducer 索引 [0,32)。这是一个 reader 覆盖原来的 32 个哈希桶，不是把原 Exchange 改成只有一个桶。上游规则使用运行时 map 输出统计。[固定版本规则源码](https://github.com/apache/spark/blob/v3.5.8/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala)

局部聚合与原 Exchange 仍然存在。比较三方案的 Shuffle 字节：合并 reader 能减少调度工作，却未必减少最初写出的字节。SQL partitionDataSize 是 map-status 估算，与 TaskEnd 实际字节不必完全相等；numCoalescedPartitions 数的是合并桶的输出 spec，不是被移除的桶数。本机远程读取仍为零。

## 运行反例

```sh
python scripts/run_aqe.py --advisory-bytes 1 --repeats 1 --out build/aqe-tiny-results
python scripts/check_aqe.py build/aqe-tiny-results/aqe.json --expect unchanged
```

只改变目标字节数；一次计时重复用于机制检查，不作性能估计。保存的真实反例在 AQE 与合并均开启时仍为 32 分区、40 次尝试。原桶已超过极小目标，而合并规则不能把它们拆成一字节桶。这反驳了“开启 AQE 就减少任务”与“advisory bytes 是硬上限”。[真实反例 JSON](/distributed-compute-lab/results/aqe-1-byte.json)

## 修改输入，保留约定

```sh
python scripts/run_aqe.py --rows 23017 --seed 73 --shuffle-partitions 16 --repeats 1 --out build/aqe-changed-results
python scripts/check_aqe.py build/aqe-changed-results/aqe.json --expect coalesced
```

预测完整输出及输入/读取任务数如何变化。行数不是 1,000 的整数倍，各键数量不相等，硬编码“每键 240 行”无法通过。这组修改在验收时实际执行。seed 改变数据，不固定操作系统调度或 JVM 耗时。除非有意制作新版本，自己的证据应保存到提交基线以外。

## 用证据支持结论

比较全部重复，不只看最快的一次。计时包含查询构建、collect、Python 结果核验与计划/指标读取，不含 JVM 启动。合并在此减少任务，但也可能在更大负载上丢失有用并行度。这里只有两个本地任务线程，验证的是调度与分区机制，不是网络节省或生产容量，不提供置信区间或统计确立的加速结论。

AQE skew Join、Join 策略转换、多个实际 Executor、主机故障与分布式训练/推理均未实现。[AI 阅读桥梁](/distributed-compute-lab/zh/courses/#ai-bridge)把分片与尾延迟连到推理问题，但不声称 Spark 运行评测了 LLM 引擎。

? 为什么需要开启 AQE、关闭合并的对照？

! 它隔离总优化器与特定规则。即便最终自适应计划已完成，也可能保留 32 个 reducer，因此只看最终计划标志仍不够。

? 哪些证据会推翻“32 合为 1”的结论？

! 未完成计划、缺少合并 reader、spec 不兼容、逐键输出不同，或 TaskEnd 阶段任务数与实际分区不一致。验收器把这些信号联合核对。

? 一字节目标是否能产生一字节任务？

! 不能。这是建议值，而且此规则只合并已有完整桶。拆分倾斜 Join 分区是另一个功能，这里已关闭。

## 过关任务

提交预测、一次合并运行和一次不合并反例，附精确配置、完整输出验收、执行前/最终计划和任务阶段。解释计时只支持机制观测，还是支持可重复性能结论。没有实测加速也可，只要证据与边界可靠。
