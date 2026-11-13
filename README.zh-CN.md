# 分布式计算学习 Lab

[中文课程](https://lynnlee1229.github.io/distributed-compute-lab/zh/) · [English course](https://lynnlee1229.github.io/distributed-compute-lab/en/) · [完整运行与证据说明](README.md)

面向一般学习者的原创课程，按概念依赖推进：并行成本、MapReduce 与执行图、聚合和数据移动、Join 与倾斜、重试重算、资源诊断。每章都有前置知识、目标、讲解、可执行示例、动手任务、结果解释、自测与原始参考。不依赖特定业务背景。

五类实验均实际执行，使用确定性合成数据。网站可查看保存结果、全部重复、任务尝试、指标、Lineage、执行计划和关键源码。浏览器成本计算器是独立标注的示意模型，不生成实测结果。

## 本地运行

要求 Python 3.11 与 JDK 11；固定 Spark 3.5.8、Py4J 0.10.9.9。实测环境是 Python 3.11.2、Zulu JDK 11.0.15、macOS arm64、M1 Pro、8 核与 16 GB。应用使用 local[2,2]，一个 JVM 中两条任务线程及 Python Worker，JVM 堆 1 GiB。这是单机本地执行，不是多 Executor 集群，也不能证明多机扩展性。进程总内存会超过 Java 堆。

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
# Linux 先设置 JDK 11 的 JAVA_HOME；macOS 自动选择 JDK 11。
python scripts/run_all.py --smoke
python scripts/run_all.py
python -m unittest discover -s tests -v
python scripts/build_site.py
python scripts/check_site.py
python -m http.server 18913 --directory build/site
```

访问 `http://localhost:18913/distributed-compute-lab/zh/`。完整实验为 240,000 行，会更新自己的 results/；冒烟为 16,000 行，仅导出到 build/smoke-results/。可以用 `--only aggregation join` 选择实验。应用串行执行，子进程超时十分钟。无需 Docker、付费资源或 API Key。

性能实验每方案预热一次，再交替顺序重复三次。容错实验重复三个无预热机制场景，不据此作缓存性能归因。资源实验使用明确标注的 8,192 元素阈值触发真实 Spill，不制造自然 OOM。AQE 与自动广播在基线关闭，Join 的广播为显式选择。数据布局与 AQE 是课程中的未测扩展。

容错只在自己任务中抛出一次异常，不杀节点或 Executor。缓存移除是重算探针，不是节点故障；本地磁盘检查点不能抵抗整机丢失。没有外部 Sink，也不声称外部副作用恰好一次。

原始事件、日志、检查点与临时文件留在忽略的 build/，不分发。results/ 只保存允许指标与清理路径后的计划，并有 SHA-256 清单。导出器按串行动作的实际事件归属分组，不是通用并发 Job 分析工具。Shuffle 字节不是抓包，峰值执行内存不是进程 RSS，本机远程 Shuffle 读取为零。

只有本配套项目开源，个人主页源码仍私有。项目不包含真实业务数据、凭据、主机日志或虚拟环境。静态构建器只打包明确允许的源文件，下载包经过独立解压验证。原创内容为 MIT 许可；引用论文和 Spark 保留各自许可，不复制其讲义。详见 [复现方法](docs/reproduce.zh.md)、[实验记录模板](docs/experiment-template.md) 和 [验收记录](VALIDATION.md)。
