# 本地复现与扩展

网页用于阅读和浏览证据，不会在浏览器执行 Spark。公开配套项目包含可执行实验、确定性数据生成器、断言、测试、固定依赖和课程。它独立于私有主页仓库。

## 已验证环境

保存的基线使用 Spark 3.5.8、Py4J 0.10.9.9、Python 3.11.2、Zulu OpenJDK 11.0.15，运行于 macOS arm64、Apple M1 Pro、8 核与 16 GB 内存。每个应用使用 local[2,2]，一个 JVM 中两条任务线程和配套 Python Worker，Driver 堆为 1 GiB。这是单机本地执行，不是多 Executor 集群或扩展性基准。总常驻内存还包括 Python、原生和操作系统内存。

复现基线建议使用 JDK 11 与 Python 3.11。兼容补丁版本或 Linux 可能可用，但不是相同的基准环境。CI 在 Linux、JDK 11、Python 3.11 上运行有界冒烟工作负载，与笔记本实测分开记录。不需要 Docker、付费云账户或模型 API。

## 安装与运行

```sh
git clone https://github.com/lynnlee1229/distributed-compute-lab.git
cd distributed-compute-lab
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
# macOS 自动选择 /usr/libexec/java_home -v 11。
# Linux 请先把 JAVA_HOME 设为 JDK 11 安装目录。
python scripts/run_all.py --smoke
python scripts/run_all.py
python scripts/run_aqe.py
python scripts/check_aqe.py build/aqe-results/aqe.json --expect coalesced
python -m unittest discover -s tests -v
python scripts/build_site.py
python scripts/check_site.py
python -m http.server 18913 --directory build/site
```

下载压缩包时，解压后从建立虚拟环境开始。依赖安装在虚拟环境里。完整实验使用 240,000 行，冒烟实验为 16,000 行，各应用串行执行。每个子进程最长十分钟，行数限制为 10,000–1,000,000，重复次数为 1–5。建议在其他应用之外留出数 GiB 可用内存与至少 2 GB 磁盘；这只是实用起点，不是测得的峰值内存保证。

## 找到输出

原始事件日志、运行消息、检查点和临时文件保留在忽略的 `build/` 中，可能含本机环境细节，不随项目分发。`scripts/export_results.py` 按允许字段导出任务指标到 `results/`，并清理计划中的路径。每个结果保留版本、规模、运行分组、逻辑结果、任务计数和重复计时。`results/index.json` 提供 SHA-256。

完整运行会重新生成 `results/`，需要对比时先保存基线。冒烟结果输出到 `build/smoke-results/`，不替换实测基线。只跑一个实验用 `--only join`，也可以在后面列多个名字。Runner 只清理自己在 build/measured 或 build/smoke 下的指定实验目录。

## 配置与失败处理

Runner 清除继承的 Spark/Hadoop 配置变量，固定回环地址并显式使用本地文件系统。macOS 会选择 JDK 11，即使 Shell 指向其他版本。Spark 需要可用的本地端口。若安全沙箱禁止回环 Socket，请在已授权的本地终端运行同一命令。Python 或 Java 异常不算实验结果；检查私有运行日志，修复原因后重跑。

`local[2,2]` 允许每个任务最多两次尝试。容错探针故意只失败一次，应最终成功；持续失败应让 Job 失败。这没有测试网络分区、Driver 崩溃或主机丢失。可靠检查点 API 在这里仅使用本地磁盘，存储本身不能抵抗整机故障。

## 第一次修改

在工作副本的 `docs/experiment-template.md` 写下假设，改变一个因素，保留正确性断言，重跑并比较每次重复。数据公式以 `experiments/run.py` 为准。浏览器成本计算器是示意模型，其他图是概念图，都不生成保存的实测数据。

## 后续边界

核心课程止于基础执行机制，不覆盖多机扩展、调度公平性、分布式共识、高阶分布式算法、存储引擎内部或生产容量规划。第七单元已执行 AQE 分区合并；I/O 布局、skew Join 与实际 Executor 故障仍是可选扩展。有状态流计算与变化数据请阅读交叉链接的 Streaming Lab。

## 复现新增自适应计划闭环

原五类运行器保留 AQE 关闭的基线。新增实验单独用 `python scripts/run_aqe.py`，默认写 `build/aqe-results/`，不覆盖提交结果。按[第 07 单元](/distributed-compute-lab/zh/07-aqe/)依次运行对照、一字节反例和修改种子/行数/分区练习。每份结果用 `scripts/check_aqe.py` 独立核对完整逐键输出、最终计划、reader spec 与实际任务阶段。有意刷新发布基线时才运行 `python scripts/run_aqe.py --out results` 和 `python scripts/run_aqe.py --advisory-bytes 1 --repeats 1 --out results --name aqe-1-byte`；选择性导出会保留早先结果清单。

新运行器为每次调用创建自己的临时目录，导出后删除原始事件、日志与 scratch，失败时也清理，只保留脱敏 JSON；它不使用强制 Spill 阈值。旧 Join 策略计划是执行前证据，不能当作最终 AQE 计划；新增 AQE 动作分别保存同一次 collect 前与完成后的计划。I/O 布局、AQE skew Join 和实际 Executor 故障恢复仍未测。具体阅读与原创练习见[核验课程映射](/distributed-compute-lab/zh/courses/)。
