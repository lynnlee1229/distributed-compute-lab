# 阅读、预测、执行、解释

两门大学课程提供互补基础：Berkeley CS267 讲并行机器、通信与负载均衡，MIT 6.5840 讲分布式故障与 MapReduce。具体实现参考固定为 Spark 3.5.8 官方指南。不把整门研究生课强加为本入门 Lab 的先修，而是选真正相关的单元。

下方核验目录可中英检索并下载为结构化 JSON。每个学习单元指向具体 lecture、论文段落或作业题面，再配一个独立原创可运行练习。先预测，运行后验证不变量，再解释计划与计数器。此映射不提供大学学分，也不公开原课程作业解答。

核验日期：北京时间 2026-11-14。区分已发布材料与暂定课表。CS267 选用有年代标注的 2016 归档，硬件数字不是 2026 容量建议；2025 官方页注明视频需 Berkeley 登录。MIT 春季 2026 的选用 Lecture 1 笔记与 Lab 1 题面已经公开，课表本身仍标为暂定。公开阅读与注册、评分、学费分开，不使用私人课程账号或付费材料。

在项目根目录可以离线检索，不需要浏览器或服务：

```sh
python scripts/search_resources.py MapReduce
python scripts/search_resources.py --unit 05-recovery
python scripts/search_resources.py 倾斜
```

MIT 协作政策禁止公开作业解答。私下尝试 Go 原作业前请读[官方政策](https://pdos.csail.mit.edu/6.5840/labs/collab.html)。本 Lab 不使用其作业骨架、不实现其作业协调器。第三方材料保留在上游并适用原条款；本仓库只存链接、书目信息与原创学习任务。
