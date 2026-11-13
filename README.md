# Distributed Compute Lab

[English course](https://lynnlee1229.github.io/distributed-compute-lab/en/) · [中文课程](https://lynnlee1229.github.io/distributed-compute-lab/zh/) · [中文说明](README.zh-CN.md)

An original foundation-first course with real, bounded Spark experiments. Learn work and scheduling overhead; MapReduce, dependencies and stages; aggregation and data movement; Join and skew; retry, cache and checkpoint; resource diagnosis. Each chapter provides prerequisites, goals, explanations, runnable examples, practice, interpretation and self-check answers. No domain-specific background is assumed.

Five executed experiments use deterministic synthetic data. The site lets you inspect saved outputs, all repetitions, task attempts, metrics, lineage, physical plans and executable source. The cost calculator is an illustrative model, explicitly separate from measured evidence.

## Reproduce

Use Python 3.11 and JDK 11. Tested baseline: Spark 3.5.8, Py4J 0.10.9.9, Python 3.11.2, Zulu JDK 11.0.15, macOS arm64, M1 Pro / 8 cores / 16 GB RAM. Each application uses **local[2,2]**, two task threads in one JVM plus Python workers, and a 1 GiB JVM heap. This is **single-host local execution**, not a multi-executor or multi-machine scaling study. Process memory exceeds the Java heap.

```sh
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
# Linux: set JAVA_HOME to JDK 11. macOS: runner selects JDK 11.
python scripts/run_all.py --smoke
python scripts/run_all.py
python -m unittest discover -s tests -v
python scripts/build_site.py
python scripts/check_site.py
python -m http.server 18913 --directory build/site
```

Open `http://localhost:18913/distributed-compute-lab/en/`. Run `--only aggregation join` to select experiments. Full runs use 240,000 rows and overwrite their own baseline reports in results/. Smoke uses 16,000 rows and exports to build/smoke-results/. Apps run sequentially with a ten-minute subprocess timeout. No Docker, paid services or API keys are needed.

## Experiments and contracts

| Experiment | Change | Correctness / evidence |
| --- | --- | --- |
| parallel | 1 / 8 / 64 partitions, two slots | independent integer sum; task counts; warm-up + 3 repeats |
| aggregation | group values / locally combine | full 1,000-key output versus independent oracle; Shuffle bytes |
| join | sort-merge / broadcast / salted | unique dimension; count + weight sum; output partition histogram; plans |
| recovery | first-attempt failure; cached / removed; checkpoint | successful retry; 8/0/8 partition evaluations; lineage truncation; 3 traces |
| resources | 2 / 16 sort partitions | row count, ID sum, payload length and per-partition ordering; actual Spill |

Resources uses an explicitly **forced 8,192-element Spill threshold** to exercise an external sorter safely. It is not a natural OOM benchmark or a recommended production knob. Baseline AQE and automatic broadcasting are disabled; Join broadcasts are explicit. Chapter 06 explains AQE and I/O layout as unmeasured extensions.

The recovery probe raises an exception only inside its own Spark task. It does not kill a node, driver or executor. Its local checkpoints do not survive host loss. Cache removal demonstrates lineage recomputation, not a node-failure simulation. No external sink is involved and no exactly-once side-effect claim is made.

## Evidence and boundaries

`results/*.json` contains sanitized baseline reports; `results/index.json` carries SHA-256 hashes. Timing spans each variant function, including plan construction, execution, Python/JVM result transfer and validation inside the variant; it excludes application startup and shared input cache materialization. Performance probes warm each variant, then alternate order over three repeats. Recovery traces have no warm-up and are mechanism evidence. All observations are retained; medians are descriptive, not statistical confidence claims. Remote Shuffle reads are zero in this local mode.

Raw Spark events, scratch, checkpoints and runtime logs remain in ignored build/. Never publish those files: they may include environment details. The exporter allowlists task metrics and scrubs plan paths. Stage attribution assumes these sequential actions and assigns TaskEnd events to the group current at execution time; it is not a general concurrent-job analytics tool. Metrics reflect Spark counters, not a packet capture or process RSS. PySpark Shuffle record counts can represent batches rather than source records.

Only this companion is public. It contains no personal homepage source, private datasets, credentials, host logs or local virtual environment. `scripts/build_site.py` creates an allowlisted portable ZIP and bilingual static export using the standard library. The ZIP is independently extracted and checked in validation. Original material is MIT licensed; linked third-party papers and Apache Spark retain their own terms and are not copied into the course.

See [methods and setup](docs/reproduce.en.md), [experiment template](docs/experiment-template.md), [validation evidence](VALIDATION.md), and upstream links within each chapter. CI executes the Linux smoke workload; the committed laptop measurements are separate evidence. This course does not cover production capacity planning, multi-host scaling or advanced distributed algorithms.
