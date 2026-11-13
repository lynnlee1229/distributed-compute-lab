# Reproduce and extend

The website is a reading and evidence browser. It does not run Spark in your browser. The public companion contains executable experiments, deterministic data generators, assertions, tests, pinned dependencies and this course. It is independent of the private homepage repository.

## Tested environment

The saved baseline used Spark 3.5.8, Py4J 0.10.9.9, Python 3.11.2 and Zulu OpenJDK 11.0.15 on macOS arm64, Apple M1 Pro, 8 physical cores and 16 GB host RAM. Each application uses local[2,2], two task threads in one JVM with Python workers, and a 1 GiB driver heap. It is a single-host local execution, not a multi-executor cluster or a scaling benchmark. Total RSS includes Python, native and OS memory beyond the JVM heap.

Use JDK 11 and Python 3.11 to follow the baseline. Other compatible patch versions or Linux may work; they are not identical benchmark environments. CI runs the bounded smoke workload on Linux with JDK 11 and Python 3.11, separately from this measured laptop baseline. No Docker, paid cloud account or model API is required.

## Install and run

```sh
git clone https://github.com/lynnlee1229/distributed-compute-lab.git
cd distributed-compute-lab
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
# macOS: runner chooses /usr/libexec/java_home -v 11.
# Linux: set JAVA_HOME to your JDK 11 installation first.
python scripts/run_all.py --smoke
python scripts/run_all.py
python scripts/run_aqe.py
python scripts/check_aqe.py build/aqe-results/aqe.json --expect coalesced
python -m unittest discover -s tests -v
python scripts/build_site.py
python scripts/check_site.py
python -m http.server 18913 --directory build/site
```

If downloading the archive instead, extract it and begin at `python3.11 -m venv .venv`. Python dependencies are installed into that environment. The full run uses 240,000 rows; smoke uses 16,000. Applications run sequentially. Each subprocess has a ten-minute timeout; rows are bounded to 10,000–1,000,000 and repeats to 1–5. Keep several GiB of RAM and at least 2 GB free disk available alongside other applications. Those are practical starting allowances, not a measured peak-memory guarantee.

## Locate the outputs

Raw event logs, runtime messages, checkpoints and scratch files stay in ignored `build/`. They can contain local environment details and are not distributed. `scripts/export_results.py` extracts an allowlist of task metrics into `results/` and scrubs paths from plans. Each result includes version, input size, run group, output, task counters and all repetitions. `results/index.json` carries SHA-256 hashes.

A full run regenerates `results/`; copy the baseline if you want a separate comparison. Smoke exports to `build/smoke-results/` and never replaces the measured baseline. To rerun one experiment use `--only join`, or several names after `--only`. The runner only deletes its own named experiment directory under build/measured or build/smoke.

## Configuration and failure recovery

The runner clears inherited Spark/Hadoop configuration variables, sets loopback addressing and explicitly uses the local file system. On macOS it selects JDK 11 even when a shell points at another JDK. A local Spark port must be available. If your security sandbox prevents loopback sockets, run the same command in an authorized local terminal. A Python or Java exception is not a measured result; inspect the private runtime log, fix the cause and rerun that experiment.

`local[2,2]` allows two attempts for a task. The recovery probe intentionally raises once and should finish successfully. Persistent failures should fail the job. This is not a network-partition, driver-crash or host-loss test. Reliable-checkpoint APIs are used with local disk only, so the storage is not host-fault-tolerant.

## Your first modification

Clone or extract into a working copy. Make a hypothesis in `docs/experiment-template.md`, change one factor, retain correctness assertions, rerun and compare every repetition. Use `experiments/run.py` as the source of truth for formulas. The browser cost calculator is an illustrative model, and the other diagrams are conceptual. Neither generates the saved measurements.

## Next scope

The core deliberately stops before multi-host scaling, scheduling fairness, distributed consensus, advanced distributed algorithms, storage-engine internals and production capacity planning. Chapter 07 now executes AQE coalescing, its one-byte counterexample and a changed-input exercise. I/O layout, AQE skew Join and actual executor-failure recovery remain follow-ups. See the [verified course map](/distributed-compute-lab/en/courses/) for precise readings and original exercises. For stateful streaming and changing data, use the linked Streaming Lab.

## Reproduce the new adaptive-plan learning loop

The five-experiment runner deliberately retains the original AQE-disabled baseline. Use `python scripts/run_aqe.py` separately for the new experiment; its default writes `build/aqe-results/` without replacing committed evidence. Follow [Chapter 07](/distributed-compute-lab/en/07-aqe/) for controls, the one-byte counterexample, and a seed/size/partition modification. Run `scripts/check_aqe.py` on each result to independently verify full per-key output, finalized plans, reader specs and actual task stages. To deliberately refresh the release reports, use `python scripts/run_aqe.py --out results` and `python scripts/run_aqe.py --advisory-bytes 1 --repeats 1 --out results --name aqe-1-byte`; selective exports preserve the earlier result inventory.

The new runner uses a fresh session-owned temporary directory and removes its raw events, logs and scratch after export, including on failure. Only sanitized JSON remains. It applies no forced Spill threshold. Do not interpret old Join strategy plans as final AQE plans: the new AQE action saves both the exact pre-collect and finalized post-collect plans.
