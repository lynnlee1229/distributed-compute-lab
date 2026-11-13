#!/usr/bin/env python3
"""Rerun the AQE exercise without replacing any committed baseline by default."""
import argparse
import os
import signal
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--rows', type=int, default=240000)
p.add_argument('--repeats', type=int, default=3)
p.add_argument('--seed', type=int, default=42)
p.add_argument('--shuffle-partitions', type=int, default=32)
p.add_argument('--advisory-bytes', type=int, default=1048576)
p.add_argument('--out', type=Path, default=ROOT / 'build/aqe-results')
p.add_argument('--name', choices=['aqe', 'aqe-1-byte'], default='aqe', help='Sanitized report filename')
a = p.parse_args()
env = os.environ.copy()
for key in ('SPARK_HOME', 'PYTHONPATH', 'PYSPARK_SUBMIT_ARGS', 'HADOOP_CONF_DIR',
            'YARN_CONF_DIR', 'SPARK_CONF_DIR', 'SPARK_DIST_CLASSPATH'):
    env.pop(key, None)
env.update(SPARK_LOCAL_IP='127.0.0.1', PYSPARK_PYTHON=sys.executable,
           PYSPARK_DRIVER_PYTHON=sys.executable,
           PYSPARK_SUBMIT_ARGS='--driver-memory 1g pyspark-shell')
if sys.platform == 'darwin':
    env['JAVA_HOME'] = subprocess.check_output(['/usr/libexec/java_home', '-v', '11'], text=True).strip()
(ROOT / 'build').mkdir(exist_ok=True)
# Raw events, scratch and logs are session-owned and cleaned on success/failure.
with tempfile.TemporaryDirectory(prefix='aqe-runtime-', dir=ROOT / 'build') as tmp:
    work = Path(tmp) / a.name
    work.mkdir()
    command = [sys.executable, str(ROOT / 'experiments/aqe.py'), '--out', str(work)]
    for name in ('rows', 'repeats', 'seed', 'shuffle_partitions', 'advisory_bytes'):
        command += ['--' + name.replace('_', '-'), str(getattr(a, name))]
    with (work / 'runtime.log').open('w') as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            returncode = process.wait(timeout=600)
        finally:
            if process.poll() is None:
                # Terminate only this owned process group, including its JVM.
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
    if returncode:
        # Print only a bounded diagnostic tail to the local terminal. Never export it.
        print((work / 'runtime.log').read_text()[-6000:], file=sys.stderr)
        raise SystemExit(returncode)
    subprocess.run([sys.executable, str(ROOT / 'scripts/export_results.py'),
                    '--root', tmp, '--out', str(a.out.resolve())], check=True)
print('PASS aqe; sanitized report:', a.out / (a.name + '.json'), flush=True)
