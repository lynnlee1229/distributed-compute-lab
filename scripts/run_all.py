#!/usr/bin/env python3
"""Run sequentially with bounded resources. Raw runtime files remain private in build/."""
import argparse, os, shutil, subprocess, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--smoke',action='store_true');p.add_argument('--only',nargs='+',choices=['parallel','aggregation','join','recovery','resources']);a=p.parse_args()
env=os.environ.copy()
for key in ('SPARK_HOME','PYTHONPATH','PYSPARK_SUBMIT_ARGS','HADOOP_CONF_DIR','YARN_CONF_DIR','SPARK_CONF_DIR','SPARK_DIST_CLASSPATH'):env.pop(key,None)
env['SPARK_LOCAL_IP']='127.0.0.1';env['PYSPARK_PYTHON']=sys.executable
env['PYSPARK_DRIVER_PYTHON']=sys.executable
env['PYSPARK_SUBMIT_ARGS']='--driver-memory 1g pyspark-shell'
if sys.platform=='darwin':
    env['JAVA_HOME']=subprocess.check_output(['/usr/libexec/java_home','-v','11'],text=True).strip()
for exp in a.only or ['parallel','aggregation','join','recovery','resources']:
    dest=ROOT/'build'/('smoke' if a.smoke else 'measured')/exp
    if dest.exists():shutil.rmtree(dest)
    dest.mkdir(parents=True)
    cmd=[sys.executable,str(ROOT/'experiments/run.py'),'--experiment',exp,'--out',str(dest),
         '--rows','16000' if a.smoke else '240000','--repeats','1' if a.smoke else '3']
    with (dest/'runtime.log').open('w') as log:
        try:subprocess.run(cmd,env=env,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=600)
        except Exception:
            print(f'FAILED {exp}; inspect local build runtime.log (not published).',flush=True);raise
    print(f'PASS {exp}',flush=True)
subprocess.run([sys.executable,str(ROOT/'scripts/export_results.py'),'--root',str(ROOT/'build'/('smoke' if a.smoke else 'measured')),
    '--out',str(ROOT/'build/smoke-results' if a.smoke else ROOT/'results')],check=True)
