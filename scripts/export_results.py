#!/usr/bin/env python3
"""Allowlist Spark events; never ship raw Spark environment, exceptions or logs."""
import argparse, json, statistics, re, hashlib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
a.out.mkdir(parents=True,exist_ok=True)

def summarize(event_dir):
    stages={};tasks=[]
    for file in sorted(event_dir.glob('*')):
        if not file.is_file() or file.suffix=='.crc':continue
        for line in file.open():
            e=json.loads(line);kind=e.get('Event')
            if kind=='SparkListenerJobStart':
                group=(e.get('Properties') or {}).get('spark.jobGroup.id')
                for s in e['Stage IDs']:
                    if group:stages[s]=group
            if kind!='SparkListenerTaskEnd':continue
            t=e['Task Info'];m=e.get('Task Metrics') or {};sr=m.get('Shuffle Read Metrics') or {};sw=m.get('Shuffle Write Metrics') or {}
            reason=e.get('Task End Reason',{}).get('Reason','Unknown')
            tasks.append({'groups':[stages[e['Stage ID']]] if e['Stage ID'] in stages else [],'stage':e['Stage ID'],'stage_attempt':e.get('Stage Attempt ID',0),'partition':t['Index'],
                'attempt':t['Attempt'],'successful':reason=='Success','reason':reason,
                'duration_ms':t['Finish Time']-t['Launch Time'],
                'executor_run_ms':m.get('Executor Run Time',0),'jvm_gc_ms':m.get('JVM GC Time',0),
                'shuffle_write_bytes':sw.get('Shuffle Bytes Written',0),'shuffle_write_records':sw.get('Shuffle Records Written',0),
                'shuffle_read_bytes':sr.get('Remote Bytes Read',0)+sr.get('Local Bytes Read',0),
                'remote_read_bytes':sr.get('Remote Bytes Read',0),
                'memory_spill_bytes':m.get('Memory Bytes Spilled',0),'disk_spill_bytes':m.get('Disk Bytes Spilled',0),
                'peak_execution_bytes':m.get('Peak Execution Memory',0)})
    return tasks

index=[]
for directory in sorted(a.root.iterdir()):
    report=directory/'report.json'
    if not report.exists():continue
    r=json.loads(report.read_text());tasks=summarize(directory/'events')
    for row in r['runs']:
        own=[t for t in tasks if row['group'] in t['groups']]
        keys=['shuffle_write_bytes','shuffle_read_bytes','remote_read_bytes','memory_spill_bytes','disk_spill_bytes','shuffle_write_records']
        row['metrics']={k:sum(t[k] for t in own) for k in keys}
        row['metrics'].update({'tasks':len(own),'failed_attempts':sum(not t['successful'] for t in own),
             'stages':len({t['stage'] for t in own}),'max_task_ms':max([t['duration_ms'] for t in own] or [0]),
             'peak_execution_bytes':max([t['peak_execution_bytes'] for t in own] or [0])})
    r['tasks']=tasks
    r['summary']={}
    for variant in dict.fromkeys(row['variant'] for row in r['runs']):
        measured=[row for row in r['runs'] if not row['warmup'] and row['variant']==variant]
        times=[row['wall_ms'] for row in measured]
        r['summary'][variant]={'repeats':len(times),'median_wall_ms':round(statistics.median(times),3),
            'min_wall_ms':min(times),'max_wall_ms':max(times)}
    text=json.dumps(r,ensure_ascii=False,indent=2)+'\n'
    if re.search(r'/Users/|/home/|/private/|mailto:|sk-proj-|gh[pous]_',text):raise SystemExit('Private content in result')
    target=a.out/(directory.name+'.json');target.write_text(text)
    index.append({'experiment':directory.name,'file':target.name,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
        'measured_runs':sum(not x['warmup'] for x in r['runs']),'tasks':len(tasks)})
(a.out/'index.json').write_text(json.dumps(index,indent=2)+'\n')
print('EXPORTED',len(index),'allowlisted experiment reports')
