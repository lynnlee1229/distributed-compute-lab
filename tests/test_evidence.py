import hashlib,json,subprocess,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def load(name,root=ROOT/'results'):
    return json.loads((root/(name+'.json')).read_text())

class EvidenceContracts(unittest.TestCase):
    def test_hashed_inventory_and_runtime(self):
        index=json.loads((ROOT/'results/index.json').read_text())
        self.assertEqual({r['experiment'] for r in index},{'parallel','aggregation','join','recovery','resources'})
        for item in index:
            self.assertEqual(hashlib.sha256((ROOT/'results'/item['file']).read_bytes()).hexdigest(),item['sha256'])
            r=load(item['experiment']);self.assertEqual(r['environment']['spark'],'3.5.8')
            self.assertTrue(r['environment']['java'].startswith('11.'))
            self.assertEqual(r['environment']['master'],'local[2,2]')
            self.assertEqual(r['input']['rows'],240000)
            self.assertFalse(r['environment']['aqe'])
            self.assertTrue(all(t['remote_read_bytes']==0 for t in r['tasks']))
            self.assertTrue(all(s['repeats']==3 for s in r['summary'].values()))

    def test_parallel_preserves_independent_sum(self):
        r=load('parallel')
        self.assertEqual(r['observations']['expected_sum'],119516556)
        for row in r['runs']:
            self.assertEqual(row['result'],119516556)
            self.assertEqual(row['metrics']['tasks'],int(row['variant'].split('-')[1]))

    def test_aggregation_full_output_and_movement(self):
        r=load('aggregation')
        self.assertEqual(r['output'],[[k,sum(i%17 for i in range(k,240000,1000))] for k in range(1000)])
        grouped=[x for x in r['runs'] if x['variant']=='group-values' and not x['warmup']]
        combined=[x for x in r['runs'] if x['variant']=='combine-locally' and not x['warmup']]
        self.assertTrue(all(x['metrics']['shuffle_write_bytes']>0 for x in grouped))
        self.assertTrue(all(c['metrics']['shuffle_write_bytes']<g['metrics']['shuffle_write_bytes'] for c,g in zip(combined,grouped)))
        self.assertEqual(len({json.dumps(x['result'],sort_keys=True) for x in r['runs']}),1)

    def test_join_result_distribution_and_actual_plans(self):
        r=load('join')
        self.assertIn('SortMergeJoin',r['plans']['shuffle'])
        self.assertIn('BroadcastHashJoin',r['plans']['broadcast'])
        for row in r['runs']:
            self.assertEqual(row['result']['rows'],240000);self.assertEqual(row['result']['weighted_sum'],384001)
            self.assertEqual(sum(row['result']['partition_rows']),240000)
            if row['variant']=='shuffle':self.assertGreater(max(row['result']['partition_rows']),190000)
            if row['variant']=='broadcast':self.assertEqual(row['result']['partition_rows'],[30000]*8)
        salted=next(x for x in r['runs'] if x['variant']=='salted')
        self.assertLess(max(salted['result']['partition_rows']),100000)

    def test_recovery_failure_cache_and_checkpoint(self):
        r=load('recovery')
        self.assertEqual(len(r['observations']['traces']),3)
        for trace in r['observations']['traces']:
            self.assertEqual([x['successful_partition_evaluations'] for x in trace['cache_evaluations']],[8,0,8])
            self.assertTrue(trace['checkpoint_lineage_truncated'])
        for row in [x for x in r['runs'] if x['variant']=='retry']:
            self.assertEqual(row['metrics']['failed_attempts'],1);self.assertEqual(row['metrics']['tasks'],9)
            tasks=[t for t in r['tasks'] if row['group'] in t['groups'] and t['partition']==0]
            self.assertEqual([(t['attempt'],t['successful']) for t in tasks],[(0,False),(1,True)])
        self.assertIn('ReliableCheckpointRDD',r['plans']['after_checkpoint'])

    def test_resources_consumed_sorted_payload_and_real_spill(self):
        r=load('resources');self.assertTrue(r['observations']['spill_is_forced'])
        for row in r['runs']:
            self.assertEqual(row['result'],{'rows':240000,'id_sum':28799880000,'payload_chars':30720000,'locally_sorted':True})
            self.assertGreater(row['metrics']['disk_spill_bytes'],0)
            self.assertGreater(row['metrics']['memory_spill_bytes'],0)

    def test_exporter_excludes_environment_and_does_not_reassign_reused_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);exp=root/'input/probe';events=exp/'events';events.mkdir(parents=True)
            report={'runs':[{'group':'first','variant':'a','warmup':False,'wall_ms':1},
                            {'group':'second','variant':'b','warmup':False,'wall_ms':2}]}
            (exp/'report.json').write_text(json.dumps(report))
            seq=[{'Event':'SparkListenerEnvironmentUpdate','PRIVATE_TOKEN':'never-export-this'},
                {'Event':'SparkListenerJobStart','Properties':{'spark.jobGroup.id':'first'},'Stage IDs':[0]},
                {'Event':'SparkListenerTaskEnd','Stage ID':0,'Task Info':{'Index':0,'Attempt':0,'Finish Time':2,'Launch Time':1},
                 'Task End Reason':{'Reason':'Success','Stack Trace':'private-stack'},
                 'Task Metrics':{'Shuffle Write Metrics':{'Shuffle Bytes Written':123}}},
                {'Event':'SparkListenerJobStart','Properties':{'spark.jobGroup.id':'second'},'Stage IDs':[0]}]
            (events/'events').write_text('\n'.join(map(json.dumps,seq)))
            subprocess.run([sys.executable,str(ROOT/'scripts/export_results.py'),'--root',str(root/'input'),'--out',str(root/'out')],check=True,capture_output=True)
            result=(root/'out/probe.json').read_text();r=json.loads(result)
            self.assertNotIn('never-export-this',result);self.assertNotIn('private-stack',result)
            self.assertEqual(r['runs'][0]['metrics']['shuffle_write_bytes'],123)
            self.assertEqual(r['runs'][1]['metrics']['tasks'],0)

    def test_smoke_when_present_has_successful_mechanisms(self):
        smoke=ROOT/'build/smoke-results'
        if not (smoke/'index.json').exists():self.skipTest('smoke is executed separately by CI or extracted-kit verification')
        for name in ['parallel','aggregation','join','recovery','resources']:
            r=load(name,smoke);self.assertEqual(r['input']['rows'],16000);self.assertTrue(r['runs'])
        recovery=load('recovery',smoke)
        self.assertEqual(recovery['runs'][0]['metrics']['failed_attempts'],1)
        self.assertEqual([x['successful_partition_evaluations'] for x in recovery['observations']['traces'][0]['cache_evaluations']],[8,0,8])

if __name__=='__main__':unittest.main()
