import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from check_aqe import verify, expected_output
from search_resources import search


class AdaptiveEvidence(unittest.TestCase):
    def test_real_final_plans_sql_metrics_and_task_stages(self):
        r = json.loads((ROOT / 'results/aqe.json').read_text())
        summary = verify(r, 'coalesced')
        self.assertEqual(summary['partitions'], {'aqe-off': [32], 'aqe-on-coalesce': [1], 'aqe-on-no-coalesce': [32]})
        self.assertEqual(summary['runs'], 12)

    def test_real_one_byte_counterexample(self):
        r = json.loads((ROOT / 'results/aqe-1-byte.json').read_text())
        verify(r, 'unchanged')
        self.assertEqual(r['observations']['advisory_bytes'], 1)
        self.assertTrue(all(x['adaptive']['output_partitions'] == 32 for x in r['runs']))

    def test_rejects_wrong_keys_even_when_global_sum_and_count_match(self):
        r = json.loads((ROOT / 'results/aqe.json').read_text())
        bad = copy.deepcopy(r)
        bad['output'][0][2] += 1
        bad['output'][1][2] -= 1
        self.assertEqual(sum(x[2] for x in bad['output']), sum(x[2] for x in r['output']))
        with self.assertRaisesRegex(AssertionError, 'Full collected'):
            verify(bad)

    def test_rejects_provisional_plan_and_inconsistent_tasks(self):
        r = json.loads((ROOT / 'results/aqe.json').read_text())
        bad = copy.deepcopy(r)
        next(x for x in bad['runs'] if x['adaptive']['enabled'])['adaptive']['is_final_plan'] = False
        with self.assertRaises(AssertionError):verify(bad)
        bad = copy.deepcopy(r)
        next(x for x in bad['runs'] if x['variant'] == 'aqe-on-coalesce')['stage_metrics'][-1]['attempts'] = 32
        with self.assertRaisesRegex(AssertionError, 'Task stages'):verify(bad)

    def test_modified_seed_and_size_evidence_when_present(self):
        path = ROOT / 'build/aqe-changed-results/aqe.json'
        if not path.exists():self.skipTest('separate documented parameter exercise')
        r = json.loads(path.read_text())
        verify(r, 'coalesced')
        self.assertEqual(r['input']['seed'], 73)
        self.assertEqual(r['input']['rows'], 23017)
        self.assertNotEqual(expected_output(23017, 73), expected_output(23017, 42))


class CurriculumContracts(unittest.TestCase):
    def test_structured_sources_have_precise_course_metadata_and_readings(self):
        r = json.loads((ROOT / 'resources/catalog.json').read_text())
        self.assertEqual(len(r['courses']), 2)
        sources = {s['id']:s for s in r['sources']}
        self.assertEqual(len(sources), len(r['sources']))
        for c in r['courses']:
            for key in ('school','course_number','name','term','status','verified_on','prerequisites','public_materials','enrollment','url'):
                self.assertTrue(c[key])
        self.assertEqual({u['id'] for u in r['units']}, {f'{i:02d}-{n}' for i,n in enumerate(['parallel','model','aggregation','join','recovery','resources','aqe'],1)})
        for u in r['units']:
            self.assertTrue(u['readings'])
            for reading in u['readings']:
                self.assertIn(reading['source_id'], sources)
                self.assertTrue(reading['locator'])
            for key in ('command','predict','acceptance','boundary','authorship'):
                self.assertTrue(u['exercise'][key])
            self.assertTrue((ROOT / u['exercise']['command'].split()[1]).is_file())
        self.assertEqual(search(r, unit='05-recovery')[0]['id'], '05-recovery')
        self.assertTrue(search(r, 'MapReduce'))
        self.assertTrue(search(r, '倾斜'))
        self.assertEqual(search(r, 'definitely-no-such-resource'), [])


if __name__ == '__main__':unittest.main()
