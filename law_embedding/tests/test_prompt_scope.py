import ast
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('prompt_scope', ROOT/'src/law_indexer/prompt_scope.py')
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)

def payload(params):
    return {'code': 0, 'data': {'prompt_id': 748, 'revision_id': 9, 'label': 'production', 'prompt': json.dumps(params)}}

class Tests(unittest.TestCase):
    def test_names_and_both_ministry_formats(self):
        scope = m.parse_scope(payload({'law_names':[' 가사소송규칙 '], 'ministries':['법무부']}), 748)
        for raw in ({'law_name':'가사소송규칙'}, {'basic_info':{'소관부처':{'content':'법무부'}}}, {'basic_info':{'소관부처명':'법무부'}}):
            self.assertTrue(scope.matches(raw))
        self.assertFalse(scope.matches({'law_name':'민법'}))
        self.assertFalse(scope.matches({'law_name':'가사소송규칙 시행령'}))
    def test_reject_invalid_lists(self):
        for params in ({}, {'law_names':[]}, {'law_names':'법'}, {'law_names':['']}, {'law_names':[1]}, {'laws':['법']}, []):
            with self.subTest(params=params), self.assertRaises(ValueError):
                m.parse_scope(payload(params),748)
    def test_many_names(self):
        scope=m.parse_scope(payload({'law_names':[f'법{i}' for i in range(600)]}),748)
        self.assertTrue(scope.matches({'law_name':'법599'}))
    def test_response_validation(self):
        for key,value in [('prompt_id',749),('label','latest'),('revision_id',None),('prompt','not json')]:
            p=payload({'law_names':['법']});p['data'][key]=value
            with self.subTest(key=key), self.assertRaises(ValueError): m.parse_scope(p,748)
    def test_fetch_fresh_and_no_auth(self):
        with patch.dict(os.environ,{'LAW_SCOPE_PROMPT_ID':'748','LLMOPS_ADMIN_API_URL':'http://example.invalid'},clear=True):
            with patch.object(m,'urlopen',side_effect=[io.StringIO(json.dumps(payload({'law_names':['법1']}))),io.StringIO(json.dumps(payload({'law_names':['법2']})))]) as call:
                self.assertTrue(m.load_prompt_scope().matches({'law_name':'법1'}))
                self.assertTrue(m.load_prompt_scope().matches({'law_name':'법2'}))
                self.assertEqual(call.call_count,2)
                self.assertIsNone(call.call_args.args[0].get_header('Authorization'))
                self.assertIn('prompt_id=748&label=production',call.call_args.args[0].full_url)
    def test_http_failure_stops(self):
        with patch.dict(os.environ,{'LAW_SCOPE_PROMPT_ID':'748'}), patch.object(m,'urlopen',side_effect=HTTPError('x',403,'denied',{},None)):
            with self.assertRaises(RuntimeError):m.load_prompt_scope()
    def test_excluded_document_never_mapped(self):
        tree=ast.parse((ROOT/'src/law_indexer/pipeline.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='index_documents')
        for arg in fn.args.args: arg.annotation=None
        fn.returns=None
        mapper=Mock(side_effect=AssertionError('excluded document was mapped'))
        env={'json':json,'os':os,'map_law_data':mapper,'map_admrul_data':mapper,'SKIP_JSON_NAMES':set(),'_new_totals':lambda:{'errors':[],'files_failed':0},'_BulkBuffer':Mock(return_value=Mock()),'logger':Mock()}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'pipeline.py','exec'),env)
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'other.json';p.write_text(json.dumps({'law_name':'제외법'}))
            result=env['index_documents'](Mock(),Mock(),types.SimpleNamespace(collection_for=lambda _: 'law', doc_parser_base_url=''),p,'law',p.parent,None,None,paths=[p],preprocess_files=False,scope=m.parse_scope(payload({'law_names':['선택법']}),748))
            self.assertEqual(result['scope_skipped'],1)
            mapper.assert_not_called()

if __name__=='__main__':unittest.main()
