"""GenOS 법 목록 관리 화면(`/data/law-scope`) 연동 — 파싱·정규화·조회·실제 제외 분기.

정규화 규칙은 수집기(temporal_law `collector/common.compact()`)와 **같아야 한다**.
어긋나면 수집기는 담고 색인기는 버리는 조용한 누락이 난다 — 그 계약을 여기서 고정한다.
"""
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('law_scope', ROOT/'src/law_indexer/law_scope.py')
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)

ON = {'LAW_SCOPE_ENABLED': 'true', 'LLMOPS_ADMIN_API_URL': 'http://example.invalid'}


def payload(data):
    return {'code': 0, 'errMsg': 'success', 'data': data}


def scope(names=(), ministries=()):
    return m.parse_scope(payload({'law_names': list(names), 'ministries': list(ministries)}))


class Parsing(unittest.TestCase):
    def test_names_and_both_ministry_formats(self):
        s = scope([' 가사소송규칙 '], ['법무부'])
        for raw in ({'law_name': '가사소송규칙'},
                    {'basic_info': {'소관부처': {'content': '법무부'}}},
                    {'basic_info': {'소관부처명': '법무부'}}):
            self.assertTrue(s.matches(raw))
        self.assertFalse(s.matches({'law_name': '민법'}))
        self.assertFalse(s.matches({'law_name': '가사소송규칙 시행령'}))

    def test_reject_invalid_lists(self):
        # 빈 목록(= 화면 미설정 포함)은 '전량 적재'가 아니라 중단이다.
        for data in ({'law_names': [], 'ministries': []}, {'law_names': ['법']},
                     {'law_names': '법', 'ministries': []}, {'law_names': [''], 'ministries': []},
                     {'law_names': [1], 'ministries': []}, []):
            with self.subTest(data=data), self.assertRaises(ValueError):
                m.parse_scope(payload(data))

    def test_many_names(self):
        s = scope([f'법{i}' for i in range(600)])
        self.assertTrue(s.matches({'law_name': '법599'}))

    def test_response_validation(self):
        for broken in ({'code': 1, 'data': {'law_names': ['법'], 'ministries': []}},
                       {'data': {'law_names': ['법'], 'ministries': []}},
                       {'code': 0, 'data': None}, {'code': 0}, 'not a dict'):
            with self.subTest(broken=broken), self.assertRaises(ValueError):
                m.parse_scope(broken)


class Normalization(unittest.TestCase):
    """수집기 compact() 와 같은 규칙을 쓰는지."""

    def test_compact_matches_collector_rules(self):
        # 공백(일반·전각)·개행·탭, 가운뎃점 5종, 전각 영숫자, HTML 엔티티.
        self.assertEqual(m.compact(' 법무부 '), '법무부')
        self.assertEqual(m.compact('법무　부'), '법무부')
        self.assertEqual(m.compact('법무\t부\n'), '법무부')
        for dot in 'ㆍ·․‧・':
            self.assertEqual(m.compact(f'교육{dot}과학부'), '교육과학부')
        self.assertEqual(m.compact('ＡＢ１２'), 'AB12')
        self.assertEqual(m.compact('법무부&amp;청'), '법무부&청')
        self.assertEqual(m.compact(None), '')
        self.assertEqual(m.compact(''), '')

    def test_spacing_and_dot_differences_still_match(self):
        s = scope(['가사소송 규칙'], ['교육·과학부'])
        self.assertTrue(s.matches({'law_name': '가사소송규칙'}))
        self.assertTrue(s.matches({'basic_info': {'소관부처명': '교육ㆍ과학부'}}))

    def test_comma_joined_ministries_match_each_part(self):
        """실측 '경찰청,법무부' 형태 — 전체 문자열 비교만 하면 이런 문서가 통째로 빠진다."""
        s = scope(ministries=['법무부'])
        for value in ('경찰청,법무부', '법무부,경찰청', '경찰청, 법무부', '경찰청;법무부',
                      '경찰청，법무부'):
            with self.subTest(value=value):
                self.assertTrue(s.matches({'basic_info': {'소관부처명': value}}))
        self.assertFalse(s.matches({'basic_info': {'소관부처명': '경찰청,해양수산부'}}))

    def test_ministry_list_and_nested_dict_forms(self):
        s = scope(ministries=['법무부'])
        self.assertTrue(s.matches({'basic_info': {'소관부처명': ['경찰청', '법무부']}}))
        self.assertTrue(s.matches({'basic_info': {'소관부처': [{'content': '법무부'}]}}))
        self.assertFalse(s.matches({'basic_info': {'소관부처명': 12345}}))


class UnmatchedWarning(unittest.TestCase):
    def test_unmatched_reports_original_spelling(self):
        s = scope(['민법', '없는법'], ['법무부', '안전행정부'])
        s.matches({'law_name': '민법', 'basic_info': {'소관부처명': '법무부'}})
        names, ministries = s.unmatched()
        self.assertEqual(names, ['없는법'])
        self.assertEqual(ministries, ['안전행정부'])

    def test_name_hit_still_records_ministry(self):
        """이름이 걸렸다고 조기 반환하면 멀쩡한 부처가 오타로 신고된다."""
        s = scope(['민법'], ['법무부'])
        self.assertTrue(s.matches({'law_name': '민법', 'basic_info': {'소관부처명': '법무부'}}))
        self.assertEqual(s.unmatched(), ([], []))

    def test_unmatched_uses_screen_spelling_not_normalized_key(self):
        s = scope(ministries=['교육·과학부'])
        self.assertEqual(s.unmatched()[1], ['교육·과학부'])


class Loading(unittest.TestCase):
    def test_disabled_means_no_filter(self):
        for env in ({}, {'LAW_SCOPE_ENABLED': 'false'}, {'LAW_SCOPE_ENABLED': ''}):
            with self.subTest(env=env), patch.dict(os.environ, env, clear=True):
                with patch.object(m, 'urlopen', side_effect=AssertionError('조회하면 안 된다')):
                    self.assertIsNone(m.load_law_scope())

    def test_fetch_fresh_and_no_auth(self):
        with patch.dict(os.environ, ON, clear=True):
            responses = [io.StringIO(json.dumps(payload({'law_names': ['법1'], 'ministries': []}))),
                         io.StringIO(json.dumps(payload({'law_names': ['법2'], 'ministries': []})))]
            with patch.object(m, 'urlopen', side_effect=responses) as call:
                self.assertTrue(m.load_law_scope().matches({'law_name': '법1'}))
                self.assertTrue(m.load_law_scope().matches({'law_name': '법2'}))
                self.assertEqual(call.call_count, 2)
                self.assertIsNone(call.call_args.args[0].get_header('Authorization'))
                self.assertEqual(call.call_args.args[0].full_url,
                                 'http://example.invalid/data/law-scope')

    def test_default_admin_api_url(self):
        with patch.dict(os.environ, {'LAW_SCOPE_ENABLED': '1'}, clear=True):
            body = io.StringIO(json.dumps(payload({'law_names': ['법'], 'ministries': []})))
            with patch.object(m, 'urlopen', return_value=body) as call:
                m.load_law_scope()
                self.assertEqual(call.call_args.args[0].full_url,
                                 'http://llmops-admin-api-service:8080/data/law-scope')

    def test_http_failure_stops(self):
        with patch.dict(os.environ, ON, clear=True), \
             patch.object(m, 'urlopen', side_effect=HTTPError('x', 403, 'denied', {}, None)):
            with self.assertRaises(RuntimeError):
                m.load_law_scope()


class _FakeEmbedder:
    model_name = 'fake'
    dimension = 4

    def embed_documents(self, texts):
        return [[0.1, 0.2, 0.3, 0.4] for _ in texts]


class _FakeStore:
    def __init__(self):
        self.objects = []

    def upsert(self, objects, dimension, model_name, collection):
        self.objects.extend(objects)
        return {'success': len(objects), 'failed': 0, 'failed_ids': []}

    def delete_orphan_file_chunks(self, *a, **k):
        return 0

    def delete_orphan_json_chunks(self, *a, **k):
        return 0


class PipelineBranch(unittest.TestCase):
    """실제 index_documents 의 scope 분기 — 제외 문서는 청크가 하나도 생기면 안 된다."""

    def _run(self, doc, sc):
        from law_indexer.config import Settings
        from law_indexer.pipeline import index_documents

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'law_data'
            doc_dir = root/doc['law_name']
            doc_dir.mkdir(parents=True)
            path = doc_dir/f"{doc['law_name']}.json"
            path.write_text(json.dumps(doc, ensure_ascii=False), encoding='utf-8')
            store = _FakeStore()
            result = index_documents(store, _FakeEmbedder(), Settings.from_env(), root, 'law',
                                     root, 'c1', None, recursive=True,
                                     preprocess_files=False, scope=sc)
        return result, store

    @staticmethod
    def _doc(name, ministry=None):
        data = {
            'law_id': 'l1', 'mst': 'm1', 'version_uid': 'l1:m1:20240101', 'law_name': name,
            'law_type': '법률', 'enforcement_date': '20240101',
            'body': {'format': 'articles', 'article_count': 1, 'articles': [{
                'provision_id': 'law:x#JO0001', 'article_no': '제1조', 'article_title': '목적',
                'content': '제1조(목적) 본문.', 'relations': [],
            }]},
            'addenda': [], 'appendices': [],
        }
        if ministry is not None:
            data['basic_info'] = {'소관부처명': ministry}
        return data

    def test_excluded_document_produces_no_chunks(self):
        result, store = self._run(self._doc('제외법'), scope(['선택법']))
        self.assertEqual(result['scope_skipped'], 1)
        self.assertEqual(result['scope_matched'], 0)
        self.assertEqual(result['scope_law_names'], 1)
        self.assertEqual(store.objects, [])

    def test_unmatched_counts_surface_in_totals(self):
        result, _ = self._run(self._doc('제외법'), scope(['선택법'], ['안전행정부']))
        self.assertEqual(result['scope_names_unmatched'], 1)
        self.assertEqual(result['scope_ministries_unmatched'], 1)

    def test_comma_joined_ministry_document_is_indexed(self):
        """회귀 방지 — 이 문서가 조용히 빠지던 것이 이번 수정의 이유다."""
        doc = self._doc('교통사고처리 특례법', ministry='경찰청,법무부')
        result, store = self._run(doc, scope(ministries=['법무부']))
        self.assertEqual(result['scope_skipped'], 0)
        self.assertEqual(result['scope_matched'], 1)
        self.assertEqual(result['scope_ministries_unmatched'], 0)
        self.assertTrue(store.objects)


if __name__ == '__main__':
    unittest.main()
