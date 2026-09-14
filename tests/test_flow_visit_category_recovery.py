from types import SimpleNamespace
import json

import pandas as pd
import pytest

from modules.transform.pipelines.strategy import SMP_flow_visit_mart as mart
from modules.transform.pipelines.strategy import flow_visit_segmenter as segmenter
from modules.transform.utility import qwen_client
from modules.transform.pipelines.strategy import flow_visit_quality as quality


def test_project_id_conf_does_not_union_default_stores():
    context = {
        'flow_visit_targets': [{'project_id': '1', 'store_name': '기본점'}],
        'dag_run': SimpleNamespace(conf={'flow_visit_project_ids': ['2548056']}),
    }
    assert mart._target_project_ids(pd.DataFrame(), context) == {'2548056'}


def test_target_corpus_rebuild_preserves_other_projects(tmp_path, monkeypatch):
    path = tmp_path / 'corpus.jsonl'
    other = {'doc_id': 'other', 'project_id': '2', 'text': '다른 매장'}
    old = {'doc_id': 'old', 'project_id': '1', 'text': '지난 내용'}
    path.write_text('\n'.join(json.dumps(row, ensure_ascii=False) for row in [other, old]), encoding='utf-8')
    monkeypatch.setattr(mart, 'FLOW_VISIT_CORPUS_JSONL', path)
    mart.export_llm_corpus({'posts': [{'project_id': '1', 'post_id': 'p', 'issues': []}]})
    assert [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()] == [other]


def test_empty_body_stops_before_cache_or_model(monkeypatch):
    def unexpected():
        pytest.fail('본문 누락은 모델이나 캐시 접근 전에 확인해야 합니다.')
    monkeypatch.setattr(mart, '_load_cache', unexpected)
    with pytest.raises(RuntimeError, match='84849550'):
        mart.llm_extract_issues({'posts': [{'post_id': '84849550', 'content_text': '  '}]})


def test_quality_includes_empty_visit_with_no_issue_rows():
    issues = pd.DataFrame([{'post_id': 'old', 'issue_key': '기타', 'category': None}])
    visits = pd.DataFrame([{'post_id': '84849550', 'content_clean': None}])
    _, result = quality.evaluate_visit_quality(issues, [{'post_id': 'old'}], visits)
    assert result['empty_post_ids'] == ['84849550']
    assert result['invalid_category_count'] == 1


def test_contract_policy_is_not_dropped_without_taxonomy_alias():
    segment = {'raw_text': '가맹 해지 위약금 관련 협의\n가맹계약 잔여 기간 비례 산출\n위약금 전액 면제 불가'}
    assert mart._segment_has_issue_signal(segment, [{'key': '기타'}])
    assert mart._infer_category('기타', segment['raw_text']) == '정책'


def test_unrelated_contract_fact_and_parenting_burden_are_not_policy():
    assert not mart._has_policy_boundary('계약 현황: 잔여기간 7개월\n육아 부담으로 운영에 어려움')
    assert not mart._has_policy_boundary('계약 현황: 잔여기간 7개월\n타 가맹점 폐점 소식을 들음')


def test_recovering_store_premium_is_not_product_recall():
    assert mart._infer_category('기타', '미용실 창업을 위한 권리금 회수가 필수') == '기타'
    assert mart._infer_category('기타', '불량 상품을 회수 처리') == '물류/사입'


def test_topic_number_preserves_single_person_bag_and_two_digit_topics():
    text = ('주제41인 봉투 제작에 대한 피드백전달내용수발주 프로그램에서 발주 가능'
            '가맹점의견이용해보고 판단하겠다.주제12신메뉴전달내용출시 안내가맹점의견만족')
    segments = segmenter.segment_post({'post_id': 'p', 'content_text': text})
    assert segments[0]['topic'] == '1인 봉투 제작에 대한 피드백'
    assert segments[0]['owner_voice_raw'] == '이용해보고 판단하겠다.'
    assert segments[1]['topic'] == '신메뉴'
    _, taxonomy = mart._issue_maps()
    assert mart._rule_class_result(segments[0], taxonomy, [])['issue_key'] == '부자재_1인봉투'


@pytest.mark.parametrize('text,forbidden', [
    ('주제1떡볶이 판매 의견 및 개선점전달내용화구 부족 현상 확인'
     '가맹점의견계란과 파를 필수로 넣었으면 한다.', '운영_홀배달동시한계'),
    ('주제4본사에 바라는 점전달내용수익률이 나쁘다는 주제가 아닌 본사 요청'
     '가맹점의견전국 가맹점 순위를 공개하면 동기부여가 될 것 같다.', '수익_감소체감'),
])
def test_owner_opinion_is_not_classified_from_template_only(text, forbidden):
    post = {'post_id': 'p', 'content_text': text}
    segment = segmenter.segment_post(post)[0]
    _, taxonomy = mart._issue_maps()
    result = mart._rule_class_result(segment, taxonomy, [])
    assert not result or result['issue_key'] != forbidden
    assert forbidden not in {r['issue_key'] for r in mart._supplement_post_issue_coverage(post, [])}


def test_coverage_evidence_stays_in_matching_topic():
    post = {'post_id': 'p', 'content_text':
            '주제3신메뉴 출시전달내용누룽지 출시가맹점의견고소해서 만족'
            '주제41인 봉투 제작전달내용제작중가맹점의견사용해보고 판단'}
    rows = mart._supplement_post_issue_coverage(post, [])
    bag = next(row for row in rows if row['issue_key'] == '부자재_1인봉투')
    assert bag['owner_voice_raw'] == '사용해보고 판단'
    assert '누룽지' not in bag['raw_text']


def test_owner_marker_does_not_split_explanatory_sentence():
    text = '주제5대파 옵션전달내용가맹점 의견을 반영하여 테스트가맹점의견별다른 의견없음.'
    segment = segmenter.segment_post({'post_id': 'p', 'content_text': text})[0]
    assert segment['owner_voice_raw'] == '별다른 의견없음.'
    assert '가맹점 의견을 반영' in segment['sv_action_raw']


def test_cached_and_rule_segments_do_not_consume_model_budget(monkeypatch):
    segments = [{'seg_id': str(i), 'raw_text': f'문의사항 {i}', 'post_id': 'p'} for i in range(3)]
    cache_result = {'issue_key': '기타', 'category': '기타'}
    monkeypatch.setattr(mart, '_load_cache', lambda: {'0': cache_result})
    monkeypatch.setattr(mart, '_save_cache', lambda cache: None)
    monkeypatch.setattr(mart, '_cache_key', lambda kind, segment, comments: segment['seg_id'])
    monkeypatch.setattr(segmenter, 'segment_post', lambda post: segments)
    monkeypatch.setattr(mart, '_segment_has_issue_signal', lambda *args: True)
    monkeypatch.setattr(mart, '_rule_class_result', lambda s, *args: cache_result if s['seg_id'] == '1' else None)
    monkeypatch.setattr(mart, '_supplement_post_issue_coverage', lambda *args: [])
    monkeypatch.setattr(mart, '_apply_llm_summary', lambda *args, **kwargs: False)
    monkeypatch.setattr(qwen_client, 'get_ollama_client_with_candidates', lambda: (object(), ['test']))
    monkeypatch.setattr(mart, 'LLM_MAX_SEGMENTS', 1)
    monkeypatch.setattr(mart, 'FORCE_REBUILD', False)
    calls = []
    def query(*args):
        calls.append(args)
        return cache_result
    monkeypatch.setattr(mart, '_query_flow_json', query)
    result = mart.llm_extract_issues({'posts': [{'post_id': 'p', 'content_text': '문의사항 0 문의사항 1 문의사항 2'}]})
    assert len(calls) == 1
    assert len(result['posts'][0]['issues']) == 3
    assert all(issue['category'] == '기타' for issue in result['posts'][0]['issues'])
