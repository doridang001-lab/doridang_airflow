"""
전처리 파이프라인
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR, DOWN_DIR
from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes
import pandas as pd
import datetime as dt


# 데이터 로드
# from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes
def load_flow_vist_log_df(**context):
    """변경이력 수집"""
    return load_files(
        patterns=['flow_visit_*'], # 토더 리뷰 파일 패턴
        search_paths=[
            DOWN_DIR / '업로드_temp', # 업로드 임시 폴더
        ],
        xcom_key='flow_visit_log_path', # XCom 키
        file_priority = 'csv',
        file_type='auto', 
        use_glob=True, # 병합
        dedup_key=['post_date', 'title'],
        **context
    )
    

def load_flow_vist_log_master_df(**context):
    """변경이력 수집"""
    return load_files(
        patterns=['visit_sales_log_master.csv'], # Flow 방문 판매 로그 마스터 파일
        search_paths=[
            Path(LOCAL_DB / '영업관리부_DB'), # 영업관리부 DB 폴더
        ],
        xcom_key='visit_sales_log_master_path', # XCom 키
        file_priority = 'csv',
        file_type='auto', 
        use_glob=False, # 병합
        **context
    )


# load_flow_vist_log_master_df 마스터 수정
def transform_master_tmp_df(df):
    # 전처리 시작
    df = df.copy()
    df = df[df["priority"] == 1]
    return df

def preprocess_master_tmp_df(input_xcom_key, output_xcom_key, **context):
    """ 처리"""
    return preprocess_df(
        # load_flow_vist_log_master_df
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=transform_master_tmp_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )
    


# 조인후 필터링    
def join_master_flow_df(
    left_task,
    right_task,
    on=None,
    left_on=None,
    right_on=None,
    how='left',
    drop_right_keys=True,  # 조인 후 오른쪽 키 컬럼을 자동으로 삭제할지 여부
    output_xcom_key='joined_data_path',
    **context
):
    """
    두 Task의 Parquet 데이터를 로드하여 Join하는 범용 함수
    """
    ti = context['task_instance']
    
    # 1. 데이터 경로 가져오기 (Task ID와 Key가 딕셔너리 형태라고 가정)
    left_path = ti.xcom_pull(task_ids=left_task['task_id'], key=left_task['xcom_key'])
    right_path = ti.xcom_pull(task_ids=right_task['task_id'], key=right_task['xcom_key'])
    
    if not left_path:
        print(f"[ERROR] 왼쪽 데이터 경로 없음: {left_task['task_id']}")
        return None

    left_df = pd.read_parquet(left_path)
    
    # 2. 오른쪽 데이터가 없는 경우 처리 (Graceful Degradation)
    if not right_path:
        print(f"[WARNING] 오른쪽 데이터 없음: {right_task['task_id']}. 왼쪽 데이터만 보존합니다.")
        save_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(save_path, index=False)
        ti.xcom_push(key=output_xcom_key, value=str(save_path))
        return f"오른쪽 데이터 없음, 원본 유지 ({len(left_df)}행)"

    right_df = pd.read_parquet(right_path)

    # 3. Join 실행
    if on:
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on and right_on:
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
        
        # 4. 중복 키 컬럼 제거 (drop_right_keys=True 일 때)
        if drop_right_keys:
            # left_on과 right_on의 이름이 다를 경우에만 right_on 컬럼들을 삭제
            r_keys = [right_on] if isinstance(right_on, str) else right_on
            l_keys = [left_on] if isinstance(left_on, str) else left_on
            
            cols_to_drop = [c for c in r_keys if c in joined_df.columns and c not in l_keys]
            if cols_to_drop:
                joined_df.drop(columns=cols_to_drop, inplace=True)
                print(f"[INFO] 중복 방지를 위해 삭제된 오른쪽 키 컬럼: {cols_to_drop}")
    else:
        raise ValueError("조인을 위해 'on' 또는 'left_on/right_on' 매개변수가 필요합니다.")

    # 5. 결과 저장 및 XCom Push
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    print(f"[SUCCESS] Join 완료: {len(joined_df):,}행")
    return str(output_path)



def transform_visit_df(df):
    # 전처리 시작
    def _pick_col(series_df, candidates, default=None):
        for col in candidates:
            if col in series_df.columns:
                return series_df[col]
        return pd.Series([default] * len(series_df), index=series_df.index)

    # visit_date_y (마스터)가 있으면 사용, 없으면 visit_date_x (새 데이터) 사용
    visit_date_series = _pick_col(df, ['visit_date_y', 'visit_date_x', 'visit_date'])
    df['visit_date'] = visit_date_series
    
    # visit_date가 없으면 post_date로 보정 (가능한 경우)
    if 'post_date' in df.columns:
        df['visit_date'] = df['visit_date'].fillna(pd.to_datetime(df['post_date'], errors='coerce'))

    # visit_date와 post_date가 모두 없는 행만 제외
    df = df[~df["visit_date"].isnull() | ~df["post_date"].isnull()]
    
    result_df = pd.DataFrame({
        'visit_date': df['visit_date'],
        'post_date': _pick_col(df, ['post_date', 'post_date_x', 'post_date_y']),
        'project': _pick_col(df, ['project', 'project_x', 'project_y']),
        'title': _pick_col(df, ['title', 'title_x', 'title_y']),
        'author': _pick_col(df, ['author', 'author_x', 'author_y']),
        'content_text': _pick_col(df, ['content_text', 'content_text_x', 'content_text_y']),
        'collected_at': _pick_col(df, ['collected_at', 'collected_at_x', 'collected_at_y']),
        '_source_file': _pick_col(df, ['_source_file_x', '_source_file', '_source_file_y']),
    })

    df = result_df
    return df

def preprocess_visit_new_df(input_xcom_key, output_xcom_key, **context):
    """ 처리"""
    return preprocess_df(
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=transform_visit_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )
    

def transform_llm_df(df):
    """LLM을 사용하여 방문일지 자동 분석 (v8.2 - 카테고리별 개별 호출 + FP 차단)
    
    1개 방문일지 → 카테고리별 개별 LLM 호출 → 이슈가 있는 카테고리만 행 생성
    """
    import re
    import json
    import time
    from pathlib import Path
    import warnings
    warnings.filterwarnings('ignore')
    
    print("\n🤖 LLM 방문일지 처리 시작 (v8.2 - 카테고리별 개별 호출 + FP 차단)")
    
    # ============================================
    # Ollama 연결
    # ============================================
    try:
        import ollama
        
        # Docker 환경에서 호스트의 Ollama에 접근
        client = ollama.Client(host='http://host.docker.internal:11434')
        models = client.list()
        model_names = [m['model'] for m in models.get('models', [])]
        
        LLM_MODEL = None
        for candidate in ['qwen2.5:14b', 'qwen2.5:7b', 'qwen2.5:latest', 'qwen2.5', 'gemma2:2b']:
            matching = [name for name in model_names if candidate in name]
            if matching:
                LLM_MODEL = matching[0]
                break
        
        if not LLM_MODEL:
            raise Exception("사용 가능한 Ollama 모델 없음")
        print(f"✅ LLM 모델: {LLM_MODEL}")
    except Exception as e:
        print(f"❌ Ollama 연결 실패: {e}")
        return df
    
    # ============================================
    # LLM 프롬프트 정의 (v8.0 - 카테고리별 개별 호출)
    # ============================================
    SYSTEM_PROMPT = """너는 프랜차이즈 본사 영업관리 매니저다.
방문일지를 읽고 지정 카테고리의 이슈를 분석하여 JSON만 출력한다.
JSON 외 문장, 설명, 코드블록 출력 금지."""

    # 카테고리별 이슈 목록 + 키워드 (판단 기준)
    CATEGORY_CONFIG = {
        '식자재이슈': {
            'desc': '식자재(계육·소스·낙지·용기·포장·배송·발주) 품질·배송·발주 문제',
            'items': [
                ('계육 손질 불량',   '닭 손질 불량, 뼈 조각, 내장 혼입, 이물질, 살이 제대로 안 잘림, 살 안 잘림'),
                ('계육 절각 불량',   '닭 절단 크기/모양 규격 벗어남, 크기 불균일, 잔뼈 혼입, 잔뼈'),
                ('계육 털 잔존',     '닭 털, 잔털, 깃털 발견'),
                ('계육 중량 미달',   '닭 무게 부족, 중량 미달, 용수 제외 중량, 그램 부족'),
                ('계육 품질 편차',   '닭 품질 불균일, 지방·이취, 신선도 문제, 날마다 품질 다름, 핏물, 뼈 변색, 비계 과다, 비계 많음, 지방 많음, 색깔 일정치 않음, 날짜 섞임'),
                ('소스 맵기 편차',   '소스 맵기 배마다 다름'),
                ('소스 맵기 과다',   '소스 너무 맵다'),
                ('소스 맵기 부족',   '소스 싱겁다, 안 맵다'),
                ('소스 맛 편차',     '소스 맛이 다름, 풍미 차이'),
                ('소스 품질 이슈',   '소스 변질, 뭉침, 이상한 색, 유통기한, 소스 봉지 터짐, 소분 봉지 터짐, 소스 공기 없음, 실링 불량, 이음새 불량'),
                ('낙지 중량 미달',   '낙지 무게 부족, 낙지 중량 미달'),
                ('용기 불량',        '용기 파손, 불량, 규격 안 맞음, 부풀어오름, 물렁거림'),
                ('사각 용기 요청',   '사각 용기 필요, 사각형 용기 요청'),
                ('특대 용기 요청',   '특대 사이즈 용기 요청'),
                ('포장 불량',        '포장 불량, 봉투 불량, 1인 봉투 사제, 봉투 내폭 문제'),
                ('배송 지연',        '배송 늦음, 납품 지연, 냉장 배송 불량, 냉장고 미보관, 바닥에 두고 감'),
                ('발주 누락',        '주문 누락, 빠진 품목, 발주했는데 안 옴'),
                ('기타 식자재',      '위 항목에 해당하지 않는 식자재 이슈 (최후 수단)'),
            ]
        },
        '매출컨설팅': {
            'desc': '매출·수익·마케팅 문제 (도리당 브랜드 한정, 타 브랜드 제외)',
            'items': [
                ('매출 저조',        '도리당 매출 하락·감소·전월 대비 하락·매출 부진·줄었다 (점주가 직접 언급)'),
                ('매출 정체',        '매출 변화 없음, 성장 없음, 제자리, 한계점, 한계 (점주 언급)'),
                ('인건비 부담',      '인건비 많다, 직원 비용, 노동 대비 수익 부족'),
                ('원가율 부담',      '원가 높다, 마진 낮다, 수익성 악화, 원가율'),
                ('가격 인상 저항',   '가격 올리기 어렵다, 고객 저항, 가격 민감'),
                ('신메뉴 참여 저조', '신메뉴 운영 안 함, 신메뉴 판매 낮음, 신메뉴 참여'),
                ('사이드 판매 저조', '사이드 판매 부진, 추가 판매 안 됨'),
                ('리뷰 이벤트 미이행','리뷰 이벤트 진행 안 함, 리뷰 혜택 미운영'),
                ('기타 매출',        '위 항목에 해당하지 않는 매출/마케팅 이슈 (최후 수단)'),
            ]
        },
        '운영이슈': {
            'desc': '매장 운영 전반 문제',
            'items': [
                ('인력 운영 이슈',   '직원 실제 퇴사·이탈로 인한 현재 인원 부족, 인력 부족으로 영업시간 단축·매출 차질 발생'),
                ('영업시간 조정',    '영업시간 변경, 단축, 연장, 휴무일 논의'),
                ('발주 시스템 불편', '발주닷컴 불편, 시스템 오류, 사용법 모름, QC 기능 문의'),
                ('매장 시설 이슈',   '시설 노후, 기기 고장, 에어컨·냉장고 문제, 인테리어, 위생, 청결 문제, 화구 청소, 성애(서리)'),
                ('매장 이전 논의',   '이전 계획, 재계약, 위치 변경 검토'),
                ('기타 운영',        '위 항목에 해당하지 않는 운영 이슈 (최후 수단)'),
            ]
        }
    }

    # ============================================
    # TYPE_CODES / TYPE_DETAILS
    # ============================================
    TYPE_CODES = ['식자재이슈', '매출컨설팅', '운영이슈', '특이사항없음']

    TYPE_DETAILS = {
        '식자재이슈': [i[0] for i in CATEGORY_CONFIG['식자재이슈']['items']] + ['해당없음'],
        '매출컨설팅': [i[0] for i in CATEGORY_CONFIG['매출컨설팅']['items']] + ['해당없음'],
        '운영이슈':   [i[0] for i in CATEGORY_CONFIG['운영이슈']['items']]   + ['해당없음'],
        '특이사항없음': ['특이사항 없음']
    }

    # ============================================
    # Helper 함수
    # ============================================
    def clean_text(text):
        if pd.isna(text): return ""
        text = str(text)
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'\d{2,4}-\d{2,4}-\d{4}', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def clean_project_name(name):
        if pd.isna(name): return ''
        name = str(name).strip()
        name = re.sub(r'\[.*?\]', '', name).strip()
        regions = ['서울', '경기', '인천', '부산', '대전', '광주', '전북', '충남', '경북', '세종']
        for region in regions:
            for sep in ['_', ' - ', '-']:
                if name.startswith(region + sep):
                    name = name[len(region + sep):].strip()
                    break
        name = re.sub(r'\s*\([^)]*\)', '', name)
        return re.sub(r'\s+', ' ', name).strip()

    def fix_visit_date(date_val):
        if pd.isna(date_val): return pd.NaT
        if isinstance(date_val, pd.Timestamp):
            return pd.NaT if date_val.year < 2020 else date_val
        text = str(date_val).strip()
        for fmt in [r'(\d{4})[.-](\d{1,2})[.-](\d{1,2})', r'(\d{1,2})[.-](\d{1,2})[.-](\d{4})']:
            m = re.search(fmt, text)
            if m:
                g = m.groups()
                try:
                    if len(g[0]) == 4:
                        y, mo, d = int(g[0]), int(g[1]), int(g[2])
                    else:
                        d, mo, y = int(g[0]), int(g[1]), int(g[2])
                    if 1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
                        return pd.Timestamp(y, mo, d)
                except:
                    pass
        return pd.NaT

    # ============================================
    # Rule Engine FP 차단 (v8.2)
    # ============================================
    _SAUCE_QUALITY_TRIGGERS = [
        '소스 봉지 터짐', '소분 봉지 터짐', '소스공기 없이', '소스 공기 없이',
        '이음새 불량', '실링 불량', '소스봉지 터짐', '소분봉지 터짐',
    ]
    _SAUCE_WRONG_DETAILS = {'발주 시스템 불편', '소스 맵기 편차', '소스 맛 편차'}

    _RESOLVED_TRIGGERS = [
        '육안상 이상 없어보이며', '확인 결과 이상 없음', '크게 문제 없는 점 안내',
        '이상 없다고 함', '크게 문제 없음', '최근 개선됐다', '개선됐다고',
        '문제없다고', '문제 없다고', '이상 없음 확인',
        '많이 좋아졌고', '좋아져서 그냥', '요즘처럼만', '요즘은 좋아',
    ]
    _CHICKEN_DETAILS = {'계육 중량 미달', '계육 품질 편차', '계육 손질 불량', '계육 절각 불량', '계육 털 잔존'}

    _INVOICE_TRIGGERS = ['거래명세서', '명세서 미수령', '명세서를 못 받']
    _INVOICE_WRONG_DETAILS = {'계육 중량 미달', '발주 누락', '기타 식자재'}

    _OTHER_BRANDS = ['진아구', '스쿨푸드', '고기극찬', '낙지브랜드']
    _SALES_KW = ['매출', '수익', '하락', '줄었', '감소', '빠짐', '빠졌', '부진']
    _SALES_WRONG_DETAILS = {'매출 저조', '매출 정체'}

    _ORDER_OMISSION_TRIGGERS = ['주문 누락', '주문누락', '주문이 누락']
    _BALJUDOTCOM_CS_TRIGGERS = ['발주닷컴 CS', '발주닷컴에 CS', '발주닷컴CS']
    _UGAKUL_TRIGGERS = ['우가클', '우가클 비용', '우가클 광고']
    _SALES_UP_TRIGGERS = ['매출이 올라', '매출이 오르', '수익율이 개선', '매출 상승', '매출이 늘', '매출 증가']

    def _brand_in_sales_context(text):
        for brand in _OTHER_BRANDS:
            if brand not in text:
                continue
            pos = text.index(brand)
            ctx = text[max(0, pos - 50): pos + len(brand) + 100]
            if any(kw in ctx for kw in _SALES_KW):
                return True
        return False

    def apply_fp_rules(text, raw_result):
        result = {k: v.copy() for k, v in raw_result.items()}

        # Rule 1: 소스 봉지·실링 불량 → 소스 품질 이슈로 보정
        if any(p in text for p in _SAUCE_QUALITY_TRIGGERS):
            si = result.get('식자재이슈', {})
            if si.get('has_issue') and si.get('type_detail') in _SAUCE_WRONG_DETAILS:
                result['식자재이슈'].update({
                    'type_detail': '소스 품질 이슈',
                    'summary': '소스 봉지/포장 불량 이슈 발생',
                    'advice': '소스 포장 상태 본사 품질팀 확인 요청',
                    'priority': 2,
                })

        # Rule 2: 이슈 해소 표현 → 계육 관련 FP 차단
        if any(p in text for p in _RESOLVED_TRIGGERS):
            si = result.get('식자재이슈', {})
            if si.get('has_issue') and si.get('type_detail') in _CHICKEN_DETAILS:
                result['식자재이슈'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }

        # Rule 3: 거래명세서 언급 → 식자재 FP 차단, 운영이슈로 변경
        if any(p in text for p in _INVOICE_TRIGGERS):
            si = result.get('식자재이슈', {})
            if si.get('has_issue') and si.get('type_detail') in _INVOICE_WRONG_DETAILS:
                result['식자재이슈'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }
                op = result.get('운영이슈', {})
                if not op.get('has_issue'):
                    result['운영이슈'] = {
                        'has_issue': True, 'priority': 3,
                        'type_detail': '발주 시스템 불편',
                        'summary': '거래명세서 미수령 문의 발생',
                        'advice': '명세서 발행 프로세스 안내',
                    }

        # Rule 4: 타 브랜드 매출 언급 → 매출컨설팅 FP 차단
        if _brand_in_sales_context(text):
            mc = result.get('매출컨설팅', {})
            if mc.get('has_issue') and mc.get('type_detail') in _SALES_WRONG_DETAILS:
                result['매출컨설팅'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }

        # Rule 5: 주문 누락(배달) → 발주 누락/기타 식자재 FP 차단
        if any(p in text for p in _ORDER_OMISSION_TRIGGERS):
            si = result.get('식자재이슈', {})
            if si.get('has_issue') and si.get('type_detail') in {'발주 누락', '기타 식자재'}:
                result['식자재이슈'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }

        # Rule 6: 발주닷컴 CS → 발주 시스템 불편 FP 차단
        if any(p in text for p in _BALJUDOTCOM_CS_TRIGGERS):
            op = result.get('운영이슈', {})
            if op.get('has_issue') and op.get('type_detail') == '발주 시스템 불편':
                result['운영이슈'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }

        # Rule 7: 우가클 광고비 → 인건비 부담 FP 차단
        if any(p in text for p in _UGAKUL_TRIGGERS):
            op = result.get('운영이슈', {})
            if op.get('has_issue') and op.get('type_detail') == '인건비 부담':
                result['운영이슈'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }

        # Rule 8: 매출 상승 표현 → 매출 저조/정체 FP 차단
        if any(p in text for p in _SALES_UP_TRIGGERS):
            mc = result.get('매출컨설팅', {})
            if mc.get('has_issue') and mc.get('type_detail') in _SALES_WRONG_DETAILS:
                result['매출컨설팅'] = {
                    'has_issue': False, 'priority': 4,
                    'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요',
                }

        return result

    # ============================================
    # Few-shot 예시 로더
    # ============================================
    FEW_SHOT_PATH = str(LOCAL_DB / '영업관리부_DB' / 'few_shot_examples.json')

    def load_few_shot_examples(type_code):
        try:
            with open(FEW_SHOT_PATH, 'r', encoding='utf-8') as f:
                all_examples = json.load(f)
            examples = all_examples.get(type_code, [])
            if not examples:
                return ""
            lines = []
            for ex in examples:
                lines.append(f"입력: {ex['input']}")
                lines.append(f"출력: {json.dumps(ex['output'], ensure_ascii=False)}\n")
            return "\n".join(lines)
        except:
            return ""

    # ============================================
    # 카테고리별 단일 호출 함수
    # ============================================
    def _build_user_prompt(text, type_code, config):
        items_text = '\n'.join([f'  {name}: {kw}' for name, kw in config['items']])
        extra_rules = ''
        if type_code == '운영이슈':
            extra_rules = """
[인력 운영 이슈 추가 판단 기준]
✅ true: 직원이 실제 퇴사·이탈해서 현재 인원 부족 / 인력 부족으로 영업시간 단축·매출 차질 발생
❌ false: 점주가 피로감 표현("힘들다", "지친다") / 점주 개인 사정(건강·논문·결혼준비) / 직원은 있는데 점주 성격상 직접 나오는 경우 / 매출이 잘 나와서 바쁜 경우(긍정 상황) / 알바 채용 고려 중(미래 계획)"""
        elif type_code == '매출컨설팅':
            extra_rules = """
[매출 저조 추가 판단 기준]
✅ true: 점주가 직접 "매출이 낮다", "매출이 빠진다"고 언급 / 전월·전년 대비 실제 감소 수치가 본문에 존재
❌ false: 점주가 현 매출에 만족한다고 언급 / 담당자가 매출 개선을 제안했지만 점주는 만족 중 / 목표 미달이지만 점주가 긍정 평가 / 도리당 외 타 브랜드의 매출 이슈인 경우"""

        examples_text = load_few_shot_examples(type_code)
        examples_block = f"[분류 예시]\n{examples_text}\n" if examples_text else ""

        return f"""[분석 카테고리] {type_code} ({config['desc']})

[이슈 분류 목록 — 키워드가 본문에 있으면 해당 항목 선택]
{items_text}

[has_issue = true 조건 — 4가지 모두 충족해야만 true]
1. 점주·가맹점이 직접 언급하거나 개선 요청한 이슈 (담당자의 일방 판단 "~로 보임", "~해야 할 것 같다" 제외)
2. 현재 진행 중인 이슈 ("이상 없다", "개선됐다", "문제없다"로 해소된 것 제외)
3. 부정적 결과가 실제 발생했거나 발생 중 ("예정", "고려 중", "생각 중" 같은 미래 계획 제외)
4. 영업·품질에 실질적 영향이 있는 이슈 (점주 피로감, 개인 감정, 희망사항 제외)
{extra_rules}
[priority 기준]
1=즉시(매출30%↑급감/부패이물질/폐업위기)
2=금주내(매출10~30%하락/품질문제/인력부족/원가급등)
3=정기(매출정체/배송지연/시스템불편/영업시간논의)
4=이슈없음

{examples_block}[방문일지]
{text}

[출력] JSON만:
{{
  "has_issue": true 또는 false,
  "type_detail": "분류 목록에서 하나 선택 / 이슈 없으면 해당없음",
  "priority": 숫자,
  "summary": "요약 1문장 / 이슈 없으면 해당 이슈 없음",
  "advice": "조치 1문장 / 이슈 없으면 추가 조치 불필요"
}}"""

    def _call_llm_single(text, type_code):
        config = CATEGORY_CONFIG[type_code]
        valid_details = TYPE_DETAILS[type_code]
        default = {
            'has_issue': False, 'priority': 4,
            'type_detail': '해당없음', 'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요'
        }
        try:
            response = client.generate(
                model=LLM_MODEL,
                prompt=_build_user_prompt(text[:2000], type_code, config),
                system=SYSTEM_PROMPT,
                options={'temperature': 0.05, 'num_predict': 400}
            )
            raw = response['response'].strip()
            raw = re.sub(r'^```json\s*', '', raw)
            raw = re.sub(r'\s*```$', '', raw)
            m = re.search(r'\{[\s\S]+\}', raw)
            if m: raw = m.group()
            item = json.loads(raw)

            if 'has_issue' not in item:
                item['has_issue'] = False
            if 'priority' not in item or not isinstance(item['priority'], int):
                item['priority'] = 4 if not item['has_issue'] else 3
            item['priority'] = max(1, min(4, item['priority']))
            if item.get('type_detail') not in valid_details:
                if item['has_issue']:
                    matched = next(
                        (v for v in valid_details
                         if v not in ('해당없음',) and
                         (v in str(item.get('type_detail', '')) or
                          str(item.get('type_detail', '')) in v)),
                        None
                    )
                    item['type_detail'] = matched if matched else next(
                        (v for v in valid_details if v.startswith('기타')), '기타')
                else:
                    item['type_detail'] = '해당없음'
            if not item.get('summary'):
                item['summary'] = '해당 이슈 없음' if not item['has_issue'] else '-'
            if not item.get('advice'):
                item['advice'] = '추가 조치 불필요' if not item['has_issue'] else '-'
            return item
        except Exception as e:
            return default

    # ============================================
    # LLM 분석 함수 (v8.2 - 카테고리별 개별 호출 + FP 차단)
    # ============================================
    def get_default_result():
        return {
            '식자재이슈': {'has_issue': False, 'priority': 4, 'type_detail': '해당없음',
                        'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요'},
            '매출컨설팅': {'has_issue': False, 'priority': 4, 'type_detail': '해당없음',
                        'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요'},
            '운영이슈':   {'has_issue': False, 'priority': 4, 'type_detail': '해당없음',
                        'summary': '해당 이슈 없음', 'advice': '추가 조치 불필요'},
            '특이사항없음': {'has_issue': True, 'priority': 4, 'type_detail': '특이사항 없음',
                         'summary': '정기 방문 점검 결과 특이 이슈 없는 상태로 확인함',
                         'advice': '현 운영 상태 유지하며 특이 이슈 발생 여부 모니터링'},
        }

    def analyze_with_llm(text):
        """v8.2: 카테고리별 개별 LLM 호출 + Rule Engine FP 차단"""
        result = get_default_result()
        if not text or len(text) < 30:
            return result
        for type_code in ['식자재이슈', '매출컨설팅', '운영이슈']:
            result[type_code] = _call_llm_single(text, type_code)
        result = apply_fp_rules(text, result)
        has_any = any(result[tc]['has_issue'] for tc in ['식자재이슈', '매출컨설팅', '운영이슈'])
        result['특이사항없음']['has_issue'] = not has_any
        return result

    # ============================================
    # flatten_result
    # ============================================
    def flatten_result(raw_result, base_info):
        results = []
        type_map = {'식자재이슈': '식자재', '매출컨설팅': '매출', '운영이슈': '운영'}
        issue_list = []
        for type_code, category_name in type_map.items():
            item = raw_result.get(type_code, {})
            if item.get('has_issue', False):
                issue_list.append({
                    'type_code': type_code,
                    'category_name': category_name,
                    'llm_priority': item.get('priority', 4),
                    'detail': item.get('type_detail', '해당없음'),
                    'summary': item.get('summary', '해당 이슈 없음'),
                    'advice': item.get('advice', '추가 조치 불필요')
                })
        if issue_list:
            issue_list.sort(key=lambda x: x['llm_priority'])
            for idx, issue in enumerate(issue_list, 1):
                row = base_info.copy()
                row['type_code'] = issue['type_code']
                row['category_name'] = issue['category_name']
                row['priority'] = idx
                row['type_detail'] = issue['detail']
                row['summary'] = issue['summary']
                row['advice'] = issue['advice']
                results.append(row)
        else:
            item = raw_result.get('특이사항없음', {})
            row = base_info.copy()
            row['type_code'] = '특이사항없음'
            row['category_name'] = '기타'
            row['priority'] = 1
            row['type_detail'] = item.get('type_detail', '특이사항 없음')
            row['summary'] = item.get('summary', '정기 방문 점검 결과 특이 이슈 없는 상태로 확인함')
            row['advice'] = item.get('advice', '현 운영 상태 유지하며 특이 이슈 발생 여부 모니터링')
            results.append(row)
        return results

    # ============================================
    # 배치 처리
    # ============================================
    if 'content_text' not in df.columns:
        print("⚠️ content_text 컬럼 없음")
        return df
    
    df_work = df.copy()
    
    # visit_date 정제
    if 'visit_date' in df_work.columns:
        df_work['visit_date'] = df_work['visit_date'].apply(fix_visit_date)
        print(f"✅ visit_date 정제: 유효 {(~df_work['visit_date'].isna()).sum()}건, NaT {df_work['visit_date'].isna().sum()}건")
    
    # content_text 정리
    df_work['content_clean'] = df_work['content_text'].apply(clean_text)
    
    # project 정제
    if 'project' in df_work.columns:
        df_work['project_clean'] = df_work['project'].apply(clean_project_name)
    
    print(f"🤖 LLM 멀티 분류 분석 시작 (총 {len(df_work)}건)")
    print(f"   분류 항목: 식자재이슈, 매출컨설팅, 운영이슈")
    print(f"   결과: 1개 방문일지 → 감지된 모든 카테고리별 행 생성")
    
    start_time = time.time()
    
    all_results = []
    for idx, row in df_work.iterrows():
        text = row.get('content_clean', '')
        
        # 기본 정보 추출
        base_info = {}
        for col in ['visit_date', 'post_date', 'project', 'project_clean', 'title', 'author']:
            if col in row.index:
                base_info[col] = row[col]
        base_info['uploaded_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # LLM 분석 (멀티 분류)
        raw_result = analyze_with_llm(text)
        
        # Flat 변환 (이슈가 있는 카테고리만 행 생성)
        flat_results = flatten_result(raw_result, base_info)
        all_results.extend(flat_results)
        
        i = idx + 1
        if i % 5 == 0 or i == len(df_work):
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(df_work) - i) / rate if rate > 0 else 0
            print(f"\r  [{i:4d}/{len(df_work):4d}] {rate:5.2f}건/초 | 남은시간: {remaining/60:4.1f}분", 
                  end="", flush=True)
    
    elapsed = time.time() - start_time
    print(f"\n✅ 처리 완료! (소요: {elapsed/60:.2f}분)")
    print(f"   원본 방문일지: {len(df_work)}건 → 카테고리별 행: {len(all_results)}건")
    
    # ============================================
    # 결과 DataFrame 생성
    # ============================================
    df_result = pd.DataFrame(all_results)
    
    if df_result.empty:
        print("⚠️ 결과가 비어있음")
        return df
    
    # top_priority 추가 (각 방문일지별 최고 우선순위)
    if 'project' in df_result.columns and 'priority' in df_result.columns:
        df_result['top_priority'] = df_result.groupby('project')['priority'].transform('min')
    elif 'priority' in df_result.columns:
        df_result['top_priority'] = df_result['priority']
    else:
        df_result['top_priority'] = 1
    
    # 날짜 정리 및 정렬
    if 'visit_date' in df_result.columns:
        df_result['visit_date'] = pd.to_datetime(df_result['visit_date'], errors='coerce')
    
    # 우선순위 → 날짜 순 정렬
    df_result = df_result.sort_values(
        ['top_priority', 'visit_date'], 
        ascending=[True, False]
    ).reset_index(drop=True)
    
    # 통계 출력
    print(f"\n📊 분석 결과:")
    print(f"   type_code 분포:")
    if 'type_code' in df_result.columns and len(df_result) > 0:
        for code, count in df_result['type_code'].value_counts().items():
            print(f"     {code}: {count}건")
    else:
        print("     (데이터 없음)")
    
    print(f"\n   category_name 분포:")
    if 'category_name' in df_result.columns and len(df_result) > 0:
        for cat, count in df_result['category_name'].value_counts().items():
            print(f"     {cat}: {count}건")
    
    print(f"\n   priority 분포:")
    if 'priority' in df_result.columns and len(df_result) > 0:
        for pri, count in df_result['priority'].value_counts().sort_index().items():
            print(f"     P{pri}: {count}건")
    
    print(f"\n✅ 최종 처리 완료: {len(df_result)}행 x {len(df_result.columns)}열")
    
    return df_result


def preprocess_llm_df(input_xcom_key, output_xcom_key, **context):
    """LLM 멀티 분류 처리 래퍼 함수"""
    return preprocess_df(
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=transform_llm_df,
        **context
    )


def append_to_master(**context):
    """LLM 처리된 데이터를 마스터 파일에 중복 검사 후 append (멀티 분류 대응)"""
    ti = context['task_instance']

    DEFAULT_MASTER_COLUMNS = [
        'visit_date', 'post_date', 'project', 'project_clean',
        'title', 'author', 'category_name', 'type_code', 'priority',
        'type_detail', 'summary', 'advice'
    ]
    
    # 1. LLM 처리된 데이터 경로 가져오기
    llm_data_path = ti.xcom_pull(task_ids='preprocess_llm', key='llm_processed_visit_log_path')
    if not llm_data_path:
        print("[ERROR] LLM 처리 데이터 없음")
        return "LLM 데이터 없음"
    
    # 2. 마스터 파일 경로
    master_path = LOCAL_DB / '영업관리부_DB' / 'visit_sales_log_master.csv'
    
    # 3. 데이터 로드
    new_data = pd.read_parquet(llm_data_path)
    print(f"[INFO] 신규 데이터: {len(new_data)}행")
    print(f"[INFO] 신규 데이터 컬럼: {list(new_data.columns)}")
    
    # 4. 마스터 파일 로드 (없으면 빈 데이터프레임)
    if master_path.exists():
        master_df = pd.read_csv(master_path, encoding='utf-8-sig')
        print(f"[INFO] 기존 마스터: {len(master_df)}행")
        print(f"[INFO] 마스터 파일 컬럼: {list(master_df.columns)}")
    else:
        master_df = pd.DataFrame()
        print("[INFO] 마스터 파일 없음 - 새로 생성")

    # project_clean이 없으면 project에서 자동 생성 (기존 마스터 백필)
    if len(master_df) > 0 and 'project_clean' not in master_df.columns and 'project' in master_df.columns:
        import re as _re
        def _backfill_clean_name(name):
            if pd.isna(name): return ''
            name = str(name).strip()
            name = _re.sub(r'\[.*?\]', '', name).strip()
            regions = ['서울', '경기', '인천', '부산', '대전', '광주', '전북', '충남', '경북', '세종']
            for region in regions:
                for sep in ['_', ' - ', '-']:
                    if name.startswith(region + sep):
                        name = name[len(region + sep):].strip()
                        break
            name = _re.sub(r'\s*\([^)]*\)', '', name)
            return _re.sub(r'\s+', ' ', name).strip()
        # project 컬럼 바로 뒤에 project_clean 삽입
        proj_idx = list(master_df.columns).index('project') + 1
        master_df.insert(proj_idx, 'project_clean', master_df['project'].apply(_backfill_clean_name))
        print(f"[INFO] project_clean 컬럼 backfill 완료 ({len(master_df)}행)")

    # 기준 컬럼 설정 (마스터가 있으면 마스터 기준, 없으면 기본 스키마)
    if len(master_df) > 0:
        target_columns = list(master_df.columns)
    else:
        target_columns = DEFAULT_MASTER_COLUMNS
        master_df = pd.DataFrame(columns=target_columns)

    # 신규 데이터가 비어있으면 종료
    if new_data.empty:
        print("[INFO] 신규 데이터 없음 - 마스터 업데이트 생략")
        return "신규 데이터 없음"
    
    # ============================================
    # 5. 중복 검사 - post_date + title + author + category_name 기준
    # ============================================
    if len(master_df) > 0:
        # 📌 post_date 정규화 함수 (초 제거, 분 단위로 통일)
        def normalize_post_date(val):
            """post_date를 YYYY-MM-DD HH:MM 형식으로 통일 (초 제거)"""
            if pd.isna(val):
                return None
            try:
                dt = pd.to_datetime(val)
                return dt.strftime('%Y-%m-%d %H:%M')
            except:
                return str(val)
        
        # 📌 post_date 정규화 적용
        if 'post_date' in master_df.columns:
            master_df['post_date_norm'] = master_df['post_date'].apply(normalize_post_date)
        if 'post_date' in new_data.columns:
            new_data['post_date_norm'] = new_data['post_date'].apply(normalize_post_date)
        
        # 공통 컬럼 찾기 (멀티 분류 대응: category_name 추가)
        common_cols = []
        
        # ★ category_name이 양쪽에 있으면 포함
        if 'category_name' in master_df.columns and 'category_name' in new_data.columns:
            for col in ['post_date_norm', 'title', 'author', 'category_name']:
                if col in master_df.columns and col in new_data.columns:
                    common_cols.append(col)
        else:
            # category_name 없으면 기존 방식
            for col in ['post_date_norm', 'title', 'author']:
                if col in master_df.columns and col in new_data.columns:
                    common_cols.append(col)
        
        # fallback: post_date_norm이 없으면 원본 post_date 사용
        if not common_cols:
            for col in ['post_date', 'title', 'author']:
                if col in master_df.columns and col in new_data.columns:
                    common_cols.append(col)
        
        # 최소한 post_date + author라도 있으면 사용
        if not common_cols:
            if 'post_date' in master_df.columns and 'post_date' in new_data.columns:
                if 'author' in master_df.columns and 'author' in new_data.columns:
                    common_cols = ['post_date', 'author']
        
        if common_cols:
            print(f"[INFO] 중복 검사 기준 컬럼: {common_cols}")
            
            # 중복 키 생성
            master_df['_dedup_key'] = master_df[common_cols].astype(str).apply('|'.join, axis=1)
            new_data['_dedup_key'] = new_data[common_cols].astype(str).apply('|'.join, axis=1)
            
            # 📊 중복 상세 로그 (디버깅용)
            print(f"\n[DEBUG] 중복 검사 샘플:")
            print(f"  마스터 키 샘플 (첫 3개):")
            for key in master_df['_dedup_key'].head(3):
                print(f"    - {key}")
            print(f"  신규 키 샘플 (첫 3개):")
            for key in new_data['_dedup_key'].head(3):
                print(f"    - {key}")
            
            # 중복 제거
            before_count = len(new_data)
            duplicates = new_data[new_data['_dedup_key'].isin(master_df['_dedup_key'])]
            new_data = new_data[~new_data['_dedup_key'].isin(master_df['_dedup_key'])]
            duplicate_count = before_count - len(new_data)
            
            # 중복된 항목 상세 로그
            if duplicate_count > 0:
                print(f"\n[INFO] 중복 발견 ({duplicate_count}건):")
                for idx, row in duplicates.head(5).iterrows():
                    cat = row.get('category_name', 'N/A')
                    print(f"  - {row.get('post_date', 'N/A')} | {row.get('title', 'N/A')[:30]}... | {row.get('author', 'N/A')} | {cat}")
                if duplicate_count > 5:
                    print(f"  ... 외 {duplicate_count - 5}건")
            
            # _dedup_key와 정규화 컬럼 제거
            master_df = master_df.drop(columns=['_dedup_key'], errors='ignore')
            new_data = new_data.drop(columns=['_dedup_key'], errors='ignore')
            if 'post_date_norm' in master_df.columns:
                master_df = master_df.drop(columns=['post_date_norm'])
            if 'post_date_norm' in new_data.columns:
                new_data = new_data.drop(columns=['post_date_norm'])
            
            print(f"[INFO] 중복 제거 완료: {duplicate_count}행 제외, 추가할 데이터: {len(new_data)}행")
        else:
            print(f"[WARNING] 중복 검사 불가능 - 공통 컬럼 없음")
            print(f"[WARNING] 마스터 컬럼: {list(master_df.columns)}")
            print(f"[WARNING] 신규 컬럼: {list(new_data.columns)}")
            print(f"[WARNING] 전체 추가 진행: {len(new_data)}행")
    else:
        print(f"[INFO] 마스터가 비어있음 - 전체 추가: {len(new_data)}행")
    
    # ============================================
    # 6. Append
    # ============================================
    if len(new_data) > 0:
        # 마스터와 신규 데이터의 컬럼 맞추기 (마스터 기준)
        for col in target_columns:
            if col not in new_data.columns:
                new_data[col] = None

        # 불필요한 컬럼 제거 및 순서 맞추기
        new_data = new_data[target_columns]
        master_df = master_df.reindex(columns=target_columns)
        
        updated_master = pd.concat([master_df, new_data], ignore_index=True)
        
        # visit_date 기준 정렬 (있는 경우)
        if 'visit_date' in updated_master.columns:
            updated_master['visit_date'] = pd.to_datetime(updated_master['visit_date'], errors='coerce')
            updated_master['_has_date'] = updated_master['visit_date'].notna()
            updated_master = updated_master.sort_values(
                ['_has_date', 'visit_date'], 
                ascending=[False, False]
            ).drop('_has_date', axis=1).reset_index(drop=True)
            
            # visit_date를 문자열로 저장 (CSV 호환성)
            updated_master['visit_date'] = updated_master['visit_date'].dt.strftime('%Y-%m-%d')
        
        # 7. 마스터 파일 저장
        master_path.parent.mkdir(exist_ok=True, parents=True)
        updated_master.to_csv(master_path, index=False, encoding='utf-8-sig')
        
        print(f"\n[SUCCESS] 마스터 업데이트 완료: {len(master_df)}행 → {len(updated_master)}행 (+{len(new_data)}행)")
        
        # 통계 출력
        if 'type_code' in updated_master.columns:
            print(f"\n[INFO] type_code 분포:")
            for code, count in updated_master['type_code'].value_counts().items():
                print(f"  {code}: {count}건")
        
        if 'category_name' in updated_master.columns:
            print(f"\n[INFO] category_name 분포:")
            for cat, count in updated_master['category_name'].value_counts().items():
                print(f"  {cat}: {count}건")
        
        return f"마스터 업데이트 완료: +{len(new_data)}행"
    else:
        print("[INFO] 추가할 신규 데이터 없음")
        return "신규 데이터 없음"
    
    

# 사용한 파일 이동
def move_files(patterns, dest_dir, source_dir=None, **context):
    """
    수집 폴더의 CSV 파일들을 지정된 경로로 이동
    
    Args:
        patterns: 이동할 파일 패턴 리스트 (예: ['baemin_*.csv'])
        dest_dir: 목적지 디렉토리 (컨테이너 경로)
        source_dir: 이동할 원본 디렉토리 (미지정 시 DOWN_DIR)
    """
    import glob as glob_module
    import shutil
    from pathlib import Path
    from modules.transform.utility.paths import COLLECT_DB

    collect_dir = Path(source_dir) if source_dir else DOWN_DIR
    dest_path = Path(dest_dir)
    
    # 목적지 디렉토리 생성
    dest_path.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    for pattern in patterns:
        file_pattern = str(collect_dir / pattern)
        files = glob_module.glob(file_pattern)
        
        print(f"\n[이동] 패턴: {pattern}")
        print(f"   찾은 파일: {len(files)}개")
        
        for file_path in files:
            try:
                source = Path(file_path)
                destination = dest_path / source.name
                
                shutil.move(str(source), str(destination))
                print(f"   ✅ 이동: {source.name} → {destination}")
                moved_count += 1
            except Exception as e:
                print(f"   ⚠️  이동 실패: {Path(file_path).name} - {e}")
    
    print(f"\n✅ 총 {moved_count}개 파일 이동 완료")
    return f"이동 완료: {moved_count}개"
