"""
전처리 파이프라인
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR, DOWN_DIR
from modules.transform.utility.io3 import load_files, preprocess_df, save_to_csv, join_dataframes
import pandas as pd
import datetime as dt


# 데이터 로드
# from modules.transform.utility.io3 import load_files, preprocess_df, save_to_csv, join_dataframes
def load_flow_vist_log_df(**context):
    """변경이력 수집"""
    return load_files(
        patterns=['flow_visit_*'], # 토더 리뷰 파일 패턴
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'), # 업로드 임시 폴더
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
    """LLM을 사용하여 방문일지 자동 분석 (v5.0 - 시각화 중심)
    
    type_code, type_detail, summary, advice 4개 컬럼만 생성
    """
    import re
    import json
    import time
    from pathlib import Path
    import warnings
    warnings.filterwarnings('ignore')
    
    print("\n🤖 LLM 방문일지 처리 시작 (v5.0)")
    
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
        for candidate in ['qwen2.5:7b', 'qwen2.5:latest', 'qwen2.5', 'gemma2:2b']:
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
    # LLM 프롬프트 정의
    # ============================================
    SYSTEM_PROMPT = """
너는 프랜차이즈 본사 소속
시니어 영업관리 매니저이자
방문일지 데이터 품질을 최종 책임지는 리뷰 담당자다.

너의 임무는
방문일지 1건을 읽고
'본사에서 반드시 관리·추적해야 할
가장 중요한 이슈 단 하나'
를 판단하여 구조화하는 것이다.

모호하더라도 반드시 하나의 결론을 내려라.
중립 요약이 아니라
'본사 관리 관점의 판단'이 최우선이다.

────────────────────────
[출력 규칙] ★절대 준수★
────────────────────────
출력은 반드시 JSON 하나만 생성한다.
JSON 외의 문장, 설명, 코드블록, 여백을 절대 출력하지 않는다.
JSON 키는 정확히 아래 4개만 사용한다.

{
  "type_code": "...",
  "type_detail": "...",
  "summary": "...",
  "advice": "..."
}

────────────────────────
[type_code 정의]
────────────────────────
type_code는
이 방문일지에서
본사가 관리 대상으로 인식해야 할
가장 중요한 이슈 유형 단 하나다.

아래 중 하나만 선택한다.

- 식자재이슈
- 매출컨설팅
- 운영이슈
- 특이사항없음

※ 정책 안내는 절대 type_code가 아니다.
정책은 항상 배경이며, 단독 이슈가 아니다.

판단 우선순위:
식자재이슈 > 매출컨설팅 > 운영이슈 > 특이사항없음

────────────────────────
[type_code 강제 분류 규칙]
────────────────────────

★ 식자재이슈 (최우선)
아래 내용이 하나라도 언급되면 반드시 식자재이슈:
- 계육, 닭, 닭가슴살, 절각, 손질, 껍데기, 털
- 중량 미달, 품질 불량, 손질 문제
- 반복, 지속, 근래, 한달 전부터
- 사진 첨부, 클레임, 개선 요청
- 소스 맵기, 소스 맛, 용기 불량

★ 매출컨설팅
아래 내용이 하나라도 명확히 언급되면 매출컨설팅:
- 매출 저조, 매출 정체
- 순이익 감소, 수익 구조 부담
- 인건비 부담, 운영이 타이트함
- 샵인샵, 1인 메뉴 고민
- 옵션 테스트 효과 없음, 객단가 정체

★ 운영이슈
- 인력 운영 문제
- 영업시간 조정
- 매장 이전, 시설, 집기
- 발주·시스템 사용 불편

★ 특이사항없음
위 모든 조건에 해당하지 않을 때만 선택 가능.

※ 매출·식자재·운영 관련 수치, 불만, 의견, 고민이
단 한 번이라도 등장하면 "특이사항없음" 선택 불가.

────────────────────────
[type_detail 규칙] ★핵심★
────────────────────────
type_detail은
아래 <표준 이슈 사전>에서
type_code에 해당하는 항목 중
반드시 하나만 정확히 선택한다.

- 새로운 표현 생성 금지
- 단어 요약·변형·결합 금지
- 유사어 사용 금지
- 두 개 이상 선택 금지
- 쉼표(,) 사용 금지

────────────────────────
<표준 이슈 사전>
────────────────────────

[식자재이슈]
- 계육 손질 불량
- 계육 절각 불량
- 계육 털 잔존
- 계육 중량 미달
- 계육 품질 편차
- 소스 맵기 편차
- 소스 맵기 과다
- 소스 맵기 부족
- 소스 맛 편차
- 소스 품질 이슈
- 낙지 중량 미달
- 용기 불량
- 사각 용기 요청
- 특대 용기 요청
- 포장 불량
- 배송 지연
- 발주 누락
- 기타 식자재

[매출컨설팅]
- 매출 저조
- 매출 정체
- 인건비 부담
- 원가율 부담
- 가격 인상 저항
- 신메뉴 참여 저조
- 사이드 판매 저조
- 리뷰 이벤트 미이행
- 기타 매출

[운영이슈]
- 인력 운영 이슈
- 영업시간 조정
- 발주 시스템 불편
- 매장 시설 이슈
- 매장 이전 논의
- 기타 운영

[특이사항없음]
- 특이사항 없음

────────────────────────
[summary 규칙]
────────────────────────
summary는 상사에게 보고하는 방문 결과 요약 1문장이다.
summary에는
"점주가", "요청함", "답변함", "말씀함"
같은 화자 중심 표현을 사용할 수 없다.

구조:
① 핵심 상황 또는 문제
② 이번 방문에서 수행한 핵심 행위
③ 방문 종료 시점의 상태 판단

규칙:
- 조언, 해결책, 계획, 의도 표현 금지
- "~필요", "~검토", "~예정" 사용 금지
- 반드시 한 문장으로 작성

summary 마지막은 아래 중 하나로 끝난다:
- 특이 이슈 없는 상태로 확인함
- 문제 지속 상태로 확인함
- 개선이 필요한 상태로 파악함
- 추가 확인이 필요한 상태로 파악함
- 안정적인 운영 상태로 확인함

────────────────────────
[advice 규칙]
────────────────────────
advice는 다음 방문 전까지 본사가 무엇을 보면 되는지 알려주는 문장이다.

- 행동 중심
- 해결책·지시·명령 금지
- 아래 표현 중 하나를 반드시 포함: 확인 / 점검 / 모니터링 / 검토

type_code별 기본 형식:
- 식자재이슈: 식자재 품질 및 물류 상태 점검
- 매출컨설팅: 매출 및 수익 구조 모니터링
- 운영이슈: 운영 방식 및 인력 구조 점검
- 특이사항없음: 현 운영 상태를 유지하며 특이 이슈 발생 여부 모니터링

advice 문장은 반드시
"~여부 점검",
"~상태 점검",
"~구조 모니터링"
중 하나의 형태로 끝나야 한다.
다른 종결형은 허용되지 않는다.
"""
    
    # type_code 허용값
    TYPE_CODES = ['식자재이슈', '매출컨설팅', '운영이슈', '특이사항없음']
    
    # type_detail 허용값 (검증용)
    TYPE_DETAILS = {
        '식자재이슈': [
            '계육 손질 불량', '계육 절각 불량', '계육 털 잔존', '계육 중량 미달', '계육 품질 편차',
            '소스 맵기 편차', '소스 맵기 과다', '소스 맵기 부족', '소스 맛 편차', '소스 품질 이슈',
            '낙지 중량 미달', '용기 불량', '사각 용기 요청', '특대 용기 요청',
            '포장 불량', '배송 지연', '발주 누락', '기타 식자재'
        ],
        '매출컨설팅': [
            '매출 저조', '매출 정체', '인건비 부담', '원가율 부담', '가격 인상 저항',
            '신메뉴 참여 저조', '사이드 판매 저조', '리뷰 이벤트 미이행', '기타 매출'
        ],
        '운영이슈': [
            '인력 운영 이슈', '영업시간 조정', '발주 시스템 불편', 
            '매장 시설 이슈', '매장 이전 논의', '기타 운영'
        ],
        '특이사항없음': ['특이사항 없음']
    }
    
    # ============================================
    # Helper 함수
    # ============================================
    def clean_text(text):
        if pd.isna(text):
            return ""
        text = str(text)
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'\d{2,4}-\d{2,4}-\d{4}', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def clean_project_name(name):
        if pd.isna(name):
            return ''
        name = str(name).strip()
        name = re.sub(r'\[.*?\]', '', name).strip()
        
        regions = ['서울', '경기', '인천', '부산', '대전', '광주', '전북', '충남', '경북', '세종']
        for region in regions:
            for sep in ['_', ' - ', '-']:
                prefix = region + sep
                if name.startswith(prefix):
                    name = name[len(prefix):].strip()
                    break
        
        name = re.sub(r'\s*\([^)]*\)', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name
    
    def fix_visit_date(date_val):
        """visit_date 정제 및 복구"""
        if pd.isna(date_val):
            return pd.NaT
        
        # 이미 datetime이면 반환
        if isinstance(date_val, pd.Timestamp):
            if date_val.year < 2020:  # 1970-01-01 같은 이상값 감지
                return pd.NaT
            return date_val
        
        # 문자열 정제
        text = str(date_val).strip()
        
        # 날짜 형식 감지 및 파싱
        formats = [
            r'(\d{4})[.-](\d{1,2})[.-](\d{1,2})',  # YYYY-MM-DD or YYYY.MM.DD
            r'(\d{1,2})[.-](\d{1,2})[.-](\d{4})',  # DD-MM-YYYY
        ]
        
        for fmt in formats:
            match = re.search(fmt, text)
            if match:
                groups = match.groups()
                try:
                    # 첫 번째 포맷 (4자리가 연도)
                    if len(groups[0]) == 4:
                        year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                    else:
                        # 세 번째가 4자리 (연도)
                        day, month, year = int(groups[0]), int(groups[1]), int(groups[2])
                    
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return pd.Timestamp(year, month, day)
                except:
                    pass
        
        return pd.NaT
    
    def analyze_with_llm(text):
        """LLM으로 type_code, type_detail, summary, advice 생성"""
        
        default_result = {
            'type_code': '특이사항없음',
            'type_detail': '특이사항 없음',
            'summary': '정기 방문 점검 결과 특이 이슈 없는 상태로 확인함',
            'advice': '현 운영 상태를 유지하며 특이 이슈 발생 여부 모니터링'
        }
        
        if not text or len(text) < 30:
            return default_result
        
        text_truncated = text[:1800]
        
        user_prompt = f"""다음은 매장 방문일지 원문이다.

[방문일지 원문]
{text_truncated}

JSON만 출력:"""
        
        try:
            response = client.generate(
                model=LLM_MODEL,
                prompt=user_prompt,
                system=SYSTEM_PROMPT,
                options={'temperature': 0.05, 'num_predict': 300}
            )
            
            result_text = response['response'].strip()
            
            # JSON 추출
            result_text = re.sub(r'^```json\s*', '', result_text)
            result_text = re.sub(r'\s*```$', '', result_text)
            
            json_match = re.search(r'\{[\s\S]*?\}', result_text)
            if json_match:
                result_text = json_match.group()
            
            parsed = json.loads(result_text)
            
            # ========== 검증 및 보정 ==========
            
            # 1) type_code 검증
            if parsed.get('type_code') not in TYPE_CODES:
                parsed['type_code'] = '특이사항없음'
            
            # 2) type_detail 검증 (type_code에 맞는 값인지)
            type_code = parsed['type_code']
            type_detail = parsed.get('type_detail', '')
            
            valid_details = TYPE_DETAILS.get(type_code, [])
            
            if type_detail not in valid_details:
                # 유사 매칭 시도
                matched = False
                for valid in valid_details:
                    if valid in type_detail or type_detail in valid:
                        parsed['type_detail'] = valid
                        matched = True
                        break
                
                if not matched:
                    # 기본값 할당
                    if type_code == '식자재이슈':
                        parsed['type_detail'] = '기타 식자재'
                    elif type_code == '매출컨설팅':
                        parsed['type_detail'] = '기타 매출'
                    elif type_code == '운영이슈':
                        parsed['type_detail'] = '기타 운영'
                    else:
                        parsed['type_detail'] = '특이사항 없음'
            
            # 3) 필수 키 보정
            for key in ['type_code', 'type_detail', 'summary', 'advice']:
                if key not in parsed or not parsed[key]:
                    parsed[key] = default_result[key]
            
            return parsed
        
        except Exception as e:
            print(f"⚠️ LLM 분석 실패: {e}")
            return default_result
    
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
    
    print(f"🤖 LLM 분석 시작: {len(df_work)}건")
    start_time = time.time()
    
    results = []
    for idx, text in enumerate(df_work['content_clean'].tolist(), 1):
        result = analyze_with_llm(text)
        results.append(result)
        
        if idx % 5 == 0 or idx == len(df_work):
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            print(f"  [{idx:4d}/{len(df_work):4d}] {rate:5.1f}건/초", flush=True)
    
    print(f"✅ LLM 분석 완료 ({(time.time()-start_time)/60:.2f}분)")
    
    # ============================================
    # 결과 병합
    # ============================================
    df_llm = pd.DataFrame(results)
    df_result = pd.concat([df_work.reset_index(drop=True), df_llm], axis=1)
    
    if df_result.columns.duplicated().any():
        df_result = df_result.loc[:, ~df_result.columns.duplicated(keep='first')]
    
    # 날짜 정렬 (NaT는 끝에)
    if 'visit_date' in df_result.columns:
        df_result['visit_date_valid'] = df_result['visit_date'].notna()
        df_result = df_result.sort_values(
            ['visit_date_valid', 'visit_date'], 
            ascending=[False, False]
        ).drop('visit_date_valid', axis=1).reset_index(drop=True)
    
    # 통계 출력
    print(f"\n📊 분석 결과:")
    print(f"   type_code 분포:")
    if 'type_code' in df_result.columns and len(df_result) > 0:
        for code, count in df_result['type_code'].value_counts().items():
            print(f"     {code}: {count}건")
    else:
        print("     (데이터 없음)")
    
    print(f"\n✅ 최종 처리 완료: {len(df_result)}행 x {len(df_result.columns)}열")
    
    return df_result

def preprocess_llm_df(input_xcom_key, output_xcom_key, **context):
    """ 처리"""
    return preprocess_df(
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=transform_llm_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )


def append_to_master(**context):
    """LLM 처리된 데이터를 마스터 파일에 중복 검사 후 append"""
    ti = context['task_instance']

    DEFAULT_MASTER_COLUMNS = [
        'visit_date', 'post_date', 'project', 'project_clean',
        'title', 'author', 'type_code', 'type_detail', 'summary', 'advice'
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
    
    # 5. 중복 검사 - post_date + title + author 기준
    if len(master_df) > 0:
        # 공통 컬럼 찾기
        common_cols = []
        for col in ['post_date', 'title', 'author']:
            if col in master_df.columns and col in new_data.columns:
                common_cols.append(col)
        
        if not common_cols:
            # post_date + author 시도
            if 'post_date' in master_df.columns and 'post_date' in new_data.columns:
                if 'author' in master_df.columns and 'author' in new_data.columns:
                    common_cols = ['post_date', 'author']
        
        if common_cols:
            print(f"[INFO] 중복 검사 기준 컬럼: {common_cols}")
            
            # 중복 키 생성
            master_df['_dedup_key'] = master_df[common_cols].astype(str).apply('|'.join, axis=1)
            new_data['_dedup_key'] = new_data[common_cols].astype(str).apply('|'.join, axis=1)
            
            # 중복 제거
            before_count = len(new_data)
            new_data = new_data[~new_data['_dedup_key'].isin(master_df['_dedup_key'])]
            duplicate_count = before_count - len(new_data)
            
            # _dedup_key 컬럼 제거
            master_df = master_df.drop(columns=['_dedup_key'])
            new_data = new_data.drop(columns=['_dedup_key'])
            
            print(f"[INFO] 중복 제거: {duplicate_count}행, 추가할 데이터: {len(new_data)}행")
        else:
            print(f"[INFO] 중복 검사 불가능 - 전체 추가: {len(new_data)}행")
    else:
        print(f"[INFO] 마스터가 비어있음 - 전체 추가: {len(new_data)}행")
    
    # 6. Append
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
        
        print(f"[SUCCESS] 마스터 업데이트 완료: {len(master_df)}행 → {len(updated_master)}행 (+{len(new_data)}행)")
        
        # 통계 출력
        if 'type_code' in updated_master.columns:
            print(f"\n[INFO] type_code 분포:")
            for code, count in updated_master['type_code'].value_counts().items():
                print(f"  {code}: {count}건")
        
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