"""
Relay FMS 매장CS 데이터 수집 → 전처리 → LLM 분류 → 구글시트 적재 DAG

📋 처리 흐름:
1. [최초 1회] 기존 엑셀 파일 glob → 전처리 → LLM 분류 → 구글시트 append
2. Relay FMS 로그인 → 매장CS 엑셀 다운로드 (Selenium)
3. 전처리 → LLM 분류 → 구글시트 append 적재
4. 작업 완료 후 파일 삭제

🆕 v3.0 변경사항:
- LLM 프롬프트 전면 재설계: 2단계 판단 (품질 여부 → 세부 분류)
- issue_type 확장: 부자재 불량, 비품질 문의 추가
- 오분류 방지: 판단 기준표 + Few-shot 예시 + 금지 규칙 명시
- issue_summary, issue_type, severity, action_hint 컬럼 생성
"""

import pendulum
import pandas as pd
import numpy as np
import os
import glob
import json
import re
import time
import datetime as dt
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.transform.utility.paths import DOWN_DIR
from modules.transform.utility.io import SMP_FDAM_CS_TIME
from modules.load.load_gsheet import save_to_gsheet

filename = os.path.basename(__file__)

# ============================================================
# ✅ 경로 / 접속정보 / 시트 설정
# ============================================================
DOWNLOAD_DIR        = str(DOWN_DIR)
CREDENTIALS_PATH    = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
FALLBACK_CREDENTIALS_PATH = "/opt/airflow/config/glowing-palace-465904-h6-7f82df929812.json"
CS_GSHEET_URL       = "https://docs.google.com/spreadsheets/d/1DHbZX5jDkiZfe0SPr3eRinPXuSFomYm2AN-gMADoBkI/edit?gid=1488056813#gid=1488056813"
CS_SHEET_NAME       = "시트1"
RELAY_USER_ID       = "조민준"
RELAY_PASSWORD      = "1234"
RELAY_SERVER_NAME   = "도리당"
RELAY_SERVER_NUMBER = "10625"

# ============================================================
# LLM 설정
# ============================================================
LLM_MODEL_CANDIDATES = ['qwen2.5:7b', 'qwen2.5:latest', 'qwen2.5', 'gemma2:2b']
OLLAMA_HOST = 'http://host.docker.internal:11434'

# ============================================================
# 이메일 알림 설정
# ============================================================
CS_ALERT_EMAIL_CONN_ID = 'doridang_conn_smtp_gmail'
CS_ALERT_OVERDUE_DAYS  = 7        # 처리소요기간 기준 (일)

# 비공개메모 규격 필수 키 (모두 존재 + 값 비어있지 않아야 유효)
MEMO_REQUIRED_KEYS = ['업체:', '이슈유형:', '완료예정일:', '지연사유:', '현재상태:', '메모:']

# ── 운영 수신자 ──────────────────────────────────────────────
# 보낼 이메일 (TO): 담당자 이메일 외 항상 포함할 수신자 목록
CS_ALERT_TO_EMAILS  = ["simjeong01@kakao.com", "simjeong00@kakao.com"]

# 참조 이메일 (CC): 항상 참조로 추가 — 없으면 빈 리스트
CS_ALERT_CC_EMAILS  = ['bulu1017@kakao.com', 'a17019@kakao.com', 'siw22222@kakao.com']
# siw22222@kakao.com 대표님 이메일

# ── 테스트 설정 ──────────────────────────────────────────────
# True  → 아래 테스트 주소로만 발송 (담당자·CC 무시)
# False → 운영 모드: 담당자 + TO_EMAILS 에 발송, CC_EMAILS 참조
CS_ALERT_TEST_MODE  = False
CS_ALERT_TEST_EMAIL = 'a17019@kakao.com'   # 테스트 수신 주소

# ============================================================
# 매장 → 매입처 매핑
# ============================================================
STORE_SUPPLIER_MAP = {
    "도리당 백석점": "선경cs", "도리당 송파점": "우전", "도리당 교대점": "우전",
    "도리당 강동점": "우전", "도리당 법흥리점": "선경cs", "도리당 삼송점": "선경cs",
    "도리당 미사점": "우전", "도리당 용현점": "우전", "도리당 역삼점": "우전",
    "도리당 의정부점": "우전", "도리당 가락점": "선경cs", "도리당 초월점": "선경cs",
    "도리당 노원점": "우전", "도리당 서울대점": "우전", "도리당 대전둔산점": "올랑푸드",
    "도리당 청라점": "선경cs", "도리당 김포장기점": "우전", "도리당 응암점": "선경cs",
    "도리당 부천옥길점": "선경cs", "도리당 대전용전점": "선경cs", "도리당 하늘도시점": "선경cs",
    "도리당 천안성정점": "선경cs", "도리당 행신점": "선경cs", "도리당 시흥장현점": "선경cs",
    "도리당 상도점": "선경cs", "도리당 평택비전점": "선경cs", "도리당 부천심곡점": "선경cs",
    "도리당 부산장림점": "올랑푸드", "도리당 구로디지털단지점": "우전", "도리당 동탄영천점": "선경cs",
    "도리당 광주경안점": "선경cs", "도리당 양주옥정점": "우전", "도리당 안산중앙점": "선경cs",
    "도리당 구리다산점": "우전", "도리당 평택서정점": "선경cs", "도리당 성신여대점": "우전",
    "도리당 부산재송센텀점": "올랑푸드", "도리당 화성봉담점": "선경cs", "도리당 김포풍무점": "우전",
    "도리당 수유점": "우전", "도리당 대화점": "선경cs", "도리당 광주태전점": "선경cs",
    "도리당 수원매탄점": "선경cs", "도리당 대전장대점": "선경cs", "도리당 광주상무점": "태성그린푸드",
    "도리당 공덕역점": "우전", "도리당 충주역점": "선경cs", "도리당 대전중구점": "올랑푸드",
    "도리당 인천간석점": "태성그린푸드", "도리당 인천작전점": "태성그린푸드", "도리당 익산영등점": "태성그린푸드",
    "도리당 중랑점": "우전", "도리당 부산대신점": "올랑푸드", "도리당 파주운정점": "선경cs",
    "도리당 기흥테라타워점": "선경cs", "도리당 연신내점": "선경cs", "도리당 부산광안점": "올랑푸드",
    "도리당 세종나성점": "올랑푸드", "도리당 동두천지행점": "우전", "도리당 경북상주점": "올랑푸드",
    "도리당 창원내서점": "올랑푸드", "도리당 용인동천점": "선경cs", "도리당 대전관저점": "올랑푸드",
    "도리당 양천목동점": "우전", "도리당 광명철산점": "선경cs", "도리당 김해인제대점": "올랑푸드",
    "도리당 오산시청점": "선경cs", "도리당 대전관평점": "올랑푸드",
}

# ============================================================
# LLM 분류 허용값 (검증용)
# ============================================================
VALID_ISSUE_TYPES = [
    '순살 뼈 혼입', '계육 이물질', '중량 미달', '절단 불량',
    '지방 과다', '변색/이취', '포장 불량', '소스 불량',
    '부자재 불량', '배송 오류', '비품질 문의', '기타'
]


# ============================================================
# LLM 분류 프롬프트 v3.0
# ============================================================
LLM_SYSTEM_PROMPT = """너는 닭도리탕 프랜차이즈 "도리당" 본사 품질관리팀 시니어 매니저다.
가맹점에서 접수된 CS 원문 1건을 읽고, 본사 내부 보고용 분류표를 작성한다.

────────────────────────
[출력 규칙] ★절대 준수★
────────────────────────
반드시 JSON 하나만 출력한다.
JSON 외 설명, 문장, 코드블록 절대 출력하지 않는다.

{
  "issue_summary": "...",
  "issue_type": "..."
}

────────────────────────
[1단계] 품질 CS인지 먼저 판단
────────────────────────
아래에 해당하면 "비품질 문의"로 분류한다:
- 계약 해지, 환불, 환급, 보증금
- 발주 시스템 오류, 버튼 안 눌림
- 단순 배송 일정 문의 (품질 불량 아닌 것)
- 가격, 결제, 정산 관련
- 매장 시설, 인테리어
- 기타 행정/운영 문의

위에 해당하면 → issue_type = "비품질 문의", severity = 3 으로 즉시 분류.
아래 2단계로 넘어가지 않는다.

────────────────────────
[2단계] 품질 CS → 대상 품목 식별
────────────────────────
원문에서 문제가 발생한 품목을 먼저 파악한다.

<품목 분류 기준>

[계육류] = 순살, 뼈닭, 닭다리, 닭날개, 가슴살, 윙, 봉
→ 이 품목에서 발생한 문제만 계육 관련 issue_type 사용

[소스류] = 소중대소스, 양념소스, 간장소스, 매운소스
→ 소스 자체의 맛/품질 문제일 때만 "소스 불량"

[부자재류] = 김치, 치킨무, 용기, 수저, 봉투, 일회용품, 포장재
→ 계육/소스가 아닌 모든 식자재·비품 → "부자재 불량"

────────────────────────
[2단계] 품질 CS → issue_type 선택
────────────────────────
반드시 아래 12개 중 하나만 선택한다.

순살 뼈 혼입
  → 순살 제품에서 뼈/뼈조각/날카로운 뼈 발견
  → ★ 반드시 "순살"에서 "뼈"가 나온 경우만 해당
  → 뼈닭 제품 문제는 여기 해당 안 됨

계육 이물질
  → 계육(순살/뼈닭)에서 머리카락, 털, 이물질, 벌레 발견
  → ★ 치킨무/김치 등 부자재에서 나온 이물질은 "부자재 불량"

중량 미달
  → 계육 무게가 기준보다 부족 (g 수치 언급, 중량 미달 명시)
  → ★ 물빠짐/실링 불량으로 인한 것은 "포장 불량"

절단 불량
  → 조각수 이상, 부위 구성 오류, 가슴살/윙/봉 비율 문제
  → 계육이 분리 안 됨, 절각 불량
  → 예: "가슴살 3개 윙봉 3개" = 조각 구성 문제 → 절단 불량

지방 과다
  → 계육에 지방/기름이 과도하게 붙어있음

변색/이취
  → 계육 피멍, 변색, 냄새, 부패 징후

포장 불량
  → 실링 파손, 포장 찢김, 물빠짐, 진공 불량
  → ★ 소스 용기 터짐/찢김도 여기 해당
  → ★ 물빠짐 = 실링 불량의 결과 → "포장 불량"

소스 불량
  → 소스 자체의 맛이 이상, 맵기 편차, 풍미 변질
  → ★ 소스 용기가 터지거나 찢어진 것은 "포장 불량"
  → ★ 김치 양념 문제는 "부자재 불량"

부자재 불량
  → 김치(양념 부족, 흐물거림, 이물질), 치킨무(이물질, 변질)
  → 용기 불량, 수저/봉투 문제
  → ★ 계육/소스가 아닌 모든 품목의 품질 문제

배송 오류
  → 오배송, 미배송, 수량 착오, 잘못된 품목 배송
  → 예: "뼈닭 박스에 순살이 담겨옴" = 배송/포장 착오 → 배송 오류

비품질 문의
  → 1단계에서 걸러진 비품질 건

기타
  → 위 어디에도 해당하지 않는 경우

────────────────────────
[severity 기준]
────────────────────────
1 = 즉시 조치 필요
  고객 사고 위험 (날카로운 뼈, 이물질 섭취)
  반복 접수 명시 ("또", "계속", "반복", "이전에도")
  위생 심각 (벌레, 부패)

2 = 금주 내 확인
  품질 불량 확인되나 단발성
  고객 클레임 접수됨
  보상/교환 필요한 수준

3 = 모니터링
  경미한 불량 (경미한 중량 차이 등)
  단순 요청/문의
  비품질 문의

★ 판단 팁:
"또", "계속", "반복", "이전에도", "저번부터" → severity 1 이상
고객이 직접 항의/컴플레인 언급 → severity 1~2
단순 보고/요청 → severity 2~3

────────────────────────
[issue_summary 규칙]
────────────────────────
1문장, 15자~40자.
반드시 [품목] + [문제현상]을 포함한다.
예시:
  "순살 계육에서 뼈 혼입 반복 발생"
  "뼈닭 실링 불량으로 물빠짐 발생"
  "치킨무에서 이물질(곤충 형태) 발견"
  "동성김치 식감 불량(흐물거림)"
  "가맹계약 해지 및 보증금 환급 요청"

────────────────────────
[action_hint 규칙]
────────────────────────
본사 품질관리팀 관점의 다음 조치 1문장.
예시:
  "해당 LOT 전수 점검 및 생산라인 확인 필요"
  "협력업체 클레임 접수 및 재발 방지 요청"
  "부자재 업체(동원 등) 품질 개선 요청"
  "해당 건 영업팀 이관 처리"

────────────────────────
[금지 규칙] ★반드시 지켜라★
────────────────────────
1. 계육이 아닌 품목(김치, 치킨무)의 문제를 "계육 이물질"로 분류 금지
2. 소스 용기 파손을 "소스 불량"으로 분류 금지 → "포장 불량"
3. 물빠짐/실링 불량을 "중량 미달"로 분류 금지 → "포장 불량"
4. 품질과 무관한 건(계약, 환불, 시스템)을 품질 유형으로 분류 금지
5. 뼈닭 박스↔순살 박스 바뀜을 "순살 뼈 혼입"으로 분류 금지 → "배송 오류"
6. 조각 구성 문제(가슴살/윙/봉 비율)를 "순살 뼈 혼입"으로 분류 금지 → "절단 불량"
7. 김치 양념 문제를 "소스 불량"으로 분류 금지 → "부자재 불량"

────────────────────────
[예시] ★참고용★
────────────────────────

원문: "순살에서 뼈가나옴"
→ {"issue_summary":"순살 계육에서 뼈 혼입 발생","issue_type":"순살 뼈 혼입","severity":2,"action_hint":"해당 생산 LOT 전수 점검 필요"}

원문: "순살에서 저번부터 작은뼈가 계속해서 나옵니다"
→ {"issue_summary":"순살 계육 뼈 혼입 반복 발생","issue_type":"순살 뼈 혼입","severity":1,"action_hint":"해당 협력업체 생산라인 긴급 점검 및 개선 경고"}

원문: "머리카락 또 나왔습니다"
→ {"issue_summary":"계육에서 머리카락 반복 혼입","issue_type":"계육 이물질","severity":1,"action_hint":"협력업체 위생관리 긴급 점검 및 클레임 접수"}

원문: "소중대소스 맛이 아예 이상합니다"
→ {"issue_summary":"소중대소스 맛 이상 발생","issue_type":"소스 불량","severity":2,"action_hint":"해당 제조 배치 맛 테스트 및 원인 파악 필요"}

원문: "소스가 터져서왔습니다"
→ {"issue_summary":"소스 용기 파손 상태로 입고","issue_type":"포장 불량","severity":3,"action_hint":"포장 공정 점검 및 운송 과정 확인 필요"}

원문: "뼈닭 박스에 순살담겨있고 순살 박스에 뼈닭담겨서옴"
→ {"issue_summary":"뼈닭/순살 박스 교차 오포장 발생","issue_type":"배송 오류","severity":2,"action_hint":"출고 검수 프로세스 점검 필요"}

원문: "치킨무에 무우에 바퀴벌레모양 있어요"
→ {"issue_summary":"치킨무에서 이물질(곤충 형태) 발견","issue_type":"부자재 불량","severity":2,"action_hint":"부자재 업체 클레임 접수 및 선별 공정 점검 요청"}

원문: "동성김치가 기존보다 너무 흐물거려 이용을 못하겠으며"
→ {"issue_summary":"동성김치 식감 불량(흐물거림) 발생","issue_type":"부자재 불량","severity":2,"action_hint":"김치 업체 품질 확인 및 클레임 접수"}

원문: "김치에 양념이 너무 안발려서 왔습니다"
→ {"issue_summary":"김치 양념 도포 불량","issue_type":"부자재 불량","severity":3,"action_hint":"김치 업체 제조 공정 확인 요청"}

원문: "1.22일 입고 뼈닭 물빠짐"
→ {"issue_summary":"뼈닭 실링 불량으로 물빠짐 발생","issue_type":"포장 불량","severity":2,"action_hint":"실링 설비 점검 및 협력업체 개선 요청"}

원문: "뼈닭 중량이 955나옵니다"
→ {"issue_summary":"뼈닭 중량 955g으로 미달 의심","issue_type":"중량 미달","severity":3,"action_hint":"중량 관리 기준 안내 및 모니터링"}

원문: "가슴살 3개 윙,봉 3개"
→ {"issue_summary":"순살 조각 구성 비율 이상","issue_type":"절단 불량","severity":3,"action_hint":"절각 작업 기준 준수 여부 확인 요청"}

원문: "가맹계약 해지를 하게 되어 입금 되어 있는 금액 환급 요청"
→ {"issue_summary":"가맹계약 해지 및 보증금 환급 요청","issue_type":"비품질 문의","severity":3,"action_hint":"해당 건 영업팀 이관 처리"}

원문: "돈을 넣어놓고 발주버튼을 못눌렀는데 방법이 없을까요?"
→ {"issue_summary":"발주 시스템 버튼 오류 문의","issue_type":"비품질 문의","severity":3,"action_hint":"발주 시스템 담당팀 확인 요청"}

원문: "수저.용기 입고가 너무 일정 하지 않아서 불편 합니다"
→ {"issue_summary":"용기/수저 입고 일정 불규칙","issue_type":"배송 오류","severity":3,"action_hint":"물류 배송 스케줄 확인 및 안내"}

원문: "1.22일 입고뼈닭 1마리 피멍"
→ {"issue_summary":"뼈닭 피멍 발견","issue_type":"변색/이취","severity":2,"action_hint":"해당 LOT 품질 확인 및 협력업체 개선 요청"}

원문: "14일자에받은 계육입니다 젓가락으로 분리하려니 안떨어진답니다"
→ {"issue_summary":"계육 부위 간 분리 불량","issue_type":"절단 불량","severity":2,"action_hint":"절각 작업 품질 점검 및 개선 요청"}"""


def _get_ollama_client():
    """Ollama 클라이언트 및 모델명 반환"""
    import ollama
    client = ollama.Client(host=OLLAMA_HOST)
    models = client.list()
    model_names = [m['model'] for m in models.get('models', [])]

    for candidate in LLM_MODEL_CANDIDATES:
        matching = [name for name in model_names if candidate in name]
        if matching:
            return client, matching[0]

    raise Exception(f"사용 가능한 Ollama 모델 없음. 설치된 모델: {model_names}")


def _get_default_llm_result():
    """LLM 분류 기본값 (분석 실패/내용 부족 시)"""
    return {
        'issue_summary': '내용 부족으로 분류 불가',
        'issue_type': '기타',
    }


def _analyze_cs_with_llm(client, model_name, text):
    """단일 CS 내용을 LLM으로 분석하여 분류 결과 반환"""
    default = _get_default_llm_result()

    if not text or len(str(text).strip()) < 5:
        return default

    text_clean = str(text).strip()[:1000]

    user_prompt = f"""다음은 가맹점에서 접수된 CS 원문이다.

내용:
{text_clean}

JSON만 출력."""

    try:
        response = client.generate(
            model=model_name,
            prompt=user_prompt,
            system=LLM_SYSTEM_PROMPT,
            options={'temperature': 0.05, 'num_predict': 300}
        )

        result_text = response['response'].strip()

        # JSON 추출
        result_text = re.sub(r'^```json\s*', '', result_text)
        result_text = re.sub(r'\s*```$', '', result_text)

        json_match = re.search(r'\{[\s\S]+\}', result_text)
        if json_match:
            result_text = json_match.group()

        parsed = json.loads(result_text)

        # ========== 검증 및 보정 ==========
        # issue_summary
        if not parsed.get('issue_summary') or not isinstance(parsed['issue_summary'], str):
            parsed['issue_summary'] = default['issue_summary']

        # issue_type 검증
        if parsed.get('issue_type') not in VALID_ISSUE_TYPES:
            matched = False
            raw_type = str(parsed.get('issue_type', ''))
            for valid in VALID_ISSUE_TYPES:
                if valid in raw_type or raw_type in valid:
                    parsed['issue_type'] = valid
                    matched = True
                    break
            if not matched:
                parsed['issue_type'] = '기타'

        return parsed

    except Exception as e:
        print(f"  ⚠️ LLM 파싱 실패: {e}")
        return default


def classify_cs_with_llm(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame의 '내용' 컬럼을 LLM으로 분석하여 4개 컬럼 추가:
    - issue_summary: 1문장 핵심 요약
    - issue_type: 이슈 유형 (표준 분류)
    - severity: 심각도 (1~3)
    - action_hint: 다음 조치 가이드
    """
    if '내용' not in df.columns or df.empty:
        print("  ⚠️ '내용' 컬럼 없거나 데이터 비어있음 → LLM 분류 스킵")
        for col in ['issue_summary', 'issue_type']:
            df[col] = ''
        return df

    print(f"\n🤖 LLM CS 분류 시작 v3.0 (총 {len(df)}건)")

    try:
        client, model_name = _get_ollama_client()
        print(f"  ✅ Ollama 연결 성공: {model_name}")
    except Exception as e:
        print(f"  ❌ Ollama 연결 실패: {e}")
        print(f"  → LLM 분류 없이 진행")
        for col in ['issue_summary', 'issue_type']:
            df[col] = ''
        return df

    results = []
    start_time = time.time()

    for idx, row in df.iterrows():
        text = row.get('내용', '')
        result = _analyze_cs_with_llm(client, model_name, text)
        results.append(result)

        i = len(results)
        if i % 5 == 0 or i == len(df):
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(df) - i) / rate if rate > 0 else 0
            print(f"\r  [{i:4d}/{len(df):4d}] {rate:5.2f}건/초 | 남은시간: {remaining/60:4.1f}분",
                  end="", flush=True)

    elapsed = time.time() - start_time
    print(f"\n  ✅ LLM 분류 완료! ({elapsed:.1f}초, {len(results)}건)")

    # 결과를 DataFrame에 추가
    result_df = pd.DataFrame(results)
    for col in ['issue_summary', 'issue_type']:
        if col in result_df.columns:
            df[col] = result_df[col].values
        else:
            df[col] = ''

    # 통계 출력
    if 'issue_type' in df.columns:
        print(f"\n  📊 issue_type 분포:")
        for itype, count in df['issue_type'].value_counts().items():
            print(f"    {itype}: {count}건")

    return df


# ============================================================
# 공통 전처리
# ============================================================
def preprocess_cs_df(df: pd.DataFrame) -> pd.DataFrame:
    target_cols = [
        '접수번호', '등록일', '등록시간', '회사구분', '매장명',
        'CS구분', '유형', '내용', '유입경로', '매입처',
        '접수자', '내용.1', '처리일자', '처리자', '진행상태', '비공개메모'
    ]
    available_cols = [c for c in target_cols if c in df.columns]
    missing_cols   = [c for c in target_cols if c not in df.columns]
    if missing_cols:
        print(f"  ⚠️ 없는 컬럼 (스킵): {missing_cols}")

    df = df[available_cols].copy()

    if '매장명' in df.columns:
        df['매입처'] = df['매장명'].map(STORE_SUPPLIER_MAP)
        unmapped = df[df['매입처'].isna()]['매장명'].dropna().unique().tolist()
        if unmapped:
            print(f"  ⚠️ 매핑 안된 매장 ({len(unmapped)}개): {unmapped[:5]}")

    for col in ['등록일', '처리일자']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace({'NaT': '', 'None': ''})

    df['수집일'] = dt.datetime.now().strftime('%Y-%m-%d')
    # ※ uploaded_at 은 save_to_gsheet 에서 자동 추가됨 → 여기서 추가하지 않음

    for col in df.select_dtypes(include=[np.floating]).columns:
        df.loc[np.isinf(df[col]), col] = None
    df = df.fillna('')

    # 🆕 LLM 분류 추가
    df = classify_cs_with_llm(df)

    # 🆕 sales_employee.csv 가닸다 LEFT JOIN → 담당자 컬럼 추가
    sales_emp_path = '/opt/airflow/Local_DB/영업관리부_DB/sales_employee.csv'
    try:
        emp_df = pd.read_csv(sales_emp_path, encoding='utf-8-sig')
        # 매장명 중복 시 첫 번째 값 사용
        emp_map = (
            emp_df.drop_duplicates(subset='매장명')[['매장명', '담당자']]
            .set_index('매장명')['담당자']
        )

        # ============================================================
        # ✏️ 매장명 정규화 매핑 (CS 데이터 ↔ sales_employee.csv 불일치 보정)
        # CS 데이터의 매장명이 sales_employee.csv와 다를 경우 여기에 추가
        # 형식: "CS에서 수신된 매장명": "sales_employee.csv의 정확한 매장명"
        # ============================================================
        STORE_NAME_NORMALIZE = {
            "도리당 파주운정점": "도리당 운정점",
            # "도리당 구매장명예시": "도리당 신매장명예시",  # 예시: 추가 시 이 형식으로
        }
        # ============================================================

        df['_매장명_조인키'] = df['매장명'].replace(STORE_NAME_NORMALIZE)
        df['담당자'] = df['_매장명_조인키'].map(emp_map).fillna('')
        df.drop(columns=['_매장명_조인키'], inplace=True)

        mapped_count = (df['담당자'] != '').sum()
        print(f"  ✅ 담당자 JOIN 완료: {mapped_count}건 매핑")
        if mapped_count < len(df):
            unmapped = df[df['담당자'] == '']['매장명'].unique().tolist()
            print(f"  ⚠️ 매핑 안된 매장 ({len(unmapped)}개): {unmapped[:10]}")
    except Exception as e:
        print(f"  ⚠️ sales_employee.csv 로드 실패 → 담당자 컬럼 공백: {e}")
        df['담당자'] = ''

    # 🆕 처리완료율 계산 (현재 배치 전체 기준)
    # = 진행상태가 '처리중(처리내용 노출)'인 건수 / 전체 문의 건수
    if '진행상태' in df.columns:
        total = len(df)
        done  = (df['진행상태'] == '처리중(처리내용 노출)').sum()
        df['처리완료율'] = round(done / total, 4) if total > 0 else 0.0
        print(f"  ✅ 처리완료율 계산 완료: {done}/{total} = {done/total:.4f}")
    else:
        df['처리완료율'] = ''

    # 🆕 응답소요일 계산 (비공개메모 최초 입력일 기준)
    # = 접수번호별 비공개메모가 처음 등장한 수집일 - 등록일 (일수)
    # ※ 비공개메모 없는 행은 공백, 있는 행은 최초 응답일 기준으로 고정
    required = {'비공개메모', '등록일', '수집일', '접수번호'}
    if required.issubset(df.columns):
        has_memo = df['비공개메모'].apply(lambda x: pd.notna(x) and str(x).strip() != '')

        # 접수번호별 첫 비공개메모 수집일
        first_memo_date = df[has_memo].groupby('접수번호')['수집일'].min()
        df['_first_memo'] = df['접수번호'].map(first_memo_date)

        def _response_days(row) -> str:
            if not (pd.notna(row.get('비공개메모')) and str(row.get('비공개메모', '')).strip() != ''):
                return ''
            try:
                reg  = pd.to_datetime(row['등록일'], errors='coerce')
                first = pd.to_datetime(row['_first_memo'], errors='coerce')
                if pd.isna(reg) or pd.isna(first):
                    return ''
                return (first - reg).days
            except Exception:
                return ''

        df['응답소요일'] = df.apply(_response_days, axis=1)
        df.drop(columns=['_first_memo'], inplace=True, errors='ignore')
        answered = (df['응답소요일'] != '').sum()
        print(f"  ✅ 응답소요일 계산 완료: {answered}건 응답")
    else:
        df['응답소요일'] = ''

    return df


def _extract_service_account_email(credentials_path: str) -> str:
    try:
        with open(credentials_path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data.get("client_email", "unknown")
    except Exception:
        return "unknown"


def _credential_candidates() -> list[str]:
    candidates = []

    env_cred = os.getenv("SMP_FDAM_CS_GSHEET_CREDENTIALS")
    if env_cred:
        candidates.append(env_cred)

    candidates.extend([CREDENTIALS_PATH, FALLBACK_CREDENTIALS_PATH])

    deduped = []
    for path in candidates:
        if path and path not in deduped and os.path.exists(path):
            deduped.append(path)
    return deduped


def upload_df_to_gsheet(df: pd.DataFrame, label: str = "") -> str:
    print(f"  [구글시트] {label} append 시작: {len(df)}건")

    candidates = _credential_candidates()
    if not candidates:
        raise RuntimeError("구글시트 업로드 실패: 사용 가능한 서비스 계정 JSON 파일이 없습니다.")

    last_error = None
    last_cred = None

    for cred_path in candidates:
        sa_email = _extract_service_account_email(cred_path)
        print(f"  [구글시트] 인증 시도: {sa_email} ({cred_path})")

        result = save_to_gsheet(
            df=df,
            sheet_name=CS_SHEET_NAME,
            mode="append",
            credentials_path=cred_path,
            url=CS_GSHEET_URL,
        )

        if result.get('success'):
            print(f"  [구글시트] ✅ {label} append 완료: {len(df)}건")
            return f"✅ {label} {len(df)}건"

        last_error = result.get('error')
        last_cred = cred_path
        print(f"  [구글시트] 인증 실패: {sa_email} → {last_error}")

        error_text = str(last_error).upper()
        is_permission_error = (
            "403" in error_text or
            "PERMISSION_DENIED" in error_text or
            "DOES NOT HAVE PERMISSION" in error_text
        )

        if not is_permission_error:
            break

    failed_email = _extract_service_account_email(last_cred) if last_cred else "unknown"
    raise RuntimeError(
        "구글시트 업로드 실패 "
        f"({label}): {last_error} | "
        f"확인 필요: 스프레드시트에 서비스계정 편집 권한 공유 ({failed_email})"
    )


# ============================================================
# Task 1: 기존 파일 glob → append
# ============================================================
def load_existing_files(**context):
    print(f"\n{'='*60}")
    print(f"[기존파일 적재] DOWNLOAD_DIR: {DOWNLOAD_DIR}")

    existing_files = sorted(
        glob.glob(os.path.join(DOWNLOAD_DIR, "매장CS*.xlsx")) +
        glob.glob(os.path.join(DOWNLOAD_DIR, "매장CS*.xls")),
        key=os.path.getmtime
    )

    if not existing_files:
        print(f"[기존파일 적재] 기존 파일 없음 → 스킵")
        context['ti'].xcom_push(key='existing_files', value=[])
        return "기존 파일 없음"

    print(f"[기존파일 적재] 발견 {len(existing_files)}개:")
    for f in existing_files:
        print(f"  - {os.path.basename(f)}")

    processed_files, failed_files, total_rows = [], [], 0

    for file_path in existing_files:
        try:
            print(f"\n  📂 처리 중: {os.path.basename(file_path)}")
            df_raw = pd.read_excel(file_path)
            print(f"  원본: {len(df_raw)}건")
            df = preprocess_cs_df(df_raw)
            upload_df_to_gsheet(df, label=os.path.basename(file_path))
            processed_files.append(file_path)
            total_rows += len(df)
        except Exception as e:
            import traceback
            print(f"  ❌ 실패 ({os.path.basename(file_path)}): {e}")
            print(traceback.format_exc())
            failed_files.append(file_path)

    print(f"\n[기존파일 적재] ✅ {len(processed_files)}개 / 총 {total_rows}건")
    if failed_files and not processed_files:
        raise RuntimeError(f"기존 파일 업로드 전체 실패: {len(failed_files)}개")

    context['ti'].xcom_push(key='existing_files', value=processed_files)
    return f"✅ {len(processed_files)}개 / {total_rows}건"


# ============================================================
# Task 2: Relay FMS 신규 파일 다운로드
# ============================================================
def download_cs_excel(**context):
    from modules.extract.crawling_relay_cs_fdam import run_relay_cs_crawling

    print(f"\n{'='*60}")
    print(f"[다운로드] Relay FMS 매장CS 엑셀 다운로드 시작")
    print(f"[다운로드] DOWNLOAD_DIR: {DOWNLOAD_DIR}")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    file_path = run_relay_cs_crawling(
        user_id=RELAY_USER_ID,
        password=RELAY_PASSWORD,
        headless=True,
        server_name=RELAY_SERVER_NAME,
        server_number=RELAY_SERVER_NUMBER,
        download_dir=DOWNLOAD_DIR,
    )

    print(f"[다운로드] ✅ 완료: {file_path}")
    context['ti'].xcom_push(key='new_cs_file', value=file_path)
    return file_path


# ============================================================
# Task 3: 전처리 + LLM 분류 + 구글시트 append
# ============================================================
def _recalc_completion_rate(df_new: pd.DataFrame, df_existing: pd.DataFrame) -> pd.DataFrame:
    """
    구글시트 기존 데이터(df_existing) + 신규 데이터(df_new)를 합산하여
    처리완료율 / 응답소요일을 재계산한다.

    처리완료율 = (수집일별) 처리중(처리내용 노출) 건수 / (수집일별) 전체 건수  [0.0~1.0]
    응답소요일 = 접수번호별 비공개메모 최초 수집일 - 등록일  (예: "5일", 없으면 공백)
    """
    needed_cols = ['진행상태', '수집일', '비공개메모', '접수번호', '등록일']
    df_new_calc = df_new.copy()

    if '수집일' not in df_new_calc.columns:
        df_new_calc['수집일'] = dt.datetime.now().strftime('%Y-%m-%d')

    # 기존 데이터에 수집일이 없으면 uploaded_at(YYYY-MM-DD HH:MM:SS)에서 날짜 추출해 보정
    if df_existing is not None and not df_existing.empty:
        df_existing_calc = df_existing.copy()
        if '수집일' not in df_existing_calc.columns:
            if 'uploaded_at' in df_existing_calc.columns:
                df_existing_calc['수집일'] = pd.to_datetime(
                    df_existing_calc['uploaded_at'], errors='coerce'
                ).dt.strftime('%Y-%m-%d').fillna('')
            else:
                df_existing_calc['수집일'] = ''
    else:
        df_existing_calc = pd.DataFrame(columns=needed_cols)

    for col in needed_cols:
        if col not in df_existing_calc.columns:
            df_existing_calc[col] = ''
        if col not in df_new_calc.columns:
            df_new_calc[col] = ''

    base = pd.concat(
        [df_existing_calc[needed_cols], df_new_calc[needed_cols]],
        ignore_index=True
    )

    # ── 처리완료율 ────────────────────────────────────────────
    done_mask = base['진행상태'] == '처리중(처리내용 노출)'
    daily_total = base.groupby('수집일', dropna=False)['진행상태'].count()
    daily_done = base[done_mask].groupby('수집일', dropna=False)['진행상태'].count()
    daily_rate = (daily_done / daily_total).fillna(0).round(4)

    df_new_calc['처리완료율'] = df_new_calc['수집일'].map(daily_rate).fillna(0.0)

    # ── 응답소요일 (접수번호별 비공개메모 최초 등장일 - 등록일) ──
    has_memo_mask = base['비공개메모'].apply(lambda x: pd.notna(x) and str(x).strip() != '')
    # 접수번호별 비공개메모 최초 수집일 (기존+신규 합산 기준으로 정확한 첫 응답일 확정)
    first_memo_date = (
        base[has_memo_mask]
        .groupby('접수번호', dropna=False)['수집일']
        .min()
    )
    # 접수번호별 등록일 (첫 번째 값 사용)
    reg_date_map = base.groupby('접수번호', dropna=False)['등록일'].first()

    def _response_days_recalc(row) -> str:
        if not (pd.notna(row.get('비공개메모')) and str(row.get('비공개메모', '')).strip() != ''):
            return ''
        try:
            접수번호 = row['접수번호']
            first = pd.to_datetime(first_memo_date.get(접수번호), errors='coerce')
            reg   = pd.to_datetime(reg_date_map.get(접수번호), errors='coerce')
            if pd.isna(first) or pd.isna(reg):
                return ''
            return (first - reg).days
        except Exception:
            return ''

    df_new_calc['응답소요일'] = df_new_calc.apply(_response_days_recalc, axis=1)

    unique_days = df_new_calc['수집일'].dropna().astype(str).unique().tolist()
    answered = (df_new_calc['응답소요일'] != '').sum()
    print(f"  📊 수집일 기준 재계산 완료: {len(unique_days)}일")
    print(f"  📊 응답소요일: {answered}건 산출 (기존+신규 합산 기준)")
    for day in unique_days[:5]:
        total = int(daily_total.get(day, 0))
        done = int(daily_done.get(day, 0))
        comp_rate = float(daily_rate.get(day, 0.0))
        print(f"    - {day}: 처리완료율 {done}/{total}={comp_rate:.4f}")

    return df_new_calc


def transform_and_upload(**context):
    import gspread
    from google.oauth2.service_account import Credentials

    ti         = context['ti']
    excel_path = ti.xcom_pull(task_ids='download_cs_excel', key='new_cs_file')

    print(f"\n{'='*60}")
    print(f"[전처리/LLM분류/업로드] 파일: {excel_path}")

    if not excel_path or not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")

    df_raw = pd.read_excel(excel_path)
    print(f"[전처리/업로드] 원본: {len(df_raw)}건")
    df = preprocess_cs_df(df_raw)

    if df.empty:
        print("[전처리/업로드] 업로드할 데이터 없음")
        context['ti'].xcom_push(key='uploaded_new_file', value=excel_path)
        return "업로드 건 없음"

    # ✅ 구글시트 기존 데이터 조회 → 처리완료율 전체 기준으로 재계산
    print("[처리완료율] 구글시트 기존 데이터 조회 중...")
    df_existing = None
    try:
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        candidates = _credential_candidates()
        if not candidates:
            raise RuntimeError("사용 가능한 서비스 계정 없음")
        creds = Credentials.from_service_account_file(candidates[0], scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(CS_GSHEET_URL)
        ws = sh.worksheet(CS_SHEET_NAME)
        df_existing = pd.DataFrame(ws.get_all_records())
        print(f"[처리완료율] 기존 데이터 {len(df_existing)}건 로드 완료")
    except Exception as e:
        print(f"[처리완료율] 기존 데이터 조회 실패 → 현재 배치 기준으로만 계산: {e}")

    df = _recalc_completion_rate(df, df_existing)

    result = upload_df_to_gsheet(df, label="신규다운로드")
    context['ti'].xcom_push(key='uploaded_new_file', value=excel_path)
    print(f"[전처리/업로드] ✅ 완료")
    return result


# ============================================================
# Task 4: 처리 지연 CS 이메일 알림
# ============================================================
def _build_overdue_email_html(df: pd.DataFrame, today_str: str) -> str:
    """처리 지연 CS 알림 HTML 이메일 본문 생성"""
    from html import escape

    row_html = ''
    for _, row in df.sort_values('처리소요기간', ascending=False).iterrows():
        days = row.get('처리소요기간', '')
        try:
            days_int = int(days)
            days_color = '#e74c3c' if days_int >= 14 else '#f39c12'
            days_text = f'<span style="color:{days_color};font-weight:bold;">{days_int}일</span>'
        except (ValueError, TypeError):
            days_text = str(days)

        issue_summary = str(row.get('issue_summary', '') or '')[:50]

        memo_raw_html = str(row.get('비공개메모', '') or '').replace('\r\n', '\n').replace('\r', '\n')
        memo_escaped = escape(memo_raw_html).replace('\n', '<br>')
        private_memo = memo_escaped if memo_escaped.strip() else '<span style="color:#aaa;">(메모 없음)</span>'

        memo_raw  = str(row.get('비공개메모', '') or '')
        due_match = re.search(r'완료예정일[:\s]*([0-9]{4}-[0-9]{2}-[0-9]{2})', memo_raw)
        if due_match:
            due_date_str  = due_match.group(1)
            due_date_cell = due_date_str
            try:
                reg_dt   = dt.date.fromisoformat(str(row.get('등록일', '')))
                due_dt   = dt.date.fromisoformat(due_date_str)
                today_dt = dt.date.fromisoformat(today_str)
                total    = (due_dt - reg_dt).days
                elapsed  = (today_dt - reg_dt).days
                if total > 0:
                    pct = round(elapsed / total * 100)
                    if pct >= 100:
                        progress_cell = f'<span style="color:#e74c3c;font-weight:bold;">{pct}% 초과</span>'
                    elif pct >= 80:
                        progress_cell = f'<span style="color:#e67e22;font-weight:bold;">{pct}%</span>'
                    elif pct >= 50:
                        progress_cell = f'<span style="color:#f39c12;">{pct}%</span>'
                    else:
                        progress_cell = f'<span style="color:#27ae60;">{pct}%</span>'
                else:
                    progress_cell = '<span style="color:#aaa;">-</span>'
            except Exception:
                progress_cell = '<span style="color:#aaa;">-</span>'
        else:
            due_date_cell = '<span style="color:#aaa;">미작성</span>'
            progress_cell = '<span style="color:#aaa;">-</span>'

        row_html += f"""
                                <tr style="border-bottom: 1px solid #eee;">
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{row.get('접수번호', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{row.get('등록일', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{due_date_cell}</td>
                                    <td style="padding: 8px; font-size: 12px; text-align:center; white-space:nowrap;">{progress_cell}</td>
                                    <td style="padding: 8px; text-align:center; white-space:nowrap;">{days_text}</td>
                                    <td style="padding: 8px; font-size: 12px;">{row.get('매장명', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{row.get('CS구분', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:normal; word-break:break-word;">{issue_summary}</td>
                                    <td style="padding: 8px; font-size: 12px; color: #e67e22;">{row.get('진행상태', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; color: #7f8c8d; white-space:pre-wrap; word-break:break-word; line-height:1.45; min-width:320px;">{private_memo}</td>
                                </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Apple SD Gothic Neo','Malgun Gothic',sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:20px 0;">
        <tr>
            <td>
                  <table width="1200" cellpadding="0" cellspacing="0" align="center"
                      style="background:white;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,0.1);">

                    <!-- 헤더 -->
                    <tr>
                        <td style="background:#c0392b;padding:24px 30px;">
                            <h2 style="margin:0;color:white;font-size:18px;">⚠️ 매장CS 처리 지연 알림</h2>
                            <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">
                                기준일: {today_str} &nbsp;|&nbsp;
                                총 <strong>{len(df)}건</strong> ({CS_ALERT_OVERDUE_DAYS}일 이상 미처리)
                            </p>
                        </td>
                    </tr>

                    <!-- 안내 배너 -->
                    <tr>
                        <td style="padding:14px 30px;background:#fdf2f2;border-bottom:1px solid #fce4e4;">
                            <p style="margin:0;font-size:13px;color:#c0392b;">
                                📋 아래 CS 건들은 등록일로부터 <strong>{CS_ALERT_OVERDUE_DAYS}일 이상</strong>
                                경과하였으나 아직 처리가 완료되지 않았습니다. 빠른 확인 및 조치 부탁드립니다.
                            </p>
                        </td>
                    </tr>

                    <!-- 데이터 테이블 -->
                    <tr>
                        <td style="padding:20px 30px;">
                            <table width="100%" cellpadding="0" cellspacing="0"
                                   style="border-collapse:collapse;font-size:12px;">
                                <thead>
                                    <tr style="background:#f8f9fa;">
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">접수번호</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">등록일</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">예정일</th>
                                        <th style="padding:8px;text-align:center;border-bottom:2px solid #dee2e6;white-space:nowrap;">진행률</th>
                                        <th style="padding:8px;text-align:center;border-bottom:2px solid #dee2e6;white-space:nowrap;">소요기간</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;">매장명</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">CS구분</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;">이슈요약</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">진행상태</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;">비공개메모</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {row_html}
                                </tbody>
                            </table>
                        </td>
                    </tr>

                    <!-- 푸터 -->
                    <tr>
                        <td style="padding:16px 30px;text-align:center;border-top:1px solid #eee;">
                            <p style="margin:0;font-size:11px;color:#7f8c8d;">
                                이 이메일은 자동 발송되었습니다 | 문의: {CS_ALERT_TEST_EMAIL}
                            </p>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""
    return html


def _send_cs_alert_email(subject: str, html_content: str,
                         to_emails: list, cc_emails: list = None):
    """CS 알림 전용 SMTP 발송 — TO / CC 헤더 분리 지원"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CS_ALERT_EMAIL_CONN_ID)
    from_email = conn.extra_dejson.get('from_email') or conn.login

    cc_list = cc_emails or []
    to_list = to_emails if to_emails else []

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = from_email
    msg['To']      = ', '.join(to_list)
    if cc_list:
        msg['Cc']  = ', '.join(cc_list)

    msg.attach(MIMEText(html_content, 'html', 'utf-8'))

    all_recipients = to_list + cc_list
    print(f"  📧 발송: TO={to_list} | CC={cc_list}")

    with smtplib.SMTP(conn.host, conn.port) as server:
        server.starttls()
        server.login(conn.login, conn.password)
        server.sendmail(from_email, all_recipients, msg.as_string())

    return f"TO={to_list} CC={cc_list}"


def _validate_memo(memo_str) -> tuple:
    """
    비공개메모 유효성 검사
    Returns (is_valid: bool, reason: str)
      - reason: '미입력' | '형식불일치' | ''
    MEMO_REQUIRED_KEYS 의 모든 키가 존재하고, 각 키 뒤 값이 비어있지 않아야 유효.
    """
    if not memo_str or str(memo_str).strip() == '':
        return False, '미입력'

    text = str(memo_str).strip()
    for key in MEMO_REQUIRED_KEYS:
        idx = text.find(key)
        if idx == -1:
            return False, '형식불일치'
        after_key = text[idx + len(key):]
        line_end  = after_key.find('\n')
        value     = after_key[:line_end].strip() if line_end != -1 else after_key.strip()
        if not value:
            return False, '형식불일치'

    return True, ''


def _build_memo_alert_email_html(df: pd.DataFrame, today_str: str) -> str:
    """비공개메모 미입력/형식불일치 CS 알림 HTML 이메일 본문 생성"""
    row_html = ''
    for _, row in df.sort_values('처리소요기간', ascending=False).iterrows():
        days = row.get('처리소요기간', '')
        try:
            days_int   = int(days)
            days_color = '#e74c3c' if days_int >= 14 else '#f39c12'
            days_text  = f'<span style="color:{days_color};font-weight:bold;">{days_int}일</span>'
        except (ValueError, TypeError):
            days_text = str(days)

        reason = row.get('_memo_reason', '')
        if reason == '미입력':
            problem_color = '#e74c3c'
            problem_text  = '① 비공개 메모를 입력하세요'
        else:
            problem_color = '#e67e22'
            problem_text  = '② 크롬확장으로 규격에 맞게 재작성하세요'

        issue_summary = str(row.get('issue_summary', '') or '')[:50]
        issue_type    = str(row.get('issue_type',    '') or '')

        row_html += f"""
                                <tr style="border-bottom: 1px solid #eee;">
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{row.get('접수번호', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{row.get('등록일', '')}</td>
                                    <td style="padding: 8px; text-align:center; white-space:nowrap;">{days_text}</td>
                                    <td style="padding: 8px; font-size: 12px;">{row.get('매장명', '')}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{row.get('CS구분', '')}</td>
                                    <td style="padding: 8px; font-size: 12px;">{issue_summary}</td>
                                    <td style="padding: 8px; font-size: 12px; white-space:nowrap;">{issue_type}</td>
                                    <td style="padding: 8px; font-size: 13px; font-weight:bold; color:{problem_color}; white-space:nowrap;">{problem_text}</td>
                                </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Apple SD Gothic Neo','Malgun Gothic',sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:20px 0;">
        <tr>
            <td>
                <table width="800" cellpadding="0" cellspacing="0" align="center"
                       style="background:white;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1);">

                    <!-- 헤더 -->
                    <tr>
                        <td style="background:#e67e22;padding:24px 30px;">
                            <h2 style="margin:0;color:white;font-size:18px;">📝 매장CS 비공개메모 입력 요청</h2>
                            <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">
                                기준일: {today_str} &nbsp;|&nbsp;
                                총 <strong>{len(df)}건</strong> (비공개메모 미입력 또는 형식 불일치)
                            </p>
                        </td>
                    </tr>

                    <!-- 안내 배너 -->
                    <tr>
                        <td style="padding:14px 30px;background:#fef9f0;border-bottom:1px solid #fde8c3;">
                            <p style="margin:0 0 8px;font-size:13px;color:#d35400;">
                                📋 아래 CS 건들은 <strong>비공개메모가 없거나 규격에 맞지 않습니다.</strong>
                                올바르게 입력될 때까지 매일 알림이 발송됩니다.
                            </p>
                            <p style="margin:0 0 4px;font-size:12px;color:#555;">
                                ① 비공개 메모 없음 → Relay FMS에서 비공개메모를 직접 입력해 주세요<br>
                                ② 형식 불일치 → 크롬 확장 프로그램으로 규격에 맞게 재작성해 주세요
                            </p>
                            <p style="margin:6px 0 0;font-size:11px;color:#7f8c8d;">
                                📌 필수 항목: 업체 / 이슈유형 / 완료예정일 / 지연사유 / 현재상태 / 메모
                            </p>
                        </td>
                    </tr>

                    <!-- 데이터 테이블 -->
                    <tr>
                        <td style="padding:20px 30px;">
                            <table width="100%" cellpadding="0" cellspacing="0"
                                   style="border-collapse:collapse;font-size:12px;">
                                <thead>
                                    <tr style="background:#f8f9fa;">
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">접수번호</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">등록일</th>
                                        <th style="padding:8px;text-align:center;border-bottom:2px solid #dee2e6;white-space:nowrap;">소요기간</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;">매장명</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">CS구분</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;">이슈요약</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">issue_type</th>
                                        <th style="padding:8px;text-align:left;border-bottom:2px solid #dee2e6;white-space:nowrap;">문제점</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {row_html}
                                </tbody>
                            </table>
                        </td>
                    </tr>

                    <!-- 푸터 -->
                    <tr>
                        <td style="padding:16px 30px;text-align:center;border-top:1px solid #eee;">
                            <p style="margin:0;font-size:11px;color:#7f8c8d;">
                                이 이메일은 자동 발송되었습니다 | 문의: {CS_ALERT_TEST_EMAIL}
                            </p>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""
    return html


def send_overdue_cs_alert(**context):
    """
    수집일(금일) 기준 CS 데이터 중 처리소요기간 >= 7일이고
    진행상태 != '완료'인 건을 CS_ALERT_TO_EMAILS / CC_EMAILS 로 단일 발송
    """
    import gspread
    from google.oauth2.service_account import Credentials

    print(f"\n{'='*60}")
    print(f"[CS지연알림] 이메일 발송 시작")

    now_kst = pendulum.now("Asia/Seoul")
    today_str = now_kst.to_date_string()
    print(f"[CS지연알림] 기준일(수집일, KST): {today_str}")

    # 스케줄러 재기동 시, 어제(또는 과거) 미실행 스케줄이 즉시 실행될 수 있음.
    # 운영 요구사항: 당일 09:10 스케줄 건만 발송 대상으로 처리.
    dag_run = context.get('dag_run')
    data_interval_end = context.get('data_interval_end')
    if dag_run and dag_run.run_type == 'scheduled' and data_interval_end is not None:
        end_kst = data_interval_end.in_timezone("Asia/Seoul")
        if end_kst.date() != now_kst.date():
            print(
                "[CS지연알림] 과거 스케줄 런 감지 → 발송 스킵 "
                f"(run_type={dag_run.run_type}, data_interval_end={end_kst.to_datetime_string()}, now={now_kst.to_datetime_string()})"
            )
            return f"스킵 (과거 스케줄 런: {end_kst.to_date_string()})"

    # ── 1. 구글시트에서 전체 데이터 로드 ──────────────────────
    print("[CS지연알림] 구글시트 데이터 로드 중...")
    try:
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        candidates = _credential_candidates()
        if not candidates:
            raise RuntimeError("사용 가능한 서비스 계정 없음")
        creds = Credentials.from_service_account_file(candidates[0], scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(CS_GSHEET_URL)
        ws = sh.worksheet(CS_SHEET_NAME)
        df_sheet = pd.DataFrame(ws.get_all_records())
        print(f"[CS지연알림] 구글시트 로드 완료: {len(df_sheet)}건")
    except Exception as e:
        print(f"[CS지연알림] 구글시트 로드 실패: {e}")
        return f"스킵 (구글시트 로드 실패: {e})"

    if df_sheet.empty:
        print("[CS지연알림] 데이터 없음 → 스킵")
        return "스킵 (데이터 없음)"

    if '수집일' not in df_sheet.columns:
        print("[CS지연알림] '수집일' 컬럼 없음 → 스킵")
        return "스킵 ('수집일' 컬럼 없음)"

    if '등록일' not in df_sheet.columns:
        print("[CS지연알림] '등록일' 컬럼 없음 → 스킵")
        return "스킵 ('등록일' 컬럼 없음)"

    # ── 2. 수집일 기준 데이터 선택 (오늘 없으면 최신 수집일/uploaded_at 사용) ──
    all_dates_col = df_sheet['수집일'].astype(str).str[:10]
    df_today_chk  = df_sheet[all_dates_col == today_str]

    if df_today_chk.empty:
        # 최신 수집일 결정: 수집일 + uploaded_at 컬럼 모두 고려
        date_candidates = [all_dates_col]
        if 'uploaded_at' in df_sheet.columns:
            date_candidates.append(df_sheet['uploaded_at'].astype(str).str[:10])
        valid_dates = (
            pd.concat(date_candidates)
            .pipe(lambda s: s[s.str.match(r'^\d{4}-\d{2}-\d{2}$')])
        )
        latest_date = valid_dates.max() if not valid_dates.empty else today_str
        print(f"[CS지연알림] 오늘({today_str}) 수집 데이터 없음 → 최신 수집일({latest_date}) 기준으로 대체")
        df_today = df_sheet[all_dates_col == latest_date].copy()
    else:
        print(f"[CS지연알림] 수집일={today_str} 데이터: {len(df_today_chk)}건")
        df_today = df_today_chk.copy()

    if df_today.empty:
        print("[CS지연알림] 시트에 데이터 없음 → 스킵")
        return "스킵 (시트에 데이터 없음)"

    # ── 3. 완료 제외 후 처리소요기간 계산 (오늘 - 등록일) ──────────────
    cond_not_done = df_today['진행상태'].astype(str).str.strip() != '완료'
    df_open = df_today[cond_not_done].copy()
    print(f"[CS지연알림] 미완료 데이터: {len(df_open)}건")

    if df_open.empty:
        print("[CS지연알림] 미완료 건 없음 → 발송 생략")
        return "미완료 건 없음"

    # 경과일 = 오늘 - 등록일 (수집일 기준이 아닌 실제 오늘 기준)
    df_open['_today_dt']   = pd.Timestamp(today_str)
    df_open['_등록일_dt']  = pd.to_datetime(df_open['등록일'], errors='coerce')
    df_open['처리소요기간'] = (df_open['_today_dt'] - df_open['_등록일_dt']).dt.days
    df_open.drop(columns=['_today_dt', '_등록일_dt'], inplace=True)

    # ── 4. 처리소요기간 >= 기준일 필터 ───────────────────────────
    cond_overdue = df_open['처리소요기간'] >= CS_ALERT_OVERDUE_DAYS
    df_alert = df_open[cond_overdue].copy()

    print(f"[CS지연알림] 알림 대상: {len(df_alert)}건 (소요기간 {CS_ALERT_OVERDUE_DAYS}일↑, 미완료)")

    if df_alert.empty:
        print("[CS지연알림] 알림 대상 없음 → 발송 생략")
        return "알림 대상 0건"

    # ── 5. 수신자 결정 ────────────────────────────────────────
    if CS_ALERT_TEST_MODE:
        to_list = [CS_ALERT_TEST_EMAIL]
        cc_list = []
        print(f"[CS지연알림] 🧪 테스트 모드 → TO={to_list}")
    else:
        to_list = list(CS_ALERT_TO_EMAILS)
        cc_list = list(CS_ALERT_CC_EMAILS)
        print(f"[CS지연알림] 🚀 운영 모드 → TO={to_list} | CC={cc_list}")

    # ── 6. 이메일 단일 발송 (처리 지연 알림) ────────────────────
    subject = f"[도리당 CS] 처리 지연 알림 - {len(df_alert)}건 | {today_str}"
    html    = _build_overdue_email_html(df_alert, today_str)

    try:
        _send_cs_alert_email(
            subject=subject,
            html_content=html,
            to_emails=to_list,
            cc_emails=cc_list,
        )
        print(f"[CS지연알림] ✅ 지연 알림 발송 성공 ({len(df_alert)}건)")
    except Exception as e:
        print(f"[CS지연알림] ❌ 지연 알림 발송 실패: {e}")
        raise

    # ── 7. 비공개메모 누락/형식불일치 알림 ──────────────────────
    if '비공개메모' not in df_alert.columns:
        print("[CS지연알림] '비공개메모' 컬럼 없음 → 메모 알림 스킵")
        return f"발송 완료 {len(df_alert)}건"

    memo_check = df_alert['비공개메모'].apply(_validate_memo)
    df_alert['_memo_valid']  = memo_check.apply(lambda x: x[0])
    df_alert['_memo_reason'] = memo_check.apply(lambda x: x[1])
    df_memo_alert = df_alert[~df_alert['_memo_valid']].copy()

    cnt_missing = (df_memo_alert['_memo_reason'] == '미입력').sum()
    cnt_bad     = (df_memo_alert['_memo_reason'] == '형식불일치').sum()
    print(f"[CS지연알림] 비공개메모 이상 건: {len(df_memo_alert)}건 "
          f"(미입력: {cnt_missing}, 형식불일치: {cnt_bad})")

    # 임시 컬럼은 두 번째 메일 발송 후 정리
    df_alert.drop(columns=['_memo_valid', '_memo_reason'], inplace=True, errors='ignore')

    if df_memo_alert.empty:
        print("[CS지연알림] 비공개메모 이상 건 없음 → 메모 알림 생략")
        return f"발송 완료 {len(df_alert)}건"

    memo_subject = f"[도리당 CS] 비공개메모 입력 요청 - {len(df_memo_alert)}건 | {today_str}"
    memo_html    = _build_memo_alert_email_html(df_memo_alert, today_str)

    try:
        _send_cs_alert_email(
            subject=memo_subject,
            html_content=memo_html,
            to_emails=to_list,
            cc_emails=cc_list,
        )
        print(f"[CS지연알림] ✅ 메모 알림 발송 성공 ({len(df_memo_alert)}건)")
    except Exception as e:
        print(f"[CS지연알림] ❌ 메모 알림 발송 실패: {e}")
        raise

    return f"발송 완료 지연={len(df_alert)}건 | 메모알림={len(df_memo_alert)}건"


# ============================================================
# Task 5: 파일 삭제
# ============================================================
def cleanup_files(**context):
    ti = context['ti']
    print(f"\n{'='*60}")
    print(f"[파일 삭제] 시작")

    files_to_delete = []

    existing_files = ti.xcom_pull(task_ids='load_existing_files', key='existing_files') or []
    files_to_delete.extend(existing_files)

    new_file = (
        ti.xcom_pull(task_ids='transform_and_upload', key='uploaded_new_file')
        or ti.xcom_pull(task_ids='download_cs_excel',  key='new_cs_file')
    )
    if new_file:
        files_to_delete.append(new_file)

    if not files_to_delete:
        print(f"[파일 삭제] 삭제할 파일 없음")
        return "삭제할 파일 없음"

    deleted, failed = [], []
    for file_path in files_to_delete:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"  🗑️ 삭제: {os.path.basename(file_path)}")
                deleted.append(file_path)
            else:
                print(f"  ⚠️ 이미 없음: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"  ❌ 삭제 실패 ({os.path.basename(file_path)}): {e}")
            failed.append(file_path)

    print(f"[파일 삭제] ✅ 삭제 {len(deleted)}개 / 실패 {len(failed)}개")
    return f"삭제 {len(deleted)}개"


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='Relay FMS 매장CS 엑셀 다운로드 → 전처리 → LLM 분류 v3.0 → 구글시트 누적 적재 → 처리 지연 알림',
    schedule=SMP_FDAM_CS_TIME,  # io3.py 참조 (매일 09:05 KST)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': pd.Timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['01_extract', '03_gsheet', 'relay_cs', '매장cs', 'llm_v3'],
) as dag:

    t1_load_existing = PythonOperator(
        task_id='load_existing_files',
        python_callable=load_existing_files,
        execution_timeout=pd.Timedelta(minutes=30),
    )

    t2_download = PythonOperator(
        task_id='download_cs_excel',
        python_callable=download_cs_excel,
        execution_timeout=pd.Timedelta(minutes=15),
    )

    t3_upload = PythonOperator(
        task_id='transform_and_upload',
        python_callable=transform_and_upload,
        execution_timeout=pd.Timedelta(minutes=30),
    )

    t5_alert = PythonOperator(
        task_id='send_overdue_cs_alert',
        python_callable=send_overdue_cs_alert,
        execution_timeout=pd.Timedelta(minutes=10),
    )

    t4_cleanup = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=pd.Timedelta(minutes=5),
    )

    t1_load_existing >> t2_download >> t3_upload >> t5_alert >> t4_cleanup