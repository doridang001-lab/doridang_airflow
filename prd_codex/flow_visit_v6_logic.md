# Flow 방문관리 v6 로직 인수인계

## 1. 목적

Flow 방문관리 Power BI 화면은 담당자가 새 매장 또는 오랜만에 방문하는 매장을 보기 전에 10초 안에 아래를 파악하는 것이 목적이다.

- 점주 상태가 어떤지
- 점주가 반복해서 관심을 보인 이슈가 무엇인지
- 방문자가 현장에서 무엇을 확인하거나 설명해야 하는지
- 과거 방문 히스토리와 현재 본문 요약이 서로 맞는지

숫자 KPI는 별도 매출/수수료 카드에서 담당한다. 이 문서의 로직은 텍스트 브리핑 품질만 다룬다.

## 2. 핵심 파일

| 역할 | 파일 |
|---|---|
| LLM 프롬프트 버전과 매장별 힌트 | `modules/transform/pipelines/strategy/flow_visit_prompts.py` |
| 방문일지 mart, issue, store profile 생성 | `modules/transform/pipelines/strategy/SMP_flow_visit_mart.py` |
| Power BI 단일 시각화 테이블 생성 | `modules/transform/pipelines/strategy/flow_visit_viz.py` |
| 회귀 테스트 | `tests/test_flow_visit_mart.py` |
| 운영 DAG | `dags/sales/Sales_FlowVisit_01_Mart_Dags.py` |

현재 v6 프롬프트 버전은 다음 값이어야 한다.

```text
flow_visit_v6_brief_action_matching
```

## 3. 데이터 흐름

1. Flow 방문글을 수집한다.
2. 방문글을 세그먼트로 나눈다.
3. LLM 또는 fallback 로직이 세그먼트별 `issue_key`, `issue_label`, `owner_summary`, `sv_summary`, `status`를 만든다.
4. `SMP_flow_visit_mart.py`가 매장 단위 profile을 만든다.
5. `flow_visit_viz.py`가 Power BI에서 쓰는 단일 테이블 `flow_visit_viz.parquet`를 만든다.

중요한 점은, Power BI 본문 문구 전체를 LLM이 직접 쓰는 구조가 아니라는 것이다. LLM은 이슈 분류와 요약 근거를 만들고, 화면용 본문은 후처리 함수가 짧은 업무 문구로 조립한다.

## 4. 컬럼별 의미

컬럼명은 Power BI와 연결되어 있으므로 유지해야 한다. 새 컬럼 추가보다 기존 컬럼값 개선을 우선한다.

| 컬럼 | 화면 역할 | 생성 기준 |
|---|---|---|
| `owner_status` | 점주 상태 | 최신 방문 이슈와 전체 이슈의 감정/상황 신호 |
| `key_concerns` | 주요 관심사 | 우선순위 높은 이슈를 5개 이하 짧은 명사구로 압축 |
| `handling_points` | 응대 포인트, C패널 체크리스트 | 담당자가 방문 전 준비할 행동형 문장 |
| `manager_memo` | 담당자 메모 | 매장명 + 핵심 이슈 묶음 + 응대 방향 |
| `handover_summary` | 백업용 인수인계 | `handling_points` 기반 방문 전 체크 요약 |
| `followup_summary` | 후속답변 백업 | 본사 답변 요지와 방문 확인 항목 |
| `issue_label` | 히스토리 카드 | 방문일자별 이슈명 |

## 5. v6 문구 원칙

### 5.1 점주 상태

`owner_status`는 줄바꿈 리스트다. 길게 설명하지 않는다.

예시:

```text
불만 높음
수익 체감 악화
상권 악화로 이전 고민
품질 개선 요청 있음
```

주요 함수:

```text
_build_owner_status_items()
_build_owner_status_sentence()
```

### 5.2 주요 관심사

`key_concerns`는 원문을 길게 붙이지 않는다. 화면에서 바로 읽히는 짧은 명사구로 만든다.

좋은 예:

```text
순수익 월 300~400만원 체감
쿠팡 1인 메뉴 등록 요청
묵은지·우거지 품질 개선
즉시할인 광고 효율
배달 매출 1,400~1,500만원 정체
```

나쁜 예:

```text
우거지는 현재 상태도 좋고 괜찮으나 조금 덜 삶아 진 부분이 있는 것 같고...
최근 방문 3건에서 핵심 관심사 6개를 확인했다
```

주요 함수:

```text
_concern_text()
_dedupe_keep_order()
```

### 5.3 응대 포인트

`handling_points`는 C패널에 그대로 들어가도 되는 행동형 문장이어야 한다.

좋은 예:

```text
수수료·광고비 제외 순수익 기준 설명
쿠팡 1인 메뉴 등록 진행 상태 확인
묵은지·우거지 개선 가능 여부 확인
광고비 대비 주문·객단가 자료 준비
```

주요 함수:

```text
_handling_text()
```

### 5.4 담당자 메모

`manager_memo`는 템플릿 느낌을 줄이고 매장명을 포함한다.

예시:

```text
용인동천점은 순수익 기준·광고 효율·품질 요청·메뉴 등록이 핵심이므로, 점주 불만을 먼저 인정하고 처리 상태와 근거 자료를 짧게 맞춘다.
반복 이슈: 즉시할인 광고 · 배달 매출 정체 · 한그릇/하나만 운영
```

### 5.5 C패널

Power BI의 `C. 방문자 체크리스트`는 `handover_summary`보다 `handling_points`를 우선 사용해야 한다.

이유:

- `handling_points`는 담당자 행동으로 바로 이어진다.
- `handover_summary`는 과거에 `미해결/진행중 N건` 같은 메타 문구가 들어가 화면 도움도가 낮았다.

## 6. issue_key별 화면 문구 예시

| issue_key | `key_concerns` 예시 | `handling_points` 예시 |
|---|---|---|
| `수익_감소체감` | 순수익 월 300~400만원 체감 | 수수료·광고비 제외 순수익 기준 설명 |
| `메뉴_1인메뉴확대` | 쿠팡 1인 메뉴 등록 요청 | 쿠팡 1인 메뉴 등록 진행 상태 확인 |
| `묵은지_숙성경도`, `우거지_질김` | 묵은지·우거지 품질 개선 | 묵은지·우거지 개선 가능 여부 확인 |
| `광고_즉시할인` | 즉시할인 광고 효율 | 광고비 대비 주문·객단가 자료 준비 |
| `매출_배달정체` | 배달 매출 1,400~1,500만원 정체 | 선택기간 매출 흐름 먼저 공유 |
| `계육_순살품질` | 순살 품질 개선 요청 | 계육 품질 개선 가능 여부 확인 |
| `계육_뼈닭내장` | 뼈닭 내장 제거 개선 | 계육 품질 개선 가능 여부 확인 |
| `인테리어_내부디자인` | 매장 내부 디자인 보완 | 매장 내부 디자인 처리 상태 확인 |
| `매출_홀부진` | 홀 매출 부진·고정비 부담 | 홀 매출 부진 처리 상태 확인 |

## 7. 현재 샘플 기대값

### 용인동천점

```text
owner_status:
불만 높음
수익 체감 악화
상권 악화로 이전 고민
품질 개선 요청 있음

key_concerns:
순수익 월 300~400만원 체감
쿠팡 1인 메뉴 등록 요청
묵은지·우거지 품질 개선
즉시할인 광고 효율
배달 매출 1,400~1,500만원 정체

handling_points:
수수료·광고비 제외 순수익 기준 설명
쿠팡 1인 메뉴 등록 진행 상태 확인
묵은지·우거지 개선 가능 여부 확인
광고비 대비 주문·객단가 자료 준비
```

### 동탄영천점

```text
key_concerns:
순살 품질 개선 요청
매장 내부 디자인 보완
홀 매출 부진·고정비 부담
매장이전 검토
뼈닭 내장 제거 개선

handling_points:
계육 품질 개선 가능 여부 확인
매장 내부 디자인 처리 상태 확인
홀 매출 부진 처리 상태 확인
이전 상담 기준 자료 준비
```

## 8. 검증 방법

코드 변경 후 최소 검증:

```powershell
python -X utf8 -m py_compile modules/transform/pipelines/strategy/SMP_flow_visit_mart.py modules/transform/pipelines/strategy/flow_visit_viz.py modules/transform/pipelines/strategy/flow_visit_prompts.py tests/test_flow_visit_mart.py
$env:PYTHONPATH='.'; pytest -q -p no:cacheprovider tests/test_flow_visit_mart.py
```

한글 깨짐 확인:

```powershell
python -X utf8 -c "from pathlib import Path; p=Path('modules/transform/pipelines/strategy/SMP_flow_visit_mart.py'); s=p.read_text(encoding='utf-8'); print('ok', '\ufffd' not in s)"
```

OneDrive 산출물 확인은 한글 경로가 깨질 수 있으므로 Python 내부에서 `Path.home().glob('OneDrive - *')`로 경로를 찾는 방식을 사용한다.

## 9. 수정 시 주의사항

- 컬럼명은 유지한다.
- 숫자 KPI, 매출, 수수료 계산 로직은 건드리지 않는다.
- `key_concerns`에 원문 긴 문장을 그대로 노출하지 않는다.
- C패널 본문에 `최근 방문 N건`, `미해결 N건 확인` 같은 메타 요약을 쓰지 않는다.
- `handover_summary`는 백업용으로 유지하고, C패널은 `handling_points` 중심으로 둔다.
- OneDrive mart 재생성은 사용자 승인 후 DAG로 실행한다.
- Power BI 화면 반영 여부는 parquet 버전뿐 아니라 Power BI 새로고침까지 확인해야 한다.

## 10. 재생성 절차

사용자가 OneDrive mart 재생성을 승인하면 `Sales_FlowVisit_01_Mart_Dags`를 수동 실행한다.

실행 후 확인할 것:

- `flow_visit_viz.parquet` row 수
- `flow_store_profile.parquet` row 수
- `prompt_version = flow_visit_v6_brief_action_matching`
- 주요 텍스트 컬럼 빈값 0건
- 한글 깨짐 0건
- 용인동천점/동탄영천점 샘플이 이 문서의 기대값과 같은 방향인지
