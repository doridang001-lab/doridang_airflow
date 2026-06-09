# 데이터 거버넌스 기획서

## 1. 개요

`C:\Users\민준\OneDrive - 주식회사 도리당\data`는 도리당의 분석·운영 데이터 자산이 저장되는 중앙 저장소입니다. 이 기획서는 현재 OneDrive 폴더 구조와 Airflow DAG 기반 데이터 파이프라인을 바탕으로, 회사의 데이터 거버넌스 체계를 정의하고 실행 계획을 제시합니다.

## 2. 현재 데이터 자산 구조

### 2.1 주요 폴더

- `data/analytics/`
  - 자동 수집 및 원본/가공 데이터 저장
  - 주요 서브폴더:
    - `ai_daily_collection`
    - `baemin_macro`
    - `baemin_marketing`
    - `coupang_macro`
    - `coupang_marketing`
    - `easypos_sales_raw`
    - `okpos_sales_raw`
    - `posfeed_sales`
    - `toorder_daily_store`
    - `unified_sales_validation`

- `data/mart/`
  - 정제/집계/BI 분석 결과 저장
  - 주요 서브폴더:
    - `unified_sales_grp`
    - `fdam_cs_report`
    - `dag_monitoring`
    - `hall_sales_target`
    - `unified_review`
    - `closing_rate`

- `data/report/`
  - 리포트, 분석 문서, 발표 자료 저장
  - 예: `PowerPoint/`, Markdown 보고서

- `data/planning/`
  - 정책, DAG 모니터링, 수집 계획, 알림 정의

- `data/llm/`
  - LLM 출력물 및 AI 관련 결과 저장

- `data/dashboard/`
  - 대시보드 연동 자산

### 2.2 DAG 기반 주요 경로

- `dags/db/DB_Beamin_Macro_Dags.py`
  - `data/analytics/baemin_macro/*`
  - `ad_funnel`, `orders`, `shop_change`, `metrics_now`, `metrics_our_store_clicks`

- `dags/db/DB_Posfeed_Sales_Dags.py`
  - `data/analytics/posfeed_sales/*`
  - 브랜드·지점·월별 파티션 저장

- `dags/sales/Sales_CoupangEats_CMG_Partition_Dags.py`
  - `data/analytics/coupang_marketing/*`

- 기타 DAG들
  - `data/analytics/easypos_sales_raw`, `okpos_sales_raw`, `unionpos_sales_raw`
  - `data/mart/*` 집계/검증 결과 생성

## 3. 거버넌스 목표

### 3.1 핵심 목표

1. 매장 운영 현황을 객관적으로 파악하고 개선 근거를 제공
2. 의사결정의 일관성, 속도, 정확도 강화
3. 데이터 품질 및 무결성 확보
4. 접근 권한과 데이터 책임 분담 명확화
5. 감사와 추적이 가능한 데이터 변경 이력 관리

### 3.2 기대 효과

- 동일한 기준의 데이터를 기반으로 경영진과 운영팀이 공통 의사결정을 수행
- 원본 데이터와 가공 결과가 혼재되는 문제 감소
- 수집부터 리포트까지 데이터 품질 점검 체계화
- 데이터 접근 권한의 최소 권한 원칙 적용

## 4. 거버넌스 구성 요소

### 4.1 데이터 분류

- 원본/수집 데이터: `data/analytics/*`
- 정제/집계 데이터: `data/mart/*`
- 리포트/문서 자료: `data/report/*`
- 정책/운영 계획: `data/planning/*`
- AI 출력물: `data/llm/*`
- 대시보드 자산: `data/dashboard/*`

### 4.2 데이터 소유자 및 책임

- 데이터 소유자(Data Owner)
  - 각 영역별 책임자 지정
  - 예: `baemin_macro` 수집 담당, `posfeed_sales` 분석 담당, `unified_sales_grp` BI 담당

- 데이터 스테워드(Data Steward)
  - 품질 검증 및 운영 유지보수 담당
  - DAG 실행 상태 확인, 이상값 탐지, 오류 대응

- 데이터 사용자(Data Consumer)
  - 경영진, 운영팀, 마케팅팀, 분석 담당자
  - 보고서·대시보드 활용자

### 4.3 접근 권한

- 원본 데이터(`data/analytics/*`)
  - 수집/분석 담당자 중심 최소 권한
- 정제 데이터(`data/mart/*`)
  - 운영/경영/분석 담당자 접근 허용
- 리포트(`data/report/*`)
  - 최종 사용자 중심 공유
- 정책/계획(`data/planning/*`)
  - 운영/기획 담당자 중심
- 민감/내부 데이터
  - 별도 제한 폴더 또는 접근 제어

### 4.4 품질 검증 기준

- 필수 검증 항목
  - 날짜 누락 여부
  - 매장/브랜드/플랫폼 일관성
  - 주문 건수/금액의 음수 또는 비정상 값
  - 중복 주문 ID
  - 파티션별 저장 상태
  - 원본 합계 대비 가공 합계 일치 여부
  - 급격한 증감 탐지

- DAG 레벨 검증
  - `DB_Beamin_Macro_Dags.py`의 orders, ad_funnel 검증 로직
  - `DB_Posfeed_Sales_Dags.py` 파티션/누락 검증
  - `mart` 집계 이후 합계 검증

### 4.5 변경 이력 관리

- 파일명에 날짜/버전 포함
- 변경 시 `data/report/` 또는 `Repository`에 수정 사유 기록
- DAG 재실행 및 재수집 이력 추적
- 주요 결과물에 대한 버전 관리

## 5. 폴더 표준화 제안

### 5.1 권장 저장 구조

```text
C:\Users\민준\OneDrive - 주식회사 도리당\data\
  analytics\
    baemin_macro\
      brand={brand}\store={store}\ym={YYYY-MM}\
    posfeed_sales\
      brand={brand}\store={store}\ym={YYYY-MM}\
    coupang_marketing\
      brand={brand}\store={store}\ym={YYYY-MM}\
    easypos_sales_raw\
    okpos_sales_raw\
  mart\
    unified_sales_grp\
    fdam_cs_report\
    dag_monitoring\
  report\
    PowerPoint\
  planning\
  llm\
  dashboard\
```

### 5.2 파일명/파티션 규칙

- CSV 파일명 예시: `orders_{YYYY-MM-DD}.csv`, `posfeed_orders.csv`, `baemin_now.csv`
- 파티션 기준: `brand`, `store`, `ym`
- 파일명에는 생성일, 기준일, 버전 정보 포함 권장

## 6. 실행 전략

### 6.1 1단계: 현황 정리

- `data/analytics`, `data/mart`, `data/report`, `data/planning`, `data/llm` 역할 정의
- 각 폴더별 주요 데이터 출처 및 DAG 매핑
- 현재 사용 중인 데이터 책임자/담당자 목록 작성

### 6.2 2단계: 기준 정의

- 데이터 분류 기준 정의
- 폴더·파일명 규칙 문서화
- `brand/store/ym` 파티션 표준 정립
- 데이터 품질 체크리스트 수립

### 6.3 3단계: 접근·권한 체계 수립

- OneDrive 폴더별 접근 권한 분류
- 민감/내부 데이터 접근 최소화
- 공유 필요자 기준 정의

### 6.4 4단계: 운영 프로세스 적용

- 수집 → 정제 → 검증 → 보고 흐름 고정
- DAG별 검증/알림 로직 점검
- 변경 이력 및 버전 관리 프로세스 적용

### 6.5 5단계: 단계적 정착

- 6개월 내 우선 적용 항목
  - 데이터 저장 구조 표준화
  - 파일명/업데이트 주기 규칙
  - 책임자 지정
  - 품질 검증 규칙
  - 변경 이력 관리
- 이후 지속 개선
  - `data/planning` 기반 정책 고도화
  - `data/dashboard` 품질 검증 강화
  - LLM 결과물 거버넌스 적용

## 7. 추천 거버넌스 체계

### 7.1 데이터 계층

- 레벨 1: 원천/수집 데이터 (`data/analytics/*`)
- 레벨 2: 정제/집계 데이터 (`data/mart/*`)
- 레벨 3: 보고/결과 데이터 (`data/report/*`, `data/dashboard/*`)
- 레벨 4: 운영/정책 데이터 (`data/planning/*`)
- 레벨 5: AI/실험 데이터 (`data/llm/*`)

### 7.2 운영 원칙

- 단순화: `analytics`는 수집, `mart`는 정제, `report`는 결과
- 일관성: 동일한 `brand/store/ym` 파티션 규칙 사용
- 검증: DAG 실행 시 품질 체크 자동화
- 역할 분담: 책임자·스테워드·사용자 구분
- 추적: 변경 이력·버전 관리

## 8. 결론

`C:\Users\민준\OneDrive - 주식회사 도리당\data`는 이미 분석과 운영의 핵심 데이터 자산이 모여 있는 구조입니다. 다만 원본/가공/보고의 역할이 명확히 정리되지 않았고, 책임·권한·검증 체계가 문서화되지 않은 상태입니다.

이 기획서는 다음을 목표로 합니다.

- `analytics`, `mart`, `report`, `planning`, `llm`의 역할을 명확히 구분
- 폴더·파일명 규칙을 표준화
- 데이터 책임자와 검증 단계를 정의
- OneDrive 기반 데이터 운영을 거버넌스 체계로 전환

이를 통해 도리당은 동일한 기준의 데이터를 기반으로 의사결정을 강화하고, 매장 운영 데이터를 안정적으로 관리할 수 있습니다.
