# 쿠팡이츠 CMG 파티션 저장 파이프라인

## 개요
쿠팡이츠 CMG(광고) 마케팅 데이터를 수집하여 파티션 구조로 저장하고 OneDrive에 자동 백업하는 파이프라인입니다.

## 파일 구조

### DAG
- **경로**: `dags/strategy/Strategy_CoupangEats_CMG_Partition_Dags.py`
- **Schedule**: 매일 22:00 (cron: `0 22 * * *`)
- **DAG ID**: `Strategy_CoupangEats_CMG_Partition_Dags`
- **재시도**: 2회 (5분 간격)

### 파이프라인 모듈
- **경로**: `modules/transform/pipelines/strategy/SMP_CoupangEats_CMG_Partition.py`
- **함수**: `load_coupangeats_cmg_partition()`
- **반환값**: JSON 문자열 (XCom으로 전달)

## 처리 절차

### 1단계: 파일 수집
```
E:/down/업로드_temp/coupangeats_cmg_*.csv
```
- glob으로 모든 CSV 파일 검색
- UTF-8 with BOM (utf-8-sig) 인코딩으로 읽기
- 파일명을 `_source_file` 컬럼으로 추가

### 2단계: DataFrame 병합
- 수집한 모든 CSV를 concat으로 병합
- 로깅: 파일 개수 및 행 수

### 3단계: 전처리
1. **null 제거**: `collected_at`, `조회일자` 컬럼의 null 행 제거
2. **datetime 변환**:
   - `collected_at` → 타임스탐프 (중복 제거 기준)
   - `조회일자` → 날짜형 (ym 파티션 생성 기준)
3. **파티션 컬럼 생성**:
   - `ym`: `조회일자`에서 YYYY-MM 형식 추출
   - `store`: `매장명`에서 공백 제거 (언더스코어로 대체)
   - `brand`: 고정값 "도리당"

### 4단계: 중복 제거
- `collected_at` 기준 내림차순 정렬
- `(매장명, 조회일자)` 조합으로 중복 제거 (최신 데이터 유지)

### 5단계: 파티션 저장
각 `(brand, store, ym)` 조합별로:
1. 파티션 디렉토리 생성
2. 기존 파일이 있으면:
   - 기존 데이터 로드
   - 신규 데이터와 concat
   - 중복 제거 후 저장 (idempotent)
3. 없으면 신규 저장

**저장 경로**:
```
C:/Users/민준/OneDrive - 주식회사 도리당/data/analytics/coupang_marketing/
└── brand=도리당/
    └── store={매장명}/
        └── ym={YYYY-MM}/
            └── data.csv
```

### 6단계: OneDrive 백업
각 저장된 파일을:
- `modules.transform.utility.onedrive.backup_to_onedrive()` 호출
- OneDrive 경로: `{dataset_name}/brand={brand}/store={store}/ym={ym}/data.csv`
- 최대 재시도: 3회 (2초 간격)

## 데이터 스키마

### 입력 (CSV)
| 컬럼명 | 데이터타입 | 설명 |
|--------|-----------|------|
| collected_at | ISO 8601 DateTime | 수집 시간 (중복 제거 기준) |
| 조회일자 | YYYY-MM-DD | 조회 날짜 (ym 파티션 기준) |
| 매장명 | string | 매장명 (store 파티션 기준) |
| 광고상태 | string | 광고 활성 상태 |
| 광고기간 | string | 광고 기간 |
| 광고비율 | numeric | 광고 비율 |
| 광고비용 | numeric | 광고 비용 |
| 신규고객 | numeric | 신규 고객 수 |
| 광고주문수 | numeric | 광고로 인한 주문 수 |
| 광고매출 | numeric | 광고로 인한 매출 |
| 전체매출 | numeric | 전체 매출 |
| 광고클릭수 | numeric | 광고 클릭 수 |
| 광고노출수 | numeric | 광고 노출 수 |

### 파티션 컬럼 (생성)
| 컬럼명 | 설명 |
|--------|------|
| brand | 브랜드 (고정: "도리당") |
| store | 매장명 (공백 제거) |
| ym | 연월 (YYYY-MM) |

## 출력 결과

### XCom 메시지 (JSON)
```json
{
  "dataset_name": "coupang_marketing",
  "total_rows": 1250,
  "total_files": 616,
  "saved_partitions": [
    {
      "path": "C:/Users/민준/OneDrive - 주식회사 도리당/data/analytics/coupang_marketing/brand=도리당/store=오산시청점/ym=2026-03/data.csv",
      "rows": 7,
      "brand": "도리당",
      "store": "오산시청점",
      "ym": "2026-03",
      "onedrive_result": {
        "success": true,
        "message": "백업 완료 (1234 bytes)",
        "local_path": "...",
        "onedrive_path": "..."
      }
    }
  ],
  "summary": {
    "partitions_created": 8,
    "total_rows_saved": 1250,
    "onedrive_uploaded": 8,
    "status": "SUCCESS"
  }
}
```

## 특징

### Idempotent
- 이미 존재하는 파티션에 새 데이터 병합
- 같은 데이터 반복 실행 시 중복 저장 안됨
- `(매장명, 조회일자)` 조합으로 중복 제거

### 에러 처리
- 개별 파일 읽기 오류 시: 해당 파일 건너뜀 (계속 진행)
- OneDrive 백업 실패: 경고 로깅하고 계속 진행
- 파티션 저장 실패: 전체 파이프라인 실패

### 로깅
- `logging.getLogger(__name__)` 사용
- 각 단계별 로깅 (파일 수집, 병합, 전처리, 저장, 백업)
- print 사용 금지

## 설정 상수

| 항목 | 값 |
|------|-----|
| source_dir | `E:/down/업로드_temp` |
| file_pattern | `coupangeats_cmg_*.csv` |
| dataset_name | `coupang_marketing` |
| brand | `도리당` (고정) |
| store_col | `매장명` |
| date_col | `조회일자` |
| timestamp_col | `collected_at` |
| dedup_cols | `["매장명", "조회일자"]` |
| encoding | `utf-8-sig` |
| base_path | `ANALYTICS_DB / "coupang_marketing"` |

## 경로 상수
- **ANALYTICS_DB**: `modules.transform.utility.paths.ANALYTICS_DB`
- 자동 감지: Windows OneDrive / Docker 컨테이너 / Fallback

## 의존성
```python
from pathlib import Path
import pandas as pd
import logging
import json
from glob import glob

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.onedrive import backup_to_onedrive
```

## 사용 예시

### 수동 실행
```python
from modules.transform.pipelines.strategy.SMP_CoupangEats_CMG_Partition import load_coupangeats_cmg_partition

result_json = load_coupangeats_cmg_partition()
print(result_json)
```

### Airflow DAG
```python
from airflow.operators.python import PythonOperator
from modules.transform.pipelines.strategy.SMP_CoupangEats_CMG_Partition import load_coupangeats_cmg_partition

task = PythonOperator(
    task_id='load_cmg',
    python_callable=load_coupangeats_cmg_partition,
    dag=dag
)
```

## 모니터링

### 주요 메트릭
- `total_rows`: 전체 처리 행 수
- `total_files`: 수집 파일 수
- `partitions_created`: 생성된 파티션 수
- `total_rows_saved`: 저장된 총 행 수
- `onedrive_uploaded`: OneDrive 성공 건수
- `status`: "SUCCESS" 또는 "PARTIAL"

### 로그 확인
```bash
# Airflow logs
airflow logs Strategy_CoupangEats_CMG_Partition_Dags load_coupangeats_cmg_partition

# 파이프라인 모듈 로그
# logging.getLogger('modules.transform.pipelines.strategy.SMP_CoupangEats_CMG_Partition')
```

## 문제 해결

### 파일을 찾을 수 없음
- `E:/down/업로드_temp/` 디렉토리 존재 확인
- CSV 파일명이 `coupangeats_cmg_` 패턴 맞는지 확인

### OneDrive 백업 실패
- OneDrive 경로 존재 확인
- 용량 부족 확인
- 권한 문제 확인

### 인코딩 오류
- CSV 파일 인코딩이 UTF-8 with BOM (utf-8-sig)인지 확인

## 관련 문서
- `AGENTS.md` - 프로젝트 운영 규칙 원본
- `modules/CLAUDE.md` - 모듈 개발 규칙 참조
- `dags/CLAUDE.md` - DAG 네이밍 규칙 참조
- `modules/transform/utility/paths.py` - 경로 상수
- `modules/transform/utility/onedrive.py` - OneDrive 백업 유틸
