# 데이터 자산화 스킬

CSV 파일을 자동으로 파티션 저장하고 OneDrive에 백업하는 3단계 파이프라인입니다.

## 사용 방법

```
/data-asset --dataset <이름> --source-dir <경로> [--options]
```

### 필수 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--dataset` | 데이터셋 이름 (ANALYTICS_DB 하위 폴더명) | `coupangeats`, `baemin_marketing` |
| `--source-dir` | CSV 파일 수집 폴더 | `E:/down/업로드_temp` |

### 선택 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--file-pattern` | `{dataset}_*.csv` | 파일명 패턴 |
| `--brand-strategy` | `from_file` | `fixed` \| `from_column` \| `from_file` |
| `--brand-fixed` | 없음 | brand_strategy=fixed 시 고정 브랜드명 |
| `--brand-col` | 없음 | brand_strategy=from_column 시 컬럼명 |
| `--store-col` | 없음 | store 파티션 컬럼명 (필수 추론 불가 시) |
| `--date-col` | 없음 | ym 추출 기준 컬럼명 (필수 추론 불가 시) |
| `--timestamp-col` | `collected_at` | 중복 제거 기준 컬럼명 |
| `--no-upload` | `false` | OneDrive 업로드 스킵 |

---

## 실행 예시

### 예시 1: 쿠팡이츠 데이터 (브랜드 고정)
```
/data-asset --dataset coupangeats --source-dir "E:/down/업로드_temp" \
  --brand-strategy fixed --brand-fixed "도리당" \
  --store-col "매장명" --date-col "조회일자"
```

### 예시 2: 배민 마케팅 (파일명에서 추론)
```
/data-asset --dataset baemin_marketing --source-dir "E:/down/영업관리부_수집" \
  --brand-strategy from_file --store-col "store_name" --date-col "날짜"
```

### 예시 3: 자동 추론 (완전 자동)
```
/data-asset --dataset new_dataset --source-dir "E:/down/업로드_temp"
```

---

## 실행 흐름 (3단계)

```
┌─────────────────────────────────────────────┐
│ 1단계: 스키마 분석 (schema-analyzer)       │
│  CSV 샘플 읽기 → 컬럼 타입 분석            │
│  → brand/store/date 컬럼 자동 추론          │
│  출력: confirmed_config                    │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│ 2단계: ETL 실행 (partitioner)              │
│  파일 수집 → 병합 → 전처리                 │
│  → 파티션 저장 (ANALYTICS_DB/)             │
│  → OneDrive 백업                           │
│  출력: saved_partitions[]                  │
└─────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────┐
│ 3단계: 결과 검증 (validator)               │
│  파티션 경로 존재 확인                     │
│  → 행수 검증                               │
│  → OneDrive 경로 확인                      │
│  출력: overall: PASS/FAIL                  │
└─────────────────────────────────────────────┘
```

---

## 수동 호출 (단계별)

`/data-asset` 스킬이 아직 미완성인 경우, 각 에이전트를 수동으로 호출할 수 있습니다:

### Step 1: 스키마 분석
```
@"data-asset-schema-analyzer (agent)"
E:/down/업로드_temp 폴더의 coupangeats_cmg_*.csv 파일들을 분석해줘.
brand 전략은 fixed:"도리당", store 컬럼은 "매장명", date 컬럼은 "조회일자"로 확인해줘.
```

출력: `confirmed_config` JSON

### Step 2: 파티션 저장 + OneDrive 업로드
```
@"data-asset-partitioner (agent)"
위의 confirmed_config를 사용해서:
- E:/down/업로드_temp의 coupangeats_cmg_*.csv 파일들을 수집
- brand=도리당/store=매장명/ym=YYYY-MM 파티션으로 저장
- ANALYTICS_DB/coupangeats/ 경로에 저장
- OneDrive에 백업
```

출력: `saved_partitions[]` + `summary`

### Step 3: 결과 검증
```
@"data-asset-validator (agent)"
ANALYTICS_DB/coupangeats/ 폴더의 모든 data.csv 파일을 검증해줘.
행수, 컬럼, OneDrive 경로 확인.
```

출력: `overall: PASS/FAIL`

---

## 결과 확인

### 로컬 경로
```
C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\
├── coupangeats/
│   ├── brand=도리당/
│   │   ├── store=오산시청점/
│   │   │   └── ym=2026-03/
│   │   │       └── data.csv
│   │   └── store=강원영월점/
│   │       └── ym=2026-03/
│   │           └── data.csv
│   └── ...
```

### OneDrive 경로
```
OneDrive Repository/data/analytics/coupangeats/brand=.../store=.../ym=.../data.csv
(자동 동기화)
```

---

## 트러블슈팅

| 상황 | 해결책 |
|------|--------|
| "파일을 찾을 수 없음" | `--source-dir` 경로 확인, `--file-pattern` 재지정 |
| "컬럼 추론 실패" | `--store-col`, `--date-col` 명시적 지정 |
| "OneDrive 경로 없음" | `paths.py`의 ONEDRIVE_DB 상수 확인 |
| "파티션이 비어있음" | CSV 파일 내용 확인, 전처리 로직 디버깅 |

---

## 권장 사항

1. **첫 실행**: `--no-upload` 플래그로 로컬 저장만 먼저 테스트
2. **스키마 확인**: Step 1 결과를 보고 `--store-col`, `--date-col` 수정
3. **OneDrive 확인**: 저장 후 OneDrive 웹 계정에서 경로 수동 확인

---

## 참고

- 프로젝트 규칙: `modules/CLAUDE.md`, `dags/CLAUDE.md` 참조
- 경로 상수: `modules/transform/utility/paths.py` 확인
- OneDrive 업로드: `modules/transform/utility/onedrive.py` 함수 사용
