# 누락 데이터 재업로드 솔루션 가이드

## 📌 문제 상황
1개 매장의 1/1~1/10 데이터 중에서 **1/3 일자의 매출 데이터가 덜 쌓임**
- 이를 깨달은 후 1/3 데이터를 재계산하여 재업로드하고 싶음
- 기존 데이터(1/1, 1/2, 1/4~1/10)는 절대 손실되면 안 됨
- 1/3 데이터만 업데이트되어야 함

---

## ✅ 해결 방법 (4단계)

### 1단계: 배민/토더 데이터 수집
- **위치**: `E:\down\업로드_temp` (또는 `/opt/airflow/download/업로드_temp` 도커 내부)
- **내용**: 이전 `sales_store_amount_join_dags` 실행 결과
  - `baemin_metrics_*.csv` (배민 우리가게 데이터)
  - `baemin_change_history_*.csv` (배민 변경이력)
  - `toorder_review_*.csv` (토더 리뷰 데이터)
- **용도**: 이전 계산 결과 재사용 (재다운로드 불필요)

### 2단계: 1/3 매출 데이터 재계산
- 수정된 매출 정보를 다시 계산
- 결과를 CSV로 저장 (경로: `/opt/airflow/영업관리부_DB/sales_daily_orders_upload.csv`)

### 3단계: sales_store_amount_join_dags 수동 실행 (재업로드 모드)

#### 방법 A: Airflow UI 사용
1. **Airflow UI 접속**
   - URL: `http://localhost:8080` (로컬) 또는 실제 Airflow 서버

2. **DAG 선택**
   - 좌측 "DAGs" 메뉴
   - `sales_store_amount_join_dags` 검색 및 선택

3. **수동 실행**
   - DAG 우측 상단 "Trigger DAG" 버튼 클릭
   - 또는 DAG 이름 우측 "▶" 버튼 클릭

4. **파라미터 설정** (중요!)
   ```json
   {
     "reupload_mode": true
   }
   ```
   - "Trigger DAG" 다이얼로그에서 
   - "Conf" 또는 "Configuration" 란에 위 JSON 입력

5. **실행**
   - "Trigger" 버튼 클릭
   - DAG 실행 시작

#### 방법 B: 명령줄 (Docker)
```bash
# Docker 컨테이너 내부에서 실행
airflow dags trigger sales_store_amount_join_dags \
  --conf '{"reupload_mode": true}'
```

### 4단계: Google Sheets 자동 업로드 (append_unique)

#### 흐름
1. `sales_store_amount_join_dags` 완료 후
   - 새로운 `sales_daily_orders_upload.csv` 생성
   - 배민(이전) + 토더(이전) + 매출(신규) JOIN 완료

2. 자동으로 `sales_order_csv_upload_alerts_to_gsheet` DAG 실행
   - CSV → Google Sheets 업로드
   - 모드: **append_unique** (기존 데이터 보존)

3. 결과
   - 1/1 데이터: 유지 ✅
   - 1/2 데이터: 유지 ✅
   - 1/3 데이터: 업데이트됨 (새로 계산) ✅
   - 1/4~1/10 데이터: 유지 ✅
   - **데이터 손실: 0%** ✅

---

## 🔒 안전장치 (데이터 손실 방지)

### 1. CSV 파일 유효성 검사 (`sales_store_amount_join_dags`)
```python
# CSV 없으면 즉시 DAG 중단
if not ALERTS_CSV_PATH.exists():
    raise FileNotFoundError("CSV 파일 없음!")
```

### 2. 데이터 유효성 검사
```python
# 빈 데이터면 DAG 중단
if len(df) == 0:
    raise ValueError("데이터가 비어있음!")
```

### 3. 재업로드 모드 지원 (`sales_store_amount_join_dags`)
- 정기 실행: 모든 원본 데이터 새로 로드
- 재업로드 모드: 이전 파일 재사용 + 새로운 매출만 JOIN

### 4. 안전한 Google Sheets 업로드 (`sales_order_csv_upload_alerts_to_gsheet`)
```python
UPLOAD_MODE = 'append_unique'  # 기본값: 기존 데이터 보존
```
- 기존 데이터 삭제 없음
- `order_daily` 기준으로 중복 제거
- 신규/수정 데이터만 추가

### 5. 업로드 후 검증
```python
# 업로드 완료 후 Google Sheets에서 데이터 재확인
verify_df = save_to_gsheet(df=None, ...)  # 읽기 모드
if len(verify_df) > 0:
    print("[검증] ✅ 업로드 완료!")
```

---

## 📋 파일 구조

### DAG: `sales_store_amount_join_dags.py` (재업로드 모드 추가)
- **정기 실행** (월/수 10:45)
  - 모든 원본 파일 새로 로드
  - 배민 → 토더 → 매출 순서로 JOIN
  - CSV 저장 및 수집파일 이동

- **재업로드 모드** (수동, `reupload_mode=true`)
  - 이전 배민/토더 파일 재사용
  - 새로운 매출 데이터와만 JOIN
  - CSV 저장 및 수집파일 이동

### DAG: `sales_order_csv_upload_alerts_to_gsheet.py` (안전 업로드)
- CSV 파일 존재 확인
- 데이터 유효성 검사
- **append_unique 모드로 업로드** (기존 데이터 보존)
- 업로드 후 검증

---

## 🚀 빠른 체크리스트

### 재업로드 전
- [ ] 1/3 매출 데이터 재계산 완료
- [ ] 파일 저장 위치 확인: `/opt/airflow/영업관리부_DB/sales_daily_orders_upload.csv`
- [ ] `E:\down\업로드_temp` 폴더에 이전 배민/토더 파일 확인

### 재업로드 실행
- [ ] Airflow UI 접속
- [ ] `sales_store_amount_join_dags` DAG 선택
- [ ] "Trigger DAG" 클릭
- [ ] 파라미터 입력: `{"reupload_mode": true}`
- [ ] "Trigger" 버튼 클릭

### 재업로드 후
- [ ] DAG 실행 완료 확인 (초록색 ✓)
- [ ] `sales_order_csv_upload_alerts_to_gsheet` DAG 자동 실행 대기
- [ ] Google Sheets 업로드 완료 확인
- [ ] 1/1, 1/2, 1/4~1/10 데이터 유지 확인
- [ ] 1/3 데이터 업데이트됨 확인

---

## ⚠️ 주의사항

### DO (하세요)
✅ 재업로드 전에 새 매출 데이터 검증
✅ `reupload_mode=true` 파라미터 정확히 입력
✅ DAG 실행 로그 확인
✅ Google Sheets에서 최종 데이터 확인

### DON'T (하지 마세요)
❌ 매출 CSV 파일 없이 DAG 실행
❌ 빈 CSV 파일 업로드
❌ `UPLOAD_MODE`를 `overwrite`로 변경 (데이터 손실!)
❌ Google Sheets 수동 삭제 (자동화된 접근이 좋습니다)

---

## 📞 트러블슈팅

### Q: DAG 실행 시 "CSV 파일 없음" 에러
**A:** `/opt/airflow/영업관리부_DB/sales_daily_orders_upload.csv` 파일 존재 확인
- 파일이 없으면 DAG가 의도적으로 중단됨 (데이터 손실 방지)
- 매출 데이터를 먼저 생성 후 재시도

### Q: `업로드_temp` 폴더에 파일이 없음
**A:** 이전 `sales_store_amount_join_dags` 실행이 완료되지 않았거나 파일이 이동됨
- 정기 실행(월/수 10:45)을 기다렸다가 재업로드 시작
- 또는 원본 파일을 재다운로드 후 정기 실행으로 처리

### Q: Google Sheets에 데이터가 안 보임
**A:** 다음 중 하나 확인
1. `sales_order_csv_upload_alerts_to_gsheet` DAG 실행 상태 확인
2. Google Sheets 탭이 올바른지 확인 (현재: "시트1")
3. 로그에서 업로드 오류 메시지 확인

### Q: 기존 데이터가 삭제됨
**A:** 이미 손실된 상황입니다. 다음 재업로드부터 주의
- `UPLOAD_MODE`가 `append_unique`인지 확인
- 이전 버전(overwrite)이 실행되었을 가능성
- 신규 설정 파일 확인

---

## 📊 모니터링

### Airflow UI에서 확인
1. DAG 선택 후 "Graph View" → 각 Task 상태 확인
2. "Code" 탭 → 소스 코드 확인 (UPLOAD_MODE 설정 등)
3. "Logs" 탭 → 실행 로그 확인

### Google Sheets에서 확인
1. 데이터 행 수 확인
2. 각 날짜별 데이터 확인
3. 중복 행 없음 확인

---

## 🔄 정기 실행 vs 재업로드 모드

| 항목 | 정기 실행 (월/수 10:45) | 재업로드 모드 |
|------|----------------------|-------------|
| 트리거 | 자동 스케줄 | 수동 (UI) |
| 배민 데이터 | 새로 다운로드 | 이전 파일 재사용 |
| 토더 데이터 | 새로 다운로드 | 이전 파일 재사용 |
| 매출 데이터 | 새로 계산 | 새로 계산 (필수) |
| JOIN 방식 | 모두 JOIN | 수정된 매출만 JOIN |
| Google Sheets | append_unique | append_unique |
| 기존 데이터 | 보존 | 보존 |
| 사용 시나리오 | 정상적인 일일 업데이트 | 누락 데이터 보정 |

---

## 💡 추가 정보

### 파일 경로 (Docker 내부 기준)
```
/opt/airflow/
├── 영업관리부_DB/
│   └── sales_daily_orders_upload.csv        # 최종 JOIN 결과
├── download/
│   └── 업로드_temp/                        # 이전 계산 결과
│       ├── baemin_metrics_*.csv
│       ├── baemin_change_history_*.csv
│       └── toorder_review_*.csv
└── dags/
    └── etl/
        ├── sales_store_amount_join_dags.py          # JOIN 및 CSV 생성
        └── sales_order_csv_upload_alerts_to_gsheet.py  # Google Sheets 업로드
```

### Google Sheets 설정
- **URL**: https://docs.google.com/spreadsheets/d/1JJSPLuqAgSSVaXQjZUwdBug-IyBouwUlsXHZiE20VZU/edit
- **시트명**: "시트1"
- **주요 컬럼**: order_daily (날짜, PK)
- **업로드 모드**: append_unique (중복 제거 기준 = order_daily)

---

**마지막 업데이트**: 2026-01-15
**담당**: 데이터 엔지니어링
