# 📊 sales_daily_orders_alerts 컬럼 추가 가이드

## ✅ 완료된 작업

### 1. **실오픈일** 컬럼 추가
- ✅ `sales_orders` (line 650): col 리스트에 '실오픈일' 추가 완료
- ✅ `sales_daily_orders` (line 2087): column_order에 '실오픈일' 추가 완료
- ✅ `sales_daily_orders_alerts` (line 1686-1715): groupby에 `min('실오픈일')` 추가 완료

### 2. **platform** 컬럼 추가
- ✅ `sales_orders` (line 650): col 리스트에 'platform' 추가 완료
- ✅ `sales_daily_orders` (line 2087): column_order에 'platform' 추가 완료
- ✅ `sales_daily_orders_alerts` (line 1686-1715): groupby에 `lambda x: ','.join(sorted(x.dropna().astype(str).unique()))` 추가 완료

### 3. **settlement_amount** 컬럼 추가
- ✅ `sales_daily_orders` (line 2087): column_order에 'settlement_amount' 추가 완료
- ✅ `sales_daily_orders_alerts` (line 1686-1715): groupby에 `sum('settlement_amount')` 추가 완료

---

## 📝 코드 위치 및 설명

### 🔹 1단계: aggregate_daily_sales 함수 (Line 1686-1715)

**파일**: `c:\airflow\modules\transform\pipelines\sales_daily_orders.py`

**위치**: 1686번 라인부터 시작

```python
# ============================================================
# ⭐ sales_daily_orders_alerts.csv 집계 설정
# ============================================================
# 📌 여기서 groupby와 agg를 수정하여 추가 컬럼을 집계합니다.
#
# 【추가할 컬럼】
# 1. 실오픈일: min('실오픈일') - 매장의 최초 오픈일 (날짜가 여러개면 최소값)
# 2. platform: lambda x: ','.join(sorted(x.dropna().unique())) - 플랫폼 목록 (배민,쿠팡 등)
# 3. settlement_amount: sum('settlement_amount') - 정산금액 합계
#
# 【수정 방법】
# .agg() 안에 다음 3줄을 추가하세요:
#     실오픈일=('실오픈일', 'min'),  # 가장 오래된 오픈일
#     platform=('platform', lambda x: ','.join(sorted(x.dropna().astype(str).unique()))),  # 플랫폼 목록
#     settlement_amount=('settlement_amount', 'sum'),  # 정산금액 합계
# ============================================================

daily_agg = orders_df.groupby(['order_daily', '매장명_clean', '담당자', 'email']).agg(
    total_order_count=('order_id', 'nunique'),
    total_amount=('total_amount', 'sum'),
    fee_ad=('fee_ad', 'sum'),
    # ============================================================
    # ⭐ 여기 아래에 추가 집계 컬럼을 넣으세요 ⭐
    # ============================================================
    실오픈일=('실오픈일', 'min'),  # 매장 최초 오픈일
    platform=('platform', lambda x: ','.join(sorted(x.dropna().astype(str).unique()))),  # 플랫폼 목록 (배민,쿠팡)
    settlement_amount=('settlement_amount', 'sum')  # 정산금액 합계
).reset_index()
```

**설명**:
- `min('실오픈일')`: 같은 매장의 여러 주문 중 가장 오래된 오픈일 선택
- `lambda x: ','.join(...)`: 배민, 쿠팡 등 여러 플랫폼을 쉼표로 연결 (예: "배민,쿠팡")
- `sum('settlement_amount')`: 일별 정산금액 합계

---

### 🔹 2단계: calculate_scores 함수 (Line 2068-2087)

**파일**: `c:\airflow\modules\transform\pipelines\sales_daily_orders.py`

**위치**: 2068번 라인부터 시작

```python
# ============================================================
# ⭐ sales_daily_orders.csv 컬럼 순서 (Parquet 저장용)
# ============================================================
# 📌 여기에 최종 저장할 컬럼 순서를 정의합니다.
# 이 리스트에 없는 컬럼은 CSV에 저장되지 않습니다!
#
# 【추가된 컬럼】
# - 실오픈일: 매장 오픈일 (aggregate_daily_sales에서 min으로 집계)
# - platform: 플랫폼 목록 (aggregate_daily_sales에서 쉼표로 연결)
# - settlement_amount: 정산금액 합계 (aggregate_daily_sales에서 sum으로 집계)
# ============================================================
column_order = [
    'order_daily', '매장명', '담당자', 'email',
    'total_order_count', 'total_amount', 'fee_ad', 'ARPU',
    'ma_14', 'ma_28', 'weekday',
    'prev_week_same_day', 'current_avg_2week', 'prev_2week_same_day',
    'prev_3week_same_day', 'current_avg_4week',
    'sum_7d_recent', 'sum_7d_prev',
    'score_trend', 'score_total', 'score_7d_total', 'score_4week_total', 'score',
    'status', 'pre_status', 'uploaded_at',
    '금일여부', '전일여부', 
    '전일_매출', '전일_주문건수', '전일대비_증감액', '전일대비_증감률',
    '전주_매출', '전주_주문건수', '전주대비_증감액', '전주대비_증감률',
    '전월_매출', '전월_주문건수', '전월대비_증감액', '전월대비_증감률',
    
    # ⭐ 새로 추가된 컬럼 (오른쪽 끝)
    '실오픈일', 'platform', 'settlement_amount'
]
```

**설명**:
- 이 리스트 순서대로 CSV 파일에 저장됩니다
- 오른쪽 끝에 새 컬럼 3개 추가 완료

---

### 🔹 3단계: 검증 로직 추가 (Line 1716-1737)

**위치**: 1716번 라인부터 시작

```python
# ============================================================
# ⭐ 추가된 컬럼 검증
# ============================================================
print(f"\n[검증] 추가 컬럼 확인:")
if '실오픈일' in daily_agg.columns:
    print(f"  ✅ 실오픈일: {daily_agg['실오픈일'].notna().sum()}건 존재")
    print(f"     샘플: {daily_agg['실오픈일'].dropna().head(3).tolist()}")
else:
    print(f"  ❌ 실오픈일 컬럼 없음!")

if 'platform' in daily_agg.columns:
    print(f"  ✅ platform: {daily_agg['platform'].notna().sum()}건 존재")
    print(f"     샘플: {daily_agg['platform'].dropna().head(3).tolist()}")
else:
    print(f"  ❌ platform 컬럼 없음!")

if 'settlement_amount' in daily_agg.columns:
    total_settlement = daily_agg['settlement_amount'].sum()
    print(f"  ✅ settlement_amount: 총합계 {total_settlement:,.0f}원")
    print(f"     평균: {daily_agg['settlement_amount'].mean():,.0f}원")
else:
    print(f"  ❌ settlement_amount 컬럼 없음!")
```

**설명**:
- DAG 실행 시 로그에서 3개 컬럼이 제대로 생성되었는지 확인 가능
- 샘플 데이터로 값이 정상적으로 들어갔는지 검증

---

## 🚀 테스트 방법

### 1. DAG 실행
```bash
# Airflow UI에서 sales_load_baemin_data DAG 수동 실행
```

### 2. 로그 확인
DAG 실행 후 `aggregate_daily_sales` task 로그에서 다음 메시지 확인:

```
[검증] 추가 컬럼 확인:
  ✅ 실오픈일: 150건 존재
     샘플: ['2024-01-15', '2024-02-20', '2024-03-10']
  ✅ platform: 150건 존재
     샘플: ['배민,쿠팡', '배민', '쿠팡']
  ✅ settlement_amount: 총합계 5,234,567원
     평균: 34,897원
```

### 3. CSV 파일 확인
```bash
# 저장된 CSV 확인
cat C:\airflow\LOCAL_DB\영업관리부_DB\sales_daily_orders_alerts.csv | head
```

**기대 결과**:
- 마지막 3개 컬럼: `실오픈일`, `platform`, `settlement_amount`가 추가되어 있어야 함
- `platform` 컬럼: "배민,쿠팡" 형태로 쉼표로 구분된 값
- `settlement_amount`: 숫자 값 (정산금액 합계)

---

## 🔍 문제 해결

### ❌ 컬럼이 CSV에 저장되지 않는 경우

**원인**:
1. `aggregate_daily_sales`의 groupby에 추가했는지 확인
2. `calculate_scores`의 column_order에 추가했는지 확인

**해결**:
- 위 가이드의 1단계, 2단계를 다시 확인하여 두 곳 모두 추가되었는지 검증

### ❌ 값이 비어있는 경우

**원인**:
- 원본 데이터(`sales_orders`)에 해당 컬럼이 없거나 NaN인 경우

**해결**:
1. `preprocess_join_orders_with_stores` 함수(Line 650) 확인
2. employee CSV에 '실오픈일' 컬럼이 있는지 확인
3. 배민/쿠팡 전처리에서 'platform' 컬럼이 추가되었는지 확인

---

## 📌 요약

| 단계 | 파일 위치 | 작업 내용 | 완료 여부 |
|------|----------|----------|----------|
| 1 | Line 1686-1715 | aggregate_daily_sales groupby에 3개 컬럼 추가 | ✅ |
| 2 | Line 2068-2087 | calculate_scores column_order에 3개 컬럼 추가 | ✅ |
| 3 | Line 1716-1737 | 검증 로직 추가 (로그 출력) | ✅ |
| 4 | Line 650 | sales_orders col 리스트에 컬럼 추가 | ✅ |

**다음 실행 시 확인사항**:
- ✅ Airflow 로그에서 검증 메시지 확인
- ✅ CSV 파일 마지막 3개 컬럼 확인
- ✅ Google Sheets 업로드 시 3개 컬럼 포함되었는지 확인

---

**작성일**: 2026-01-12  
**수정자**: GitHub Copilot
