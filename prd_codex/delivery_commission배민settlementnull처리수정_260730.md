# delivery_commission 배민 settlement null 처리 수정

## Task

delivery_commission 마트의 배민 행 `settlement_amount`가 123행 null이다. 원인은 우가클(광고비) 미수집이 아니라 `입금예정금액` 미수집이며, 현재 코드가 주문 1건만 미수집이어도 그 매장×날짜 정산금액 전체를 `pd.NA`로 비운다. 이를 **수집된 주문만 합산(미수집 주문은 0 취급)** 으로 바꾸고, 배민 행의 우가클 지표 3종 null도 0으로 채운다.

실측 근거(배민 8,379행):

| 케이스 | 행 수 | 현재 settlement |
|---|---|---|
| 우가클 없음 + 입금예정금액 있음 | 5,037 | 정상 계산 (deposit − 0, 이미 정상) |
| 우가클 있음 + 입금예정금액 없음 | 99 | null |
| 우가클 없음 + 입금예정금액 없음 | 24 | null |

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

- 수정: `modules/transform/pipelines/db/DB_DeliveryCommission.py`
- 수정: `tests/test_delivery_commission.py`
- 신규 생성 파일 없음. `dags/db/DB_DeliveryCommission_Dags.py`는 수정하지 않는다.

## Implementation Steps

1. `_load_baemin_orders_agg()` — 정산 미수집 매장×날짜 blanking 제거 (현재 258-298행)
   - `unsettled` 마스크 계산과 `logger.warning`은 **유지**한다. 단 문구를 "정산금액 비움" →
     미수집 주문을 0으로 보고 부분 합산한다는 의미로 바꾸고, 미수집 주문수/전체 주문수를 함께 남긴다.
   - 삭제할 3곳:
     - `grouped["_unsettled"] = unsettled`
     - 매장×날짜 groupby agg의 `_unsettled=("_unsettled", "max")`
     - `grouped.loc[grouped["_unsettled"], "입금예정금액"] = pd.NA` 와 `grouped.drop(columns="_unsettled")`
   - **유지**: `grouped["입금예정금액"] = grouped["입금예정금액"].fillna(0)` — 이 fillna가 부분 합산의 근거다.
   - `_nullable_money(df["입금예정금액"])`과 주문 단위 `입금예정금액=("입금예정금액", "max")` 집계는 그대로 둔다(미수집 탐지용).
   - 마지막 dtype: NA가 더는 생기지 않으므로
     `grouped["baemin_deposit_amt"].round().astype("Int64")` → `.round().astype(int)`.

2. `build_delivery_commission()` 배민 분기 — 우가클 지표 0 채우기 (현재 611-626행)
   - `baemin["ad_spend"] = baemin["ad_spend"].fillna(0)` → `.fillna(0).round().astype(int)`
     (settlement를 int64로 유지)
   - `rename(...)` **이전에** 우가클 원본 3종을 0으로 채운다:
     - `baemin["wgc_avg_cost"] = baemin["wgc_avg_cost"].fillna(0)`  (float64 유지)
     - `baemin["wgc_orders"] = baemin["wgc_orders"].fillna(0).astype("Int64")`
     - `baemin["wgc_ctr"] = baemin["wgc_ctr"].fillna(0)`
   - 쿠팡 분기의 `우가클_평균비용/주문수/클릭율` = `NA` 세팅은 **절대 건드리지 않는다**(플랫폼 구분용).
   - `_finalize`, `OUTPUT_COLUMNS`는 수정하지 않는다. `settlement_amount`/`diff_amt` dtype이
     `Float64` → `int64`로 바뀌는 것은 의도된 결과다.

3. `tests/test_delivery_commission.py` 수정 — 기존 헬퍼(`_configure_paths`, `_write_baemin_order`,
   `_write_baemin_ad`, `_write_coupang_order`, `_write_coupang_cmg`)를 그대로 재사용한다.
   - `test_baemin_keeps_sales_and_blanks_settlement_when_positive_order_settlement_is_blank` (현재 373행)
     → 이름을 `test_baemin_settlement_sums_collected_orders_when_some_are_blank`로 바꾸고
     부분 합산 기대로 재작성. 같은 brand/store/date에 정산 8,000 주문 + 정산 빈칸(`""`) 주문을 넣고
     `baemin_deposit_amt == 8_000`, `total_amt`는 두 주문 합계임을 검증. 기존 `pd.isna(...)` 단정은 삭제.
   - `test_wgc_metrics_are_null_for_coupang_and_zero_clicks` (현재 277행)
     → 이름을 `test_wgc_metrics_are_zero_for_baemin_and_null_for_coupang`으로 바꾸고
     `assert pd.isna(baemin["우가클_평균비용"])` → `assert baemin["우가클_평균비용"] == 0`.
     쿠팡 NA 단정 라인은 유지.
   - 신규 테스트 추가: 우가클 CSV에 행이 없는 배민 매장×날짜에서
     `settlement_amount == 입금예정금액`(−0)이고 우가클 3종이 0인지 검증.
     `_validate_source_brands`가 도리당·나홀로 두 브랜드 광고 원천을 모두 요구하므로,
     도리당 광고 행은 `ad_date="2026-07-20"`으로 기록해 07-19 도리당 행만 우가클 미수집으로 만든다.

## Reference Code

### modules/transform/pipelines/db/DB_DeliveryCommission.py (현재 `_load_baemin_orders_agg` 후반부)

```python
    grouped["date"] = _baemin_order_date(grouped["주문시각"])
    grouped["store"] = _normalize_store_name(grouped["brand"], grouped["store"])
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]

    unsettled = grouped["총결제금액"].gt(0) & grouped["입금예정금액"].isna()
    grouped["_unsettled"] = unsettled                       # ← 삭제
    if unsettled.any():
        unsettled_keys = grouped.loc[unsettled, ["date", "store", "brand"]].drop_duplicates()
        samples = ", ".join(
            f"{row.date}/{row.store}/{row.brand}"
            for row in unsettled_keys.head(10).itertuples(index=False)
        )
        logger.warning(                                     # ← 문구 수정
            "baemin 정산정보 미수집 날짜×매장×브랜드 정산금액 비움: %s개 | sample=%s",
            len(unsettled_keys), samples,
        )

    grouped["입금예정금액"] = grouped["입금예정금액"].fillna(0)   # ← 유지
    if grouped.empty:
        raise RuntimeError("baemin orders 집계 결과 없음; 기존 마트 보존")

    grouped = grouped.groupby(["date", "store", "brand"], as_index=False).agg(
        결제금액=("결제금액", "sum"),
        즉시할인_파트너부담=("즉시할인_파트너부담", "sum"),
        입금예정금액=("입금예정금액", "sum"),
        _unsettled=("_unsettled", "max"),                   # ← 삭제
    )
    grouped.loc[grouped["_unsettled"], "입금예정금액"] = pd.NA  # ← 삭제
    grouped = grouped.drop(columns="_unsettled")            # ← 삭제
    grouped = grouped.rename(columns={
        "결제금액": "total_amt",
        "즉시할인_파트너부담": "baemin_partner_instant_discount",
        "입금예정금액": "baemin_deposit_amt",
    })
    grouped["total_amt"] = grouped["total_amt"].round().astype(int)
    grouped["baemin_partner_instant_discount"] = grouped["baemin_partner_instant_discount"].round().astype(int)
    grouped["baemin_deposit_amt"] = grouped["baemin_deposit_amt"].round().astype("Int64")  # ← astype(int)
    return grouped[columns]
```

### modules/transform/pipelines/db/DB_DeliveryCommission.py (현재 배민 분기)

```python
    baemin_orders = _load_baemin_orders_agg()
    baemin_ads = _load_baemin_ad_spend_agg()
    baemin = baemin_orders.merge(baemin_ads, on=["date", "store", "brand"], how="left")
    if not baemin.empty:
        baemin["ad_spend"] = baemin["ad_spend"].fillna(0)      # ← .round().astype(int) 추가
        # ← 여기서 wgc_avg_cost / wgc_orders / wgc_ctr fillna(0)
        baemin["settlement_amount"] = baemin["baemin_deposit_amt"] - baemin["ad_spend"]
        baemin["platform"] = "배달의민족"
        baemin = baemin.rename(
            columns={"baemin_partner_instant_discount": "배민_즉시할인", **wgc_columns}
        )
        baemin["쿠팡_신규비율"] = pd.Series(float("nan"), index=baemin.index, dtype="float64")
        ...
```
`wgc_columns = {"wgc_avg_cost": "우가클_평균비용", "wgc_orders": "우가클_주문수", "wgc_ctr": "우가클_클릭율"}`
`_load_baemin_ad_spend_agg` 산출: `ad_spend`(int), `wgc_avg_cost = ad_spend/clicks`(clicks 0이면 NaN),
`wgc_orders`(Int64), `wgc_ctr = clicks/impressions`(impressions 0이면 NaN).

### tests/test_delivery_commission.py (헬퍼 시그니처)

```python
def _write_baemin_order(root, brand, store, total, deposit, *, order_id="공통주문번호",
                        order_time="2026. 07. 19. (일) 오후 12:00:00", status="배달완료",
                        payment=None, partner_instant_discount=0) -> None: ...
    # deposit=""  → 입금예정금액 미수집 케이스

def _write_baemin_ad(root, brand, store_name, spend, *, ad_date="2026-07-19",
                     impressions=1_000, clicks=50, orders=5) -> None: ...
    # root=delivery.BAEMIN_OUR_STORE_CLICKS_DB, store_name은 브랜드 포함 원문명
```

## Test Cases

1. [단위 테스트 전체] `python -m pytest tests/test_delivery_commission.py -q` → 기대: 전체 PASS, 실패 0
2. [부분 합산] `python -m pytest tests/test_delivery_commission.py -q -k "settlement_sums_collected"` → 기대: PASS
3. [우가클 0] `python -m pytest tests/test_delivery_commission.py -q -k "wgc_metrics"` → 기대: PASS
4. [DAG import] `python -c "from dags.db.DB_DeliveryCommission_Dags import dag; print(dag.dag_id)"` → 기대: `DB_DeliveryCommission_Dags`, ImportError 없음
5. [실측 재빌드] `python -c "from modules.transform.pipelines.db.DB_DeliveryCommission import build_delivery_commission as b; print(b())"` → 기대: `delivery_commission N행 -> ...` 정상 종료
6. [null 소거 확인]
   ```
   python -c "import pandas as pd; from modules.transform.utility.paths import DELIVERY_COMMISSION_PATH as p; d=pd.read_parquet(p); m=d[d.platform=='배달의민족']; c=d[d.platform=='쿠팡이츠']; print(len(m), m.settlement_amount.isna().sum(), m['우가클_주문수'].isna().sum(), m['우가클_평균비용'].isna().sum(), c['우가클_주문수'].isna().all(), d['settlement_amount'].dtype, d['diff_amt'].dtype)"
   ```
   → 기대: 배민 settlement null `0`, 배민 우가클_주문수/평균비용 null `0`, 쿠팡 우가클 전부 NA `True`, dtype `int64 int64`
7. [기존 값 불변] 재빌드 전 마트를 복사해두고 비교 → 기대: 기존 non-null 8,256행의 `settlement_amount` 값 변화 없음, 정산 미수집 123행만 새로 값이 채워짐
   ```
   python -c "import pandas as pd; a=pd.read_parquet(r'C:\Users\민준\AppData\Local\Temp\claude\before.parquet'); b=pd.read_parquet(__import__('modules.transform.utility.paths',fromlist=['x']).DELIVERY_COMMISSION_PATH); k=['sale_date','store','platform','brand']; j=a.merge(b,on=k,suffixes=('_a','_b')); m=j[j.platform=='배달의민족']; print(len(m[m.settlement_amount_a.notna() & (m.settlement_amount_a!=m.settlement_amount_b)]))"
   ```
   → 기대: `0`

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- 쿠팡 분기의 우가클 3종 `NA`, 배민 분기의 쿠팡 지표 6종 `NA`는 플랫폼 구분용이므로 0으로 채우지 않는다.
- `OUTPUT_COLUMNS` 목록과 컬럼 순서를 변경하지 않는다(Power BI 소비처 있음).
- `_write_parquet_atomic`, `_finalize`의 키 중복·플랫폼·브랜드 검증 로직을 완화하지 않는다.
- 정산 미수집 `logger.warning`은 삭제하지 않는다 — 부분 합산 후에도 미수집 식별 수단이 이 로그뿐이다.
- `_money`/`_nullable_money` 헬퍼의 동작을 바꾸지 않는다(쿠팡 경로가 공유).
- 알려진 부작용(수용됨): 클릭수 0인데 광고지출>0인 날은 `우가클_평균비용`이 NaN 대신 0으로 표시된다.
- 알려진 부작용(수용됨): 배민 정산이 아직 확정되지 않은 최근 2~3일은 부분 합산 결과가 0에 가까워
  `diff_amt`가 매출 전액이 된다. 코드로 보정하지 않고 로그로만 식별한다.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
