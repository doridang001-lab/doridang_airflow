# OKPOS daily 교차검증 mismatch 알림 정리 (order 신뢰)

## Task
`[OKPOS daily 교차검증]` 텔레그램 알림이 `daily < order` 상황에서 오탐으로 뜬다. 실측 결과 `unified_sales`에는 이미 order+receipt 병합값(정답)이 저장돼 있고, `daily`(일자별 종합매출 페이지)는 **정산 지연으로 거의 하루 지나야 완전해지는 참고값**일 뿐이다. order를 신뢰 소스로 확정하고 `order > daily`(diff>0)일 때 mismatch 알림을 억제한다. 저장 로직·저장값은 변경하지 않는다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- env 플래그: `os.getenv("OKPOS_...", 기본값)` 패턴 (기존 gate 로직과 동일)

## Files to Create / Modify
- 수정: `C:\airflow\modules\transform\pipelines\db\DB_UnifiedSales_okpos.py`
  - 함수 `_okpos_daily_gate` 의 mismatch 분기 (현재 대략 라인 1055–1090)
- 생성 파일 없음.

## Implementation Steps

1. `_okpos_daily_gate` 의 `else:`(mismatch) 분기에서 **late-hour PASS 체크 직후, 알림 append 전에** "order 신뢰" 게이트를 추가한다.

   기존:
   ```python
   else:
       msg = (...)  # mismatch 메시지
       alert_reason = "mismatch"
       late_total = _okpos_late_hour_total(group)
       if diff > 0 and abs(diff - late_total) <= tol:
           logger.info("... PASS(late-hour diff) ...")
           continue
       logger.info(msg.replace("\n", " | "))
       fp = hashlib.md5(...).hexdigest()
       if _should_send_gate_alert(store_short, date_str, fp):
           pending_alerts.append({... "reason": "mismatch" ...})
       else:
           logger.info("gate alert 스로틀 스킵: ...")
       if strict_gate:
           keep.loc[group.index] = False
   ```

   수정 (late-hour PASS `continue` 다음 줄에 삽입):
   ```python
       # order+receipt 병합값이 신뢰 소스. daily 종합매출 페이지는 정산 지연으로
       # 익일까지 과소보고되므로 diff>0(order>daily)는 예상된 지연 → 알림 억제.
       trust_order = os.getenv("OKPOS_DAILY_TRUST_ORDER", "1").strip() != "0"
       if trust_order and diff > 0:
           logger.info(
               "OKPOS daily 교차검증 PASS(order 신뢰, daily 정산지연 과소보고): %s | %s | daily=%s order=%s diff=%s",
               date_str, store_short, daily_net, order_total, diff,
           )
           continue
       # 여기 도달 = diff < 0 (order 과소수집 의심) → 기존처럼 알림/스로틀/strict_gate 처리
   ```

2. 나머지 분기(`daily_net==0`, `order_total==0`, `abs(diff)<=tol` PASS)는 **변경하지 않는다**. 0원 방지 로직 유지.

3. `os`, `hashlib`, `logger`는 이미 import돼 있음 — 추가 import 불필요.

## Reference Code
### modules/transform/pipelines/db/DB_UnifiedSales_okpos.py (`_okpos_daily_gate` mismatch 분기 발췌)
```python
        diff = order_total - int(daily_net)
        if abs(order_total) <= tol:
            alert_reason = "order_total_zero"
            ...
            keep.loc[group.index] = False
        elif abs(diff) <= tol:
            logger.info("OKPOS daily 교차검증 PASS: %s | %s | daily=%s order=%s",
                        date_str, store_short, daily_net, order_total)
        else:
            msg = ("[OKPOS daily 교차검증] daily/order mismatch\n"
                   f"date={date_str}\nstore={store_short}\n"
                   f"daily_net={daily_net}\norder_total={order_total}\ndiff={diff}\n"
                   f"strict_gate={int(strict_gate)}")
            alert_reason = "mismatch"
            late_total = _okpos_late_hour_total(group)
            if diff > 0 and abs(diff - late_total) <= tol:
                logger.info("OKPOS daily 교차검증 PASS(late-hour diff): ...")
                continue
            # ← 여기에 order 신뢰 게이트 삽입 (Implementation Step 1)
            logger.info(msg.replace("\n", " | "))
            fp = hashlib.md5(f"{date_str}|{store_short}|{daily_net}|{order_total}|{alert_reason}".encode()).hexdigest()
            if _should_send_gate_alert(store_short, date_str, fp):
                pending_alerts.append({
                    "store": store_short, "reason": alert_reason,
                    "daily_net": daily_net, "order_total": order_total,
                    "diff": diff, "strict_gate": int(strict_gate),
                })
            else:
                logger.info("gate alert 스로틀 스킵: %s | %s | reason=%s", date_str, store_short, alert_reason)
            if strict_gate:
                keep.loc[group.index] = False

    _send_okpos_daily_gate_alert_summary(pending_alerts, date_str)
    dropped = int((~keep).sum())
    if dropped:
        logger.warning("OKPOS daily 교차검증 제외: %s | rows=%d", date_str, dropped)
    return df[keep].reset_index(drop=True)
```

## Test Cases
1. [import] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_okpos as m; print('ok')"` → 기대: `ok`, ImportError/SyntaxError 없음
2. [diff>0 억제 + 저장값 불변] 아래 스크립트 실행 →
   ```python
   python -c "
   import pandas as pd, logging
   logging.basicConfig(level=logging.INFO)
   from pathlib import Path
   from modules.transform.utility.paths import MART_DB
   from modules.transform.pipelines.db.DB_UnifiedSales_okpos import _okpos_daily_gate
   df = pd.read_parquet(Path(MART_DB)/'unified_sales_grp'/'unified_sales_260715.parquet')
   df = df[df['source']=='okpos'].reset_index(drop=True)
   out = _okpos_daily_gate(df, '2026-07-15')
   sub = out[out['store'].astype(str).str.contains('송파삼전')]
   print('rows_in', len(df), 'rows_out', len(out))
   print('송파삼전 total_price', int(pd.to_numeric(sub['total_price'],errors='coerce').sum()))
   "
   ```
   기대: 로그에 `PASS(order 신뢰, daily 정산지연 과소보고)` 다수, `[OKPOS daily 교차검증] 알림` 텔레그램 발송 로그 **없음**, `rows_in == rows_out`(드롭 0), 송파삼전 total_price `1268800` 유지.
3. [diff<0 회귀] 인위적으로 daily > order 인 그룹을 만들거나(테스트용 daily_csv), diff<0 케이스가 있는 다른 날짜에서 실행 → 기대: 여전히 `mismatch` alert append (억제되지 않음).

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→2→3 순서대로 실행
  2. FAIL 항목 → 원인 분석 (삽입 위치/조건식 확인)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 저장값(`total_price`, 반환 DataFrame 행)은 변경 금지 — diff>0에서 행 드롭 절대 없음.
- `daily_net==0`, `order_total==0`, late-hour PASS 등 기존 분기 로직 유지.
- `reconcile_against_daily_summary`(daily=정답 가정) 등 다른 함수는 건드리지 않음.
- 신규 env 기본값 `OKPOS_DAILY_TRUST_ORDER=1`(활성), `0`으로 되돌림 가능.
- `run_okpos`/`run_lookback_okpos`/`backfill_okpos` 모두 동일 gate 호출 → 별도 수정 불필요.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
