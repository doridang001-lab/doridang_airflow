# unified_sales 검증 알림 개편 (매장 unique 카운트 + 심각도순 리스트 + 쿠팡/배민/기타 상세 + 분할 발송)

## Task
unified_sales 일별/월별 검증 Telegram 알림을 개편한다. 현재 카운트가 "행 수"(오차 매장 1곳당 3행 → 30건=10매장)라 오해를 주고, 20행 truncation이 매장을 중간에 자르며, 채널 상세가 배민/쿠팡만 보여 총합 차이를 설명하지 못한다. 또한 4096자 초과 시 Telegram이 400을 반환해 알림이 조용히 사라지는 버그가 있다. → 카운트를 매장 unique(곳)로, 상단에 심각도순 오차 매장 리스트, 하단에 총합/쿠팡/배민/기타 정합 상세, 한도 초과 시 여러 메시지로 분할 발송하도록 고친다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정** `modules/transform/utility/notifier.py` — 분할 발송 유틸 `send_telegram_chunks` 추가 (기존 `send_telegram`은 불변)
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_validate.py` — `_ALERT_CHANNELS`, `_alert_summary_rows`, `_format_telegram_rows`, `_build_telegram_message`, `_send_alert`, `_send_monthly_alert` 수정 + `_format_store_list` 신규

## Implementation Steps

### 1. `notifier.py` — `send_telegram_chunks` 신규
`send_telegram` 아래에 추가. `"\n"` 단위로 라인을 묶어 각 청크 ≤ `limit`(기본 4000)으로 순차 발송.
```python
def send_telegram_chunks(text: str, limit: int = 4000) -> bool:
    lines = (text or "").split("\n")
    chunks: list[str] = []
    buf: list[str] = []
    cur = 0
    for line in lines:
        # 단일 라인이 limit 초과인 극단 케이스 → 하드 슬라이스
        while len(line) > limit:
            if buf:
                chunks.append("\n".join(buf)); buf = []; cur = 0
            chunks.append(line[:limit]); line = line[limit:]
        add = len(line) + (1 if buf else 0)
        if cur + add > limit:
            chunks.append("\n".join(buf)); buf = [line]; cur = len(line)
        else:
            buf.append(line); cur += add
    if buf:
        chunks.append("\n".join(buf))
    ok = True
    for chunk in chunks:
        ok = send_telegram(chunk) and ok
    return ok
```

### 2. `_ALERT_CHANNELS` (validate.py 상단 상수)
`["총합", "배민", "쿠팡"]` → `["총합", "쿠팡", "배민", "기타"]`

### 3. `_alert_summary_rows` — 기타 버킷 + 심각도 정렬
- `total_rows`(총합, 매장별 excel/unified/difference/error_rate) 계산은 유지.
- 채널 상세를 오차 매장 diff에서 생성. `채널 ∈ {배민,쿠팡}`은 유지, 나머지는 전부 `"기타"`로 매핑 후 합산:
```python
    ed = diff.merge(error_keys, on=[date_col, "store"], how="inner")
    ed["channel"] = ed["channel"].where(ed["channel"].isin(["배민", "쿠팡"]), "기타")
    ch_sum = ed.groupby([date_col, "store", "channel"], as_index=False)[["excel_total", "unified_total"]].sum()

    alert_channels = pd.DataFrame({"channel": ["쿠팡", "배민", "기타"]})
    channel_rows = error_keys.merge(alert_channels, how="cross")
    channel_rows = channel_rows.merge(ch_sum, on=[date_col, "store", "channel"], how="left")
    channel_rows[["excel_total", "unified_total"]] = (
        channel_rows[["excel_total", "unified_total"]].fillna(0).round().astype(int)
    )
```
  → `쿠팡+배민+기타 = 총합` 구조적 보장(동일 diff 파생).
- concat 후 difference/error_rate/reason/status 계산은 기존 로직 유지.
- **정렬**: 매장별 abs(총합 difference)를 `_sev`로 매핑 → `_sev` 내림차순 정렬, 그 안에서 채널을
  `pd.Categorical(details["channel"], categories=_ALERT_CHANNELS, ordered=True)`로 정렬.
```python
    sev = total_rows.assign(_sev=total_rows["difference"].abs())[[date_col, "store", "_sev"]]
    details = details.merge(sev, on=[date_col, "store"], how="left")
    details["channel"] = pd.Categorical(details["channel"], categories=_ALERT_CHANNELS, ordered=True)
    details = details.sort_values(["_sev", "store", "channel"], ascending=[False, True, True]).reset_index(drop=True)
    details["channel"] = details["channel"].astype(str)
    details = details.drop(columns=["_sev"])
```
- 반환 df 순서를 그대로 유지(하류 재정렬 금지).

### 4. `_send_alert` / `_send_monthly_alert` — 재정렬 제거 + 분할 발송
- `.sort_values(["store", "channel"], ...)` **제거**(심각도 순서 보존). 컬럼 select만 유지.
- `send_telegram(message)` → `send_telegram_chunks(message)`.
- import: `from modules.transform.utility.notifier import send_telegram, send_telegram_chunks`

### 5. `_format_store_list(df, *, date_col)` — 신규 (상단 심각도순 리스트)
```python
def _format_store_list(df: pd.DataFrame, *, date_col: str) -> str:
    total = df[df["channel"] == "총합"].copy()
    total = total.sort_values("difference", key=lambda s: s.abs(), ascending=False)
    lines = []
    for i, (_, row) in enumerate(total.iterrows(), 1):
        lines.append(
            f"{i}. {row['store']}  차이 {int(row['difference']):+,} ({float(row['error_rate']):.1f}%)"
        )
    return "\n".join(lines)
```

### 6. `_format_telegram_rows` — truncation 제거
- `limit` 인자 제거, 입력 df 순서(심각도순) 그대로 전량 출력.
- 행 포맷 유지: `- {date} {store} / {channel}: DB {u:,} / 토더 {e:,} / 차이 {d:,} / {reason}`

### 7. `_build_telegram_message` — 카운트/리스트/상세 재구성
```python
def _build_telegram_message(title, target_label, error_rows, csv_path, *, date_col):
    win_path, _ = _to_win_file_uri(csv_path)
    store_count = error_rows["store"].nunique()
    store_list = _format_store_list(error_rows, date_col=date_col)
    rows_text = _format_telegram_rows(error_rows, date_col=date_col)
    return (
        f"{title}\n{target_label}\n"
        f"오차율 2% 이상 매장: {store_count}곳\n"
        f"CSV: {win_path}\n\n"
        f"■ 오차 매장 ({store_count}곳)\n{store_list}\n\n"
        f"■ 상세내역\n{rows_text}"
    )
```

## Reference Code

### modules/transform/utility/notifier.py (기존 send_telegram — 시그니처 불변)
```python
def send_telegram(text: str) -> bool:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return False
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("Telegram alert sent")
        return True
    except Exception as e:
        logger.warning("Telegram send failed (ignored): %s", e)
        return False
```

### modules/transform/pipelines/db/DB_UnifiedSales_validate.py (현재 관련 부분)
```python
from modules.transform.utility.notifier import send_telegram
_ALERT_CHANNELS = ["총합", "배민", "쿠팡"]

def _reason(unified_total: int, excel_total: int) -> str:
    if unified_total > 0 and excel_total == 0: return "ToOrder 누락"
    if unified_total == 0 and excel_total > 0: return "DB 누락"
    return "금액상이"

def _format_telegram_rows(df, *, date_col, limit=20):  # limit 제거 예정
    lines = []
    for _, row in df.head(limit).iterrows():
        channel = row.get("channel", ""); reason = row.get("reason", "")
        lines.append(
            f"- {row.get(date_col,'')} {row.get('store','')} / {channel}: "
            f"DB {int(row.get('unified_total',0)):,} / 토더 {int(row.get('excel_total',0)):,} / "
            f"차이 {int(row.get('difference',0)):,} / {reason}"
        )
    if len(df) > limit: lines.append(f"... 외 {len(df)-limit}건")
    return "\n".join(lines)

def _build_telegram_message(title, target_label, error_rows, csv_path, *, date_col):
    win_path, _ = _to_win_file_uri(csv_path)
    rows_text = _format_telegram_rows(error_rows, date_col=date_col)
    return (f"{title}\n{target_label}\n오차율 2% 이상: {len(error_rows)}건\n"
            f"CSV: {win_path}\n{rows_text}")

# _alert_summary_rows: error_keys = grouped[(error_rate>=2)&(|diff|>=100000)][[date_col,store]]
#   total_rows(=총합) + channel_rows(배민,쿠팡 cross+merge) concat → 매장당 3행
#   현재 categories=_ALERT_CHANNELS 로 정렬, sort_values([date_col,store,channel])
# _send_alert / _send_monthly_alert: display = error_rows[...].sort_values(["store","channel"]) → send_telegram(message)
```

## Test Cases
1. [notifier import] `python -c "from modules.transform.utility.notifier import send_telegram_chunks; print('ok')"` → 기대: `ok`, ImportError 없음
2. [chunker 분할] `python -c "import modules.transform.utility.notifier as n; n.send_telegram=lambda t: (globals().setdefault('C',[]).append(len(t)) or True); n.send_telegram_chunks('a\n'*3000, limit=4000); print(all(x<=4000 for x in n.C), len(n.C)>=2)"` → 기대: `True True` (모든 청크 ≤4000, 2개 이상)
3. [정합성+정렬] 더미 diff(오차 매장 3곳, 총합≠배민+쿠팡 → 기타 잔여 존재)로 `_alert_summary_rows(diff, date_col="ym")` 호출 → 매장별 (쿠팡+배민+기타) excel_total 합 == 총합 excel_total, unified_total도 동일; 매장 순서가 abs(총합 difference) 내림차순 → 기대: 정합 True, 순서 심각도순
4. [메시지 포맷] `_build_telegram_message(...)` 반환 문자열에 `오차율 2% 이상 매장: 3곳`, `■ 오차 매장 (3곳)`, `■ 상세내역` 포함 및 매장 블록이 `총합→쿠팡→배민→기타` 순 → 기대: 모두 present
5. [validate import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import validate_sales, validate_monthly_sales; print('ok')"` → 기대: `ok`, ImportError 없음
6. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag; print('ok')"` → 기대: `ok` (DAG 파싱 정상)

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `send_telegram`의 시그니처/동작은 절대 변경 금지 (다른 DAG 콜러 다수 의존) — 새 함수 `send_telegram_chunks`만 추가.
- `기타 = 총합 − 쿠팡 − 배민`은 반드시 동일 `diff`에서 파생시켜 `쿠팡+배민+기타 == 총합` 정합 유지 (별도 재조회로 계산 금지).
- `_send_alert`/`_send_monthly_alert`에서 `sort_values(["store","channel"])` 재정렬 제거 — `_alert_summary_rows`가 반환한 심각도순을 반드시 보존.
- 상단 매장 리스트는 항상 **전체** 표시(truncation 없음). 상세도 truncation 제거, 길이는 분할 발송으로 처리.
- 채널 순서 총합→쿠팡→배민→기타 고정. Korean 문자열 정렬로 재정렬하지 말 것.
- 감사만 하고 수정하지 않는 항목(범위 밖): 토더=0·DB>0 비대칭 미검출, 매장 총합 ±100,000 상쇄 미검출.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기(수정)
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
