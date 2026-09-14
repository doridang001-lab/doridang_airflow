# 쿠팡배치 orders — silent 상세패널 hang 조기탈출 + 즉시재시도 이중낭비 제거 + 페이스 느슨화

## Task
64계정 쿠팡이츠 배치 수집 성공률이 50% 미만. 원인은 **주문 목록·개수·매출 기대값은 정상인데 상세패널(`.order-details .order-detail-item`)만 조용히 안 열리는 "silent hang"** 이다. 상세가 계통적으로 안 열리면 `_parseOrderItem`(3 attempt) × `_collectCurrentPageData`(누락 3 round) 재시도가 페이지당 ~15~20분으로 폭발 → runner `COLLECT_TIMEOUT=900000`(15분)이 먼저 터져 **수집분 전부 폐기(0/0)**. 직후 `4.5) orders 즉시 재시도`가 같은 막힌 세션으로 **또 15분** 소모 → 계정당 30분+ 낭비, 뒤 계정 미도달로 성공률 붕괴. 이 hang은 `_isOrdersThrottleVisible()`/`_isOrdersHardThrottleVisible()` 어느 쪽도 감지 못해 기존 blocked-skip(PRD 260629 B)이 못 잡는다.

**해결(사용자 승인: 빠른 실패 + 페이스 완화):** (1) silent 상세-hang을 조기 감지해 15분→~1분으로 줄이고 backfill로 넘김, (2) timeout 케이스의 즉시-재시도 생략, (3) PRD 260629 Section A(미적용) 페이스 느슨화로 누적 throttle 진입을 늦춤.

## Project (Chrome Extension — 순수 vanilla JS, Python 아님)
작업 정본 = 사용자가 실제 로드하는 **개발용 확장**:
`C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\`
빌드 사본에도 미러링(존재 파일 기준): `C:\airflow\coupang_extension_build\`

> ⚠️ build 사본 `content/`에는 현재 `06_batch.js`만 있고 `03_coupangeats.js`가 없음(부분 사본). 실제 로드본은 개발용이므로 **개발용을 정본**으로 수정하고, build에 동일 파일이 있으면 diff 0으로 맞춘다. 없으면 신규 생성하지 말 것.

## Conventions
- 순수 vanilla JS(빌드 스텝 없음). `console.log` / `Utils.updateProgressModal({debug:...})` 패턴 유지.
- 2-space 들여쓰기, 기존 따옴표·세미콜론·변수명 스타일 그대로. 새 플래그는 camelCase.
- 기존 함수 시그니처/네이밍 유지. 기존 헬퍼 재사용(새 감지 로직 만들지 말 것): `_markOrdersTransientError`, `_hadRecentOrdersTransientError`, `_getExpectedOrderCount`, `_isOrdersHardThrottleVisible`.
- 개발용 ↔ build 사본의 `content/03_coupangeats.js`, `runner.js`는 동일 유지.

## Files to Modify
- `content/03_coupangeats.js` (개발용 + build 사본)
- `runner.js` (개발용 + build 사본)

경로:
- 개발용: `C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\content\03_coupangeats.js`, `...\runner.js`
- 빌드: `C:\airflow\coupang_extension_build\content\03_coupangeats.js`(있으면), `...\runner.js`

## Implementation Steps

### 1) silent 상세-hang 조기탈출 (핵심) — `03_coupangeats.js`
`_collectCurrentPageData`(약 L1919~2016) 메인 파싱 루프에서 상세패널이 계통적으로 안 열리는지 조기 판정.

- 루프 진입 전 `let capturedCount = 0; let consecutiveFail = 0; let detailBlocked = false;` 선언.
- 메인 for 루프(약 L1954~1978) 안, `_parseOrderItem` 결과 처리부에서:
  - `res.captured` 성공 시 `capturedCount++; consecutiveFail = 0;`
  - 실패 시 `consecutiveFail++;`
  - 매 항목 처리 후 조기탈출 조건 검사:
    ```js
    // 주문은 있는데(기대값>0) 상세만 계통적으로 안 열림 → silent hang. 15분 폭주 방지.
    if (consecutiveFail >= 5 && capturedCount === 0 && this._getExpectedOrderCount() > 0) {
      detailBlocked = true;
      this._markOrdersTransientError();
      Utils.updateProgressModal({ debug: `🚫 ${currentPage}페이지 상세패널 연속 실패 ${consecutiveFail}건 — silent 차단 판정, 조기 종료(백필)` });
      break;   // 남은 항목 파싱 중단
    }
    ```
- 누락 재수집 루프(약 L1980 `for round`)는 `if (detailBlocked) missing = [];` 로 **진입 금지**(systemic이면 3 round 반복 무의미).
- 반환 객체에 `detailBlocked` 추가:
  ```js
  return { rows: pageRows, pageComplete: pageComplete && !detailBlocked, missingOrderIds, stableTimedOut, detailBlocked };
  ```
- `_collectOrdersPages`(약 L2873~) 의 구조분해에 `detailBlocked` 추가하고, `stableTimedOut` 처리 직후 분기 삽입:
  ```js
  const { rows: pageData, pageComplete, missingOrderIds, stableTimedOut, detailBlocked } = await this._collectCurrentPageData(shopInfo, iso, currentPage);
  ...
  if (detailBlocked) {
    this._markOrdersTransientError();
    hasError = true;   // tryDateRestart 폭주 대신 즉시 종료 → 상위에서 fail/transient(backfill)
    break;
  }
  ```
  (이 분기는 `if (stableTimedOut) {...}` 블록 **다음**, `if (pageData.length === 0) {...}` **앞**에 넣는다. 이미 수집된 `pageData`는 `allRows`에 반영하지 않고 바로 종료해도 무방 — 어차피 부분 페이지라 backfill 재수집 대상.)

임계값 `consecutiveFail >= 5`, `capturedCount === 0`은 초기값(튜닝 여지). "앞쪽 5건 연속 실패 + 성공 0"이면 그 페이지 전체가 막힌 것으로 간주.

### 2) 즉시-재시도 이중낭비 제거 — `runner.js`
`4.5) orders 즉시 재시도`(약 L948~977): 직전 orders 실패가 **timeout**(이미 15분 소진)이면 같은 세션 재시도가 또 15분 낭비 → 생략하고 backfill에 맡김.

- 즉시재시도 진입 조건에 timeout 제외 추가. 현재:
  ```js
  if (!skip.orders && !ordersOk && !stopRequested) {
  ```
  → 변경:
  ```js
  // 직전 orders가 timeout(15분 소진)이면 같은 막힌 세션 재시도 무의미 → 백필로 이관
  const prevOrdersTimedOut = !!(op && op.timeout);
  if (!skip.orders && !ordersOk && !stopRequested && !prevOrdersTimedOut) {
    ... (기존 즉시재시도 로직 유지)
  } else if (!skip.orders && !ordersOk && !stopRequested && prevOrdersTimedOut) {
    log(`[${acc.id}] orders 타임아웃 → 즉시재시도 보류(백필 큐로 이관)`, 'info');
    setRow(i, {orders:'warn', note:'재시도 보류(백필)'});
  }
  ```
  > `op`는 최초 orders 수집 payload(약 L818 `ordersResult`/`pending.payload`로 채워진 객체). 스코프상 `op`가 이 블록에서 보이는지 확인 후, 안 보이면 orders 수집부에서 `let ordersTimedOut = op.timeout;` 를 상위 스코프에 저장해 사용.

### 3) 페이스 느슨화 (PRD 260629 Section A 미적용분) — 예방
**3-1. `runner.js` 계정 간 대기 (L37-38)**
```js
// 변경 전
const DELAY_MIN = 3000;
const DELAY_MAX = 6000;
// 변경 후 (6~12초; L1074/L1124/L1221 rand(DELAY_MIN,DELAY_MAX) 공통 적용)
const DELAY_MIN = 6000;
const DELAY_MAX = 12000;
```

**3-2. `03_coupangeats.js` 조회 최소 간격 (`_waitBeforeOrdersSearch`, L121-123)**
```js
// 변경 전
const MIN_SEARCH_INTERVAL_MS = this._isSafeOrdersPacingEnabled()
  ? 15000 + Math.floor(Math.random() * 10001)
  : 10000;
// 변경 후 (safe 22~35초)
const MIN_SEARCH_INTERVAL_MS = this._isSafeOrdersPacingEnabled()
  ? 22000 + Math.floor(Math.random() * 13001)
  : 10000;
```

**3-3. `03_coupangeats.js` `_manualOrdersPace` (L63-71)** — batch 1.15/1.35 → 1.25/1.5, manual 1.2배
```js
_manualOrdersPace(min, max) {
  if (this._collectSource === 'manual') {
    return this._randomDelay(Math.floor(min * 1.2), Math.floor(max * 1.2));
  }
  if (this._collectSource === 'batch') {
    const batchMin = Math.max(Math.floor(min * 1.25), min + 300);
    const batchMax = Math.max(Math.floor(max * 1.5), max + 700);
    return this._randomDelay(batchMin, batchMax);
  }
  return Promise.resolve();
},
```

**3-4. `03_coupangeats.js` `_humanizeManualOrdersAfterPage` (L173-181)** — 추가휴식 확률/대기 상향
```js
async _humanizeManualOrdersAfterPage() {
  if (!this._isSafeOrdersPacingEnabled()) return;
  const isBatch = this._collectSource === 'batch';
  await this._randomDelay(isBatch ? 2200 : 1800, isBatch ? 5500 : 5000);
  if (Math.random() < (isBatch ? 0.28 : 0.30)) {
    Utils.updateProgressModal({ debug: 'batch page transition stabilization wait' });
    await this._randomDelay(isBatch ? 5500 : 6500, isBatch ? 11000 : 13000);
  }
},
```

## Reference Code (현재 상태 — 이 블록만으로 수정 위치 특정 가능)

### content/03_coupangeats.js — 페이스/throttle 헬퍼 (현재)
```js
_manualOrdersPace(min, max) {
  if (this._collectSource === 'manual') return this._randomDelay(min, max);
  if (this._collectSource === 'batch') {
    const batchMin = Math.max(Math.floor(min * 1.15), min + 250);
    const batchMax = Math.max(Math.floor(max * 1.35), max + 600);
    return this._randomDelay(batchMin, batchMax);
  }
  return Promise.resolve();
},
_isOrdersHardThrottleVisible() {
  const bodyText = document.body?.innerText || '';
  return bodyText.includes('10057')
    || bodyText.includes('과도한 요청으로 인해 서비스 이용이 일시적으로 제한되었습니다');
},
_markOrdersTransientError() { this._lastOrdersTransientErrorAt = Date.now(); },
_hadRecentOrdersTransientError(windowMs = 120000) {
  return Date.now() - this._lastOrdersTransientErrorAt < windowMs;
},
async _waitBeforeOrdersSearch() {
  const MIN_SEARCH_INTERVAL_MS = this._isSafeOrdersPacingEnabled()
    ? 15000 + Math.floor(Math.random() * 10001)
    : 10000;
  // ... (throttle 감지 후 cooldown 대기)
},
async _humanizeManualOrdersAfterPage() {
  if (!this._isSafeOrdersPacingEnabled()) return;
  const isBatch = this._collectSource === 'batch';
  await this._randomDelay(isBatch ? 1800 : 1500, isBatch ? 4500 : 4500);
  if (Math.random() < (isBatch ? 0.2 : 0.22)) {
    Utils.updateProgressModal({ debug: 'batch page transition stabilization wait' });
    await this._randomDelay(isBatch ? 4500 : 6000, isBatch ? 10000 : 12000);
  }
},
```

### content/03_coupangeats.js — `_collectCurrentPageData` 메인 루프 + 누락 재수집 (현재, 약 L1954~2016)
```js
for (let i = 0; i < totalItems; i++) {
  if (this._stopFlag) break;
  Utils.updateProgressModal({ message: `${currentPage}페이지 수집 중... (${i + 1}/${totalItems}건)` });
  try {
    const res = await this._parseOrderItem(orderItems[i], shopInfo, iso);
    await this._manualOrdersPace(450, 1350);
    await this._humanizeManualOrdersAfterItem(i);
    const key = res.order_id || `__idx_${i}`;
    if (res.captured) { orderMap.set(key, res); }
    else { missing.push({ item: orderItems[i], order_id: res.order_id, key }); }
  } catch (err) {
    Utils.updateProgressModal({ debug: `❌ 주문 ${i + 1} 수집 오류: ${err.message}` });
    await this._closeErrorPopups();
    await this._manualOrdersPace(800, 1800);
    missing.push({ item: orderItems[i], order_id: '', key: `__idx_${i}` });
  }
}
for (let round = 0; round < 3 && missing.length && !this._stopFlag; round++) {
  Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 매출액 미수집 ${missing.length}건 - 세부 재수집 ${round + 1}/3` });
  // ... 각 missing 항목 _parseOrderItem 재시도
}
const pageRows = [];
for (const res of orderMap.values()) { pageRows.push(...res.rows); }
const missingOrderIds = missing.map((it, idx) => it.order_id || `unknown-${idx + 1}`);
const pageComplete = missingOrderIds.length === 0;
return { rows: pageRows, pageComplete, missingOrderIds, stableTimedOut };
```

### content/03_coupangeats.js — `_collectOrdersPages` 페이지 루프 상단 (현재, 약 L2873~2904)
```js
while (!this._stopFlag) {
  const currentPage = this._getCurrentPage();
  pageCount++;
  Utils.updateProgressModal({ currentPage, rowCount: allRows.length, message: `${currentPage}페이지 수집 중...` });
  const { rows: pageData, pageComplete, missingOrderIds, stableTimedOut } = await this._collectCurrentPageData(shopInfo, iso, currentPage);
  const restartRows = pageData.length ? [...allRows, ...pageData] : allRows;
  const expectedCount = this._getExpectedOrderCount();
  ...
  if (stableTimedOut) {
    this._markOrdersTransientError();
    if (!await tryDateRestart(`❌ ${currentPage}페이지 안정화 타임아웃 - 날짜 재시작 시도`, restartRows)) { hasError = true; }
    break;
  }
  if (pageData.length === 0) { ... }
```

### runner.js — 상수 + orders 수집/즉시재시도 (현재)
```js
const COLLECT_TIMEOUT = 900000;  // orders 수집 완료 대기 (15분)
const DELAY_MIN = 3000;
const DELAY_MAX = 6000;
// ... 3) orders 수집
let ordersResult = await waitMultistore('orders');   // 약 L818
const op = (pending && pending.payload) || ordersResult || {};
// ... 4.5) orders 즉시 재시도 (약 L948~977)
if (!skip.orders && !ordersOk && !stopRequested) {
  log(`[${acc.id}] orders 실패 → CMG 완료 직후 즉시 재시도 (로그인 유지)`, 'info');
  await chrome.tabs.update(workTabId, {url: 'https://store.coupangeats.com/merchant/management/orders'});
  const ordersNav = await waitUrl(workTabId, {orders: RE_ORDERS}, 25000);
  if (ordersNav && !stopRequested) {
    await waitForPageLoadWithRetry(workTabId, 'orders 재시도 페이지');
    await injectContent(workTabId);
    await sleep(1500);
    try { await chrome.tabs.sendMessage(workTabId, {type:'COLLECT', source:'batch', targetStores}); } catch(e) {}
    const retryResult = await waitMultistore('orders', COLLECT_TIMEOUT);
    // ...
  }
}
```

## Test Cases (JS 확장 — 빌드/유닛테스트 없음 → 정적 검증 + 수동 확인)
1. 문법(개발용 + build 각 파일):
   `node --check "content/03_coupangeats.js"` → 기대: 출력 없음, exit 0
   `node --check "runner.js"` → 기대: 출력 없음, exit 0
2. 조기탈출 반영: `03_coupangeats.js`에 `detailBlocked` 선언/반환/수신, `consecutiveFail >= 5` 존재 → grep 각 1건 이상.
3. 즉시재시도 보류: `runner.js`에 `prevOrdersTimedOut` 및 `재시도 보류(백필)` 존재.
4. 페이스 상수: `runner.js`에 `DELAY_MIN = 6000` / `DELAY_MAX = 12000`; `03_coupangeats.js`에 `22000 + Math.floor(Math.random() * 13001` 및 `_manualOrdersPace` batch `1.25`/`1.5` 존재.
5. 개발용 ↔ build 동기화(존재 파일만): `git diff --no-index "개발용\content\03_coupangeats.js" "build\content\03_coupangeats.js"` → 차이 없음 (build에 파일 있을 때만).
6. 수동(사람): 확장 reload → 소수(10~15) 계정 배치 → 로그 확인:
   - silent hang 계정이 15분 대신 ~1분 내 `상세패널 연속 실패 … 조기 종료(백필)` 로 넘어감
   - timeout 계정에서 `즉시재시도 보류(백필 큐로 이관)` 출력, 15분 재소모 없음
   - `다음 계정까지 6~12초` 대기
   - 전체 완료 성공률 상승(>50%)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (특히 node --check 문법 오류, op 스코프)
  3. 코드 수정
  4. 전체 재실행
종료 조건: TC 1~5 전체 PASS + 개발용/build 동기화 + Constraints 위반 없음
(TC6은 사람이 확장 로드 후 확인 — Codex는 코드까지만)
```

## Constraints
- 개발용을 정본으로 수정하고, build 사본은 **존재하는 파일만** diff 0으로 동기화. build에 없는 파일 신규 생성 금지.
- silent hang 조기탈출은 "주문 존재(`_getExpectedOrderCount() > 0`) + 상세 성공 0 + 연속 실패 임계"일 때만 발동 — 정상 대용량 매장/0건 매장 오탐 금지.
- `COLLECT_TIMEOUT`(900000) 상수 자체는 축소하지 말 것(정상 상세수집 여유). 조기탈출은 콘텐츠 측에서 먼저 종료 신호를 보내는 방식.
- 기존 throttle/transient 헬퍼 재사용 — 새 감지 로직 만들지 말 것.
- `ordersOk`는 조기탈출/timeout 시에도 false 유지(실제 수집 안 됨). 해당 매장은 backfill 큐/다음 배치에서 재수집.
- 페이스 강도는 "중간"(계정 6~12초, 조회 22~35초, manual 1.2배) — 과하게 늘리지 말 것.

## Do Not Ask — Decide Yourself
- 파일 이미 존재하면: 그대로 Edit(신규 생성 금지).
- 줄 번호가 다르면: 함수명/상수명으로 위치 탐색해 수정.
- `op` 스코프가 즉시재시도 블록에서 안 보이면: orders 수집부에서 `ordersTimedOut` 플래그를 상위 스코프에 저장해 사용.
- 들여쓰기·따옴표·세미콜론: 해당 파일 기존 스타일과 동일하게.
- 주석: 최소화(WHY만).
- 변수·플래그명: camelCase, 기존 파일과 동일하게.
