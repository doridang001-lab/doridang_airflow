# 쿠팡이츠 CMG 자동수집 — prompt 블록 제거 + -2~-1 2일치 수집

## Task

쿠팡이츠 자동수집(runner 대시보드)에서 CMG 페이지(`store.coupangeats.com/merchant/management/cmg`)가 수동수집용 `prompt()` 창에 걸려 5분 타임아웃으로 실패한다. 이 블록을 제거하고, CMG를 매 실행마다 **어제 하루가 아닌 -2 ~ -1 (2일치)** 재수집해 하루 실패 시 다음 실행이 자동으로 메꾸도록 한다. 단, 2일치로 바꾸면 "잘못된 날짜 라벨" 버그가 새로 노출되므로 날짜 적용 검증을 함께 넣는다.

## Project Conventions

**이 작업은 Python/Airflow가 아니라 크롬 확장(MV3, 순수 JS) 작업이다.**

- 작업 폴더(유일): `C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용`
- 답변과 작업 설명은 한글로 한다.
- 수정은 **최소 범위**로 한다. 리팩터링·정리·포맷 변경 금지.
- 로그는 `Utils.updateProgressModal({ debug: '...' })` 사용 (기존 패턴 그대로). `console.log` 신규 추가 금지.
- 문자열은 백틱 템플릿 리터럴, 들여쓰기 2칸 — 주변 코드와 동일하게.
- `content/03_coupangeats.js`는 `Sites['coupangeats'] = { ... }` 객체 리터럴이다. 새 헬퍼는 `메서드명(...) { }` / `상수명: 값,` 형태로 객체 프로퍼티로 추가한다.
- 수동수집(`source: 'manual'`)과 대시보드 배치수집(`source: 'batch'`)의 분리를 절대 깨뜨리지 않는다.
- 중복 백업 파일(`*-DESKTOP-*`, `*.bak_*`)은 수정하지 않는다.
- `C:\airflow\coupang_extension_build`(repo 스냅샷)는 이번 작업 대상이 **아니다**.

## Files to Create / Modify

**수정만 (신규 파일 없음)**

- `content/03_coupangeats.js` — 변경 1(배치 게이트 + 2일 범위), 변경 3(날짜 적용 검증)
- `runner.js` — 변경 2(빈 대상목록 시 CMG 스킵)

---

## 배경: 왜 이렇게 고치는가

**원인 1 (prompt 멈춤)**
CMG 날짜 분기가 `_isDashboardBatchOrders(opts)`를 게이트로 쓴다. 이 함수는 `source === 'batch'` **그리고** `targetStores.length > 0` 둘 다여야 `true`다:

```js
// content/03_coupangeats.js:258-260
_isDashboardBatchOrders(opts = {}) {
  return opts.source === 'batch' && Array.isArray(opts.targetStores) && opts.targetStores.length > 0;
},
```

그런데 `runner.js:1064`에서 orders 단계 throttle 처리로 `activeTargetStores`가 `[]`가 될 수 있고, `runner.js:1149`는 그 빈 배열을 그대로 CMG COLLECT에 실어 보낸다 → 게이트 `false` → 자동화 탭에서 `_promptCMGDateRange()`의 `prompt()`가 뜨고 아무도 닫을 수 없어 `CMG_TIMEOUT`(5분) 블록 후 "CMG 타임아웃" 실패.

**원인 2 (누락)**
배치 CMG는 `startDate = endDate = 어제`로 하루만 수집한다. 하루 실패 = 영구 누락.

**⚠️ 선결 결함 — 변경 3이 필수인 이유**
`_collectCMGRowsWithRetry`는 화면이 실제로 그 날짜로 갱신됐는지 **확인하지 않고** 요청한 `dateStr`을 그대로 `조회일자`에 박는다. 그런데 날짜 적용 경로는 실패해도 `true`를 반환한다:
- `_selectDateInCalendar`: "적용" 버튼을 못 찾아도 `ℹ️ 적용 버튼 없음 (자동 적용)` 로그만 남기고 `return true`
- `_navigateToMonth`: prev/next 버튼 없음 / 24회 내 미도달 등 **모든 실패 경로에서 `return true`**

하루치일 땐 무해했다(날짜가 하나뿐이라 비교 대상 없음). **2일치가 되면 2번째 날짜 적용이 조용히 실패할 때 1일차 숫자가 2일차 `조회일자`로 저장된다.** Airflow는 `(매장명, 조회일자)`로 dedup하며 `collected_at` 최신을 우선하므로 이 오염 데이터가 정상 데이터를 **덮어쓴다**. 누락보다 나쁘다.
→ 변경 1·2만 적용하고 변경 3을 빼면 안 된다.

---

## Implementation Steps

### 1. `content/03_coupangeats.js` — CMG 전용 배치 게이트 + 2일 범위 헬퍼 추가

`_resolveBatchTargetDate` / `_batchTargetDateLabel` (현재 `:555-573`) **바로 아래**에 아래 3개를 추가한다. `_resolveBatchTargetDate`는 수정하지 않고 그대로 재사용한다(이미 `opts.targetDate` YYYYMMDD 파싱 + 미지정 시 어제 계산을 갖고 있음).

```js
  // CMG는 누락 방지를 위해 매 실행마다 -2 ~ -1 (2일치)를 재수집한다.
  CMG_BATCH_LOOKBACK_DAYS: 2,

  // CMG 배치 판정 — 주문서와 달리 targetStores가 비어도 batch면 prompt를 띄우지 않는다.
  // (runner에서 throttle로 activeTargetStores가 []가 되면 자동화 탭에 prompt가 떠서 블록됨)
  // 주문서 날짜 클릭 규칙(_isDashboardBatchOrders)은 건드리지 않는다. AGENTS.md:36 CMG 예외 조항.
  _isBatchCMG(opts = {}) {
    return opts.source === 'batch';
  },

  // 배치 CMG 날짜 범위: end = 기준일(어제), start = end - (LOOKBACK - 1)
  _resolveBatchCMGRange(opts = {}) {
    const endDate = this._resolveBatchTargetDate(opts);
    const startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - (this.CMG_BATCH_LOOKBACK_DAYS - 1));
    startDate.setHours(0, 0, 0, 0);
    return { startDate, endDate };
  },
```

### 2. `content/03_coupangeats.js` — `_collectCMGMultiStore` 배치 분기 교체 (현재 `:643-658`)

**BEFORE**
```js
    let startDate, endDate;
    if (this._isDashboardBatchOrders(opts)) {
      const targetDate = this._resolveBatchTargetDate(opts);
      startDate = new Date(targetDate);
      endDate = new Date(targetDate);
      Utils.updateProgressModal({ debug: `📅 배치 수집: ${this._batchTargetDateLabel(opts)} (${this._formatDate(targetDate)})` });
    } else {
      const range = this._promptCMGDateRange(matchingStores.join(', '));
```

**AFTER** (`else` 이하 prompt 블록은 **한 글자도 바꾸지 않는다**)
```js
    let startDate, endDate;
    if (this._isBatchCMG(opts)) {
      ({ startDate, endDate } = this._resolveBatchCMGRange(opts));
      Utils.updateProgressModal({ debug: `📅 배치 수집(누락방지 ${this.CMG_BATCH_LOOKBACK_DAYS}일): ${this._formatDate(startDate)} ~ ${this._formatDate(endDate)}` });
    } else {
      const range = this._promptCMGDateRange(matchingStores.join(', '));
```

> 구조분해 할당은 반드시 괄호로 감쌀 것: `({ startDate, endDate } = ...)`. 괄호 없으면 SyntaxError.

### 3. `content/03_coupangeats.js` — `_collectCMGData` 배치 분기 교체 (현재 `:977-992`, 단일매장 경로)

**BEFORE**
```js
    let startDate, endDate;
    let parseResult = {};

    if (this._isDashboardBatchOrders(opts)) {
      const targetDate = this._resolveBatchTargetDate(opts);
      startDate = new Date(targetDate);
      endDate = new Date(targetDate);
      Utils.updateProgressModal({ debug: `📅 배치 수집: ${this._batchTargetDateLabel(opts)} 날짜 사용 (${this._formatDate(targetDate)})` });
    } else {
```

**AFTER** (`parseResult`는 배치 분기에서 기존처럼 비워 둔다)
```js
    let startDate, endDate;
    let parseResult = {};

    if (this._isBatchCMG(opts)) {
      ({ startDate, endDate } = this._resolveBatchCMGRange(opts));
      Utils.updateProgressModal({ debug: `📅 배치 수집(누락방지 ${this.CMG_BATCH_LOOKBACK_DAYS}일): ${this._formatDate(startDate)} ~ ${this._formatDate(endDate)}` });
    } else {
```

### 4. `runner.js` — 빈 대상목록일 때 CMG 자체를 건너뜀 (현재 `:1105` 부근)

**변경 1과 반드시 함께 가야 한다.** `_targetStoreSet`은 빈 배열에 `null`을 반환하고, `_filterTargetStores`는 `null`일 때 **드롭다운의 도리당/나홀로 전 매장**을 반환한다. 즉 변경 1만 넣으면 `targetStores: []`인 CMG가 prompt 대신 이번엔 *throttle로 제외한 매장까지 전부* 수집한다. 근본 원인은 runner에서 막는다.

`targetDateMode === 'today'` 분기와 `!skip.cmg` 분기 **사이**에 `else if`를 끼워 넣는다.

**BEFORE**
```js
    if (targetDateMode === 'today') {
      cmgOk = true;
      setRow(i, {cmg:'skip', note:'오늘 수집은 CMG 제외'});
      recordCategoryResults('cmg', acc, activeTargetStores, null, true, '오늘 수집은 CMG 제외');
      log(`[${acc.id}] 오늘 수집 — CMG 건너뜀`, 'info');
    } else if (!skip.cmg) {
```

**AFTER**
```js
    if (targetDateMode === 'today') {
      cmgOk = true;
      setRow(i, {cmg:'skip', note:'오늘 수집은 CMG 제외'});
      recordCategoryResults('cmg', acc, activeTargetStores, null, true, '오늘 수집은 CMG 제외');
      log(`[${acc.id}] 오늘 수집 — CMG 건너뜀`, 'info');
    } else if (activeTargetStores.length === 0) {
      // throttle로 대상 매장이 전부 빠진 상태. CMG를 열면 빈 targetStores가
      // _filterTargetStores의 전체매장 폴백을 타므로 아예 건너뛴다.
      // 제외된 매장은 orders 단계의 markStoresBlocked로 이미 백필 큐에 있다.
      cmgOk = true;
      setRow(i, {cmg:'skip', note:'대상 매장 없음(throttle 제외됨)'});
      recordCategoryResults('cmg', acc, activeTargetStores, null, true, '대상 매장 없음');
      log(`[${acc.id}] CMG 건너뜀 (대상 매장 없음)`, 'info');
    } else if (!skip.cmg) {
```

`recordCategoryResults`는 `targetStores.forEach`만 하므로 빈 배열에 안전하다(`runner.js:278`).

### 5. `content/03_coupangeats.js` — 날짜 적용 검증 (필수)

죽은 코드로 남아 있는 `_readCMGDateRange(doc)` (`:797-819`, **현재 호출부 0개**)를 되살려 쓴다. 이미 `.calendar-dropdown-input`의 `.start-date` / `.end-date`를 읽어 `YY.MM.DD`를 파싱하는 완성된 함수다. **함수 자체는 수정하지 않는다.**

#### 5-a. `_collectCMGForStore` 재시도 루프 (현재 `:916-930`)

**BEFORE**
```js
      let dateSuccess = false;
      for (let retry = 0; retry < 2; retry++) {
        const calendarBtn = doc.querySelector('.calendar-dropdown-input');
        if (!calendarBtn) { Utils.updateProgressModal({ debug: '❌ 달력 버튼 없음' }); break; }

        calendarBtn.click();
        await this._randomDelay(500, 800);

        const clicked = await this._selectDateInCalendar(doc, targetDate);
        if (!clicked) { Utils.updateProgressModal({ debug: `❌ ${dateStr} 선택 실패` }); continue; }

        Utils.updateProgressModal({ debug: `✅ ${dateStr} 선택 완료` });
        dateSuccess = true;
        break;
      }
```

**AFTER**
```js
      let dateSuccess = false;
      for (let retry = 0; retry < 2; retry++) {
        const calendarBtn = doc.querySelector('.calendar-dropdown-input');
        if (!calendarBtn) { Utils.updateProgressModal({ debug: '❌ 달력 버튼 없음' }); break; }

        calendarBtn.click();
        await this._randomDelay(500, 800);

        const clicked = await this._selectDateInCalendar(doc, targetDate);
        if (!clicked) { Utils.updateProgressModal({ debug: `❌ ${dateStr} 선택 실패` }); continue; }

        // 달력이 실제로 목표 날짜로 바뀌었는지 확인.
        // 미검증 시 이전 날짜 데이터가 이번 날짜의 조회일자로 저장된다
        // (적용버튼 미발견·월이동 실패도 _selectDateInCalendar가 true를 반환하므로).
        const applied = this._readCMGDateRange(doc);
        if (!applied || this._formatDate(applied.endDate) !== dateStr) {
          Utils.updateProgressModal({ debug: `⚠️ ${dateStr} 미반영 (화면: ${applied ? this._formatDate(applied.endDate) : '읽기실패'}) → 재시도` });
          continue;
        }

        Utils.updateProgressModal({ debug: `✅ ${dateStr} 선택 완료` });
        dateSuccess = true;
        break;
      }
```

#### 5-b. `_collectCMGData` 재시도 루프 (현재 `:1026-1050`)

동일한 검증 블록을 `const clicked = ...` 의 `if (!clicked) { ... continue; }` **직후**, `dateSuccess = true;` **앞**에 삽입한다. 이 루프의 성공 로그는 `✅ ${dateStr} 선택 및 적용 완료`이며 문구는 그대로 둔다.

`dateSuccess`가 `false`로 남으면 기존 로직이 그 날짜를 **건너뛴다**(`❌ ${dateStr} 전체 실패, 다음 날짜로`) → 오염 행을 만들지 않는다. 누락은 다음 실행의 -2 재수집이 복구한다. 수동수집 7일치 경로도 같은 코드를 타므로 함께 안전해진다.

---

## Reference Code

### content/03_coupangeats.js — `_resolveBatchTargetDate` (`:555-573`, 수정 금지 · 재사용)
```js
  _resolveBatchTargetDate(opts = {}) {
    const raw = String(opts.targetDate || '').trim();
    let m = raw.match(/^(\d{4})(\d{2})(\d{2})$/) || raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (m) {
      const d = new Date(parseInt(m[1], 10), parseInt(m[2], 10) - 1, parseInt(m[3], 10));
      if (!isNaN(d.getTime())) {
        d.setHours(0, 0, 0, 0);
        return d;
      }
    }
    const d = new Date();
    if (opts.targetDateMode !== 'today') d.setDate(d.getDate() - 1);
    d.setHours(0, 0, 0, 0);
    return d;
  },

  _batchTargetDateLabel(opts = {}) {
    return opts.targetDateMode === 'today' ? '오늘' : '어제';
  },
```

### content/03_coupangeats.js — `_readCMGDateRange` (`:797-819`, 수정 금지 · 변경 5에서 호출)
```js
  _readCMGDateRange(doc) {
    const calBtn = doc.querySelector('.calendar-dropdown-input');
    if (!calBtn) {
      Utils.updateProgressModal({ debug: '❌ 달력 읽기 실패 — .calendar-dropdown-input 없음' });
      return null;
    }
    const startTxt = calBtn.querySelector('.start-date')?.textContent.trim();
    const endTxt = calBtn.querySelector('.end-date')?.textContent.replace(/[~\s]/g, '').trim();
    const parseYYMMDD = (s) => {
      if (!s) return null;
      const [y, m, d] = s.split('.');
      if (!y || !m || !d) return null;
      const dt = new Date(2000 + parseInt(y), parseInt(m) - 1, parseInt(d));
      return isNaN(dt.getTime()) ? null : dt;
    };
    const startDate = parseYYMMDD(startTxt);
    const endDate = parseYYMMDD(endTxt);
    if (!startDate || !endDate) {
      Utils.updateProgressModal({ debug: `❌ 달력 읽기 실패 — start:"${startTxt ?? ''}" end:"${endTxt ?? ''}"` });
      return null;
    }
    return { startDate, endDate };
  },
```

### content/03_coupangeats.js — 날짜 루프 (`:660-662`, 수정 불필요 · 이미 다일 지원)
```js
    const dates = [];
    const cur = new Date(startDate);
    while (cur <= endDate) { dates.push(new Date(cur)); cur.setDate(cur.getDate() + 1); }
```

### content/03_coupangeats.js — 파일명 생성 (`:733-740`, 수정 불필요 · 자동으로 범위 파일명 전환)
```js
      const filenameDateStr = dates.length === 1
        ? this._formatDate(dates[0]).replace(/-/g, '')
        : `${this._formatDate(dates[0]).replace(/-/g, '')}-${this._formatDate(dates[dates.length - 1]).replace(/-/g, '')}`;

      const filename = await Utils.downloadCSV(csv, {
        channel: 'coupangeats', purpose: 'cmg',
        storeName, storeId: '', dateStr: filenameDateStr
      });
```

### content/03_coupangeats.js — `_filterTargetStores` / `_targetStoreSet` (`:524-537`, 변경 2가 필요한 근거)
```js
  _targetStoreSet(opts = {}) {
    if (!Array.isArray(opts.targetStores) || opts.targetStores.length === 0) return null;
    return new Set(opts.targetStores.map(s => this._normalizeStoreName(s)).filter(Boolean));
  },

  _filterTargetStores(storeNames, opts = {}) {
    const targetSet = this._targetStoreSet(opts);
    const brandStores = (storeNames || []).filter(storeName => this._isTargetBrandStoreName(storeName));
    if (!targetSet) return brandStores;   // ← 빈 targetStores면 전 브랜드 매장 반환
    ...
```

### runner.js — CMG COLLECT 전송 (`:1149`, 수정 불필요 · 참고)
```js
          try { await chrome.tabs.sendMessage(workTabId, {type:'COLLECT', source:'batch', targetStores: activeTargetStores, targetDate, targetDateMode}); }
```

### 하지 않는 것 — `CMG_TIMEOUT` 상향
`runner.js:37`의 `const CMG_TIMEOUT = 300000;` 주석이 이미 *"CMG는 6일치 × 4초"* 기준이다. 매장당 하루 약 4~5초 × 2일 + 매장선택 1초 ≈ 매장당 11초라 300초 안에 충분히 끝난다. 올리면 진짜 hang 감지만 늦어지고, 재시도 3회 구조상 계정당 최악 대기가 15분 → 21분으로 늘어난다. **이 상수는 건드리지 않는다.**

---

## Test Cases

1. **문법 검사 (필수, 자동화 가능)**
   ```
   cd "C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용"
   node --check content/03_coupangeats.js
   node --check runner.js
   ```
   → 기대: 두 명령 모두 출력 없이 exit 0

2. **`_isDashboardBatchOrders` 미변조 확인**
   ```
   rg -n "_isDashboardBatchOrders" content/03_coupangeats.js
   ```
   → 기대: 정의부 1건 + orders 경로 호출부만 남고, **CMG 경로(`_collectCMGMultiStore` / `_collectCMGData`)에는 0건**

3. **`_readCMGDateRange` 호출부 생성 확인**
   ```
   rg -n "_readCMGDateRange" content/03_coupangeats.js
   ```
   → 기대: 정의 1건 + 호출 **2건**(`_collectCMGForStore`, `_collectCMGData`)

4. **prompt 경로 보존 확인**
   ```
   rg -n "_promptCMGDateRange|_parseDateRange" content/03_coupangeats.js
   ```
   → 기대: 정의 2건 + 호출 2건(둘 다 `else` 수동수집 분기 안). 배치 분기에 없어야 함

5. **날짜 계산 로직 단위 검증** (`node -e`로 헬퍼 로직만 복제 실행)
   ```
   node -e "const e=new Date(2026,7,1);e.setHours(0,0,0,0);const s=new Date(e);s.setDate(s.getDate()-1);console.log(s.toDateString(),'|',e.toDateString())"
   ```
   → 기대: `Fri Jul 31 2026 | Sat Aug 01 2026` (월 경계에서 전월로 정상 역산)

6. **자동수집 실전 (수동 확인)** — `chrome://extensions/` 리로드 후 `runner.html`에서 계정 1개 실행
   - CMG 단계에서 prompt가 **뜨지 않아야** 함
   - debug 로그: `📅 배치 수집(누락방지 2일): 2026-07-28 ~ 2026-07-29`
   - 진행 표시가 `1/2`, `2/2` 두 날짜를 돌고, `⚠️ ... 미반영` 경고가 없어야 함

7. **결과물 검증 (수동 확인)** — `E:/down/업로드_temp/coupangeats_cmg_<매장>_20260728-20260729.csv`
   - `조회일자` 컬럼에 **서로 다른 두 날짜**가 존재
   - 두 날짜의 `광고비용`/`전체매출`이 **완전히 동일하지 않을 것** (동일하면 변경 5의 검증이 뚫린 것 → 실패로 간주)

8. **수동수집 회귀 (수동 확인)** — CMG 페이지에서 팝업 `▶ 지금 수집 실행`
   → 기대: 날짜범위 prompt가 **정상적으로 뜸** (AGENTS.md:36 규칙 보존)

9. **주문서 회귀 (수동 확인)** — 주문서 페이지에서 어제가 아닌 날짜 선택 후 수동수집
   → 기대: 날짜가 어제로 다시 클릭되지 **않음**. 다시 클릭되면 실패 (AGENTS.md:55)

## Verification Loop

구현 완료 후 아래 루프를 **Test Cases 1~5 전부 PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행 (전부 CLI로 자동 검증 가능)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 1~5 재실행
종료 조건: 1~5 전체 PASS + Constraints 위반 없음
```

Test Cases 6~9는 브라우저 실행이 필요하므로 사용자에게 확인을 요청하고, 확인 절차를 그대로 안내한다.

## Constraints

- `_isDashboardBatchOrders` **함수 자체를 수정하지 않는다.** 주문서 날짜 클릭 규칙이다 (AGENTS.md:39-41). CMG 경로에서 `_isBatchCMG`로 교체만 한다.
- `_promptCMGDateRange`, `_parseDateRange`, `_readCMGDateRange` 세 함수의 **본문은 수정하지 않는다.**
- `_selectDateInCalendar` / `_navigateToMonth`의 `return true` 실패 경로는 **이번에 고치지 않는다.** 호출부에서 검증(변경 5)으로 막는다. 함수 내부를 손대면 수동수집 7일치 경로 회귀 위험이 있다.
- `runner.js:37 CMG_TIMEOUT` 값을 바꾸지 않는다.
- 파일명 생성 로직(`:733-740`), 날짜 루프(`:660-662`, `:995-1000`), 체크포인트 키(`:668-670`), beacon 키(`:699-702`)는 **이미 다일 수집을 지원**한다. 수정하지 않는다.
- Airflow 쪽(`modules/transform/pipelines/sales/SMD_CoupangEats_CMG_Partition.py`)은 **수정 불필요**. glob은 `coupangeats_cmg_*.csv`라 파일명 변경에 영향 없고, 날짜는 CSV의 `조회일자` 컬럼에서 읽으며, `(매장명, 조회일자)` dedup + `collected_at` 최신 우선이라 2일치 재수집이 멱등이다.
- `C:\airflow\coupang_extension_build`, `*-DESKTOP-*`, `*.bak_*` 파일은 수정하지 않는다.
- 범위 밖(이번엔 하지 않음): 메뉴 단계(`runner.js:1233`)도 같은 빈 배열이 흘러가 제외 매장 메뉴까지 수집한다. 데이터가 틀리지는 않으므로 이번 작업에 포함하지 않는다.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기 (이번 작업은 전부 기존 파일 수정)
- 줄 번호가 문서와 다르면: 위 BEFORE 코드 블록을 앵커로 삼아 검색해서 찾는다
- import가 모호하면: Reference Code의 패턴 따라가기 (확장은 전역 `Sites` / `Utils` 사용, ES module import 없음)
- 타입 힌트 여부: 해당 없음 (순수 JS)
- 주석 여부: 최소화. 위 코드 블록에 포함된 주석은 **그대로 유지**할 것 (재발 방지 근거라 지우면 안 됨)
- 변수명·함수명 스타일: 확장 기존 관례대로 `_camelCase` 프라이빗 메서드, 상수는 `UPPER_SNAKE`
