# 배민 크롬확장 — 우리가게NOW 수집 제거 + ShopSelect store_id 활용

## Task
배민이 우리가게NOW 대시보드를 제거해(`cards=0`, `.WooriShopNowItem-module__TKcC` 없음) NOW 지표 수집이 불가능하다. NOW(지표 스크랩/다운로드) 단계를 완전히 들어내고, 홈 대시보드에 남은 **ShopSelect 드롭다운**에서 store_id를 추출해 우가클(woori)/변경이력(change)/주문(orders)/광고(ad) 수집에 그대로 활용한다. store_id 추출 코드는 이미 존재(`_getShopInfo`, `selectTargetStore`)하므로 재사용한다.

## Project Conventions (이 태스크 = 크롬확장 JS/HTML)
- 대상은 브라우저 확장 JS/HTML. Python 규칙 아님.
- 수정 파일은 레포 밖 OneDrive 폴더. 경로 그대로 사용.
- 기존 코드 스타일(2칸 들여쓰기, camelCase, IIFE) 유지.
- store_id 추출 코어(`_getShopInfo`, `selectTargetStore`의 select option value)는 보존 — woori/orders의 입력.
- woori/change/orders/ad 의 `urlFor`·collectCategory 분기, CSV 헤더, 매장명 정규화는 변경 금지.

## ShopSelect DOM 구조 (store_id 추출 대상)
```html
<div class="ShopSelect-module__JWCr">
  <select class="Select-module__a623 ShopSelect-module___pC1">
    <option value="14853281">[음식배달] 나홀로 1인 곱도리탕 강원영월점 / 찜·탕·찌개 14853281</option>
    <option value="14853280">[음식배달] 닭도리탕 전문 도리당 강원영월점 / 찜·탕·찌개 14853280</option>
    ...
  </select>
  <h3 class="ShopSelect-module__b8Mn">[음식배달] 닭도리탕 전문 도리당 강원영월점</h3>
  <div class="ShopSelect-module__j4Qm">14853280 | 찜·탕·찌개</div>   <!-- store_id = 앞 숫자 -->
</div>
```
→ store_id = 선택된 option.value (예 `14853280`) 또는 `.ShopSelect-module__j4Qm` 의 첫 숫자.

## Files to Create / Modify
경로 prefix: `C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\`
- `runner_baemin.js`
- `runner_baemin.html`
- `content/02_baemin.js`

레퍼런스(읽기 전용): `C:\airflow\modules\transform\pipelines\db\DB_Beamin_combined.py`

## Implementation Steps

### 1. `runner_baemin.js` — `now` 카테고리/수집 단계 제거
- `categories` (L50): `const categories = ['woori', 'change', 'orders', 'ad'];` (now 제거).
- `categoryLabels` (L51): `now:'now'` 항목 제거 → `{ woori:'우가클', change:'변경이력', orders:'주문', ad:'광고' }`.
- `runBatch` (L751~): 2단계(1단계 now / 2단계 상세) 구조를 **단일 패스**로 변경.
  - "1단계 now 수집 시작" / "2단계 상세 수집 시작" 로그, now-실패-skip 로직 제거.
  - 각 선택 인덱스에 `await processAccount(i, tabId, 'all')` 1회 호출(retry 모드와 동일 패턴).
- `processAccount` (L684~): now 수집 블록 삭제.
  - `if (phase !== 'details') { … collectCategory(tabId, i, 'now', …) … shopInfo 재확인 … }` (대략 L731~743) **전부 제거**.
  - 유지: `let storeId = shopInfo.store_id || '';` 와 `if (!storeId) throw new Error('store_id 확보 실패');`.
  - 유지: details 블록(woori×2/change/orders/ad) — 단일 패스 `'all'` 에서 실행되도록.
  - `activeCategories` 는 now 의존 없이 항상 `['woori','change','orders','ad']`.
- `collectCategory` (L627~): `if (category === 'now') { … }` 분기(L630~643) **삭제**.
- (이전 회귀 잔재) `ensureBaeminNowStorePage` 정의/호출, `waitForBaeminDashboardReady` 의 `/shops/` 의존이 남아있으면 함께 제거. NOW 값 대기가 불필요하면 `waitForBaeminDashboardReady` 호출부도 제거하고, 매장 전환 확인은 `selectTargetStore` 의 `verify`(select.value === storeId 또는 `.ShopSelect-module__j4Qm` 에 storeId 포함)로 충분.

### 2. `runner_baemin.html` — now 컬럼 제거
- thead 의 `<th style="width:64px">now</th>` (L130) **삭제**. (`buildRows`는 `categories`를 순회 → 자동 정렬.)

### 3. `content/02_baemin.js` — NOW 디스패치 제거
- `collect()` (L10~90)의 최종 `else { … this._collectMetrics(currentInfo) … }` 분기(L54~60) **삭제**.
  - category 가 woori/change/orders/ad 가 아니면(과거 now 진입) 수집 없이 종료하거나 `{ success:true, store_id: shopInfo.store_id, store_name: shopInfo.store_name }` 반환.
- `_collectMetrics` 함수(L1602~)는 호출부 제거 후 미사용 방치 또는 삭제(우선 호출부만 끊으면 됨).
- `_getShopInfo()` (L165~213)는 **변경 금지** — store_id 추출(.ShopSelect-module__j4Qm 첫 숫자 + select option value)이 핵심.

### 4. store_id 활용 경로 확인 (변경 없음, 검증만)
- `selectTargetStore` 반환 `store_id`(= option.value, 예 14853280)가 `urlFor('woori'|'change'|'ad', storeId, …)` 와 orders 의 `expectedStoreId` 로 전달되는지 확인.
- 비면 `_getShopInfo().store_id` 로 보강.

## Reference Code

### content/02_baemin.js — `_getShopInfo` (store_id 추출 코어, 보존)
```javascript
_getShopInfo() {
  const cleanText = v => (v || '').replace(/\s+/g, ' ').trim();
  const visible = el => { /* width/height/visibility 체크 */ };
  const nameEl = [...document.querySelectorAll('.ShopSelect-module__b8Mn, .ShopSelect-module__JWCr h3, h3')].find(visible);
  const infoEl = [...document.querySelectorAll('.ShopSelect-module__j4Qm')].find(visible);
  const visibleName = cleanText(nameEl?.textContent);
  const visibleStoreId = infoEl?.textContent.match(/(\d+)/)?.[1] || '';   // "14853280 | 찜·탕·찌개" → 14853280
  if (visibleName || visibleStoreId) return { store_name: visibleName, store_id: visibleStoreId, needFilter: false };
  const pathStoreId = location.pathname.match(/\/shops\/(\d+)/)?.[1] || '';
  const selectBox = document.querySelector('select.Select-module__a623, select.ShopSelect-module___pC1, .ShopSelect-module__JWCr select');
  if (selectBox) {
    const sel = pathStoreId ? [...selectBox.options].find(o => o.value === pathStoreId) : selectBox.options[selectBox.selectedIndex];
    const parsed = this._parseShopOption(sel);   // option.value = store_id
    if (parsed?.store_name || parsed?.store_id) return parsed;
  }
  // … orders needFilter 등 폴백
}
```

### runner_baemin.js — woori/change/orders/ad URL (store_id 활용, 변경 금지)
```javascript
function urlFor(category, storeId, date) {
  const ym = (date || ymd(new Date(Date.now()-86400000))).slice(0, 7);
  if (category === 'woori')  return `https://self.baemin.com/shops/${storeId}/stat/marketing/woori-shop-click?serviceType=BAEMIN&initialDateOption=MONTHLY&initialMonth=${ym}`;
  if (category === 'change') return `https://self.baemin.com/shops/${storeId}/history/change/shop`;
  if (category === 'orders') return `https://self.baemin.com/orders/history?startDate=${date}&endDate=${date}`;
  if (category === 'ad')     return `https://self.baemin.com/stat/advertisement?initialDateOption=DAY&initialMonth=${ym}&startDate=${date}&endDate=${date}`;
  return HOME_URL;
}
```

## Test Cases
1. [구문] `node --check "C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\runner_baemin.js"` → 에러 없음
2. [NOW 잔재 0건] `runner_baemin.js` + `content/02_baemin.js` 에서 `_collectMetrics` 호출 / `WooriShopNowItem` / `ensureBaeminNowStorePage` / `category === 'now'` grep → NOW 수집 경로에 0건
3. [컬럼 제거] `runner_baemin.html` 에 `>now<` th 없음, `categories` 에 `'now'` 없음
4. [실측] 확장 reload 후 도리당 강원영월점(skmo1004 / 14853280) 수집 → now 관련 로그 미출현, 매장선택 직후 `우가클 이동`
5. [실측] ShopSelect 에서 store_id=14853280 추출 → woori URL `…/shops/14853280/stat/marketing/woori-shop-click…` 이동
6. [실측] woori×2/change/orders/ad 실패 배지 없이 완료, 결과 store_id=14853280
7. [실측] 테이블에 now 컬럼 사라지고 우가클/변경이력/주문/광고만 표시
8. [실측] 2~3개 매장 배치 회귀 정상

## Verification Loop
```
LOOP until all PASS:
  1. Test 1~3 (정적) 실행 → FAIL 시 원인분석 → 수정 → 재실행
  2. 정적 PASS 후 사용자에게 확장 reload + Test 4~8 실측 요청
종료 조건: 정적 1~3 PASS + 실측 4~8 PASS + Constraints 위반 없음
```

## Constraints
- store_id 추출 코어(`_getShopInfo`, `selectTargetStore` 의 select option value) 보존 — woori/orders의 입력.
- woori/change/orders/ad 의 `urlFor`·collectCategory 분기, CSV 헤더, 매장명 정규화 변경 금지.
- NOW 관련 코드(지표 스크랩/다운로드/대기/컬럼)만 제거. 다른 카테고리 로직 보존.
- 레퍼런스 Python 파일 수정 금지(읽기 전용).
- 수정 파일은 OneDrive(레포 밖) → 변경 후 `chrome://extensions` 확장 새로고침 필요.

## Do Not Ask — Decide Yourself
- 함수가 이미 존재(수정 대상) → 해당 부분만 교체/삭제.
- now 진입 시 처리 모호 → 수집 없이 종료(no-op) 또는 store_id 반환.
- `_collectMetrics` 삭제 vs 미사용 방치 → 우선 호출부만 끊고 함수는 남겨도 무방.
- 타입/주석 스타일 → 기존 파일과 동일(주석 최소화, WHY만).
- 변수명 스타일 → 기존 파일과 동일(camelCase).
