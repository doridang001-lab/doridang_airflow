# 배민 크롬확장 NOW 수집 — /shops/{id}/ 이동 회귀 제거 (홈 대시보드 수집 복원)

## Task
크롬확장 배치(`runner_baemin.html`)의 배민 우리가게NOW 수집이, Codex가 추가한 `ensureBaeminNowStorePage()` 때문에 `https://self.baemin.com/shops/{storeId}/` 로 이동하면서 막힌다(빈 NOW 화면 → `.WooriShopNowItem-module__TKcC` 없음 → `cards=0` 타임아웃). NOW는 홈 대시보드 `https://self.baemin.com/` 에서 ShopSelect로 매장 전환 후 그 자리에서 수집해야 한다(정답 = Selenium DAG `croling_beamin.py`, `navigate_to_store` docstring이 /shops 직접이동 빈화면을 경고). NOW 경로의 `/shops/{id}/` 이동을 전부 제거하고 홈 수집으로 되돌린다.

## Project Conventions (이 태스크 = 크롬확장 JS)
- 대상은 브라우저 확장 JS. Python 규칙 아님.
- 수정 파일은 레포 밖 OneDrive 폴더. 경로 그대로 사용.
- 기존 코드 스타일(2칸 들여쓰기, camelCase, IIFE 내부 함수) 유지.
- 매장선택(ShopSelect value+verify), `_collectMetrics`(Item 파싱), CSV 헤더는 **이미 올바름 → 건드리지 말 것**.
- NOW = 홈 대시보드 수집. `/shops/{id}/` 는 NOW에 사용 금지. woori/change 의 `/shops/{id}/stat|history` 는 정상이므로 유지.

## Files to Create / Modify
수정(1개):
- `C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\runner_baemin.js`
  - `selectTargetStore` (L499 부근)
  - `collectCategory` now 블록 (L630~643)
  - `waitForBaeminDashboardReady` (L566~614)
  - `ensureBaeminNowStorePage` 함수 (L552~565) 삭제

수정 안 함(이미 정상):
- `content/02_baemin.js` `_collectMetrics` (Item 파싱 완료)

레퍼런스(읽기 전용, 수정 금지):
- `C:\airflow\modules\extract\croling_beamin.py` — `DB_Beamin_01_now` 흐름(홈에서 select→wait), `navigate_to_store`(/shops 빈화면 경고)
- `C:\airflow\modules\transform\pipelines\db\DB_Beamin_01_now.py` — `collect_now_stats`(홈 대시보드 수집 흐름)

## Implementation Steps

### 1. `ensureBaeminNowStorePage` 호출 전부 제거 (NOW는 홈에서 수집)
- `selectTargetStore` (L499): 아래 줄 **삭제**.
  ```js
  if (value.storeId) await ensureBaeminNowStorePage(tabId, value.storeId);
  ```
  매장 전환 후 홈에 머문 채 다음 줄의 `waitForBaeminDashboardReady` 로 값 로드만 확인.
- `collectCategory` now 블록 (L630~643): L634 의 아래 줄 **삭제**.
  ```js
  if (opts.expectedStoreId) await ensureBaeminNowStorePage(tabId, opts.expectedStoreId);
  ```
  로그 문구를 원복: `log('  -> now 페이지 이동 생략: 현재 선택 매장 유지', 'info')`.
  이후 `RE_LOGGEDIN.test(currentUrl)`(홈 확인) + 직전 매장확인(beforeInfo) 로직만 유지.
- 함수 `ensureBaeminNowStorePage` (L552~565) **삭제**.

### 2. `waitForBaeminDashboardReady` 의 /shops 이동/의존 제거
- 시작부(L573~575) 아래 분기 **삭제** — 홈 URL 그대로 둔다.
  ```js
  if (expectedStoreId && !(await getUrl(tabId)).includes(`/shops/${expectedStoreId}/`)) {
    await ensureBaeminNowStorePage(tabId, expectedStoreId);
  }
  ```
- `.WooriShopNowItem-module__TKcC` `hasData` probe(L582~592)와 ready 조건(L594)은 **그대로 유지**.
- 타임아웃 절반 경과 reload 폴백(L598~609): `/shops/` 분기 제거하고 **홈 reload**만 수행.
  ```js
  await chrome.tabs.reload(tabId);
  await waitUrl(tabId, u => RE_LOGGEDIN.test(u), 30000);
  ```
  홈 reload 시 ShopSelect가 기본(첫) 매장으로 리셋될 수 있으므로, **reload+홈도달 직후 expectedStoreId로 ShopSelect 재선택**(무한재귀 방지 위해 selectTargetStore 호출 대신 인라인 executeScript 스니펫 사용):
  ```js
  if (expectedStoreId) {
    await injectContent(tabId);
    await chrome.scripting.executeScript({
      target: { tabId },
      args: [String(expectedStoreId)],
      func: (sid) => {
        const sel = document.querySelector('select.Select-module__a623, select.ShopSelect-module___pC1, .ShopSelect-module__JWCr select');
        if (!sel) return;
        const opt = [...sel.options].find(o => String(o.value) === sid);
        if (!opt) return;
        const setter = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value')?.set;
        if (setter) setter.call(sel, opt.value); else sel.value = opt.value;
        sel.selectedIndex = opt.index; opt.selected = true;
        sel.dispatchEvent(new Event('input', { bubbles: true, composed: true }));
        sel.dispatchEvent(new Event('change', { bubbles: true, composed: true }));
      }
    });
    await sleepWithStop(1500);
  }
  ```
- `expectedStoreId` 파라미터는 매칭/재선택 용도로만 사용(이동 용도 제거).

### 3. 잔여 점검
- `runner_baemin.js` 전체에서 `ensureBaeminNowStorePage` 참조가 0건인지 확인.
- NOW 경로에 `/shops/${...}/` 이동이 남지 않았는지 확인(woori/change 의 `/shops/{id}/stat|history` 는 정상 → 유지).
- `urlFor` 및 collectCategory 의 woori/change/orders/ad 분기는 **변경 금지**.

## Reference Code

### croling_beamin.py — navigate_to_store (/shops 직접이동 빈화면 경고)
```python
def navigate_to_store(driver, store_id: str) -> bool:
    """store_id URL로 직접 이동 후 메트릭 데이터 로드 대기.
    드롭다운 dispatch 대신 URL 직접 이동으로 React state 문제 및
    세션 복원 URL 의존 문제를 우회한다.
    """
    # 주의: 우리가게NOW는 store URL 직접 이동 시 빈 화면이 나올 수 있어
    #       실제 ShopSelect 클릭(홈 대시보드 수집)을 우선 사용한다.
    url = f"https://self.baemin.com/shops/{store_id}/"
    driver.get(url)
    return wait_for_metrics_data(driver, timeout=45)
```

### DB_Beamin_01_now.py — collect_now_stats (홈 대시보드 수집 흐름)
```python
# 1) 홈으로 이동
driver.get("https://self.baemin.com/")
# 2) ShopSelect 드롭다운 대기
wait_for_page(driver, "select[class*='ShopSelect']", timeout=60)
# 3) 매장별로 select_store_by_id 후 홈에서 그대로 수집 (페이지 이동 없음)
for store_info in store_list:
    if not select_store_by_id(driver, store_info["store_id"]):
        continue
    if not wait_for_metrics_data(driver, timeout=90):   # 홈에서 값 로드 대기
        continue
    stats = collect_single_store_stats(driver, store_info["store_id"], account_id)
```

### croling_beamin.py — wait_for_metrics_data (홈에서 값 채워질 때까지 폴링)
```python
def wait_for_metrics_data(driver, timeout: int = 45) -> bool:
    def _has_data(d):
        return d.execute_script(r"""
            const LABELS = ['조리소요시간','주문접수시간','최근재주문율',
                            '조리시간준수율','주문접수율','최근별점'];
            const items = document.querySelectorAll('.WooriShopNowItem-module__TKcC');
            if (!items.length) return false;
            const spans = items[0].querySelectorAll('span');
            return spans.length >= 2
                && LABELS.includes(spans[0].textContent.trim())
                && spans[1].textContent.trim() !== '';
        """)
    for attempt in range(2):
        try:
            WebDriverWait(driver, timeout).until(_has_data)
            return True
        except TimeoutException:
            if attempt == 0:
                driver.refresh()   # 홈 새로고침 1회
                time.sleep(random.uniform(3.0, 5.0))
    return False
```

## Test Cases
1. [구문] `node --check "C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\runner_baemin.js"` → 기대: 에러 없음
2. [참조 0건] `runner_baemin.js` 에서 `ensureBaeminNowStorePage` grep → 기대: 0건
3. [NOW 경로 /shops 이동 0건] NOW 경로(selectTargetStore/collectCategory now/waitForBaeminDashboardReady)에 `/shops/${...}/` 이동(`tabs.update`)이 없는지 확인 → 기대: 0건 (woori/change 의 stat|history 는 무관)
4. [실측] 확장 reload 후 도리당 강원영월점(skmo1004 / 14853280) NOW 수집 → 기대 로그: `now 페이지 이동 생략: 현재 선택 매장 유지`, `NOW 매장 페이지 이동: .../shops/...` **미출현**
5. [실측] `[DBG dashboard-ready]` 가 `url=https://self.baemin.com/` 에서 `cards>0`(6지표) 통과 → 기대: hasData true
6. [실측] NOW 결과 `rows=1`(6지표 채워짐) 완료 → 기대: rows=88 아님, 빈값 아님
7. [실측] 곱도리탕(14853281)·도리당(14853280) 저장 데이터가 서로 다름(이전 매장 값 복제 없음)
8. [실측] 2단계 상세(woori/change/orders/ad) 실패 배지 없이 완료

## Verification Loop
```
LOOP until all PASS:
  1. Test 1~3 (정적) 실행 → FAIL 시 원인분석 → 코드수정 → 재실행
  2. 정적 PASS 후 사용자에게 확장 reload + Test 4~8 실측 요청
종료 조건: 정적 1~3 PASS + 실측 4~8 PASS + Constraints 위반 없음
```

## Constraints
- NOW 는 반드시 홈 대시보드(`https://self.baemin.com/`)에서 수집. `/shops/{id}/` NOW 이동 금지(빈 화면).
- 홈 reload 폴백 시 ShopSelect 리셋 가능 → reload 직후 expectedStoreId 재선택 필수.
- 매장선택(ShopSelect value+verify), `_collectMetrics`(Item 파싱), CSV 헤더, woori/change/orders/ad 분기 변경 금지.
- 레퍼런스 Python 파일 수정 금지(읽기 전용).
- 수정 파일은 OneDrive(레포 밖) → 변경 후 `chrome://extensions` 확장 새로고침 필요.

## Do Not Ask — Decide Yourself
- 함수가 이미 존재(수정 대상) → 해당 부분만 교체/삭제.
- 셀렉터/파싱 모호 → Reference Code의 DAG 구현 그대로 따른다.
- 타입/주석 스타일 → 기존 파일과 동일(주석 최소화, WHY만).
- 변수명 스타일 → 기존 파일과 동일(camelCase).
- reload 1회 플래그 → 가장 단순한 로컬 변수(`reloaded`)로 처리(기존 패턴 유지).
