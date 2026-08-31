# 배민 메뉴 그룹 분리 + menu_name 분류 수정

## Task

`DB_MenuHierarchy_Test` 파이프라인에서 배민 주문의 `menu_name`이 `1인분` 같은 수량 옵션명으로 찍혀 메뉴 식별이 안 되고, 메인메뉴 2개 이상 주문이 한 그룹으로 뭉친다. 경계(main) 탐지를 강화해 주문을 실제 메뉴 수대로 분리하고, 각 그룹에 검수된 상품표의 `대표메뉴`를 붙인다. 함께 한 주문 안의 반복 옵션 라인이 dedup으로 사라지는 버그도 고친다.

수정 대상은 `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py` **단일 파일**이다. `_ProductLookup`은 이 파일 전용 클래스라 다른 파이프라인에 영향이 없다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## 배경 사실 (원천 실측, 재조사 불필요)

배민 원천 parquet(`BAEMIN_ORDERS_DB/brand=*/store=*송파삼전점/ym=*/orders_*.parquet`) 구조:

- `주문내역` = 주문의 **첫 번째 메뉴명** + `외 N건`. 2·3번째 메뉴 이름은 담기지 않는다.
- `주문옵션상세` = 메뉴 라인과 옵션 라인이 **평면으로 나열**된 컬럼. `주문옵션금액`이 라인 단가.
- 따라서 `외 N건`은 **이름의 출처가 아니라 "이 주문의 메뉴가 몇 개인가"라는 개수 제약**으로만 쓴다. 이름은 각 경계 라인의 `item_id → 상품표 대표메뉴`에서 그룹 단위로 나온다.

문제 주문 예시:

| order_id | `주문내역` | `주문옵션상세` 구조 |
|---|---|---|
| T2F30001ZHDB | `1인 순살 닭도리탕 (밥포함)` | `1인분` 16900 + 옵션 8줄 |
| T2F30001HBF6 | `1인 순살 닭도리탕 (밥포함)` | `1인분` 16900 + 옵션 5줄 |
| T2F20001GGJ6 | `미니 계란찜 외 2건` | 메뉴 3개(`미니 계란찜` 2000 / `[삼계탕] 누룽지 닭한마리` 22500 / `1인분` 16900) 평면 나열 |

상품표에 이미 정답이 있다 — `fin_product_map.csv` item_id `200008000` → `대표메뉴 = 1인 순살 닭도리탕 (밥포함)`, `표준_메뉴명_edit = 1인 순살 닭도리탕(밥포함)`, `수동분류_edit = 1인`. 실제로 산출물의 `std_menu_name`은 이미 정확하고 `menu_name`만 틀렸다.

경계가 안 잡히는 원인 2가지:
1. `[삼계탕] 누룽지 닭한마리`(200008035)는 `수동분류=기타`라 `_NON_MAIN_CATEGORIES`에 걸려 즉시 False (`is_main_candidate=Y`인데도). 송파삼전 배민에 같은 상황 5건.
2. 별도로 주문된 음료·공기밥은 `음료`/`사이드`로 검수돼 있어 영원히 경계가 될 수 없다.

**규칙 적용 전/후 실측(798주문):**

| 지표 | 기존 | 신규 |
|---|---|---|
| 그룹 수 == 원천 메뉴 수 | 666/798 | **796/798** |
| 초과 탐지 | 0 | **0** |
| 메인 2개 이상 주문 | 4 | **135** |

승격되는 라인은 `사이드` 116 + `음료` 29뿐이고 옵션·토핑 오탐 0. 남은 2건은 `주문옵션상세`가 숫자(`23500`/`16900`)인 수집 잔재라 범위 밖.

## Files to Create / Modify

- 수정: `C:\airflow\modules\transform\pipelines\db\DB_MenuHierarchy_Test.py` (유일한 변경 파일)
- 생성 없음. DAG(`dags/db/DB_MenuHierarchy_Test_Dags.py`)는 변경하지 않는다.

## Implementation Steps

### 1. 카테고리 상수 추가

기존 상수 옆(파일 상단 `_MAIN_CATEGORIES` / `_NON_MAIN_CATEGORIES` 정의부)에 추가한다. `_NON_MAIN_CATEGORIES`는 **그대로 둔다**(`_line_role`에서 계속 쓰임).

```python
_MAIN_CATEGORIES = {"메인", "1인", "세트"}
_NON_MAIN_CATEGORIES = {"옵션", "토핑", "리뷰", "사이드", "음료", "기타", "할인", "제외", "수수료"}
# 신규: "기타"/공백을 제외한 확정 비메인. is_main 판정에서만 사용한다.
_STRICT_NON_MAIN_CATEGORIES = {"옵션", "토핑", "리뷰", "사이드", "음료", "할인", "제외", "수수료"}
# 신규: 단품(별도 메뉴)으로 승격될 수 있는 분류
_STANDALONE_CATEGORIES = {"사이드", "음료", "기타", ""}
```

### 2. `_ProductLookup.is_main()` — `기타`/공백 구제

판정 순서를 바꾼다. `category`를 먼저 비메인으로 걸러내던 것을 뒤로 미루고, `main_codes`(=`is_main_candidate=Y`, `exclude_check≠Y`) 판정을 앞으로 당긴다.

```python
def is_main(self, source, brand, store, item_id, item_name, unit_price) -> bool:
    source_s = str(source or "").strip()
    brand_s = str(brand or "").strip()
    store_s = _store_key(store)
    item_id_s = str(item_id or "").strip()
    key = (source_s, brand_s, store_s, item_id_s)
    category = self.category_by_item.get(key, "")
    if category in _MAIN_CATEGORIES:
        return True
    if key in self.main_codes and category not in _STRICT_NON_MAIN_CATEGORIES:
        return True
    if category in _NON_MAIN_CATEGORIES:
        return False
    price = pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0]
    name = str(item_name or "").strip()
    if price <= 0:
        return False
    if not str(item_id_s).startswith("TMP_"):
        return False
    return not _OPTION_LIKE_RE.search(name) and not _FEE_LIKE_RE.search(name)
```

이 변경으로 새로 main이 되는 항목은 `[삼계탕] 누룽지 닭한마리`(25행)·`[한우 대창] 순살 곱도리탕`(100행) 2종뿐이고, main에서 해제되는 항목은 없다(실측).

### 3. `_ProductLookup.is_standalone_candidate()` 신설

옵션이 아니라 따로 주문된 단품일 수 있는 후보를 판정한다. `representative_by_item`(대표메뉴)·`std_by_item`은 이미 `__init__`에서 만들어져 있으니 재사용한다. `map_target`의 `표준_메뉴명_edit`도 같은 방식으로 dict을 하나 더 만들어 쓴다(`_build_std_edit_lookup`, `_build_std_lookup`과 동일 패턴).

```python
def is_standalone_candidate(self, source, brand, store, item_id, item_name, unit_price, menu_vocab) -> bool:
    price = pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0]
    if price <= 0:
        return False
    key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
    category = self.category_by_item.get(key, "")
    if category not in _STANDALONE_CATEGORIES:
        return False
    if category == "음료":
        return True
    name = str(item_name or "").strip()
    if name in menu_vocab:
        return True
    name_key = _normalize_item_key(name)
    representative = self.representative_by_item.get(key, "")
    std_edit = self.std_edit_by_item.get(key, "")
    return bool((representative and _normalize_item_key(representative) == name_key)
                or (std_edit and _normalize_item_key(std_edit) == name_key))
```

`menu_vocab`은 `_build_baemin`에서 계산해 넘긴다(아래 4번).

### 4. `_build_baemin()` — 원천 파싱 + 그룹 수 제약 경계 확정

`_build_baemin` 안에서 다음을 계산한다.

```python
order_summary = _clean_nan_series(df["주문내역"])
menu_head = order_summary.str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
extra_cnt = order_summary.str.extract(r"외\s*(\d+)건$")[0]
expected_menus = pd.to_numeric(extra_cnt, errors="coerce").fillna(0).astype(int) + 1
menu_vocab = set(menu_head[menu_head.ne("")].unique())
```

- `current_menu`(기존 `out["menu_name"]` 소스)는 `menu_head`와 동일한 정규식이므로 그대로 재사용해도 된다.
- 경계 확정:

```python
boundary = pd.Series([lookup.is_main(...) for ...], index=out.index)   # 기존 로직 유지
candidate = pd.Series([
    lookup.is_standalone_candidate(BAEMIN_SOURCE, brand, TARGET_STORE, item_id, name, price, menu_vocab)
    for brand, item_id, name, price in zip(out["brand"], out["item_id"], out["item_name"], out["unit_price"])
], index=out.index)

for order_id, group in out.groupby("order_id", sort=False):
    expected = int(expected_menus.loc[group.index].max())
    need = expected - max(int(boundary.loc[group.index].sum()), 1)
    if need <= 0:
        continue
    for idx in reversed(list(group.index)):        # 별도 주문 음료/공기밥은 꼬리에 붙는다
        if need <= 0:
            break
        if not boundary.at[idx] and candidate.at[idx]:
            boundary.at[idx] = True
            need -= 1

out["_boundary"] = _ensure_boundary(out, boundary)
```

- 승격 수가 `need`로 상한이 걸려 초과 탐지가 구조적으로 불가능하다.
- `주문내역` 파싱 실패로 `expected`가 안 나오면 `need <= 0`이 되어 자동으로 기존 동작(`_ensure_boundary` 폴백)으로 떨어진다.
- **역순 탐색 순서는 `item_seq` 기준이어야 한다.** `out`의 index 순서가 원천 행 순서와 같으므로 `group.index` 역순이면 충분하지만, 안전하게 `item_seq`를 숫자로 정렬한 뒤 역순으로 돌아도 된다(`assign_menu_hierarchy`가 `sort_cols=["item_seq"]`로 같은 정렬을 쓴다).

### 5. `_build_baemin()` — 원천 alias 맵 (상품표 미등록 메뉴 폴백)

**단일메뉴 주문**(`expected_menus == 1`)의 첫 라인으로 `(라인명, 단가) → 메뉴명` 맵을 만든다.

```python
alias_rows = out.loc[expected_menus.eq(1)].groupby("order_id", sort=False).head(1)
baemin_menu_alias = {}
for idx in alias_rows.index:
    line_name = str(out.at[idx, "item_name"]).strip()
    head = str(menu_head.at[idx]).strip()
    if not line_name or not head or _normalize_item_key(line_name) == _normalize_item_key(head):
        continue
    baemin_menu_alias.setdefault((_normalize_item_key(line_name), int(out.at[idx, "unit_price"])), head)
```

현재 데이터 기준 4쌍이며 충돌 0이다:

```
('1인분', 16900)  -> '1인 순살 닭도리탕 (밥포함)'   (근거 주문 188건)
('16900', 12000) / ('16900', 16900) -> '[복날한정] 1인 미나리 수삼 백숙'
('23500', 23500) -> '[복날한정] 미나리 수삼 백숙'
```

이 맵을 `_finalize_hierarchy`에 `name_alias=` 인자로 넘긴다(다른 소스는 `None`).

**LLM·Ollama·qwen 호출 금지 정책 유지**: 원천 데이터만으로 유도되므로 `assert_no_model_classification_dependencies()`를 그대로 통과해야 한다.

### 6. `_ProductLookup.menu_display_name()` 신설

경계 라인의 `item_id`로 그룹 표시명을 만든다. 우선순위:

```
대표메뉴 -> (배민 한정) 원천 alias -> 표준_메뉴명_edit / standard_menu_name -> 원본 라인명
```

`대표메뉴`가 배민 원문 메뉴명과 표기까지 일치하므로 1순위다(`[한우 대창] 순살 곱도리탕` vs 표준명 `한우 순살 곱도리탕`). 기존 `std_name()`은 **건드리지 않는다** — `std_menu_name` 컬럼 동작은 불변이어야 하고, 두 컬럼이 상호보완 관계다.

```python
def menu_display_name(self, source, brand, store, item_id, fallback_name, unit_price="", alias=None) -> str:
    key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
    fallback = str(fallback_name or "").strip()
    representative = self.representative_by_item.get(key, "")
    if representative:
        return representative
    if alias:
        hit = alias.get((_normalize_item_key(fallback), int(pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0])))
        if hit:
            return hit
    return self.std_edit_by_item.get(key, "") or self.std_by_item.get(key, "") or fallback
```

### 7. `_finalize_hierarchy()` — menu_name 대입부 교체

현재 마지막 줄이 경계 라인의 **원본 이름**을 그대로 쓴다:

```python
out["menu_name"] = out["_menu_name_new"].where(out["_menu_name_new"].ne(""), out.get("menu_name", ""))
```

이를 경계 라인의 `item_id`로 `menu_display_name()`을 태운 값으로 바꾼다. 각 행의 `parent_item_seq`로 부모(경계) 라인을 찾는 로직은 바로 위 `std_menu_name` 계산부에 이미 있으니(`parent_item_id_by_key`, `parent_name_by_key`) 같은 루프에서 함께 계산한다.

- `_menu_name_new`(원본 경계명)는 `20_diff_{ym}.csv` 비교용으로 **컬럼에 남긴다**.
- **`item_name`은 절대 바꾸지 않는다.** 상품표 `item_id` join 정합성이 우선이다. (`1인분` 라인은 `item_name=1인분`, `menu_name=1인 순살 닭도리탕 (밥포함)`)
- `_finalize_hierarchy`는 전 소스 공통 경로다. posfeed/쿠팡/okpos의 `menu_name`도 검수 메뉴명으로 바뀔 수 있으므로 Test Case 7로 회귀를 확인한다. 회귀가 보이면 배민 한정으로 좁힌다.

### 8. `_build_baemin()` — dedup 교체 (라인 유실 버그)

현재 코드:

```python
key_cols = ["_src_path", "주문시각", "주문번호", "주문상태", "수령방법", "주문내역", "주문옵션상세", "주문수량", "주문옵션금액", "결제금액"]
df = df.drop_duplicates(subset=[c for c in key_cols if c in df.columns], keep="last").reset_index(drop=True)
```

문제:
- 파일 안에서 한 주문의 `collected_at`이 갈리는 경우는 **전 기간 0건**이다. 즉 이 dedup이 지우는 건 100% "한 주문에서 같은 옵션을 두 번 주문한" 정상 데이터다(전 기간 54행, GGJ6는 원천 12행 → 산출 10행).
- 반대로 진짜 위험인 **ym/next_ym 두 파일 교차 중복**은 키에 `_src_path`가 있어 못 거른다. `_build_baemin`은 `(ym, _next_ym(ym))` 두 파일을 읽으므로 실제 발생 가능하다.

교체:

```python
collected_ts = pd.to_datetime(df.get("collected_at"), utc=True, errors="coerce")  # 'Z' / '+09:00' 혼재
if collected_ts.notna().any():
    rank_key = pd.DataFrame({"_ts": collected_ts, "_src": df["_src_path"].astype(str)})
    latest = rank_key.groupby(df["주문번호"])[["_ts", "_src"]].transform("max")
    df = df[rank_key["_ts"].eq(latest["_ts"]) & rank_key["_src"].eq(latest["_src"])].reset_index(drop=True)
else:
    df = df.drop_duplicates(subset=[c for c in key_cols if c in df.columns], keep="last").reset_index(drop=True)
```

- 한 주문 안의 반복 옵션 라인 보존(GGJ6 10행 → 12행)
- ym/next_ym 교차 중복은 `_src_path`가 달라도 제거
- `collected_at`이 전부 NaT면 기존 dedup으로 폴백

### 9. `_build_readme_text()` 갱신

`## source별 계층 로직`의 배민 항목을 교체:

```
- 배민: 원본 `주문내역`의 `외 N건`을 메뉴 개수 제약으로만 쓰고, 메뉴 이름은 경계 라인의 상품표 `대표메뉴`에서 그룹별로 가져옵니다. 경계가 부족하면 주문 꼬리부터 사이드/음료 단품을 승격합니다.
```

`## 수정 시 확인할 점`에 추가:

```
- `menu_name`은 검수된 메뉴명(대표메뉴)이고 `item_name`은 원천 라인명을 유지합니다. 둘을 같게 만들지 않습니다.
- 배민 dedup은 주문 단위 최신 `collected_at` 기준입니다. 한 주문 안에서 반복된 동일 옵션 라인을 지우면 안 됩니다.
```

## Reference Code

### modules/transform/pipelines/db/DB_MenuHierarchy_Test.py — 현재 `is_main` (수정 대상)

```python
    def is_main(self, source: object, brand: object, store: object, item_id: object, item_name: object, unit_price: object) -> bool:
        source_s = str(source or "").strip()
        brand_s = str(brand or "").strip()
        store_s = _store_key(store)
        item_id_s = str(item_id or "").strip()
        category = self.category_by_item.get((source_s, brand_s, store_s, item_id_s), "")
        if category in _NON_MAIN_CATEGORIES:
            return False
        if category in _MAIN_CATEGORIES:
            return True
        if (source_s, brand_s, store_s, item_id_s) in self.main_codes:
            return True
        price = pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0]
        name = str(item_name or "").strip()
        if price <= 0:
            return False
        if not str(item_id_s).startswith("TMP_"):
            return False
        return not _OPTION_LIKE_RE.search(name) and not _FEE_LIKE_RE.search(name)
```

### 현재 `_build_baemin` 핵심부 (수정 대상)

```python
    key_cols = ["_src_path", "주문시각", "주문번호", "주문상태", "수령방법", "주문내역", "주문옵션상세", "주문수량", "주문옵션금액", "결제금액"]
    df = df.drop_duplicates(subset=[c for c in key_cols if c in df.columns], keep="last").reset_index(drop=True)
    ...
    current_menu = _clean_nan_series(df["주문내역"]).str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
    option_name = _clean_nan_series(df["주문옵션상세"])
    is_price_name = option_name.str.fullmatch(r"[\d,]+").fillna(False)
    out["item_name"] = option_name.mask(is_price_name, current_menu)
    out["menu_name"] = current_menu
    ...
    out["item_id"] = [
        lookup.item_id(BAEMIN_SOURCE, brand, TARGET_STORE, name, price)
        for brand, name, price in zip(out["brand"], out["item_name"], out["unit_price"])
    ]
    out = _attach_common_fields(out)
    out["_raw_item_name"] = option_name
    out["_menu_name_current"] = current_menu
    boundary = pd.Series([
        lookup.is_main(BAEMIN_SOURCE, brand, TARGET_STORE, item_id, name, price)
        for brand, item_id, name, price in zip(out["brand"], out["item_id"], out["item_name"], out["unit_price"])
    ], index=out.index)
    out["_boundary"] = _ensure_boundary(out, boundary)
    out["order_cnt"] = 0
    out.loc[out.groupby("order_id").head(1).index, "order_cnt"] = 1
    out["_pk"] = _make_unified_pk(out)
    return _finalize_hierarchy(out, out["_boundary"], "master_main", lookup)
```

### 현재 `_finalize_hierarchy` (수정 대상 — 마지막 2줄)

```python
    parent_item_id_by_key = dict(zip(zip(out["order_id"], out["item_seq"]), out["item_id"]))
    parent_name_by_key = dict(zip(zip(out["order_id"], out["item_seq"]), out[name_col]))
    std_values = []
    for _, row in out.iterrows():
        parent_seq = str(row.get("parent_item_seq", "")).strip()
        parent_item_id = parent_item_id_by_key.get((row["order_id"], parent_seq), row.get("item_id", ""))
        parent_name = parent_name_by_key.get((row["order_id"], parent_seq), row.get(name_col, row.get("item_name", "")))
        std_values.append(lookup.std_name(row["source"], row["brand"], row["store"], parent_item_id, parent_name))
    out["std_menu_name"] = std_values
    out["menu_name"] = out["_menu_name_new"].where(out["_menu_name_new"].ne(""), out.get("menu_name", ""))
    return out
```

### 재사용할 기존 lookup (수정 불필요, 그대로 사용)

```python
    def _build_representative_lookup(self) -> dict[tuple[str, str, str, str], str]:
        # map_target["대표메뉴"] -> {(source, brand, store, item_id): 대표메뉴}
    def _build_std_lookup(self) -> dict[tuple[str, str, str, str], str]:
        # join_target["standard_menu_name"] -> {(source, brand, store, item_id): 표준명}
    def _build_main_code_set(self) -> set[tuple[str, str, str, str]]:
        # master_target: exclude_check != Y AND (is_main_candidate == Y OR 수동분류 in _MAIN_CATEGORIES)
```

### `assign_menu_hierarchy` (수정 불필요 — 경계만 정확하면 그룹은 알아서 갈린다)

```python
def assign_menu_hierarchy(df, boundary, *, order_col="order_id", sort_cols=None,
                          name_col="item_name", seq_col="item_seq", attr_method=""):
    # boundary=True 인 행마다 menu_seq += 1, parent_item_seq = 해당 행의 item_seq
    # 첫 경계 이전 행들은 pending 으로 모았다가 첫 경계 그룹에 붙인다
    # _menu_name_new 에 경계 라인의 name_col 값을 채운다
```

## Test Cases

실행 전 현재 산출물을 스크래치패드에 백업한다(before/after 비교용).

```powershell
$sp = "C:\Users\민준\AppData\Local\Temp\claude\C--airflow\438a3f62-d78c-450e-9687-f8d58e075fc8\scratchpad"
$mart = "C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\new_classification_orders"
Copy-Item "$mart\10_orders.csv" "$sp\before_10_orders.csv" -Force
Copy-Item "$mart\12_orders_left.csv" "$sp\before_12_orders_left.csv" -Force
```

1. **[정책 유지]** `python -c "from modules.transform.pipelines.db.DB_MenuHierarchy_Test import assert_no_model_classification_dependencies as a; a(); print('OK')"` → 기대: `OK` (RuntimeError 없음)
2. **[DAG import]** `python -c "from dags.db.DB_MenuHierarchy_Test_Dags import dag; print(dag.dag_id)"` → 기대: `DB_MenuHierarchy_Test_Dags`, ImportError 없음
3. **[실행]** `python -c "from modules.transform.pipelines.db.DB_MenuHierarchy_Test import build_orders; print(build_orders('2026-08'))"` → 기대: `통합 주문서 CSV 저장 완료 | 2026-08:...` (예외 없음)
4. **[대상 3건]** 아래 스크립트 → 기대: 전부 `PASS`

```python
# verify_targets.py
import pandas as pd
from modules.transform.utility.paths import MART_DB
left = pd.read_csv(MART_DB / "new_classification_orders" / "12_orders_left.csv", dtype=str).fillna("")
ok = True
for oid in ("T2F30001ZHDB", "T2F30001HBF6"):
    g = left[left["order_id"] == oid]
    hit = set(g["menu_name"]) == {"1인 순살 닭도리탕 (밥포함)"}
    ok &= hit
    print(("PASS" if hit else "FAIL"), oid, sorted(set(g["menu_name"])))
g = left[left["order_id"] == "T2F20001GGJ6"]
rows_ok = len(g) == 12
grp_ok = g["menu_seq"].nunique() == 3
name_ok = set(g["menu_name"]) == {"미니 계란찜", "[삼계탕] 누룽지 닭한마리", "1인 순살 닭도리탕 (밥포함)"}
# item_name 은 원천 라인명 유지, item_id 는 TMP_ 로 안 바뀜
item_ok = "1인분" in set(g["item_name"]) and not g["item_id"].str.startswith("TMP_").any()
ok &= rows_ok and grp_ok and name_ok and item_ok
print(("PASS" if rows_ok else "FAIL"), "GGJ6 행수 12 ->", len(g))
print(("PASS" if grp_ok else "FAIL"), "GGJ6 그룹수 3 ->", g["menu_seq"].nunique())
print(("PASS" if name_ok else "FAIL"), "GGJ6 menu_name ->", sorted(set(g["menu_name"])))
print(("PASS" if item_ok else "FAIL"), "GGJ6 item_name/item_id 보존")
print("ALL PASS" if ok else "HAS FAIL")
```

5. **[그룹 수 정확도 / 잔존 확인]** 전 기간 실행 후 아래 스크립트 → 기대: 일치 ≥ 795/798, **초과 0건**, 사이즈 토큰 잔존 0건

```python
# verify_groups.py
import glob, re, pandas as pd
from modules.transform.pipelines.db.DB_MenuHierarchy_Test import build_orders
from modules.transform.utility.paths import MART_DB, BAEMIN_ORDERS_DB
print(build_orders(None))  # 전 기간
left = pd.read_csv(MART_DB / "new_classification_orders" / "12_orders_left.csv", dtype=str).fillna("")
b = left[left["source"] == "배민수동"]
raw = pd.concat([pd.read_parquet(p) for p in glob.glob(str(BAEMIN_ORDERS_DB / "brand=*/store=*송파삼전*/ym=*/orders_*.parquet"))], ignore_index=True)
raw = raw[raw["주문상태"].astype(str).str.strip() == "배달완료"]
exp = (raw.groupby("주문번호")["주문내역"].first().astype(str)
          .str.extract(r"외\s*(\d+)건$")[0].fillna(0).astype(int) + 1)
got = b.groupby("order_id")["menu_seq"].apply(lambda s: pd.to_numeric(s, errors="coerce").max())
cmp = pd.DataFrame({"got": got, "exp": exp}).dropna()
print("일치", int((cmp["got"] == cmp["exp"]).sum()), "/", len(cmp),
      "부족", int((cmp["got"] < cmp["exp"]).sum()), "초과", int((cmp["got"] > cmp["exp"]).sum()))
print("메인 2개 이상 주문", int((cmp["got"] >= 2).sum()))
GEN = re.compile(r"^\s*(?:\d+\s*인분?|반마리|한마리|기본맛|기본|\[(?:소|중|대|특)\].*)\s*$")
left_over = b[b["menu_name"].map(lambda v: bool(GEN.match(str(v))))]
print("사이즈 토큰 잔존", len(left_over), sorted(set(left_over["menu_name"])))
```

6. **[금액·행수 보존]** before/after 비교 → 기대: 금액 4종 합계 **완전 동일**, 배민 행 증가분 == 원천 중복 옵션 라인 수(2026-08 기준 +2행, 전 기간 +54행)

```python
# verify_amounts.py
import pandas as pd
sp = r"C:\Users\민준\AppData\Local\Temp\claude\C--airflow\438a3f62-d78c-450e-9687-f8d58e075fc8\scratchpad"
from modules.transform.utility.paths import MART_DB
before = pd.read_csv(sp + r"\before_10_orders.csv", dtype=str).fillna("")
after = pd.read_csv(MART_DB / "new_classification_orders" / "10_orders.csv", dtype=str).fillna("")
for col in ("total_price", "discount_amount", "order_cnt"):
    bs = pd.to_numeric(before[col], errors="coerce").fillna(0).sum()
    as_ = pd.to_numeric(after[col], errors="coerce").fillna(0).sum()
    print(("PASS" if bs == as_ else "FAIL"), col, bs, "->", as_)
print("행수", len(before), "->", len(after), "(배민 반복 옵션 라인 복구분만큼 증가해야 함)")
```

7. **[타 소스 회귀]** `python -c "from modules.transform.pipelines.db.DB_MenuHierarchy_Test import write_reports; print(write_reports('2026-08'))"` 후 `20_diff_2026-08.csv`에서 `source`별 `menu_name_현재` → `menu_name_신규` 변경분 확인 → 기대: 배민 변경은 의도대로, posfeed/쿠팡/okpos에서 **원천 부모명이 더 정확한데 검수명으로 덮이는 회귀가 없을 것**. 회귀가 보이면 7단계 변경을 배민 한정으로 좁힌다.
8. **[상품표 무수정]** 실행 전후 `fin_product.csv`, `fin_product_map.csv`, `fin_product_map_join.csv`, `fin_product_grp_input.csv`의 `LastWriteTime`·해시 동일 → 기대: 변화 없음
9. **[출력 경로]** 산출물이 `new_classification_orders/` 밖으로 나가지 않음 (`_write_csv`의 `허용되지 않은 출력 경로` 가드가 살아 있을 것)

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~9 순서대로 실행
  2. FAIL 항목 -> 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- **`item_name`을 절대 바꾸지 않는다.** 상품표 `item_id` join 정합성이 우선이다. `1인분` 라인은 `item_name=1인분`, `menu_name=1인 순살 닭도리탕 (밥포함)`으로 남는다.
- **상품표(`fin_product_*.csv`)는 읽기 전용.** 컬럼 추가·저장·재분류 금지. `수동분류=기타`인데 `is_main_candidate=Y`인 5건(200008035 / 200008069 / 200002002 / 200008081 / 200008080)은 코드에서 보정만 하고, 상품표 수정은 사람이 `fin_product_map_review_input.csv`의 `수동분류_edit`에서 한다.
- **LLM / Ollama / qwen / `allocate_manual_item_ids` / `llm_product_map` 등 호출 금지.** `assert_no_model_classification_dependencies()`가 AST로 검사하므로 import만 해도 실패한다.
- **출력은 `mart/new_classification_orders/` 안에만.** `_write_csv`의 경로 가드를 우회하지 않는다.
- `std_name()` / `std_menu_name` 컬럼 동작은 **불변**이어야 한다. 새 메서드를 추가하고 기존 것은 건드리지 않는다.
- `UNIFIED_COLUMNS` 24개 스키마 유지. `10_orders.csv`에 계층 컬럼이나 신규 컬럼을 추가하지 않는다.
- `_NON_MAIN_CATEGORIES`는 `_line_role`에서 계속 쓰이므로 **삭제하거나 값을 바꾸지 않는다.** 새 상수를 추가하는 방식으로만 확장한다.
- 승격 로직은 `need`(= `외 N건 + 1` - 현재 경계 수)로 상한을 걸어 **초과 탐지가 나오지 않게** 한다.
- `dags/db/DB_MenuHierarchy_Test_Dags.py`는 변경하지 않는다.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`def f(self, x: object) -> str:` 스타일)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, private helper는 `_` 접두어, 기존 파일과 동일하게
- 검증 스크립트는 스크래치패드에 임시 파일로 만들어 실행하고, 리포지토리에 커밋하지 않는다
