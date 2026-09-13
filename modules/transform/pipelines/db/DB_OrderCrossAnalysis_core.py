"""주문 교차분석의 순수 계산. 파일 저장과 Airflow 의존성이 없다."""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass

import numpy as np
import pandas as pd

VERSION = "order-cross-v2.4"
CROSS_COLUMNS = [
    "sale_date", "ym", "brand", "store", "order_type", "main_item_id", "main_name",
    "main_standard_menu_name", "main_source", "pair_type", "pair_item_id", "pair_name",
    "pair_standard_menu_name", "co_order_cnt", "co_qty", "co_amount", "is_multi_main", "updated_at",
]
DIM = ["sale_date", "brand", "store", "order_type"]
UNIQUE_KEY = DIM + ["main_item_id", "pair_type", "pair_item_id"]
IDENTITY = ["source", "brand", "store", "item_id"]
REQUIRED_SALES_COLUMNS = [
    "sale_date", "ym", "source", "brand", "store", "order_type", "order_id", "menu_name",
    "item_seq", "item_id", "item_name", "qty", "total_price", "sale_type",
]
ALLOWED_CATEGORIES = {"메인", "사이드", "옵션", "토핑", "음료", "주류", "세트", "기타", "리뷰", "리뷰참여", "미선택", "미분류"}
MAIN_CATEGORIES = {"메인", "세트"}
MAIN_COLUMNS = DIM + ["level", "main_key", "main_name", "main_order_cnt"]
STANDARD_COLUMNS = DIM + ["main_key", "main_name", "pair_key", "pair_name", "pair_type", "co_order_cnt", "co_qty", "co_amount", "is_multi_main", "main_order_cnt", "selection_rate"]
MAPPING_COLUMNS = IDENTITY + ["item_name", "disp_name", "standard_menu_name", "category", "basis", "canonical_key", "is_main"]


def text(value) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def source_name(value) -> str:
    value = text(value)
    return "unipos" if value.lower() == "unionpos" else value.lower() if value.lower() in {"okpos", "unipos", "easypos", "posfeed"} else value


def compact(value) -> str:
    return re.sub(r"\s+", "", text(value).replace("[홀]", ""))


def token(parts) -> str:
    return json.dumps(list(parts), ensure_ascii=False, separators=(",", ":"))


def normalize_category(value) -> str:
    value = compact(value)
    if value == "1인":
        return "메인"
    if value in ALLOWED_CATEGORIES:
        return value
    for pattern, category in [
        ("리뷰|후기", "리뷰"), ("토핑", "토핑"), ("사이드|곁들임", "사이드"),
        ("옵션|반반", "옵션"), ("세트", "세트"), ("메인|점심|저녁", "메인"),
    ]:
        if re.search(pattern, value):
            return category
    if "음료" in value and "주류" in value:
        return "미분류"
    if "주류" in value:
        return "주류"
    if "음료" in value:
        return "음료"
    return "미분류"


def special_category(names) -> str:
    for name in names:
        value = compact(name)
        if re.search(r"미참여|참여안|참여하지|괜찮습니다|괜찮아요|추가없음|선택안함|필요없|안받|안받겠", value) or value in {"없음", "추가안함", "선택없음"}:
            return "미선택"
    for name in names:
        value = compact(name)
        if re.fullmatch(r"(?:\[)?(?:리뷰|후기)(?:이벤트|서비스|참여|이벤트참여|참여합니다)?(?:\])?[!♡♥]*", value) or value in {"[후.참]", "[후참]"}:
            return "리뷰참여"
    return ""


def review_name(name) -> bool:
    return bool(re.search(r"리뷰|후기|\[후[.·]?참\]", compact(name)))


def is_fee(name) -> bool:
    value = compact(name)
    return bool(re.fullmatch(r"(?:배달비|배달팁|포장비|수수료|할인|쿠폰할인|주문할인|정산차액)(?:\([^)]*\)|[-+]?\d+원?)*", value))


def infer_category(name) -> str:
    value = compact(name)
    if review_name(name):
        return "리뷰"
    if re.search(r"소주|맥주|막걸리|청하|하이볼|참이슬|처음처럼|^카스|^테라|^새로$", value):
        return "주류"
    if re.search(r"콜라|사이다|스프라이트|환타|쿨피스|생수", value):
        return "음료"
    if re.search(r"공기밥|주먹밥|계란찜|치킨무|파김치", value):
        return "사이드"
    if re.search(r"토핑|사리|분모자|추가$|추가\)|^꼬치(?:오뎅|어묵)|^통가래떡$", value):
        return "토핑"
    if re.fullmatch(r"(?:기본|매운|순한)맛.*|뼈|순살|.*빼주세요|.*많이|.*적게", value):
        return "옵션"
    return "미분류"


@dataclass
class Catalog:
    base: dict
    overlay: dict
    fingerprint: str = ""
    files: dict | None = None
    trusted: dict | None = None


def make_catalog(master: pd.DataFrame, overlay: pd.DataFrame) -> Catalog:
    master = master.copy().fillna("")
    overlay = overlay.copy().fillna("")
    required_master = ["source", "brand", "store", "상품코드", "상품명"]
    required_overlay = IDENTITY + ["category", "standard_menu_name"]
    for frame, cols in [(master, required_master), (overlay, required_overlay)]:
        if missing := set(cols) - set(frame.columns):
            raise ValueError(f"상품 매핑 필수 컬럼 누락: {sorted(missing)}")
        for col in frame:
            frame[col] = frame[col].map(text)
        frame["source"] = frame.source.map(source_name)
    for col in ["중메뉴", "수동분류", "is_main_candidate", "is_latest", "updated_at"]:
        if col not in master:
            master[col] = ""
    master["_latest"] = master.is_latest.eq("Y").astype(int)
    master["_updated"] = pd.to_datetime(master.updated_at, errors="coerce", format="mixed").fillna(pd.Timestamp.min)
    mkey = ["source", "brand", "store", "상품코드"]
    ranked = master.sort_values(mkey + ["_latest", "_updated"])
    latest = ranked.drop_duplicates(mkey, keep="last")
    top = ranked.merge(latest[mkey + ["_latest", "_updated"]], on=mkey + ["_latest", "_updated"])
    if top.groupby(mkey, dropna=False)[["상품명", "중메뉴", "수동분류", "is_main_candidate"]].nunique().gt(1).any(axis=None):
        raise ValueError("동일 우선순위 상품 마스터 충돌")
    base = {}
    for r in latest.to_dict("records"):
        category = normalize_category(r["수동분류"] or r["중메뉴"])
        basis = "master_manual" if r["수동분류"] else "master"
        if not r["수동분류"] and r["is_main_candidate"] == "Y" and category in {"메인", "미분류"}:
            category = "메인"
        base[tuple(r[k] for k in mkey)] = {"category": category, "name": r["상품명"], "basis": basis}
    if overlay.groupby(IDENTITY, dropna=False)[["category", "standard_menu_name"]].nunique().gt(1).any(axis=None):
        raise ValueError("검수 매핑 키 충돌")
    over = {}
    for r in overlay.drop_duplicates(IDENTITY).to_dict("records"):
        if not all(r[k] for k in IDENTITY) or not r["category"] or not r["standard_menu_name"]:
            raise ValueError("검수 매핑 식별자/분류/표준명 누락")
        category = normalize_category(r["category"])
        if category == "미분류" and r["category"] != "미분류":
            raise ValueError(f"지원하지 않는 검수 분류: {r['category']}")
        over[tuple(r[k] for k in IDENTITY)] = {"category": category, "name": r["standard_menu_name"]}
    return Catalog(base, over)


def classify(identity, original_name, catalog: Catalog) -> dict:
    source, brand, store, item_id = identity
    base = catalog.base.get(identity, {})
    over = catalog.overlay.get(identity, {})
    standard = over.get("name", "")
    name = standard or base.get("name") or original_name
    category = over.get("category") or base.get("category", "미분류")
    basis = "reviewed_map" if over else base.get("basis", "unmapped")
    special = special_category([original_name, name])
    if special:
        category, basis = special, "explicit_selection"
    elif category == "리뷰" or review_name(original_name) or review_name(name):
        category, basis = "리뷰", "reviewed_map" if over and over["category"] == "리뷰" else "review_marker"
    elif category == "미분류":
        category = infer_category(original_name + " " + name)
        if category != "미분류":
            basis = "name_rule"
    key = token(["standard", brand, store, category, standard]) if standard else token(["item", *identity])
    return dict(zip(IDENTITY, identity), item_name=original_name, disp_name=name, standard_menu_name=standard,
                category=category, basis=basis, canonical_key=key, is_main=category in MAIN_CATEGORIES)


def typed_cross(records) -> pd.DataFrame:
    out = pd.DataFrame(records, columns=CROSS_COLUMNS)
    for col in CROSS_COLUMNS:
        out[col] = out[col].astype("int64" if col in {"co_order_cnt", "co_qty", "co_amount"} else "bool" if col == "is_multi_main" else "object")
    return out.sort_values(UNIQUE_KEY).reset_index(drop=True)


def validate_frames(cross, mains, standard):
    if list(cross.columns) != CROSS_COLUMNS:
        raise ValueError("기존 18개 컬럼 계약 위반")
    expected = {c: "int64" if c in {"co_order_cnt", "co_qty", "co_amount"} else "bool" if c == "is_multi_main" else "object" for c in CROSS_COLUMNS}
    if cross.dtypes.astype(str).to_dict() != expected:
        raise ValueError("기존 컬럼 자료형 계약 위반")
    if cross.duplicated(UNIQUE_KEY).any() or cross.isna().any(axis=None):
        raise ValueError("교차 키 중복 또는 결측")
    if not set(cross.pair_type).issubset(ALLOWED_CATEGORIES):
        raise ValueError("알 수 없는 교차 분류")
    if cross.main_item_id.eq(cross.pair_item_id).any():
        raise ValueError("동일 품목 자기 교차")
    if mains.duplicated(DIM + ["level", "main_key"]).any():
        raise ValueError("메인 분모 중복")
    for frame, level in [(cross, "item"), (standard, "standard")]:
        key = "main_item_id" if level == "item" else "main_key"
        denominator = mains[mains.level.eq(level)].rename(columns={"main_key": key})
        check = frame.drop(columns=["main_order_cnt"], errors="ignore").merge(denominator[DIM + [key, "main_order_cnt"]], on=DIM + [key], how="left", validate="many_to_one")
        if check.main_order_cnt.isna().any() or check.co_order_cnt.le(0).any() or check.co_order_cnt.gt(check.main_order_cnt).any():
            raise ValueError("함께 주문한 건수/메인 분모 불일치")
        if check.co_qty.le(0).any() or check.co_amount.lt(0).any():
            raise ValueError("교차 수량/금액 범위 오류")
    if not standard.empty:
        if standard.duplicated(DIM + ["main_key", "pair_type", "pair_key"]).any():
            raise ValueError("표준 메뉴 교차 중복")
        if not np.allclose(standard.selection_rate, standard.co_order_cnt / standard.main_order_cnt, rtol=0, atol=1e-12):
            raise ValueError("선택률 불일치")


def build_day(df: pd.DataFrame, date_str: str, catalog: Catalog, updated_at: str = "") -> dict:
    if missing := set(REQUIRED_SALES_COLUMNS) - set(df.columns):
        raise ValueError(f"매출 필수 컬럼 누락: {sorted(missing)}")
    out = df.copy()
    for col in set(REQUIRED_SALES_COLUMNS) - {"qty", "total_price"}:
        out[col] = out[col].map(text)
    out["source"] = out.source.map(source_name)
    if out.sale_date.ne(date_str).any() or out.ym.ne(date_str[:7]).any():
        raise ValueError("파일 날짜와 원본 판매일/월 불일치")
    quality = {"date": date_str, "version": VERSION, "input_rows": len(out), "excluded": {}, "stores": []}
    reasons = pd.Series("", index=out.index, dtype=object)
    reasons.loc[out.sale_type.eq("취소")] = "cancelled"
    reasons.loc[reasons.eq("") & out.item_name.map(is_fee)] = "fee_or_adjustment"
    for col in ["source", "brand", "store", "order_id", "item_id"]:
        reasons.loc[reasons.eq("") & out[col].eq("")] = "missing_identity"
    for col in ["qty", "total_price"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        active = reasons.eq("")
        if (active & (~np.isfinite(out[col]) | out[col].mod(1).ne(0))).any():
            raise ValueError(f"정수가 아닌 {col}: 원본 수정 필요")
    reasons.loc[reasons.eq("") & out.qty.le(0)] = "nonpositive_qty"
    reasons.loc[reasons.eq("") & out.total_price.lt(0)] = "negative_amount"
    if "_pk" in out:
        dup = out["_pk"].notna() & out["_pk"].astype(str).ne("") & out.duplicated("_pk", keep=False)
        if not out[dup].empty:
            unique = out[dup].drop_duplicates()
            if unique.duplicated("_pk").any():
                raise ValueError("원본 PK 충돌")
            reasons.loc[reasons.eq("") & dup & out.duplicated("_pk")] = "exact_duplicate"
    quality["excluded"] = {str(k): int(v) for k, v in reasons[reasons.ne("")].value_counts().items()}
    original = out.copy()
    original["reason"] = reasons
    out = out[reasons.eq("")].copy()
    mappings, groups, collision = {}, defaultdict(list), {}
    trusted = catalog.trusted if catalog.trusted is not None else defaultdict(dict)
    for identity in (catalog.base.keys() | catalog.overlay.keys()) if catalog.trusted is None else []:
        entry = classify(identity, catalog.base.get(identity, {}).get("name", ""), catalog)
        if entry["is_main"]:
            for name in {entry["disp_name"], catalog.base.get(identity, {}).get("name", "")} - {""}:
                scope = identity[:3]
                normalized = compact(name)
                prior = trusted[scope].get(normalized)
                if prior is None or prior.get("canonical_key") == entry["canonical_key"]:
                    trusted[scope][normalized] = entry
                else:
                    trusted[scope][normalized] = {"ambiguous": True}
    catalog.trusted = trusted
    for r in out.to_dict("records"):
        identity = tuple(r[k] for k in IDENTITY)
        entry = classify(identity, r["item_name"], catalog)
        previous = mappings.get(identity)
        if previous and any(previous[k] != entry[k] for k in ["disp_name", "category", "canonical_key"]):
            raise ValueError("동일 상품번호의 이름/분류 충돌")
        mappings[identity] = entry
        public_key = identity[1:]
        if public_key in collision and collision[public_key] != identity[0]:
            raise ValueError("수집원 사이 상품번호 충돌: 기존 컬럼으로 구분 불가")
        collision[public_key] = identity[0]
        r.update(entry)
        if is_fee(entry["disp_name"]):
            r["excluded_fee"] = True
        groups[(r["sale_date"], r["source"], r["brand"], r["store"], r["order_id"])].append(r)
    raw_agg, std_agg, denominators = {}, {}, {}
    store_stats = defaultdict(Counter)

    def denominator(dim, level, key, name):
        value = denominators.setdefault((*dim, level, key), {"main_name": name, "main_order_cnt": 0})
        value["main_order_cnt"] += 1

    def add(target, key, record, qty, amount, multi):
        value = target.setdefault(key, {**record, "co_order_cnt": 0, "co_qty": 0, "co_amount": 0, "is_multi_main": False})
        value["co_order_cnt"] += 1
        value["co_qty"] += int(qty)
        value["co_amount"] += int(amount)
        value["is_multi_main"] |= multi

    for order, rows in groups.items():
        scope = order[1:4]
        stats = store_stats[scope]
        stats["eligible_orders"] += 1
        if len({r["order_type"] for r in rows}) != 1:
            raise ValueError("한 주문에 서로 다른 주문유형")
        dim = (date_str, order[2], order[3], rows[0]["order_type"])
        dim_dict = dict(zip(DIM, dim))
        items = {}
        for r in rows:
            if r.get("excluded_fee"):
                stats["mapped_fee_rows"] += 1
                continue
            item = items.setdefault(r["item_id"], {**r, "qty": 0, "total_price": 0})
            item["qty"] += int(r["qty"])
            item["total_price"] += int(r["total_price"])
            if r["category"] == "미분류":
                stats["unclassified_rows"] += 1
        mains = {key: {**r, "main_source": "item_id"} for key, r in items.items() if r["is_main"]}
        # 기준 메뉴의 역할은 주문서에서 얻는다. 상품 분류 누락은 주문 제외 사유가 아니다.
        for menu in sorted({r["menu_name"] for r in rows} - {""}):
            if special_category([menu]) or review_name(menu) or is_fee(menu):
                continue
            matched = {r["item_id"] for r in rows if compact(r["item_name"]) == compact(menu)
                       and r["item_id"] in items and r["category"] not in {"리뷰", "리뷰참여", "미선택"}}
            if matched:
                for item_id in matched:
                    mains[item_id] = {**items[item_id], "main_source": "item_id"}
            else:
                anchor = trusted[scope].get(compact(menu))
                digest = hashlib.md5(f"{scope[0]}|{scope[2]}|{menu}".encode()).hexdigest()[:12]
                item_id = "MENU::" + digest
                if not anchor or anchor.get("ambiguous"):
                    anchor = classify((*scope, item_id), menu, catalog)
                mains[item_id] = {**anchor, "item_id": item_id, "main_source": "menu_name"}
                mappings[(*scope, item_id)] = {**anchor, "item_id": item_id, "item_name": menu, "basis": "order_menu_name"}
        if not mains:
            stats["unresolved_main_orders"] += 1
            continue
        stats["analyzed_orders"] += 1
        if any(m["main_source"] == "menu_name" for m in mains.values()):
            stats["synthetic_main_orders"] += 1
        multi = len({r["canonical_key"] for r in mains.values()}) >= 2
        standard_mains = {}
        for main in mains.values():
            standard_mains[main["canonical_key"]] = main
            denominator(dim, "item", main["item_id"], main["disp_name"])
            for pair in items.values():
                if pair["item_id"] == main["item_id"]:
                    continue
                if main["main_source"] == "menu_name" and compact(pair["disp_name"]) == compact(main["disp_name"]):
                    continue
                record = {**dim_dict, "ym": date_str[:7], "main_item_id": main["item_id"], "main_name": main["disp_name"],
                          "main_standard_menu_name": main["standard_menu_name"], "main_source": main["main_source"],
                          "pair_type": pair["category"], "pair_item_id": pair["item_id"], "pair_name": pair["disp_name"],
                          "pair_standard_menu_name": pair["standard_menu_name"], "updated_at": updated_at}
                key = tuple(record[k] for k in UNIQUE_KEY)
                if key in raw_agg and any(raw_agg[key][k] != record[k] for k in ["main_name", "main_source", "pair_name"]):
                    raise ValueError("교차 표시 이름/메인 근거 충돌")
                add(raw_agg, key, record, pair["qty"], pair["total_price"], multi)
        standard_items = {}
        for pair in items.values():
            v = standard_items.setdefault(pair["canonical_key"], {**pair, "qty": 0, "total_price": 0})
            v["qty"] += pair["qty"]
            v["total_price"] += pair["total_price"]
        for key, main in standard_mains.items():
            denominator(dim, "standard", key, main["disp_name"])
            for pkey, pair in standard_items.items():
                if key == pkey:
                    continue
                if main["main_source"] == "menu_name" and compact(pair["disp_name"]) == compact(main["disp_name"]):
                    continue
                record = {**dim_dict, "main_key": key, "main_name": main["disp_name"], "pair_key": pkey,
                          "pair_name": pair["disp_name"], "pair_type": pair["category"]}
                add(std_agg, (*dim, key, pair["category"], pkey), record, pair["qty"], pair["total_price"], multi)
    main_rows = [{**dict(zip(DIM + ["level", "main_key"], key)), **value} for key, value in denominators.items()]
    mains_df = pd.DataFrame(main_rows, columns=MAIN_COLUMNS).sort_values(DIM + ["level", "main_key"]).reset_index(drop=True)
    for key, r in std_agg.items():
        r["main_order_cnt"] = denominators[(*key[:4], "standard", r["main_key"])]["main_order_cnt"]
        r["selection_rate"] = r["co_order_cnt"] / r["main_order_cnt"]
    standard = pd.DataFrame(list(std_agg.values()), columns=STANDARD_COLUMNS).sort_values(DIM + ["main_key", "pair_type", "pair_key"]).reset_index(drop=True)
    cross = typed_cross(list(raw_agg.values()))
    mapping = pd.DataFrame(list(mappings.values()), columns=MAPPING_COLUMNS).sort_values(IDENTITY).reset_index(drop=True)
    for scope, part in original.groupby(["source", "brand", "store"], dropna=False):
        stats = store_stats[scope]
        quality["stores"].append({"source": scope[0], "brand": scope[1], "store": scope[2], "input_rows": len(part),
                                  "excluded_rows": int(part.reason.ne("").sum()), **dict(stats)})
    quality["output_rows"] = len(cross)
    quality["standard_rows"] = len(standard)
    quality["classified_rows"] = len(out)
    quality["mapping_category_counts"] = mapping.category.value_counts().to_dict()
    validate_frames(cross, mains_df, standard)
    quality["validated"] = True
    return {"cross": cross, "main_orders": mains_df, "standard_pairs": standard, "item_mapping": mapping, "quality": quality}
