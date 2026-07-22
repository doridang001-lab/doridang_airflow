"""
order_menu_structure 파이프라인 — OKPOS 주문을 구조화해 PowerBI LEFT JOIN용 테이블에 증분 저장.

처리 순서:
1. unified_sales parquet 스캔 → source=okpos 필터 → 신규 order만 추출
2. item_name 정규화 (괄호/수량/불필요어 제거)
3. 키워드 규칙 분류 (main_menu / size_option / protein_option / ...)
4. 미분류(unknown)만 LLM 보조 분류
5. order_key로 groupby → 주문 재구성 → final_menu_name 생성
6. order_menu_structure 테이블 증분 저장

from modules.transform.pipelines.db.DB_ItemMaster import run
"""

import json
import logging
import os
import re
from datetime import datetime
from functools import lru_cache
from hashlib import md5
from pathlib import Path

import ollama
import pandas as pd

from modules.extract.extract_db import db_read_table as read_table
from modules.load.load_postgre_db import postgre_db_save
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, ITEM_MASTER_CHECKPOINT_DIR, LOCAL_DB, MART_DB, existing_fin_product_csv_path

logger = logging.getLogger(__name__)

TABLE = "order_menu_structure"
ITEM_MASTER_TABLE = "item_master"
MODEL = "gpt-oss:20b"
OLLAMA_HOST = "http://host.docker.internal:11434"
BATCH_SIZE = 20
CHECKPOINT_PATH = Path(ITEM_MASTER_CHECKPOINT_DIR) / "item_master_classified.csv"
LABELS_PATH = Path(ITEM_MASTER_CHECKPOINT_DIR) / "item_master_labels.csv"
FALLBACK_LABELS_PATH = Path(LOCAL_DB) / "item_master_checkpoints" / "item_master_labels.csv"
ITEM_MASTER_CSV_PATH = Path(ITEM_MASTER_CHECKPOINT_DIR) / "item_master.csv"
try:
    LLM_CONFIDENCE_REVIEW = float(os.getenv("ITEM_MASTER_LLM_CONFIDENCE_REVIEW", "0.7"))
except Exception:
    LLM_CONFIDENCE_REVIEW = 0.7  # fallback

KEYWORD_MAP: dict[str, list[str]] = {
    "메인메뉴":    ["닭도리탕", "곱도리탕", "우도리탕"],
    "토핑":        ["라면사리", "분모자", "꼬치오뎅", "계란", "납작당면", "우삼겹", "대창"],
    "사이드":      ["공기밥", "볶음밥", "주먹밥", "계란찜", "파김치"],
    "음료":    ["콜라", "사이다", "쿨피스", "제로콜라", "환타", "마운틴듀", "밀키스", "코카콜라", "펩시"],
    "주류" : ["맥주", "테라", "카스", "하이트", "처음처럼", "참이슬", "진로", "좋은데이"],
    "닭옵션":  ["순살", "뼈", "닭다리살"],
    "사이즈옵션":  ["1인", "2인", "3인", "소", "중", "대", "한마리", "반마리"],
    "맛옵션":      ["기본맛", "중간맛", "매운맛"],
    "요청사항":    ["빼주세요", "괜찮습니다"],
    "기타요금":    ["배달비", "포장"],
}

_STRIP_RE = re.compile(r"[\[\]()\【\】+]")
_QTY_RE = re.compile(r"\d+(?:개|인분|g|ml|L|인)?")
_DROP_WORDS = ["추가", "선택", "비조리", "후참"]
_OUTPUT_COLS = [
    "_pk", "sale_date", "store", "platform", "order_id",
    "main_menu", "size_option", "protein_option", "flavor_option",
    "topping_list", "side_list", "drink_list",
    "final_menu_name", "llm_used", "processed_at",
]


def _order_pk(sale_date: str, store: str, platform: str, order_id: str) -> str:
    return md5(f"{sale_date}|{store}|{platform}|{order_id}".encode()).hexdigest()


def _item_pk(store: str, platform: str, item_id: str) -> str:
    return md5(f"{store}|{platform}|{item_id}".encode()).hexdigest()


@lru_cache(maxsize=1)
def _load_fin_product_master() -> pd.DataFrame:
    """fin_product_grp_input.csv 로드 (판매단가/제외 플래그 매핑 용도)."""
    try:
        source_path = existing_fin_product_csv_path()
        if not source_path.exists():
            return pd.DataFrame(columns=["상품코드", "판매단가", "exclude_check"])
    except Exception:
        return pd.DataFrame(columns=["상품코드", "판매단가", "exclude_check"])

    try:
        df = pd.read_csv(source_path, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=["상품코드", "판매단가", "exclude_check"])

    if "상품코드" not in df.columns:
        return pd.DataFrame(columns=["상품코드", "판매단가", "exclude_check"])

    if "판매단가" not in df.columns:
        df["판매단가"] = "0"
    if "exclude_check" not in df.columns:
        df["exclude_check"] = "N"

    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df["판매단가"] = (
        pd.to_numeric(df["판매단가"].astype(str).str.replace(",", "", regex=False), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["exclude_check"] = df["exclude_check"].fillna("N").astype(str).str.strip().str.upper().replace({"": "N"})
    # 코드별 최신 1행만 사용 (상품명/단가 변경 히스토리 append 구조 대응)
    if "updated_at" in df.columns:
        ts = pd.to_datetime(df["updated_at"], errors="coerce")
        df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values(["상품코드", "_updated_at_ts"], na_position="last").groupby("상품코드", as_index=False).last()
        df = df.drop(columns=["_updated_at_ts"], errors="ignore")
    else:
        df = df.sort_values(["상품코드"]).groupby("상품코드", as_index=False).last()
    return df[["상품코드", "판매단가", "exclude_check"]]


def normalize_item_name(name: str) -> str:
    """괄호/수량/불필요어 제거 후 공백 정규화."""
    if not name or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = _STRIP_RE.sub("", str(name))
    s = _QTY_RE.sub("", s)
    for word in _DROP_WORDS:
        s = s.replace(word, "")
    return re.sub(r"\s+", "", s).strip()


def classify_by_keyword(norm: str) -> str:
    """KEYWORD_MAP 순서로 포함 여부 확인 → 미매칭 시 '미분류'."""
    for item_type, keywords in KEYWORD_MAP.items():
        for kw in keywords:
            if kw in norm:
                return item_type
    return "미분류"


def _load_item_type_overrides() -> tuple[dict[str, str], dict[str, str]]:
    """
    수동 보정(item_type)을 다음 실행에도 반영하기 위한 override 로드.

    - 우선순위: item_pk(store+platform+item_id) 매칭 > item_id 매칭 > item_name_norm 매칭
    - 같은 key가 여러 번 나오면 마지막(가장 최근 append된) 값을 사용
    """
    # 사람이 편집하기 쉬운 labels 파일을 우선 사용하고, 없으면 기존 classified 체크포인트를 fallback으로 사용.
    override_path = (
        LABELS_PATH
        if LABELS_PATH.exists()
        else (FALLBACK_LABELS_PATH if FALLBACK_LABELS_PATH.exists() else CHECKPOINT_PATH)
    )

    try:
        if not override_path.exists():
            return {}, {}
    except Exception:
        return {}, {}

    try:
        df = pd.read_csv(override_path, encoding="utf-8-sig", dtype=str)
    except Exception:
        return {}, {}

    if df.empty or "item_type" not in df.columns:
        return {}, {}

    df = df.fillna("")

    # '미분류'는 override로 보지 않음
    try:
        df = df[df["item_type"].astype(str).ne("") & df["item_type"].astype(str).ne("미분류")]
    except Exception:
        return {}, {}

    item_id_map: dict[str, str] = {}
    item_pk_map: dict[str, str] = {}
    norm_map: dict[str, str] = {}

    if "item_pk" in df.columns:
        tmp = df.loc[df["item_pk"].astype(str).ne(""), ["item_pk", "item_type"]].drop_duplicates(
            subset=["item_pk"], keep="last"
        )
        item_pk_map = dict(zip(tmp["item_pk"].astype(str), tmp["item_type"].astype(str)))

    if "item_id" in df.columns:
        tmp = df.loc[df["item_id"].astype(str).ne(""), ["item_id", "item_type"]].drop_duplicates(
            subset=["item_id"], keep="last"
        )
        item_id_map = dict(zip(tmp["item_id"].astype(str), tmp["item_type"].astype(str)))

    if "item_name_norm" in df.columns:
        tmp = df.loc[df["item_name_norm"].astype(str).ne(""), ["item_name_norm", "item_type"]].drop_duplicates(
            subset=["item_name_norm"], keep="last"
        )
        norm_map = dict(zip(tmp["item_name_norm"].astype(str), tmp["item_type"].astype(str)))

    # item_pk 우선 적용을 위해 item_id_map 앞에 합쳐서 반환
    merged_id_map: dict[str, str] = {}
    merged_id_map.update(item_id_map)
    merged_id_map.update(item_pk_map)
    return merged_id_map, norm_map


def update_item_master_labels(df_classified: pd.DataFrame) -> pd.DataFrame:
    """
    주문 단위 분류 결과(df_classified)에서 item_id 기반 labels(수동 보정 테이블)를 갱신한다.

    - key: (store, platform, item_id)
    - 기존 라벨이 미분류가 아니면 기본적으로 보존
    - 새 라벨이 미분류이면 기존 라벨을 덮어쓰지 않음
    """
    if df_classified is None or df_classified.empty:
        return pd.DataFrame()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df_classified.copy()

    for col in (
        "store",
        "platform",
        "brand",
        "menu_name",
        "order_type",
        "item_id",
        "item_name",
        "item_name_norm",
        "item_type",
    ):
        if col not in df.columns:
            df[col] = ""
    if "llm_used" not in df.columns:
        df["llm_used"] = False
    if "llm_confidence" not in df.columns:
        df["llm_confidence"] = 0.0

    df["store"] = df["store"].fillna("").astype(str)
    df["platform"] = df["platform"].fillna("").astype(str)
    df["brand"] = df["brand"].fillna("").astype(str)
    df["menu_name"] = df["menu_name"].fillna("").astype(str)
    df["order_type"] = df["order_type"].fillna("").astype(str)
    df["item_id"] = df["item_id"].fillna("").astype(str)
    df["item_name"] = df["item_name"].fillna("").astype(str)
    df["item_name_norm"] = df["item_name_norm"].fillna("").astype(str)
    df["item_type"] = df["item_type"].fillna("").astype(str)
    df["llm_confidence"] = pd.to_numeric(df["llm_confidence"], errors="coerce").fillna(0.0)
    df["item_pk"] = df.apply(lambda r: _item_pk(r["store"], r["platform"], r["item_id"]), axis=1)

    agg = (
        df[df["item_id"].ne("")]
        .groupby(["store", "platform", "item_id", "item_pk"], as_index=False)
        .agg(
            brand=("brand", "last"),
            menu_name=("menu_name", "last"),
            order_type=("order_type", "last"),
            item_name=("item_name", "last"),
            item_name_norm=("item_name_norm", "last"),
            item_type=("item_type", "last"),
            llm_used=("llm_used", "max"),
            llm_confidence=("llm_confidence", "max"),
            seen_count=("item_id", "size"),
        )
    )
    agg["updated_at"] = now_str

    # labels는 OneDrive 경로에서 권한 문제가 날 수 있어, LOCAL_DB로 fallback 가능하게 함
    labels_read_path = LABELS_PATH if LABELS_PATH.exists() else FALLBACK_LABELS_PATH
    labels_write_path = LABELS_PATH

    try:
        labels_write_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # 디렉토리 생성도 실패하면 fallback 사용
        labels_write_path = FALLBACK_LABELS_PATH
        labels_write_path.parent.mkdir(parents=True, exist_ok=True)

    if labels_read_path.exists():
        try:
            existing = pd.read_csv(labels_read_path, encoding="utf-8-sig", dtype=str).fillna("")
        except Exception:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    if existing.empty:
        merged = agg
    else:
        for col in ("store", "platform", "item_id", "item_pk", "item_type"):
            if col not in existing.columns:
                existing[col] = ""
        existing = existing.fillna("")
        key_cols = ["store", "platform", "item_id", "item_pk"]
        merged = existing.merge(agg, on=key_cols, how="outer", suffixes=("_old", ""))

        manual_col = "is_manual"
        if manual_col in merged.columns:
            manual_mask = merged[manual_col].astype(str).isin(["1", "true", "True", "Y", "y"])
        else:
            manual_mask = pd.Series(False, index=merged.index)

        old_type = merged.get("item_type_old", "").fillna("").astype(str)
        new_type = merged.get("item_type", "").fillna("").astype(str)

        def _is_unknown(s: pd.Series) -> pd.Series:
            return s.eq("") | s.str.contains("미분류|unknown|UNK", case=False, na=False)

        keep_old = (~_is_unknown(old_type)) | manual_mask
        can_upgrade = _is_unknown(old_type) & (~_is_unknown(new_type)) & (~manual_mask)

        final_type = old_type.where(keep_old, new_type)
        final_type = final_type.where(~can_upgrade, new_type)
        merged["item_type"] = final_type

        for col in (
            "brand",
            "menu_name",
            "order_type",
            "item_name",
            "item_name_norm",
            "llm_used",
            "llm_confidence",
            "seen_count",
            "updated_at",
        ):
            old_col = f"{col}_old"
            if old_col in merged.columns:
                merged[col] = merged[col].where(merged[col].astype(str).ne(""), merged[old_col])

        drop_cols = [c for c in merged.columns if c.endswith("_old")]
        if drop_cols:
            merged = merged.drop(columns=drop_cols)

    merged = merged.fillna("")
    try:
        labels_write_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(labels_write_path, index=False, encoding="utf-8-sig")
        logger.info("labels 저장 경로: %s", labels_write_path)
    except Exception as exc:
        logger.warning("labels 저장 실패(%s): %s", labels_write_path, exc)
        # OneDrive 등 권한 문제면 LOCAL_DB fallback에 한 번 더 저장 시도
        try:
            fallback_path = FALLBACK_LABELS_PATH
            fallback_path.parent.mkdir(parents=True, exist_ok=True)
            merged.to_csv(fallback_path, index=False, encoding="utf-8-sig")
            logger.info("labels fallback 저장 경로: %s", fallback_path)
        except Exception as exc2:
            logger.warning("labels fallback 저장도 실패(%s): %s", FALLBACK_LABELS_PATH, exc2)
    return merged


def classify_by_llm(items: list[str]) -> dict[str, dict]:
    """unknown 항목만 LLM으로 분류. 반환: {item_name_norm: {item_type, confidence}}."""
    if not items:
        return {}

    valid_types = list(KEYWORD_MAP.keys()) + ["미분류"]
    system_prompt = (
        "너는 한국 F&B 주문 상품명 분류 전문가야.\n"
        f"분류 가능한 타입: {', '.join(valid_types)}\n"
        "각 항목에 대해 JSON 배열로만 응답해:\n"
        '[{"item": "상품명", "item_type": "타입", "confidence": 0.0~1.0}]'
    )

    extra = os.getenv("ITEM_MASTER_LLM_SYSTEM_PROMPT_EXTRA", "").strip()
    if extra:
        system_prompt = f"{system_prompt}\n\nAdditional instructions:\n{extra}"

    client = ollama.Client(host=OLLAMA_HOST)
    result: dict[str, dict] = {}

    for start in range(0, len(items), BATCH_SIZE):
        batch = items[start: start + BATCH_SIZE]
        try:
            resp = client.chat(
                model=MODEL,
                options={"temperature": 0},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(batch, ensure_ascii=False)},
                ],
            )
            raw = (resp.message.content or "").strip()
            match = re.search(r"\[.*\]", raw, re.DOTALL)
            if match:
                for entry in json.loads(match.group()):
                    item_name = entry.get("item", "")
                    confidence = float(entry.get("confidence", 0))
                    item_type = entry.get("item_type", "unknown")
                    if confidence <= LLM_CONFIDENCE_REVIEW:
                        item_type = "미분류"
                    result[item_name] = {"item_type": item_type, "confidence": confidence}
        except Exception as exc:
            logger.warning("LLM 분류 실패 (batch %d~%d): %s", start, start + BATCH_SIZE, exc)
        logger.info("LLM 진행: %d / %d", min(start + BATCH_SIZE, len(items)), len(items))

    return result


def classify_by_llm_items(items: list[dict]) -> dict[str, dict]:
    """
    item_id 단위(=item_pk)로 LLM 분류.

    반환: {item_pk: {item_type, confidence}}
    """
    if not items:
        return {}

    valid_types = list(KEYWORD_MAP.keys()) + ["미분류"]
    system_prompt = (
        "너는 한국 F&B 주문 상품(메인/옵션/사이드/음료/추가/요청사항)을 분류하는 전문가야.\n"
        "입력에는 item_name(원문), item_name_norm(정규화), menu_name(가능하면), brand(가능하면), qty, unit_price 관련 값이 포함될 수 있어.\n"
        "- 가능하면 unit_price_each(단가) 기준으로 판단해. (unit_price_total/qty에서 계산된 값일 수 있어)\n"
        "- fin_unit_price(=fin_product_grp_input.csv 판매단가)가 있으면 단가가 그 근처인지 참고해.\n"
        "- exclude_check=Y이면 타브랜드/제외 상품일 수 있으니 보수적으로 '미분류'로 두고 confidence를 낮게 줘.\n"
        "- 단가가 0이면 '요청사항' 또는 '옵션'일 가능성이 높아.\n"
        "- 용량/수량(예: 350ml, 1개 등)은 분류에 크게 영향 없으니 무시해.\n"
        f"분류 가능한 타입: {', '.join(valid_types)}\n"
        "반드시 JSON 배열로만 응답해:\n"
        '[{"item_pk":"...", "item_type":"타입", "confidence":0.0~1.0}]'
    )

    extra = os.getenv("ITEM_MASTER_LLM_SYSTEM_PROMPT_EXTRA", "").strip()
    if extra:
        system_prompt = f"{system_prompt}\n\nAdditional instructions:\n{extra}"

    client = ollama.Client(host=OLLAMA_HOST)
    result: dict[str, dict] = {}

    for start in range(0, len(items), BATCH_SIZE):
        batch = items[start: start + BATCH_SIZE]
        try:
            resp = client.chat(
                model=MODEL,
                options={"temperature": 0},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(batch, ensure_ascii=False)},
                ],
            )
            raw = (resp.message.content or "").strip()
            match = re.search(r"\[.*\]", raw, re.DOTALL)
            if match:
                for entry in json.loads(match.group()):
                    item_pk = str(entry.get("item_pk", "")).strip()
                    if not item_pk:
                        continue
                    confidence = float(entry.get("confidence", 0))
                    item_type = entry.get("item_type", "미분류")
                    if confidence <= LLM_CONFIDENCE_REVIEW:
                        item_type = "미분류"
                    result[item_pk] = {"item_type": item_type, "confidence": confidence}
        except Exception as exc:
            logger.warning("LLM 분류 실패 (batch %d~%d): %s", start, start + BATCH_SIZE, exc)
        logger.info("LLM 진행: %d / %d", min(start + BATCH_SIZE, len(items)), len(items))

    return result


def extract_new_orders(limit: int | None = None, skip_dedup: bool = False) -> pd.DataFrame:
    """unified_sales parquet → 신규 order-item DataFrame 반환. skip_dedup=True 시 DB 중복 체크 생략."""
    parquet_dir = MART_DB / "unified_sales_grp"
    files = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.exists() else []
    if not files:
        logger.warning("parquet 없음: %s", parquet_dir)
        return pd.DataFrame()

    # 분류 정확도를 위해 LLM 입력 컨텍스트(brand/menu_name/order_type)도 최대한 확보.
    # 구버전 parquet에 컬럼이 없을 수 있어, 읽기 실패 시 최소 컬럼으로 fallback.
    base_cols = [
        "sale_date",
        "store",
        "platform",
        "order_id",
        "item_seq",
        "item_id",
        "item_name",
        "qty",
        "unit_price",
        "source",
    ]
    extra_cols = ["brand", "menu_name", "order_type"]
    need_cols = base_cols + extra_cols
    parts: list[pd.DataFrame] = []
    for f in files:
        try:
            parts.append(pd.read_parquet(f, columns=need_cols))
        except Exception as exc:
            try:
                parts.append(pd.read_parquet(f, columns=base_cols))
                logger.info("parquet 부분 읽기(fallback) 성공: %s", f.name)
            except Exception as exc2:
                logger.warning("parquet 읽기 실패 (%s): %s / fallback도 실패: %s", f.name, exc, exc2)
    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    df = df[(df["source"].astype(str) == "okpos")].copy()
    if df.empty:
        return df

    for col in extra_cols:
        if col not in df.columns:
            df[col] = ""

    df["order_id"] = df["order_id"].fillna("").astype(str)
    df["item_id"] = df["item_id"].fillna("").astype(str)
    if "item_seq" not in df.columns:
        df["item_seq"] = ""
    df["item_seq"] = df["item_seq"].fillna("").astype(str)
    df["item_name"] = df["item_name"].fillna("").astype(str)
    df["sale_date"] = df["sale_date"].astype(str)
    df["store"] = df["store"].fillna("").astype(str)
    df["platform"] = df["platform"].fillna("").astype(str)
    df["brand"] = df["brand"].fillna("").astype(str)
    df["menu_name"] = df["menu_name"].fillna("").astype(str)
    df["order_type"] = df["order_type"].fillna("").astype(str)
    df["unit_price"] = pd.to_numeric(
        df["unit_price"].astype(str).str.replace(",", ""), errors="coerce"
    ).fillna(0).astype(int)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)

    # fin_product_grp_input.csv(판매단가)와 비교해 unit_price(총액/단가) 혼선을 완화
    # - 과거 데이터: unit_price가 "총액"으로 들어온 케이스가 있어 qty로 나눈 값이 단가에 더 근접할 수 있음
    # - 신규 데이터(보정 이후): unit_price가 이미 "단가"인 케이스가 있어 그대로 쓰는 편이 안전
    fin_df = _load_fin_product_master()
    if not fin_df.empty:
        price_map = fin_df.set_index("상품코드")["판매단가"].to_dict()
        exclude_map = fin_df.set_index("상품코드")["exclude_check"].to_dict()
    else:
        price_map = {}
        exclude_map = {}

    df["fin_unit_price"] = df["item_id"].map(lambda x: int(price_map.get(str(x).strip(), 0) or 0))
    df["exclude_check"] = df["item_id"].map(lambda x: str(exclude_map.get(str(x).strip(), "N") or "N")).astype(str)

    unit_raw = df["unit_price"].astype(int)
    qty_num = df["qty"].astype(int)
    unit_div = (unit_raw / qty_num.where(qty_num != 0, 1)).round().astype(int)

    fin_price = df["fin_unit_price"].astype(int)
    # 기본은 raw(=현재 parquet 값). fin_price가 있을 때만 "더 가까운 쪽"을 단가로 채택.
    use_div = (fin_price > 0) & ((unit_div - fin_price).abs() < (unit_raw - fin_price).abs())
    unit_each = unit_raw.where(~use_div, unit_div)

    df["unit_price_total"] = unit_raw
    df["unit_price_each"] = unit_each
    df["unit_price"] = unit_each
    df = df[df["item_name"].ne("")].copy()

    df["_pk"] = df.apply(
        lambda r: _order_pk(r["sale_date"], r["store"], r["platform"], r["order_id"]),
        axis=1,
    )

    if not skip_dedup:
        try:
            existing = read_table(TABLE)
            if not existing.empty and "_pk" in existing.columns:
                existing_pks = set(existing["_pk"].tolist())
                df = df[~df["_pk"].isin(existing_pks)].copy()
        except Exception as exc:
            logger.warning("기존 order 조회 실패 (전체 처리): %s", exc)

    if limit is not None:
        order_pks = df["_pk"].unique()[:int(limit)]
        df = df[df["_pk"].isin(order_pks)].copy()

    logger.info("신규 주문 item 행: %d행 / %d개 order", len(df), df["_pk"].nunique())
    return df.reset_index(drop=True)


def classify_items(df: pd.DataFrame) -> pd.DataFrame:
    """item_name 정규화 + 키워드 분류 + LLM 보조 분류."""
    if df.empty:
        return df

    df = df.copy()
    df["item_name_norm"] = df["item_name"].map(normalize_item_name)
    df["item_type"] = df["item_name_norm"].map(classify_by_keyword)
    if "item_id" in df.columns and "store" in df.columns and "platform" in df.columns:
        df["item_pk"] = df.apply(lambda r: _item_pk(r["store"], r["platform"], r["item_id"]), axis=1)
    else:
        df["item_pk"] = ""

    # 수동 보정값이 있으면 우선 적용 (item_pk > item_id > item_name_norm)
    override_by_id, override_by_norm = _load_item_type_overrides()
    if override_by_id or override_by_norm:
        id_hit = pd.Series(False, index=df.index)
        if override_by_id:
            mapped_pk = df["item_pk"].map(override_by_id)
            pk_hit = mapped_pk.notna()
            df.loc[pk_hit, "item_type"] = mapped_pk[pk_hit]

            mapped_id = df["item_id"].map(override_by_id) if "item_id" in df.columns else pd.Series(index=df.index)
            id_hit = mapped_id.notna() & ~pk_hit
            df.loc[id_hit, "item_type"] = mapped_id[id_hit]

        if override_by_norm:
            mapped = df["item_name_norm"].map(override_by_norm)
            norm_hit = mapped.notna() & ~id_hit
            df.loc[norm_hit, "item_type"] = mapped[norm_hit]

    df["llm_used"] = False
    df["llm_confidence"] = 0.0

    unknown = df[df["item_type"] == "미분류"].copy()
    if not unknown.empty:
        # item_id 단위로 한 번만 분류하도록 item_pk 기준 unique
        uniq = unknown.drop_duplicates(subset=["item_pk"], keep="first")
        payload = []
        for _, r in uniq.iterrows():
            item_pk = str(r.get("item_pk", "")).strip()
            if not item_pk:
                continue
            payload.append(
                {
                    "item_pk": item_pk,
                    "brand": str(r.get("brand", "")),
                    "menu_name": str(r.get("menu_name", "")),
                    "order_type": str(r.get("order_type", "")),
                    "item_id": str(r.get("item_id", "")),
                    "item_name": str(r.get("item_name", "")),
                    "item_name_norm": str(r.get("item_name_norm", "")),
                    "qty": int(r.get("qty", 0) or 0),
                    "unit_price_each": int(r.get("unit_price_each", r.get("unit_price", 0)) or 0),
                    "unit_price_total": int(r.get("unit_price_total", 0) or 0),
                    "fin_unit_price": int(r.get("fin_unit_price", 0) or 0),
                    "exclude_check": str(r.get("exclude_check", "N") or "N"),
                }
            )
        logger.info("LLM 분류 대상: %d개 unique item_pk", len(payload))
        llm_result = classify_by_llm_items(payload)
        if llm_result:
            mask = df["item_type"] == "미분류"
            df.loc[mask, "item_type"] = df.loc[mask, "item_pk"].map(
                lambda pk: llm_result.get(pk, {}).get("item_type", "미분류")
            )
            df.loc[mask, "llm_confidence"] = pd.to_numeric(
                df.loc[mask, "item_pk"].map(lambda pk: llm_result.get(pk, {}).get("confidence", 0)),
                errors="coerce",
            ).fillna(0.0)
            df.loc[mask & df["item_type"].ne("미분류"), "llm_used"] = True

    logger.info("분류 완료: %s", df["item_type"].value_counts().to_dict())
    return df


def reconstruct_orders(df: pd.DataFrame) -> pd.DataFrame:
    """order_key(_pk)로 groupby → 주문 구조 재구성 → final_menu_name 생성."""
    if df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    records = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for pk, grp in df.groupby("_pk"):
        if "item_seq" in grp.columns:
            seq = pd.to_numeric(grp["item_seq"].astype(str), errors="coerce").fillna(999999).astype(int)
            grp = grp.assign(_seq=seq).sort_values("_seq", kind="mergesort").drop(columns=["_seq"])
        first = grp.iloc[0]

        # main_menu: ①item_type=메인메뉴 → ②"도리탕" 포함 → ③unit_price 최고
        main_rows = grp[grp["item_type"] == "메인메뉴"]
        if not main_rows.empty:
            main_menu = main_rows.iloc[0]["item_name_norm"]
        else:
            doridang = grp[grp["item_name_norm"].str.contains("도리탕", na=False)]
            if not doridang.empty:
                main_menu = doridang.iloc[0]["item_name_norm"]
            elif not grp.empty:
                price_col = "unit_price_each" if "unit_price_each" in grp.columns else "unit_price"
                pool = grp
                if "exclude_check" in grp.columns:
                    keep = grp["exclude_check"].fillna("").astype(str).str.strip().str.upper() != "Y"
                    pool = grp[keep]
                    if pool.empty:
                        pool = grp
                main_menu = pool.loc[pool[price_col].idxmax(), "item_name_norm"]
            else:
                main_menu = ""

        def _first(type_: str) -> str:
            rows = grp[grp["item_type"] == type_]
            return str(rows.iloc[0]["item_name_norm"] or "") if not rows.empty else ""

        def _list(type_: str) -> str:
            rows = grp[grp["item_type"] == type_]
            return ", ".join(rows["item_name_norm"].fillna("").astype(str).tolist()) if not rows.empty else ""

        size_option = _first("사이즈옵션")
        protein_option = _first("단백질옵션")
        flavor_option = _first("맛옵션")
        topping_list = _list("토핑")
        side_list = _list("사이드")
        drink_list = _list("음료주류")

        parts = [p for p in [size_option, protein_option, main_menu] if p]
        final_menu_name = " ".join(parts)

        # 배달(=홀 아님) 주문은 주문 1건에 메인 2개 이상일 수 있어,
        # unified_sales.menu_name(=OKPOS에서 분리된 대표메뉴)를 순서대로 모두 가져온다.
        order_type = str(first.get("order_type", "") or "")
        is_hall = "홀" in order_type
        if (not is_hall) and ("menu_name" in grp.columns):
            menu_s = grp["menu_name"].fillna("").astype(str).map(str.strip)
            menu_s = menu_s[menu_s.ne("")]
            if not menu_s.empty:
                # 불필요 요금/서비스성 항목은 제외
                fee_like = menu_s.str.contains(r"(?:배달비|배달팁|포장비|쿠폰|할인|서비스|수수료)", na=False, regex=True)
                menu_s = menu_s[~fee_like]
            if not menu_s.empty:
                # 순서 유지 + 중복 제거(연속 중복은 제거, 비연속 중복은 유지하지 않음)
                menus: list[str] = []
                seen: set[str] = set()
                prev = None
                for raw in menu_s.tolist():
                    norm = normalize_item_name(raw)
                    if not norm:
                        continue
                    if prev == norm:
                        continue
                    prev = norm
                    if norm in seen:
                        continue
                    seen.add(norm)
                    menus.append(norm)
                if menus:
                    final_menu_name = " | ".join(menus)
                    # main_menu도 첫 메뉴로 맞춰 PowerBI에서 혼동을 줄임
                    main_menu = menus[0]

        records.append({
            "_pk": pk,
            "sale_date": str(first["sale_date"]),
            "store": str(first["store"]),
            "platform": str(first["platform"]),
            "order_id": str(first["order_id"]),
            "main_menu": main_menu,
            "size_option": size_option,
            "protein_option": protein_option,
            "flavor_option": flavor_option,
            "topping_list": topping_list,
            "side_list": side_list,
            "drink_list": drink_list,
            "final_menu_name": final_menu_name,
            "llm_used": bool(grp["llm_used"].any()),
            "processed_at": now_str,
        })

    result = pd.DataFrame(records, columns=_OUTPUT_COLS)
    logger.info("주문 재구성 완료: %d건", len(result))
    return result


def save_order_menu(df: pd.DataFrame) -> dict:
    """order_menu_structure 테이블에 증분 저장."""
    if df.empty:
        return {"inserted": 0, "duplicated": 0, "total": 0}
    return postgre_db_save(df, TABLE, pk_col="_pk", add_timestamp=False)


def build_item_master(df_labels: pd.DataFrame) -> pd.DataFrame:
    """PowerBI left join용 item_master 차원 테이블 생성 (key=item_id 중심)."""
    if df_labels is None or df_labels.empty:
        return pd.DataFrame()

    df = df_labels.copy()
    for col in (
        "item_pk",
        "store",
        "platform",
        "brand",
        "menu_name",
        "order_type",
        "item_id",
        "item_name",
        "item_name_norm",
        "item_type",
        "is_manual",
        "llm_used",
        "llm_confidence",
        "seen_count",
        "updated_at",
    ):
        if col not in df.columns:
            df[col] = ""

    df["store"] = df["store"].fillna("").astype(str)
    df["platform"] = df["platform"].fillna("").astype(str)
    df["item_id"] = df["item_id"].fillna("").astype(str)
    df["item_pk"] = df["item_pk"].fillna("").astype(str)

    missing_pk = df["item_pk"].eq("") & df["item_id"].ne("")
    if missing_pk.any():
        df.loc[missing_pk, "item_pk"] = df.loc[missing_pk].apply(
            lambda r: _item_pk(r["store"], r["platform"], r["item_id"]), axis=1
        )

    cols = [
        "item_pk",
        "store",
        "platform",
        "brand",
        "menu_name",
        "order_type",
        "item_id",
        "item_name",
        "item_name_norm",
        "item_type",
        "is_manual",
        "llm_used",
        "llm_confidence",
        "seen_count",
        "updated_at",
    ]
    return df[cols].drop_duplicates(subset=["item_pk"], keep="last").reset_index(drop=True)


def save_item_master(df_labels: pd.DataFrame) -> dict:
    """item_master 테이블을 최신 labels 기준으로 전체 교체 저장."""
    df = build_item_master(df_labels)
    if df.empty:
        return {"inserted": 0, "duplicated": 0, "total": 0}

    try:
        ITEM_MASTER_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(ITEM_MASTER_CSV_PATH, index=False, encoding="utf-8-sig")
        logger.info("item_master CSV 저장: %s", ITEM_MASTER_CSV_PATH)
    except Exception as exc:
        logger.warning("item_master CSV 저장 실패: %s", exc)

    return postgre_db_save(
        df,
        ITEM_MASTER_TABLE,
        pk_col="item_pk",
        add_timestamp=False,
        if_exists="replace",
    )


def load_item_master_labels() -> pd.DataFrame:
    """labels CSV를 읽어 item_master 갱신에 사용."""
    try:
        if LABELS_PATH.exists():
            return pd.read_csv(LABELS_PATH, encoding="utf-8-sig", dtype=str).fillna("")
        if FALLBACK_LABELS_PATH.exists():
            return pd.read_csv(FALLBACK_LABELS_PATH, encoding="utf-8-sig", dtype=str).fillna("")
    except Exception as exc:
        logger.warning("labels 로드 실패: %s", exc)
    return pd.DataFrame()


def run(limit: int | None = None) -> dict:
    """전체 파이프라인 실행."""
    df_items = extract_new_orders(limit=limit)
    if df_items.empty:
        df_labels = load_item_master_labels()
        if not df_labels.empty:
            try:
                save_item_master(df_labels)
                logger.info("신규 주문 없음 — item_master만 갱신")
            except Exception as exc:
                logger.warning("item_master 갱신 실패(무시): %s", exc)
        else:
            # labels 파일 없음 — 전체 parquet 재분류로 부트스트랩
            logger.info("labels 없음 — 전체 parquet 부트스트랩 시작")
            try:
                df_all = extract_new_orders(limit=limit, skip_dedup=True)
                if not df_all.empty:
                    df_classified = classify_items(df_all)
                    df_labels = update_item_master_labels(df_classified)
                    save_item_master(df_labels)
                    logger.info("부트스트랩 완료: %d개 order", df_all["_pk"].nunique())
            except Exception as exc:
                logger.warning("부트스트랩 실패(무시): %s", exc)
        return {"inserted": 0, "duplicated": 0, "total": 0, "order_count": 0, "llm_count": 0}

    order_count = df_items["_pk"].nunique()
    df_classified = classify_items(df_items)

    # item_id 단위 labels/차원 테이블 갱신 (order_id 변화/잡음과 분리)
    try:
        df_labels = update_item_master_labels(df_classified)
        save_item_master(df_labels)
    except Exception as exc:
        logger.warning("item_master 갱신 실패(무시하고 계속): %s", exc)

    df_orders = reconstruct_orders(df_classified)
    result = save_order_menu(df_orders)
    result["order_count"] = order_count
    result["llm_count"] = int(df_classified["llm_used"].sum()) if "llm_used" in df_classified.columns else 0
    return result
