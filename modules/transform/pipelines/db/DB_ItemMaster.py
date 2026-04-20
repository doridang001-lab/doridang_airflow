"""
item_master 파이프라인 — unique 상품명 추출 → 유사도 매칭 → 배치 LLM 분류 → DB 저장

처리 순서:
1. parquet 전체 스캔 → unique (brand, menu_name, item_name)
2. DB 기존 _pk 조회 → 신규 항목만 필터
3. 기존 분류 결과와 유사도 매칭 → 85% 이상 → LLM 없이 자동 배정
4. 매칭 실패 항목만 배치 LLM 분류 (20개씩 1회 호출)

from modules.transform.pipelines.db.DB_ItemMaster import run
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import ollama
import pandas as pd

from modules.extract.extract_db import db_read_table as read_table
from modules.load.load_postgre_db import postgre_db_save
from modules.transform.utility.paths import MART_DB, LLM_OUTPUT_DIR

logger = logging.getLogger(__name__)

MODEL = "gpt-oss:20b"
OLLAMA_HOST = "http://host.docker.internal:11434"
TABLE = "item_master"
BATCH_SIZE = 20
CHECKPOINT_SIZE = 300
FUZZY_THRESHOLD = 0.85  # 이 이상이면 LLM 없이 기존 분류 복사

VALID_CATEGORIES = {"메인메뉴", "사이드", "음료", "기타"}
_EMPTY_COLS = ["_pk", "brand", "item_name", "menu_name", "item_id",
               "ai_item_name", "ai_category", "classified_at", "model", "is_manual"]

# Optional: process from CSV instead of parquet (file path or directory)
ITEM_MASTER_SOURCE_CSV = os.getenv("ITEM_MASTER_SOURCE_CSV", "").strip()

_NON_WORD_RE = re.compile(r"[^0-9A-Za-z가-힣ㄱ-ㅎㅏ-ㅣ\\s]+")
_MULTI_SPACE_RE = re.compile(r"\\s+")


def _clean_name(value) -> str:
    if value is None:
        return ""
    # pandas NaN
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = _NON_WORD_RE.sub(" ", text)
    text = _MULTI_SPACE_RE.sub(" ", text).strip()
    return text


# ──────────────────────────────────────────────
# 내부 유틸
# ──────────────────────────────────────────────
def _md5(brand: str, menu_name: str, item_name: str) -> str:
    return hashlib.md5(f"{brand}|{menu_name}|{item_name}".encode("utf-8")).hexdigest()


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.strip(), b.strip()).ratio()


def _build_fewshot_examples(existing_df: pd.DataFrame, n: int = 6) -> str:
    """기존 분류 결과에서 카테고리별 균형 있게 예시 추출. 수동 수정 항목 우선."""
    if existing_df.empty:
        return ""
    df = existing_df.dropna(subset=["ai_item_name", "ai_category"])
    # 수동 수정 항목을 앞에 배치 (is_manual 컬럼 없는 구버전 DB 호환)
    if "is_manual" in df.columns:
        df = pd.concat([df[df["is_manual"] == True], df[df["is_manual"] != True]], ignore_index=True)
    samples = (
        df.groupby("ai_category", group_keys=False)
        .apply(lambda g: g.head(max(1, n // 4)))
        .head(n)
    )
    lines = []
    for _, row in samples.iterrows():
        lines.append(
            f'- 입력: brand="{row["brand"]}", item_name="{row["item_name"]}", menu_name="{row.get("menu_name","")}"'
            f'\n  출력: {{"ai_item_name": "{row["ai_item_name"]}", "ai_category": "{row["ai_category"]}"}}'
        )
    return "\n".join(lines)


def _build_system_prompt(fewshot: str) -> str:
    examples_section = fewshot if fewshot else """- 입력: brand="도리당", item_name="도리당 닭도리탕", menu_name="도리당 닭도리탕 외 3건"
  출력: {"ai_item_name": "도리당 닭도리탕", "ai_category": "메인메뉴"}
- 입력: brand="도리당", item_name="1인 순살 닭도리탕 (밥포함)(1인분)", menu_name="도리당 닭도리탕 외 5건"
  출력: {"ai_item_name": "순살 닭도리탕 1인", "ai_category": "메인메뉴"}
- 입력: brand="도리당", item_name="[한우 순살 곱도리탕]", menu_name="[한우 순살 곱도리탕] 외 22건"
    출력: {"ai_item_name": "한우 순살 곱도리탕", "ai_category": "메인메뉴"}
- 입력: brand="도리당", item_name="감튀L", menu_name="사이드 외 2건"
  출력: {"ai_item_name": "감자튀김(L)", "ai_category": "사이드"}
- 입력: brand="도리당", item_name="[후.참] 꼬치오뎅", menu_name="도리당 닭도리탕 외 10건"
  출력: {"ai_item_name": "꼬치오뎅", "ai_category": "사이드"}
- 입력: brand="도리당", item_name="[후.참] 분모자", menu_name="도리당 닭도리탕 외 10건"
  출력: {"ai_item_name": "분모자", "ai_category": "사이드"}
- 입력: brand="도리당", item_name="흑미 공기밥", menu_name="도리당 닭도리탕 외 7건"
  출력: {"ai_item_name": "흑미 공기밥", "ai_category": "사이드"}
- 입력: brand="도리당", item_name="한우 대창 75g 추가", menu_name="도리당 닭도리탕 외 8건"
  출력: {"ai_item_name": "한우 대창 75g 추가", "ai_category": "사이드"}
- 입력: brand="도리당", item_name="치킨무 대신 파김치 주세요", menu_name="도리당 닭도리탕 외 12건"
    출력: {"ai_item_name": "치킨무 대신 파김치", "ai_category": "사이드"}
- 입력: brand="도리당", item_name="콜라(M)", menu_name="음료 외 2건"
  출력: {"ai_item_name": "콜라(M)", "ai_category": "음료"}
- 입력: brand="도리당", item_name="새로", menu_name="도리당 닭도리탕 외 8건"
  출력: {"ai_item_name": "새로(소주)", "ai_category": "음료"}
- 입력: brand="도리당", item_name="참이슬", menu_name="도리당 닭도리탕 외 8건"
    출력: {"ai_item_name": "참이슬", "ai_category": "음료"}
- 입력: brand="도리당", item_name="기본맛", menu_name="도리당 닭도리탕 외 3건"
  출력: {"ai_item_name": "기본맛", "ai_category": "기타"}
- 입력: brand="도리당", item_name="중간매운맛(신라면보다매운)", menu_name="도리당 닭도리탕 외 8건"
    출력: {"ai_item_name": "중간매운맛", "ai_category": "기타"}
- 입력: brand="도리당", item_name="국물많이(중간보다 위)", menu_name="도리당 닭도리탕 외 8건"
    출력: {"ai_item_name": "국물많이", "ai_category": "기타"}
- 입력: brand="도리당", item_name="기본 치킨무 빼주세요", menu_name="도리당 닭도리탕 외 3건"
  출력: {"ai_item_name": "치킨무 빼기", "ai_category": "기타"}
- 입력: brand="도리당", item_name="소스추가", menu_name="추가 외 1건"
  출력: {"ai_item_name": "소스 추가", "ai_category": "기타"}"""

    return f"""너는 한국 F&B 브랜드 메뉴 분류 전문가야.
브랜드명(brand), 상품명(item_name), 메뉴그룹명(menu_name)을 보고 카테고리를 분류하고 표준화된 상품명을 제안해.

[카테고리 정의]
- 메인메뉴: 버거·치킨·덮밥·탕·찌개·세트·1인분 세트 등 식사의 중심이 되는 아이템
           예) 닭도리탕, 닭한마리, 곱도리탕, 묵은지도리탕, 한우순살곱도리탕, 누룽지닭한마리
- 사이드  : 감자튀김·샐러드·공기밥·오뎅·떡류·토핑 추가 등 메인과 함께 먹는 곁들이 음식
           예) 공기밥, 흑미공기밥, 꼬치오뎅, 분모자, 통가래떡, 대창추가, 야채추가
- 음료    : 콜라·사이다·주스·커피·물 등 음료류 + 소주·맥주·막걸리·와인 등 주류 전체
           예) 새로, 처음처럼, 참이슬, 진로, 카스, 하이트, 막걸리, 와인, 사케
- 기타    : 맛 옵션(기본맛·매운맛·순한맛)·제거 요청(빼주세요)·배달비·포장 용기·식별 불가 항목

[중요 규칙]
1. menu_name은 배달앱 메뉴그룹명으로 "XX 외 N건" 형태가 많음 — 분류 참고용으로만 쓰고 item_name을 우선 분석해
2. [후.참] 접두어는 후식/추가주문 항목을 의미 — 식재료·음식이면 사이드, 음료·주류이면 음료로 분류
3. 다음 옵션성 문구는 기본적으로 기타로 분류: 기본맛/매운맛/순한맛/중간맛/국물많이/국물적게/빼주세요/변경/선택
4. 다음 추가 토핑·곁들임은 기본적으로 사이드: 공기밥/오뎅/분모자/가래떡/대창 추가/치즈 추가/계란 추가/치킨무/파김치
5. 주류 키워드(새로/참이슬/처음처럼/진로/카스/하이트/테라/막걸리/와인/사케)는 반드시 음료로 분류
6. item_name에 탕/찌개/닭도리탕/곱도리탕/닭한마리 등 메인 키워드가 있으면 menu_name이 혼란스러워도 메인메뉴 우선
7. ai_item_name은 한국어로 간결하게 (불필요한 숫자코드·괄호 제거, 띄어쓰기 통일, [후.참] 접두어 제거)
8. 같은 item_name이라도 brand가 다르면 메뉴 의미가 다를 수 있으니 brand를 반드시 참고해
9. 응답은 반드시 JSON 배열로 입력 순서대로 출력 (설명 없이)
   [{{"ai_item_name": "...", "ai_category": "..."}}, ...]

[분류 예시]
{examples_section}"""


def _parse_batch_response(text: str, expected: int) -> list[dict]:
    """LLM 배치 응답(JSON 배열)에서 결과 추출. 실패 시 빈 list."""
    text = text.strip()
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        return []
    try:
        data = json.loads(match.group())
        if not isinstance(data, list) or len(data) != expected:
            return []
        for item in data:
            if item.get("ai_category") not in VALID_CATEGORIES:
                item["ai_category"] = "기타"
        return data
    except json.JSONDecodeError:
        return []


# ──────────────────────────────────────────────
# 유사도 매칭
# ──────────────────────────────────────────────
def _fuzzy_match(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    기존 분류 결과와 유사도 비교.
    Returns: (auto_matched_df, needs_llm_df)
    """
    if existing_df.empty:
        return pd.DataFrame(columns=new_df.columns), new_df

    # brand별로 기존 분류 인덱스 구성 (비교 범위 축소)
    existing_by_brand = existing_df.groupby("brand")

    matched_rows, unmatched_rows = [], []

    for _, row in new_df.iterrows():
        brand = row["brand"]
        item_name = str(row["item_name"])
        menu_name = str(row.get("menu_name", ""))

        brand_existing = existing_by_brand.get_group(brand) if brand in existing_by_brand.groups else pd.DataFrame()

        best_score, best_match = 0.0, None
        for _, ex in brand_existing.iterrows():
            # menu_name이 같을 때 가중치 부여
            name_score = _similarity(item_name, str(ex["item_name"]))
            menu_bonus = 0.05 if menu_name and menu_name == ex.get("menu_name", "") else 0.0
            score = min(name_score + menu_bonus, 1.0)
            if score > best_score:
                best_score, best_match = score, ex

        if best_score >= FUZZY_THRESHOLD and best_match is not None:
            row = row.copy()
            row["ai_item_name"] = best_match["ai_item_name"]
            row["ai_category"] = best_match["ai_category"]
            row["model"] = f"fuzzy:{best_score:.2f}"
            matched_rows.append(row)
        else:
            unmatched_rows.append(row)

    matched_df = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame(columns=new_df.columns)
    unmatched_df = pd.DataFrame(unmatched_rows) if unmatched_rows else pd.DataFrame(columns=new_df.columns)

    logger.info("유사도 매칭: 자동 %d건 / LLM 필요 %d건", len(matched_df), len(unmatched_df))
    return matched_df, unmatched_df


# ──────────────────────────────────────────────
# 배치 LLM 분류
# ──────────────────────────────────────────────
def _classify_batch(client: ollama.Client, system_prompt: str, batch: list[dict]) -> list[dict]:
    """N개 아이템을 한 번의 LLM 호출로 분류."""
    user_lines = [
        f'{i+1}. brand="{r["brand"]}", item_name="{r["item_name"]}", menu_name="{r.get("menu_name","")}"'
        for i, r in enumerate(batch)
    ]
    user_msg = "\n".join(user_lines)
    try:
        resp = client.chat(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_msg},
            ],
        )
        results = _parse_batch_response(resp.message.content, len(batch))
        if results:
            return results
        logger.warning("배치 응답 파싱 실패 — 개별 폴백 처리")
    except Exception as e:
        logger.warning("배치 LLM 호출 실패: %s", e)

    # 배치 실패 시 폴백
    return [{"ai_item_name": r["item_name"], "ai_category": "기타"} for r in batch]


def _classify_with_llm(df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """배치 LLM으로 분류. 기존 데이터를 few-shot 예시로 활용."""
    if df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    fewshot = _build_fewshot_examples(existing_df)
    system_prompt = _build_system_prompt(fewshot)
    client = ollama.Client(host=OLLAMA_HOST)

    logger.info("배치 LLM 분류 시작: %d건 / %d건씩 (모델: %s)", len(df), BATCH_SIZE, MODEL)

    records = df.to_dict("records")
    all_results = []

    for start in range(0, len(records), BATCH_SIZE):
        batch = records[start: start + BATCH_SIZE]
        results = _classify_batch(client, system_prompt, batch)
        all_results.extend(results)
        logger.info("  진행: %d / %d", min(start + BATCH_SIZE, len(records)), len(records))

    now = datetime.now()
    df = df.copy().reset_index(drop=True)
    df["ai_item_name"] = [r["ai_item_name"] for r in all_results]
    df["ai_category"] = [r["ai_category"] for r in all_results]
    df["classified_at"] = now
    df["model"] = MODEL
    df["is_manual"] = False
    return df


# ──────────────────────────────────────────────
# 퍼블릭 함수
# ──────────────────────────────────────────────
def extract_unique_items() -> pd.DataFrame:
    """MART_DB unified_sales parquet 전체에서 unique (brand, menu_name, item_name) 추출."""
    if ITEM_MASTER_SOURCE_CSV:
        source_path = Path(ITEM_MASTER_SOURCE_CSV)
        csv_files = sorted(source_path.glob("*.csv")) if source_path.is_dir() else [source_path]
        if not csv_files:
            logger.warning("CSV file not found: %s", source_path)
            return pd.DataFrame(columns=["brand", "item_name", "menu_name", "item_id"])

        dfs: list[pd.DataFrame] = []
        for f in csv_files:
            try:
                df = pd.read_csv(f, dtype=str, encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(f, dtype=str)
            dfs.append(df)

        combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        platform_col = "platform" if "platform" in combined.columns else ("플랫폼" if "플랫폼" in combined.columns else None)
        menu_col = "menuename" if "menuename" in combined.columns else ("menu_name" if "menu_name" in combined.columns else None)
        option_col = "option_item" if "option_item" in combined.columns else ("item_name" if "item_name" in combined.columns else None)

        missing = [name for name, col in [("platform", platform_col), ("menuename", menu_col), ("option_item", option_col)] if col is None]
        if missing:
            logger.warning("CSV columns missing: %s (available=%s)", missing, list(combined.columns))
            return pd.DataFrame(columns=["brand", "item_name", "menu_name", "item_id"])

        combined = combined[[platform_col, menu_col, option_col]].copy()
        combined[platform_col] = combined[platform_col].map(_clean_name)
        combined[menu_col] = combined[menu_col].map(_clean_name)
        combined[option_col] = combined[option_col].map(_clean_name)

        combined = combined.rename(columns={platform_col: "brand", menu_col: "menu_name", option_col: "item_name"})

        combined = combined[combined["item_name"].notna() & (combined["item_name"] != "")]
        combined = combined[combined["brand"].notna() & (combined["brand"] != "")]
        combined["item_id"] = ""
        result = combined.drop_duplicates(subset=["brand", "menu_name", "item_name"], keep="first").reset_index(drop=True)
        logger.info("unique item extracted from CSV: %d", len(result))
        return result[["brand", "item_name", "menu_name", "item_id"]]

    parquet_dir = MART_DB / "unified_sales_grp"
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        logger.warning("parquet 파일 없음: %s", parquet_dir)
        return pd.DataFrame(columns=["brand", "item_name", "menu_name", "item_id"])

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=["brand", "item_name", "menu_name", "item_id"])
            dfs.append(df)
        except Exception as e:
            logger.warning("parquet 읽기 실패 (%s): %s", f.name, e)

    if not dfs:
        return pd.DataFrame(columns=["brand", "item_name", "menu_name", "item_id"])

    combined = pd.concat(dfs, ignore_index=True)
    combined["brand"] = combined["brand"].map(_clean_name)
    combined["menu_name"] = combined["menu_name"].map(_clean_name)
    combined["item_name"] = combined["item_name"].map(_clean_name)
    combined = combined[combined["item_name"].notna() & (combined["item_name"] != "")]

    def mode_first(s):
        m = s.mode()
        return m.iloc[0] if not m.empty else ""

    result = (
        combined.groupby(["brand", "menu_name", "item_name"], as_index=False)
        .agg(item_id=("item_id", mode_first))
    )
    logger.info("unique item 추출: %d건", len(result))
    return result


def _load_existing_master() -> pd.DataFrame:
    """DB에서 기존 item_master 전체 로드 (유사도 매칭 + few-shot용)."""
    try:
        return read_table(TABLE)
    except Exception:
        return pd.DataFrame()


def classify_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    신규 아이템을 분류해 item_master 스키마 DataFrame 반환.
    1) 기존 DB _pk 기준 신규 필터
    2) 유사도 매칭 (LLM 없이 자동 배정)
    3) 나머지만 배치 LLM 분류
    """
    existing_df = _load_existing_master()
    existing_pks = set(existing_df["_pk"].tolist()) if not existing_df.empty else set()

    df = df.copy()
    df["_pk"] = df.apply(lambda r: _md5(r["brand"], r["menu_name"], r["item_name"]), axis=1)
    new_df = df[~df["_pk"].isin(existing_pks)].reset_index(drop=True)

    if new_df.empty:
        logger.info("신규 아이템 없음 — 분류 스킵")
        return pd.DataFrame(columns=_EMPTY_COLS)

    logger.info("신규 아이템: %d건 → 유사도 매칭 시작", len(new_df))

    # 1단계: 유사도 매칭
    matched_df, needs_llm_df = _fuzzy_match(new_df, existing_df)

    if not matched_df.empty:
        matched_df["classified_at"] = datetime.now()
        matched_df["is_manual"] = False

    # 2단계: 나머지 배치 LLM
    llm_df = _classify_with_llm(needs_llm_df, existing_df)

    # 합치기
    result = pd.concat([matched_df, llm_df], ignore_index=True)

    logger.info(
        "분류 완료: 총 %d건 (유사도 %d건 / LLM %d건) | %s",
        len(result),
        len(matched_df),
        len(llm_df),
        result["ai_category"].value_counts().to_dict() if not result.empty else {},
    )
    return result[_EMPTY_COLS]


def save_item_master(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"inserted": 0, "duplicated": 0, "total": 0}
    return postgre_db_save(df, TABLE, pk_col="_pk", add_timestamp=False)


def _checkpoint_base_dir() -> Path:
    return LLM_OUTPUT_DIR / "item_master_checkpoints"


def _save_checkpoints(df: pd.DataFrame, chunk_size: int = CHECKPOINT_SIZE) -> tuple[list[Path], Path]:
    """분류 결과를 chunk_size 단위 CSV로 저장하고 경로 목록 반환."""
    if df.empty:
        return [], _checkpoint_base_dir()

    run_dir = _checkpoint_base_dir() / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    chunk_paths: list[Path] = []
    for idx, start in enumerate(range(0, len(df), chunk_size), start=1):
        chunk_df = df.iloc[start: start + chunk_size].copy()
        chunk_path = run_dir / f"chunk_{idx:04d}.csv"
        chunk_df.to_csv(chunk_path, index=False, encoding="utf-8-sig")
        chunk_paths.append(chunk_path)
        logger.info("체크포인트 저장: %s (%d건)", chunk_path.name, len(chunk_df))

    logger.info("체크포인트 저장 완료: %d개 파일 / 경로=%s", len(chunk_paths), run_dir)
    return chunk_paths, run_dir


def _merge_checkpoints(chunk_paths: list[Path]) -> pd.DataFrame:
    if not chunk_paths:
        return pd.DataFrame(columns=_EMPTY_COLS)

    chunk_dfs = [pd.read_csv(path, encoding="utf-8-sig") for path in chunk_paths]
    merged = pd.concat(chunk_dfs, ignore_index=True)
    logger.info("체크포인트 통합 완료: %d건", len(merged))
    return merged[_EMPTY_COLS]


def _cleanup_checkpoints(chunk_paths: list[Path], run_dir: Path) -> None:
    for path in chunk_paths:
        try:
            if path.exists():
                path.unlink()
        except Exception as e:
            logger.warning("체크포인트 파일 삭제 실패 (%s): %s", path, e)

    try:
        if run_dir.exists() and not any(run_dir.iterdir()):
            run_dir.rmdir()
    except Exception as e:
        logger.warning("체크포인트 폴더 삭제 실패 (%s): %s", run_dir, e)


def classify_and_save_with_checkpoints(items_df: pd.DataFrame, chunk_size: int = CHECKPOINT_SIZE) -> dict:
    """
    1) 전체 분류 수행
    2) chunk_size 단위로 CSV 체크포인트 저장 (항상 보존)
    3) 체크포인트 통합 후 DB 저장 시도
    4) DB 저장 성공 시 체크포인트 삭제 / DB 연결 실패 시 CSV를 남기고 태스크 성공 처리
    """
    classified_df = classify_items(items_df)
    if classified_df.empty:
        return {"inserted": 0, "duplicated": 0, "total": 0}

    chunk_paths, run_dir = _save_checkpoints(classified_df, chunk_size=chunk_size)
    merged_df = _merge_checkpoints(chunk_paths)

    try:
        result = save_item_master(merged_df)
        _cleanup_checkpoints(chunk_paths, run_dir)
        return result
    except Exception as e:
        logger.warning(
            "DB 저장 실패 — CSV 체크포인트 보존 후 태스크 성공 처리.\n"
            "  경로: %s\n  오류: %s",
            run_dir,
            e,
        )
        return {"inserted": 0, "duplicated": 0, "total": len(merged_df), "csv_fallback": str(run_dir)}


def run() -> str:
    items_df = extract_unique_items()
    if items_df.empty:
        return "item_master: 소스 데이터 없음"

    result = classify_and_save_with_checkpoints(items_df, chunk_size=CHECKPOINT_SIZE)
    return f"item_master: 신규 {result['inserted']}건 저장, 중복 {result['duplicated']}건 스킵"
