"""
전처리 파이프라인
from modules.transform.pipelines.strategy.SMP_crawling_toorder_voc_02_trans import (
    load_toorder_voc_df,
    voc_df_store_summary_preprocess_df,
    voc_df_store_topic_summary_preprocess_df,
    review_df_preprocess_df,
)
"""

from pathlib import Path
import re
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR, DOWN_DIR
from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes
import pandas as pd
import numpy as np
import datetime as dt
import ollama


# 중복 제거 기준 컬럼
DEDUP_KEYS = ["번호", "매장명", "작성일자", "작성자", "리뷰내용"]
EMPLOYEE_CSV_CANDIDATES = [
    LOCAL_DB / "영업관리부_DB" / "sales_employee.csv",
    Path(r"C:/Local_DB/영업관리부_DB/sales_employee.csv"),
]


# ============================================================
# LLM 분류 관련 상수
# ============================================================
INPUT_COLS = ["토픽", "분석 하이라이트", "리뷰내용"]

SECONDARY_CATEGORIES = [
    "맛_매움과다", "맛_매움부족", "맛_단맛과다", "맛_짠맛", "맛_싱거움", "맛_국물상태",
    "재료_닭냄새비린내", "재료_신선도불량", "재료_조리불량", "재료_이물질", "재료_묵은지품질",
    "양_닭고기부족", "양_전체양부족", "양_재료편중",
    "누락_주문메뉴", "누락_리뷰이벤트", "누락_요청미반영", "누락_밥",
    "배달_지연", "배달_식음", "배달_포장파손",
    "CS_전화불응", "CS_불친절",
    "맛_기타", "재료_기타", "양_기타", "누락_기타", "배달_기타", "CS_기타",
]

CATEGORIES_STR = "\n".join(SECONDARY_CATEGORIES)

CATEGORY_DESCRIPTIONS = """
## 각 카테고리의 정의와 판단 기준

[맛 관련]
- 맛_매움과다: 음식이 예상보다 맵다, 너무 맵다. "기본맛인데 매워요", "중간맛인데 너무 매워요" 포함. 맵기 단계가 잘못 왔을 때(더 맵게 온 경우)도 포함.
- 맛_매움부족: 매운맛이 부족하다, 안 맵다, 맛이 없다, 심심하다, 밍밍하다, 별로다, 실망이다, 재주문 안한다 등 전반적 부정 평가도 포함.
- 맛_단맛과다: 너무 달다, 떡볶이 맛이다, 단맛이 강하다.
- 맛_짠맛: 너무 짜다.
- 맛_싱거움: 싱겁다, 간이 안 됨.
- 맛_국물상태: 국물이 너무 많다/적다/묽다/걸쭉하다/기름지다/없다.

[재료 관련]
- 재료_닭냄새비린내: 비린내/잡내/냄새/피맛.
- 재료_신선도불량: 신선하지 않다, 상했다, 오래됐다.
- 재료_조리불량: 덜 익었다, 너무 익었다, 퍽퍽, 질기다, 불었다.
- 재료_이물질: 머리카락, 이물질, 플라스틱, 비닐 등.
- 재료_묵은지품질: 묵은지가 없다/적다/이상하다.

[양 관련]
- 양_닭고기부족: 닭고기/고기/대창 양이 적다.
- 양_전체양부족: 전체적인 양이 적다, 비싸다, 가격 대비 양이 적다.
- 양_재료편중: 특정 재료만 많고 다른 건 적다.

[누락 관련]
- 누락_주문메뉴: 주문한 메뉴/사이드/옵션이 빠졌다.
- 누락_리뷰이벤트: 리뷰/후기이벤트 품목이 안 왔다.
- 누락_요청미반영: 요청사항이 반영되지 않았다.
- 누락_밥: 밥이 안 왔다, 밥 상태가 나쁘다(딱딱, 설익음, 냄새, 굳음).

[배달 관련]
- 배달_지연: 배달이 늦었다, 오래 기다렸다.
- 배달_식음: 음식이 식어서 왔다.
- 배달_포장파손: 용기 찌그러짐, 포장 파손, 국물 샘.

[CS 관련]
- CS_전화불응: 전화를 안 받는다.
- CS_불친절: 불친절하다, 진상취급.

도메인별 기타 사용 기준 (위 카테고리에 맞지 않을 때):
- 맛 관련 → 맛_기타
- 재료/품질 → 재료_기타
- 양/가격 → 양_기타
- 메뉴/옵션 누락 → 누락_기타
- 배달 관련 → 배달_기타
- CS/태도 → CS_기타
"기타" 단독 출력 절대 금지.
"""

FEW_SHOT_EXAMPLES = """
## 분류 예시

리뷰: "양이 전보다 작아졌어요" → 양_전체양부족
리뷰: "밥이 오래 된걸 써서 냄새나고 돌이되서옴" → 누락_밥
리뷰: "감자가 덜 익은채로 왔고" → 재료_조리불량
리뷰: "냄새나서 못먹었어요" → 재료_닭냄새비린내
리뷰: "근데 묵은지 도리탕을 시켰는데 묵은지가 없네요" → 재료_묵은지품질
리뷰: "이 가격에 닭 6조각은 아니지 않나요" → 양_닭고기부족
리뷰: "음식이 다 식어서 왔네요" → 배달_식음
리뷰: "배달이 잘못된건지 다 흘러서 와서 아쉽네요" → 배달_포장파손
리뷰: "기름이 많아서 국물 먹기가 힘들어서 아쉬웠어요" → 맛_국물상태
리뷰: "공기밥까지 퍽퍽하고 딱딱할수가 있는지 의문" → 누락_밥
리뷰: "맛도 없었습니다" → 맛_매움부족
리뷰: "환불얘기도 꺼내시더니 진상취급받는것같아서 기분나쁘네요" → CS_불친절
리뷰: "생각보다 양이 적어서" → 양_전체양부족
리뷰: "가래떡 제발 넣지마시라고 사정했는데 또 넣으셨네요" → 누락_요청미반영
리뷰: "배달이 계속 시간 추가되서 40분 늦게 받았어요" → 배달_지연
리뷰: "파김치가 너무 짜요" → 맛_짠맛
리뷰: "당면 양이 너무 많아서" → 양_재료편중
리뷰: "리뷰이벤트 품목 안왔다" → 누락_리뷰이벤트
리뷰: "전화를 안 받으시네요" → CS_전화불응
"""

SYSTEM_PROMPT = f"""[출력 규칙 — 최우선]
아래 카테고리 중 정확히 하나만 출력하라.
한 단어. 설명 없음. 이유 없음. 줄바꿈 없음. 따옴표 없음.

{CATEGORIES_STR}

{CATEGORY_DESCRIPTIONS}

{FEW_SHOT_EXAMPLES}

판단 우선순위:
- 감자/재료가 덜 익었다, 떡이 불었다 → 재료_조리불량
- 묵은지가 없다/적다/이상하다 → 재료_묵은지품질
- 밥 관련 불만 (딱딱, 설익음, 누락, 냄새, 고들밥) → 누락_밥
- 전체 양이 적다, 비싸다(양 맥락) → 양_전체양부족
- 닭/고기가 적다, 작아졌다 → 양_닭고기부족
- 국물 상태 불만 → 맛_국물상태
- 음식이 식었다, 미지근 → 배달_식음
- 용기/포장 파손 → 배달_포장파손
- 요청사항 미반영 → 누락_요청미반영
- 냄새남/비린내 → 재료_닭냄새비린내
- 맛이 없다/심심하다/밍밍/별로/재주문 안한다 → 맛_매움부족
- 분류 불가 → 반드시 도메인_기타

"기타" 단독 출력 절대 금지."""

NORMALIZE_MAP = {
    "양_매움부족":          "맛_매움부족",
    "양_주문메뉴":          "누락_주문메뉴",
    "양_주문메뉴불일치":    "누락_주문메뉴",
    "재료_주문오류":        "누락_주문메뉴",
    "재료_재료부족":        "양_재료편중",
    "배달지연":             "배달_지연",
    "배달_지연됨":          "배달_지연",
    "CS_불응":              "CS_전화불응",
    "CS_불청":              "CS_불친절",
    "맛_잡냄새":            "재료_닭냄새비린내",
    "재료_냉동":            "재료_신선도불량",
    "양_고기부족":          "양_닭고기부족",
    "재료_닭냄새":          "재료_닭냄새비린내",
    "맛_맵기과다":          "맛_매움과다",
    "맛_맵다":              "맛_매움과다",
    "맛_맵기와다":          "맛_매움과다",
    "맛_짜움과다":          "맛_짠맛",
    "맛_맵기부족":          "맛_매움부족",
    "맛_맵기불일치":        "맛_매움부족",
    "맛_달음":              "맛_단맛과다",
    "맛_짜다":              "맛_짠맛",
    "맛_비린맛":            "재료_닭냄새비린내",
    "맛_비린내과다":        "재료_닭냄새비린내",
    "맛_묵은지품질":        "재료_묵은지품질",
    "맛_기름과다":          "맛_국물상태",
    "맛_잡냄새비린내":      "재료_닭냄새비린내",
    "맛_불일치":            "맛_매움부족",
    "맛_전반적불만":        "맛_매움부족",
    "재료_냄새비린내":      "재료_닭냄새비린내",
    "재료_달냄새비린내":    "재료_닭냄새비린내",
    "재료_비린내":          "재료_닭냄새비린내",
    "재료_비린맛":          "재료_닭냄새비린내",
    "재료_냄새남":          "재료_닭냄새비린내",
    "재료_안익음":          "재료_조리불량",
    "재료_불량":            "재료_신선도불량",
    "재료_편중":            "양_재료편중",
    "재료_닭고기부족":      "양_닭고기부족",
    "재료_품질":            "재료_신선도불량",
    "양_국물상태":          "맛_국물상태",
    "양_당면부족":          "양_재료편중",
    "양_사이즈부족":        "양_전체양부족",
    "양_대창양부족":        "양_닭고기부족",
    "양_밥":                "누락_밥",
    "양_부족":              "양_전체양부족",
    "양_적다":              "양_전체양부족",
    "누락_이물질":          "재료_이물질",
    "누락_메뉴":            "누락_주문메뉴",
    "누락_옵션":            "누락_주문메뉴",
    "누락_분모자":          "누락_주문메뉴",
    "누락_고기":            "누락_주문메뉴",
    "누락_수저":            "누락_주문메뉴",
    "배달_전화불응":        "CS_전화불응",
    "배달_불친절":          "CS_불친절",
    "배달_기사태도":        "CS_불친절",
    "기타":                 None,
    "이물질":               "재료_이물질",
    "가격":                 "양_전체양부족",
    "온도":                 "배달_식음",
    "포장상태":             "배달_포장파손",
    "레시피준수":           "재료_조리불량",
    "가격_불만":            "양_전체양부족",
    "재주문거부":           "맛_매움부족",
}

KEYWORD_RULES = [
    (["머리카락", "이물질", "플라스틱", "비닐", "벌레", "뼈조각", "닭털", "탄부스럼", "검은것"], "재료_이물질"),
    (["배달이 늦", "배달 늦", "배달이 느", "1시간", "2시간", "늦게 왔", "늦게왔", "기다렸어요",
      "오래걸", "너무 늦게", "배달 시간", "배달비", "연휴동안 너무 기다"], "배달_지연"),
    (["밥이 안", "밥 안", "밥 누락", "밥이 없", "고들밥", "밥이 딱딱", "밥이 굳", "공기밥",
      "공깃밥", "밥도 안", "밥 냄새", "밥이 오래", "냉동밥"], "누락_밥"),
    (["전화 안", "전화도 안", "전화를 안", "먹통", "전화 불응"], "CS_전화불응"),
    (["불친절", "진상취급", "진상 취급", "기분나쁘", "사과도 없고", "환불얘기도 꺼내시더니"], "CS_불친절"),
    (["빼달라고 했는데", "안빼주", "넣지마시라고", "요청사항", "줄여달라고", "미반영",
      "요청이 불가", "빼고 선택 했더니", "국물 적게 선택"], "누락_요청미반영"),
    (["리뷰이벤트", "후기이벤트", "후기참여", "리뷰 이벤트", "이벤트 품목",
      "리뷰 서비스", "후기 이벤트", "이벤트 참여"], "누락_리뷰이벤트"),
    (["묵은지가 없", "묵은지인데 김치가 거의", "묵은지 도리탕인데 묵은지", "중국산 김치",
      "배추 같다", "묵은지가 녹", "묵은지가 아니", "이름만 묵은지"], "재료_묵은지품질"),
    (["안왔", "누락", "빠졌", "없네", "추가했는데 안", "시켰는데 안왔", "깜빡하셨",
      "안넣어", "안 넣어", "안주셨", "분모자가 안", "계란찜이 안", "대창이 없"],
     "누락_주문메뉴"),
    (["냄새나서 못", "비린내", "비린맛", "잡내", "닭냄새", "누린내", "피맛", "쿰쿰",
      "냄새나고", "냄새 심", "냄새가 나"], "재료_닭냄새비린내"),
    (["감자가 덜", "설익", "안 익", "안익", "덜 끓", "생감자", "질기고", "퍽퍽",
      "뻣뻣", "육즙도 없", "불어서", "덜익", "당면이 퍼", "떡이 불"], "재료_조리불량"),
    (["상한", "쉬어서", "신선하지 않", "냉동인", "오래된", "쉬었다", "닭털"], "재료_신선도불량"),
    (["양이 전보다", "양이 적", "비싸", "가격대비", "사이즈가 줄", "작아졌",
      "1인분 같", "가성비", "돈아까", "가격이 올랐"], "양_전체양부족"),
    (["고기가 아예 없", "닭이 적", "고기양이 적", "대창이 좀", "닭이 작아",
      "닭 조각", "닭이 넘 작", "곱 양", "고기 적", "닭고기 양"], "양_닭고기부족"),
    (["당면만 많", "야채가 없", "건더기가 없", "야채도 없고 닭만",
      "계란찜은 물을", "누룽지만 많", "감자만 많", "당면 양이 너무 많"], "양_재료편중"),
    (["기름이 많아서 국물", "국물만 가득", "국물이 너무없", "국물이 없",
      "한강", "수영장", "묽", "느끼", "국물이 많", "국물이 적"], "맛_국물상태"),
    (["맛도 없", "맛이 심심", "밍밍", "별로", "실망", "재주문 안", "다시는 안",
      "아쉽", "안맵", "맛이 없", "간이 안", "양념이 따로놀"], "맛_매움부족"),
    (["너무 매워", "너무 맵", "매워요", "불닭", "화끈", "진땀", "응급실"], "맛_매움과다"),
    (["너무 달", "달아요", "떡볶이 맛", "떡볶이맛", "단맛이", "달달", "설탕"], "맛_단맛과다"),
    (["너무 짜", "짜요", "짭니다", "짰어요", "파김치가 너무 짜"], "맛_짠맛"),
    (["음식이 식어서", "식어서 왔", "많이 식었", "미지근", "안뜨겁"], "배달_식음"),
    (["다 흘러서 와서", "찌그러져", "터져요", "국물이 넘쳤", "포장 파손", "용기가 팽창"], "배달_포장파손"),
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(col).replace("\n", " ").replace("\r", " ").strip().replace("  ", " ")
        for col in df.columns
    ]
    return df


def _ensure_secondary_category_column(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)
    if "secondary_category" in df.columns:
        return df

    alias_priority = [
        "secondary_category", "secondary category", "secondary_cate", "ndary_cate",
        "secondary_cat", "secondary", "2차카테고리", "2차 카테고리"
    ]
    col_map = {str(c).lower().replace(" ", ""): c for c in df.columns}

    for alias in alias_priority:
        key = alias.lower().replace(" ", "")
        if key in col_map:
            original = col_map[key]
            df = df.rename(columns={original: "secondary_category"})
            print(f"🛠️ secondary_category 컬럼 보정: {original} -> secondary_category")
            return df

    for col in df.columns:
        low = str(col).lower().replace(" ", "")
        if ("ndary" in low and "cate" in low) or ("secondary" in low and "cat" in low):
            df = df.rename(columns={col: "secondary_category"})
            print(f"🛠️ secondary_category 컬럼 추정 보정: {col} -> secondary_category")
            return df

    return df


# ============================================================
# LLM 유틸리티
# ============================================================

def _domain_gita(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["냄새", "비린", "이물질", "머리카락", "신선", "묵은지",
                              "덜익", "설익", "퍽퍽", "질기", "재료", "비닐", "플라스틱"]):
        return "재료_기타"
    if any(k in t for k in ["배달", "늦", "기다", "픽업", "식어", "찌그러", "포장", "배차"]):
        return "배달_기타"
    if any(k in t for k in ["전화", "불친절", "진상", "사과", "기분나쁘", "먹통"]):
        return "CS_기타"
    if any(k in t for k in ["안왔", "누락", "빠졌", "없네", "추가했는데", "이벤트", "후기"]):
        return "누락_기타"
    if any(k in t for k in ["양", "고기", "닭", "대창", "사이즈", "비싸", "가격"]):
        return "양_기타"
    return "맛_기타"


def _normalize_output(raw: str) -> str | None:
    raw = raw.strip().split("\n")[0].strip()
    if raw in SECONDARY_CATEGORIES:
        return raw
    if raw in NORMALIZE_MAP:
        return NORMALIZE_MAP[raw]
    if len(raw) <= 30:
        for cat in SECONDARY_CATEGORIES:
            if cat in raw:
                return cat
        for key, val in NORMALIZE_MAP.items():
            if key in raw:
                return val
    return None


def _keyword_fallback(text: str) -> str:
    text_lower = text.lower()
    for keywords, category in KEYWORD_RULES:
        for kw in keywords:
            if kw in text_lower:
                return category
    return _domain_gita(text)


def _classify_row_ollama(row: pd.Series, model: str) -> str:
    import ollama
    input_text = " ".join(str(row.get(col, '')) for col in INPUT_COLS)
    user_message = "\n".join(f"[{col}] {row.get(col, '')}" for col in INPUT_COLS)

    try:
        response = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_message},
            ],
            options={"temperature": 0.0, "top_p": 0.1, "num_predict": 15}
        )
        raw = response['message']['content'].strip()
        result = _normalize_output(raw)

        if result and not result.endswith("_기타"):
            return result

        # raw 자체 키워드 폴백
        fallback_from_raw = _keyword_fallback(raw)
        if not fallback_from_raw.endswith("_기타"):
            return fallback_from_raw

        # 원본 텍스트 키워드 폴백
        fallback = _keyword_fallback(input_text)
        if not fallback.endswith("_기타"):
            return fallback

        return _domain_gita(input_text)

    except Exception as e:
        print(f"⚠️ LLM 오류 (index={row.name}): {e}")
        return _keyword_fallback(input_text)


def _get_ollama_model() -> str | None:
    try:
        import ollama
        candidates = ['qwen2.5:7b', 'qwen2.5:latest', 'qwen2.5', 'gemma2:2b']
        model_names = [m['model'] for m in ollama.list().get('models', [])]
        for candidate in candidates:
            match = [n for n in model_names if candidate in n]
            if match:
                print(f"✅ LLM 모델 사용: {match[0]}")
                return match[0]
    except Exception as e:
        print(f"⚠️ Ollama 연결 실패 (키워드 폴백 사용): {e}")
    return None


def apply_secondary_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    LLM으로 secondary_category 컬럼 추가.
    - Ollama 사용 불가 시 키워드 폴백
    - 이미 secondary_category가 있는 행은 스킵 (누적 재처리 방지)
    """
    try:
        from tqdm import tqdm
    except ModuleNotFoundError:
        def tqdm(iterable, **kwargs):
            return iterable

        print("⚠️ tqdm 미설치 상태 — 진행바 없이 분류 진행")

    df = df.copy()

    if "secondary_category" not in df.columns:
        df["secondary_category"] = None

    unclassified_mask = df["secondary_category"].isna() | (df["secondary_category"] == "")
    unclassified_df = df[unclassified_mask]

    if len(unclassified_df) == 0:
        print("✅ 분류할 신규 행 없음")
        return df

    print(f"🔍 LLM 분류 대상: {len(unclassified_df)}행")
    llm_model = _get_ollama_model()

    results = []
    for _, row in tqdm(unclassified_df.iterrows(), total=len(unclassified_df), desc="secondary_category 분류"):
        if llm_model:
            results.append(_classify_row_ollama(row, llm_model))
        else:
            input_text = " ".join(str(row.get(col, '')) for col in INPUT_COLS)
            results.append(_keyword_fallback(input_text))

    df.loc[unclassified_mask, "secondary_category"] = results
    print(f"✅ 분류 완료: {len(results)}행")
    return df


# ============================================================
# 데이터 로드
# ============================================================

def load_toorder_voc_df(**context):
    return load_files(
        patterns=['리뷰VOC분석_*'],
        use_glob=True,
        search_paths=[
            DOWN_DIR,
            DOWN_DIR / '업로드_temp',
        ],
        xcom_key='toorder_voc_path',
        file_type='auto',
        header=6,
        **context
    )


def load_toorder_voc_upload_temp_df(**context):
    return load_files(
        patterns=['리뷰VOC분석_*'],
        use_glob=True,
        search_paths=[
            DOWN_DIR / '업로드_temp',
        ],
        xcom_key='toorder_voc_upload_temp_path',
        file_type='auto',
        header=6,
        **context
    )


# ============================================================
# 전처리 함수
# ============================================================

def voc_df_store_summary_transform(df):
    df = df.copy()
    dedup_cols = [c for c in DEDUP_KEYS if c in df.columns]
    if dedup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_cols).copy()
        print(f"[store_summary] 중복 제거: {before - len(df):,}건 (키={dedup_cols})")
    else:
        df.drop_duplicates(inplace=True)

    # 담당자 기준으로 30일 누적을 분리하기 위해 집계 전 담당자 조인
    df = _attach_manager_name(df, overwrite_existing=True)
    if "담당자" not in df.columns:
        df["담당자"] = ""

    df["작성일자"] = pd.to_datetime(df["작성일자"], errors="coerce")
    df = df.dropna(subset=["작성일자"])

    # 하나의 고객 리뷰가 토픽 수만큼 여러 행으로 분리되므로,
    # 번호(토픽별로 다름)가 아닌 "작성자+리뷰내용" 기반 실제 리뷰 고유키 사용
    true_review_key = "_true_review_key"
    true_key_parts = [c for c in ["매장명", "작성일자", "작성자", "리뷰내용"] if c in df.columns]
    if true_key_parts:
        df[true_review_key] = df[true_key_parts].astype(str).agg("|".join, axis=1)
    elif "번호" in df.columns:
        df[true_review_key] = df["번호"].astype(str)
    else:
        df[true_review_key] = df.index.astype(str)

    total_df = (
        df.groupby(["작성일자", "담당자", "매장명"], as_index=False)[true_review_key]
          .nunique()
          .rename(columns={true_review_key: "전체 리뷰수"})
    )

    negative_df = df[df["감정수준"] == "부정"].copy()

    neg_agg_df = (
        negative_df.groupby(["작성일자", "담당자", "매장명"], as_index=False)[true_review_key]
          .nunique()
          .rename(columns={true_review_key: "부정리뷰수"})
    )

    # total_df를 LEFT 기준으로 두어 전체 리뷰수가 올바르게 집계되도록 수정
    # (부정 리뷰가 없는 날짜/매장도 포함, 부정리뷰수는 0으로 채움)
    df = total_df.merge(neg_agg_df, on=["작성일자", "담당자", "매장명"], how="left")
    df["부정리뷰수"] = df["부정리뷰수"].fillna(0).astype(int)
    df["전체 리뷰수"] = pd.to_numeric(df["전체 리뷰수"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values(["담당자", "매장명", "작성일자"]).reset_index(drop=True)

    df['sum_30d_recent'] = df.groupby(['담당자', '매장명'])['부정리뷰수'].transform(
        lambda x: x.rolling(window=30, min_periods=1).sum()
    ).astype(int)

    df['sum_30d_prev'] = df.groupby(['담당자', '매장명'])['부정리뷰수'].transform(
        lambda x: x.shift(30).rolling(window=30, min_periods=1).sum()
    ).fillna(0).astype(int)

    df["작성일자"] = df["작성일자"].dt.strftime("%Y-%m-%d")

    df['pct_change_30d'] = (
        (df['sum_30d_recent'] - df['sum_30d_prev'])
        / df['sum_30d_prev'].replace(0, np.nan) * 100
    )
    return df


def voc_df_store_summary_preprocess_df(input_xcom_key, output_xcom_key, **context):
    return preprocess_df(
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=voc_df_store_summary_transform,
        **context
    )


def voc_df_store_topic_summary_transform(df):
    df = df.copy()
    df = df[df["감정수준"] == "부정"]
    dedup_cols = [c for c in DEDUP_KEYS if c in df.columns]
    if dedup_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_cols).copy()
        print(f"[topic_summary] 중복 제거: {before - len(df):,}건 (키={dedup_cols})")
    else:
        df.drop_duplicates(inplace=True)
    df_store_topic_summary = df.groupby(["작성일자", "매장명", "토픽"]).size().reset_index(name="부정수")
    return df_store_topic_summary


def voc_df_store_topic_summary_preprocess_df(input_xcom_key, output_xcom_key, **context):
    return preprocess_df(
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=voc_df_store_topic_summary_transform,
        **context
    )


def review_df_transform(df):
    """
    부정 리뷰 필터링 후 secondary_category 컬럼 추가.
    누적 파일과 병합하여 중복 제거 후 저장.
    """
    df = df.copy()
    dedup_cols = [c for c in DEDUP_KEYS if c in df.columns]
    if dedup_cols:
        df.drop_duplicates(subset=dedup_cols, inplace=True)
    else:
        df.drop_duplicates(inplace=True)
    df = df[df["감정수준"] == "부정"].reset_index(drop=True)
    df = _ensure_secondary_category_column(df)

    # source_file 컬럼 없으면 기본값
    if "_source_file" not in df.columns:
        df["_source_file"] = "unknown"

    source_file = str(df["_source_file"].iloc[0]) if len(df) > 0 else "unknown"

    # 백업 파일(리뷰VOC분석_1.xlsx) + LLM 컬럼 존재 시 즉시 업로드 경로
    if source_file.startswith("리뷰VOC분석_1") and "secondary_category" in df.columns:
        filled_ratio = df["secondary_category"].fillna("").astype(str).str.strip().ne("").mean() if len(df) else 0
        if filled_ratio > 0:
            print(f"✅ 백업 파일 LLM 컬럼 감지 ({source_file}) — 재분류/누적 병합 없이 바로 업로드")
            return df.rename(columns={"_source_file": "source_file", "_uploaded_at": "uploaded_at"})

    # LLM secondary_category 분류 (미분류 행만)
    df = apply_secondary_category(df)

    # 컬럼명 정리 (내부 컬럼 제거)
    df = df.rename(columns={
        "_source_file": "source_file",
        "_uploaded_at": "uploaded_at",
    })

    print(f"✅ review_df_transform 완료: 전체 {len(df)}행")
    return df


def review_df_preprocess_df(input_xcom_key, output_xcom_key, **context):
    return preprocess_df(
        input_xcom_key=input_xcom_key,
        output_xcom_key=output_xcom_key,
        transform_func=review_df_transform,
        **context
    )


# ============================================================
# Google Sheets 업로드
# ============================================================
from modules.load.load_gsheet import save_to_gsheet

DEFAULT_CREDENTIALS_PATH = r"/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

GSHEET_CONFIG = {
    'store_summary': {
        'xcom_task_id': 'voc_df_store_summary_preprocess_df',
        'xcom_key': 'toorder_voc_store_summary_path',
        'url': 'https://docs.google.com/spreadsheets/d/1xZ9BOmyzRJ7uBYnLG5YVuxrAacD2f6pGFiq9e_Ss9ZQ/edit?usp=sharing',
        'sheet_name': '시트1',
        'label': '매장별 VOC 요약',
    },
    'topic_summary': {
        'xcom_task_id': 'voc_df_store_topic_summary_preprocess_df',
        'xcom_key': 'toorder_voc_store_topic_summary_path',
        'url': 'https://docs.google.com/spreadsheets/d/1hAUYni56eY9q8AbIX7T7OblABbBgZCq2zwEkHMftkzY/edit?usp=sharing',
        'sheet_name': '시트1',
        'label': '매장+토픽별 VOC 요약',
    },
    'review_summary': {
        'xcom_task_id': 'review_df_preprocess_df',
        'xcom_key': 'toorder_voc_review_summary_path',
        'url': 'https://docs.google.com/spreadsheets/d/1jtz6XJ45-5qHmotaFY4jhDDT0uuuAg1s_bIH1WO_4xo/edit?usp=sharing',
        'sheet_name': '시트1',
        'label': '리뷰 요약',
    },
}


def _clean_df_for_gsheet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna('')
    for col in df.select_dtypes(include=[np.floating]).columns:
        mask = np.isinf(df[col])
        df.loc[mask, col] = None
    return df.fillna('')


def _normalize_store_name(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"^도리당\s*", "", s)
    s = re.sub(r"\s+", "", s)
    return s


def _excel_serial_to_datetime(value: float) -> pd.Timestamp:
    return pd.Timestamp("1899-12-30") + pd.to_timedelta(float(value), unit="D")


def _normalize_date_value(value):
    if pd.isna(value) or value == "":
        return ""

    if isinstance(value, (pd.Timestamp, dt.datetime)):
        return value.strftime("%Y-%m-%d") if value.time() == dt.time(0, 0) else value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, dt.date):
        return value.isoformat()

    if isinstance(value, (int, float, np.integer, np.floating)):
        numeric = float(value)
        if 20000 <= numeric <= 70000:
            converted = _excel_serial_to_datetime(numeric)
            return converted.strftime("%Y-%m-%d") if converted.time() == dt.time(0, 0) else converted.strftime("%Y-%m-%d %H:%M:%S")
        return value

    text = str(value).strip()
    if not text:
        return ""

    try:
        numeric = float(text)
        if 20000 <= numeric <= 70000:
            converted = _excel_serial_to_datetime(numeric)
            return converted.strftime("%Y-%m-%d") if converted.time() == dt.time(0, 0) else converted.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m-%d") if parsed.time() == dt.time(0, 0) else parsed.strftime("%Y-%m-%d %H:%M:%S")

    return text


def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    candidate_cols = ["작성일자", "오픈일", "collected_at"]
    for col in candidate_cols:
        if col in out.columns:
            out[col] = out[col].map(_normalize_date_value)

    if "uploaded_at" in out.columns:
        out["uploaded_at"] = out["uploaded_at"].map(_normalize_date_value)
    return out


def _load_employee_manager_df() -> pd.DataFrame:
    employee_path = next((p for p in EMPLOYEE_CSV_CANDIDATES if p.exists()), None)
    if employee_path is None:
        print("[리뷰 요약] ⚠️ 직원 파일 없음 — 담당자 조인 스킵")
        return pd.DataFrame(columns=["_store_key", "담당자"])

    last_error = None
    for encoding in ('utf-8-sig', 'cp949', 'euc-kr'):
        try:
            emp = pd.read_csv(employee_path, encoding=encoding)
            break
        except Exception as e:
            last_error = e
    else:
        print(f"[리뷰 요약] ⚠️ 직원 파일 로드 실패: {last_error}")
        return pd.DataFrame(columns=["_store_key", "담당자"])

    required_cols = {"매장명", "담당자"}
    if not required_cols.issubset(set(emp.columns)):
        print(f"[리뷰 요약] ⚠️ 직원 파일 컬럼 부족: 필요 {required_cols}")
        return pd.DataFrame(columns=["_store_key", "담당자"])

    emp = emp[["매장명", "담당자"]].copy()
    emp["_store_key"] = emp["매장명"].astype(str).map(_normalize_store_name)
    emp = emp[emp["_store_key"].ne("")]
    emp = emp.drop_duplicates(subset=["_store_key"], keep="first")
    return emp[["_store_key", "담당자"]]


def _attach_manager_name(df: pd.DataFrame, overwrite_existing: bool = True) -> pd.DataFrame:
    if "매장명" not in df.columns:
        print("[리뷰 요약] ⚠️ 매장명 컬럼 없음 — 담당자 조인 스킵")
        return df

    mapping_df = _load_employee_manager_df()
    if mapping_df.empty:
        return df

    out = df.copy()
    out["_store_key"] = out["매장명"].astype(str).map(_normalize_store_name)
    out = out.merge(mapping_df.rename(columns={"담당자": "_mapped_manager"}), how="left", on="_store_key")

    if overwrite_existing:
        out["담당자"] = out["_mapped_manager"].fillna("").astype(str)
    else:
        current = out["담당자"].fillna("").astype(str) if "담당자" in out.columns else ""
        mapped = out["_mapped_manager"].fillna("").astype(str)
        out["담당자"] = current
        empty_mask = out["담당자"].str.strip().eq("")
        out.loc[empty_mask, "담당자"] = mapped

    matched = out["담당자"].fillna("").astype(str).str.strip().ne("").sum() if "담당자" in out.columns else 0
    print(f"[리뷰 요약] 담당자 조인 완료: {matched:,}/{len(out):,} 매칭")
    return out.drop(columns=["_store_key", "_mapped_manager"], errors="ignore")


def _reset_sheet_and_upload(df: pd.DataFrame, cfg: dict):
    result = save_to_gsheet(
        df=df,
        sheet_name=cfg['sheet_name'],
        mode="overwrite",
        credentials_path=DEFAULT_CREDENTIALS_PATH,
        url=cfg['url'],
    )

    if result.get('success'):
        print(f"[{cfg['label']}] ✅ 업로드 완료: {len(df):,}건")
    else:
        raise RuntimeError(f"[{cfg['label']}] 업로드 실패: {result.get('error')}")


def _append_unique_sheet_upload(df: pd.DataFrame, cfg: dict, primary_key: list[str] | None = None):
    result = save_to_gsheet(
        df=df,
        sheet_name=cfg['sheet_name'],
        mode="append_unique",
        primary_key=primary_key,
        credentials_path=DEFAULT_CREDENTIALS_PATH,
        url=cfg['url'],
    )

    if result.get('success'):
        print(
            f"[{cfg['label']}] ✅ 누적 업로드 완료: "
            f"신규 {result.get('new', 0):,}건 / 중복 {result.get('duplicate', 0):,}건"
        )
    else:
        raise RuntimeError(f"[{cfg['label']}] 업로드 실패: {result.get('error')}")


def _backfill_review_summary_manager(cfg: dict):
    try:
        existing_df = save_to_gsheet(
            df=None,
            sheet_name=cfg['sheet_name'],
            credentials_path=DEFAULT_CREDENTIALS_PATH,
            url=cfg['url'],
        )
    except Exception as e:
        print(f"[{cfg['label']}] ⚠️ 기존 시트 읽기 실패(담당자 백필 스킵): {e}")
        return

    if not isinstance(existing_df, pd.DataFrame) or existing_df.empty:
        return

    if "uploaded_at" in existing_df.columns and "담당자" in existing_df.columns:
        uploaded_as_dt = pd.to_datetime(existing_df["uploaded_at"], errors="coerce")
        manager_text = existing_df["담당자"].fillna("").astype(str).str.strip()
        manager_date_like = manager_text.str.match(r"^\d{4}-\d{2}-\d{2}(?:\s+\d{1,2}:\d{2}:\d{2})?$")
        swap_mask = uploaded_as_dt.isna() & manager_date_like
        swap_count = int(swap_mask.sum())
        if swap_count > 0:
            tmp = existing_df.loc[swap_mask, "uploaded_at"].copy()
            existing_df.loc[swap_mask, "uploaded_at"] = existing_df.loc[swap_mask, "담당자"]
            existing_df.loc[swap_mask, "담당자"] = tmp
            print(f"[{cfg['label']}] 위치 보정: uploaded_at/담당자 스왑 {swap_count:,}건")

    before_cnt = existing_df["담당자"].fillna("").astype(str).str.strip().ne("").sum() if "담당자" in existing_df.columns else 0
    enriched_df = _attach_manager_name(existing_df, overwrite_existing=True)
    enriched_df = _normalize_date_columns(enriched_df)
    after_cnt = enriched_df["담당자"].fillna("").astype(str).str.strip().ne("").sum() if "담당자" in enriched_df.columns else 0

    result = save_to_gsheet(
        df=enriched_df,
        sheet_name=cfg['sheet_name'],
        mode="overwrite",
        credentials_path=DEFAULT_CREDENTIALS_PATH,
        url=cfg['url'],
    )

    if result.get('success'):
        print(f"[{cfg['label']}] 담당자 전체 재조인 완료: {before_cnt:,} -> {after_cnt:,}")
    else:
        raise RuntimeError(f"[{cfg['label']}] 담당자 전체 재조인 실패: {result.get('error')}")


def _update_upload_temp_files_with_sc(df: pd.DataFrame):
    """
    업로드_temp에 이미 존재하는 xlsx 파일(이전 실행분)에 secondary_category를 병합 저장.
    DOWN_DIR에 없는 파일 = 이미 이동된 파일 → 제자리 업데이트.
    다음 실행 시 LLM 재분류를 건너뛰도록 secondary_category를 유지.
    원본 xlsx의 6행 메타데이터 구조를 그대로 유지.
    """
    if "source_file" not in df.columns or "secondary_category" not in df.columns:
        return

    upload_dir = DOWN_DIR / "업로드_temp"
    merge_keys = [c for c in DEDUP_KEYS if c in df.columns]
    if not merge_keys:
        return

    updated_count = 0
    for name in sorted(set(df["source_file"].dropna().astype(str).tolist())):
        if not name or name == "unknown":
            continue

        # DOWN_DIR에 있으면 _move_processed_files_to_upload_temp가 처리 → 건너뜀
        src_in_down = DOWN_DIR / Path(name).name
        if src_in_down.exists():
            continue

        # 업로드_temp에만 있는 파일
        target = upload_dir / Path(name).name
        if not target.exists():
            continue

        try:
            # 이미 secondary_category가 다 채워져 있으면 스킵
            check_df = pd.read_excel(target, header=6)
            if "secondary_category" in check_df.columns:
                already_filled = check_df["secondary_category"].notna() & (check_df["secondary_category"].astype(str).str.strip() != "")
                if already_filled.all():
                    print(f"✅ secondary_category 이미 완료: {target.name} ({len(check_df):,}건)")
                    continue

            # 6행 메타데이터 보존
            raw_df = pd.read_excel(target, header=None)
            meta_rows = raw_df.iloc[:6].values

            data_df = pd.read_excel(target, header=6)

            # 해당 파일의 secondary_category 추출
            file_sc = (
                df[df["source_file"] == name]
                [[c for c in merge_keys + ["secondary_category"] if c in df.columns]]
                .drop_duplicates(subset=merge_keys)
                .copy()
            )
            if "secondary_category" in data_df.columns:
                data_df = data_df.drop(columns=["secondary_category"])
            data_df = data_df.merge(file_sc, on=merge_keys, how="left")
            data_df["secondary_category"] = data_df["secondary_category"].fillna("")

            # 제자리 업데이트 (임시 파일로 쓰고 교체)
            tmp_path = target.with_suffix(".tmp.xlsx")
            with pd.ExcelWriter(str(tmp_path), engine="openpyxl") as writer:
                data_df.to_excel(writer, index=False, header=True, startrow=6)
                ws = writer.sheets["Sheet1"]
                for r_idx, row_data in enumerate(meta_rows, start=1):
                    for c_idx, val in enumerate(row_data, start=1):
                        ws.cell(row=r_idx, column=c_idx, value=val)

            tmp_path.replace(target)
            updated_count += 1
            sc_count = (data_df["secondary_category"] != "").sum()
            print(f"🔄 업로드_temp 파일 secondary_category 업데이트: {target.name} (분류 {sc_count:,}/{len(data_df):,}건)")

        except Exception as e:
            print(f"⚠️ 업로드_temp 파일 업데이트 실패 ({name}): {e}")

    if updated_count > 0:
        print(f"🔄 업로드_temp 기존 파일 업데이트 총 {updated_count}건")



def _move_processed_files_to_upload_temp(df: pd.DataFrame):
    """
    처리된 xlsx 파일을 업로드_temp 폴더로 이동.
    df에 secondary_category 컬럼이 있으면 원본 xlsx에 병합하여 저장
    (다음 실행 시 LLM 재분류를 건너뛰기 위해).
    원본 xlsx의 6행 메타데이터 구조를 그대로 유지.
    """
    if "source_file" not in df.columns:
        return

    target_dir = DOWN_DIR / "업로드_temp"
    target_dir.mkdir(parents=True, exist_ok=True)

    has_sc = "secondary_category" in df.columns
    merge_keys = [c for c in DEDUP_KEYS if c in df.columns]

    moved_count = 0
    for name in sorted(set(df["source_file"].dropna().astype(str).tolist())):
        if not name or name == "unknown":
            continue

        src = DOWN_DIR / Path(name).name
        if not src.exists():
            continue

        dst = target_dir / Path(name).name
        if dst.exists():
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            dst = target_dir / f"{Path(name).stem}_{timestamp}{Path(name).suffix}"

        # secondary_category를 xlsx에 병합하여 저장 (6행 메타 구조 유지)
        if has_sc and src.suffix.lower() in ('.xlsx', '.xls') and merge_keys:
            try:
                # 1) 6행 메타데이터 raw 보존
                raw_df = pd.read_excel(src, header=None)
                meta_rows = raw_df.iloc[:6].values  # numpy array

                # 2) 실제 데이터 (header=6)
                data_df = pd.read_excel(src, header=6)

                # 3) secondary_category 병합 (해당 파일 분만)
                file_sc = (
                    df[df["source_file"] == name]
                    [[c for c in merge_keys + ["secondary_category"] if c in df.columns]]
                    .drop_duplicates(subset=merge_keys)
                    .copy()
                )
                if "secondary_category" in data_df.columns:
                    data_df = data_df.drop(columns=["secondary_category"])
                data_df = data_df.merge(file_sc, on=merge_keys, how="left")

                # 4) secondary_category 없는 행은 빈 문자열로 (NaN 방지)
                data_df["secondary_category"] = data_df["secondary_category"].fillna("")

                # 5) xlsx 저장: row 0~5에 메타 삽입, row 6(startrow=6)부터 헤더+데이터
                with pd.ExcelWriter(str(dst), engine="openpyxl") as writer:
                    data_df.to_excel(writer, index=False, header=True, startrow=6)
                    ws = writer.sheets["Sheet1"]
                    for r_idx, row_data in enumerate(meta_rows, start=1):
                        for c_idx, val in enumerate(row_data, start=1):
                            ws.cell(row=r_idx, column=c_idx, value=val)

                src.unlink()  # 원본 삭제
                moved_count += 1
                sc_count = (data_df["secondary_category"] != "").sum()
                print(f"📦 secondary_category 병합 저장: {src.name} -> {dst.name} (분류 {sc_count:,}건)")
                continue
            except Exception as e:
                print(f"⚠️ secondary_category 병합 실패 ({src.name}): {e} — 원본 이동으로 대체")

        # 병합 불가 또는 xlsx 아닌 경우: 그냥 이동
        src.replace(dst)
        moved_count += 1
        print(f"📦 처리 파일 이동: {src} -> {dst}")

    if moved_count > 0:
        print(f"📦 처리 완료 파일 이동 총 {moved_count}건")


def move_processed_voc_files(**context):
    ti = context['ti']
    parquet_path = ti.xcom_pull(task_ids='load_toorder_voc_df', key='toorder_voc_path')
    if not parquet_path:
        print("📦 이동 대상 없음: load_toorder_voc_df XCom 비어있음")
        return "0건 이동"

    file_path = Path(parquet_path)
    if not file_path.exists():
        print(f"📦 이동 대상 없음: 파일 없음 ({file_path})")
        return "0건 이동"

    df = pd.read_parquet(file_path)
    if "_source_file" in df.columns and "source_file" not in df.columns:
        df = df.rename(columns={"_source_file": "source_file"})

    # review_summary parquet에서 secondary_category를 가져와 df에 병합
    # → 이동 시 xlsx에 secondary_category 포함 저장 → 다음 실행 시 LLM 재분류 방지
    review_path = ti.xcom_pull(task_ids='review_df_preprocess_df', key='toorder_voc_review_summary_path')
    if review_path:
        rp = Path(review_path)
        if rp.exists():
            try:
                review_df = pd.read_parquet(rp)
                if "_source_file" in review_df.columns and "source_file" not in review_df.columns:
                    review_df = review_df.rename(columns={"_source_file": "source_file"})
                if "secondary_category" in review_df.columns:
                    merge_on = [c for c in DEDUP_KEYS if c in df.columns and c in review_df.columns]
                    if merge_on:
                        sc_df = (
                            review_df[merge_on + ["secondary_category"]]
                            .drop_duplicates(subset=merge_on)
                        )
                        if "secondary_category" in df.columns:
                            df = df.drop(columns=["secondary_category"])
                        df = df.merge(sc_df, on=merge_on, how="left")
                        filled = df["secondary_category"].notna().sum()
                        print(f"📦 secondary_category 병합 완료: {filled:,}건")
                    else:
                        print("⚠️ DEDUP_KEYS 교집합 없음 — secondary_category 병합 생략")
                else:
                    print("⚠️ review_summary에 secondary_category 컬럼 없음")
            except Exception as e:
                print(f"⚠️ review_summary secondary_category 병합 실패: {e}")
    else:
        print("📦 review_summary XCom 없음 — secondary_category 없이 이동")

    # 업로드_temp에 이미 있는 이전 실행분 파일들도 secondary_category 업데이트
    _update_upload_temp_files_with_sc(df)

    before_files = set((DOWN_DIR / "업로드_temp").glob("리뷰VOC분析_*"))
    _move_processed_files_to_upload_temp(df)
    after_files = set((DOWN_DIR / "업로드_temp").glob("리뷰VOC분析_*"))
    moved = len(after_files - before_files)
    return f"{moved}건 이동"
def _upload_to_gsheet(config_key: str, **context):
    cfg = GSHEET_CONFIG[config_key]
    ti = context['ti']

    file_path_value = ti.xcom_pull(task_ids=cfg['xcom_task_id'], key=cfg['xcom_key'])
    if not file_path_value:
        print(f"[{cfg['label']}] 업로드 스킵: 입력 데이터 없음 (XCom None)")
        return f"[{cfg['label']}] 스킵: 데이터 없음"

    file_path = Path(file_path_value)
    if not file_path.exists():
        raise FileNotFoundError(f"[{cfg['label']}] 파일 없음: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix in {'.parquet', '.pq'}:
        df = pd.read_parquet(file_path)
    elif suffix == '.csv':
        last_error = None
        for encoding in ('utf-8-sig', 'cp949', 'euc-kr'):
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                break
            except UnicodeDecodeError as e:
                last_error = e
        else:
            raise RuntimeError(f"[{cfg['label']}] CSV 인코딩 판독 실패") from last_error
    elif suffix in {'.xlsx', '.xls'}:
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"[{cfg['label']}] 지원하지 않는 파일 형식: {file_path}")

    df = _normalize_date_columns(df)

    if config_key in {'review_summary', 'store_summary', 'topic_summary'}:
        df = _attach_manager_name(df)

    df = _clean_df_for_gsheet(df)
    print(f"[{cfg['label']}] {len(df):,}행 × {len(df.columns)}열")

    if config_key == 'review_summary':
        _reset_sheet_and_upload(df, cfg)
    else:
        _reset_sheet_and_upload(df, cfg)


def upload_store_summary_to_gsheet(**context):
    _upload_to_gsheet('store_summary', **context)

def upload_topic_summary_to_gsheet(**context):
    _upload_to_gsheet('topic_summary', **context)

def upload_review_summary_to_gsheet(**context):
    _upload_to_gsheet('review_summary', **context)
