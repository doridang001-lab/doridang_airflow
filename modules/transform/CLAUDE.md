# 파이프라인 개발 규칙

```mermaid
graph LR
    DAG -->|호출| Pipeline
    Pipeline -->|사용| Utility[utility/]
    Pipeline -->|반환| XCom[str or DataFrame]
```

## 작성 규칙
- 반환: str(XCom 메시지) 또는 DataFrame
- 내부 로직은 `_접두사` private 함수로 분리, utility 함수 우선 사용

## 파이프라인 위치
- `pipelines/sales/` - SMD_* 주문/영업 파이프라인
- `pipelines/strategy/` - SMP_* 전략 파이프라인
- `pipelines/db/` - DB_* 원천 수집 및 검증 파이프라인
  - `DB_Beamin_05_ad_funnel.py` — stat/advertisement 광고 funnel (노출수·클릭수·주문수·주문금액)

## DB_UnifiedSales 패턴
소스별 파일로 분리, `common.py`가 스키마·저장 공유:
- `DB_UnifiedSales_common.py` — UNIFIED_COLUMNS, _save_unified_daily, _normalize_item_key
- `DB_UnifiedSales_{source}.py` — 소스별 변환 (okpos/unionpos/easypos/posfeed/toorder)
- posfeed whitelist: `fin_product_posfeed_whitelist.csv`
  - 컬럼: `item_name, is_valid, store, review_needed, classified_by`
  - 신규 item → `_classify_item_with_llm()` (qwen_client) 자동 분류 → `review_needed=Y`
  - `is_valid=N` → `sync_posfeed_blacklist()`로 기존 parquet 소급 삭제

## DB_UnifiedSales_validate.py 패턴
- `build_daily_summary()`: unified_sales parquet → `daily_summary.parquet` 단일 출력
- `store_expected_month_sales` = `bdf["es"]` (store 직접 계산) — `monthly_s` SUM 사용 금지 (max_day 오차)
- LLM 프롬프트: 실매출(`tp`) + 일평균(`ad`) 기반, `expected`/예측값 포함 금지

## 참조
- `docs/architecture.md` - utility 선택 기준표
- `docs/db-schema.md` - DB/경로 참조
