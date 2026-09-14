# UnifiedSales / UnifiedReview 정합성 체크리스트

통합 데이터 값 이상(0원/미수집/이중집계/타입오류)이 의심될 때 이 체크리스트로 실측한다.

## 사전 확인
- `harness/unified_sales_grp.md`, `harness/review_checklist.md`
- MEMORY `[[unified-toorder-double-count]]` — toorder 이중집계 결론: **okpos+posfeed 우선, toorder 제외**(okpos 매장 자체 집계는 정상)
- DAG: `dags/db/DB_UnifiedSales_Dags.py`, `DB_UnifiedReview_Dags.py`, `DB_UnifiedSales_Schedule_Guard_Dags.py`, `DB_UnifiedSales_Today_Trigger_Dags.py`

## 점검 항목
1. **0원 탐지** — 매장×날짜 그리드에서 매출 0/누락 셀 식별. 실제 미영업 vs 미수집 구분.
2. **이중집계** — 배달행이 okpos/posfeed와 toorder에 중복 계상되는지. 원칙: okpos+posfeed 우선, toorder 제외.
3. **소스별 교차합계** — 통합 합계 = Σ(소스별 합계) 성립 확인. 어긋나면 grp 매핑(`fin_product_grp`) 추적.
4. **qty/타입 검증** — qty·amount 컬럼 dtype이 숫자인지(문자열 혼입 → 합계 오류).
5. **미수집일 lookback** — 최근 N일 소스별 적재 유무. 빠진 날 알람/재수집 트리거 여부.

## 재사용 스크립트
- `scripts/_check_okpos_match.py` — okpos 매칭 점검
- `scripts/_verify_ingest_integrity.py` — 적재 무결성
- `scripts/_verify_okpos_columns.py` — 컬럼 검증
(모두 `scripts/_base.py` 베이스. 새 스크립트는 `scripts/` 또는 `.tmp/`)

## 종료 조건
5개 항목 통과 + 통합 합계와 소스 합계 일치 + 미수집일 사유 명확.
