---
name: dag-debug
description: |
  Airflow DAG 수집 실패·미수집·이중집계·타입오류를 조사→재현→수정→검증하는 표준 디버깅 스킬.
  사용자가 "DAG가 실패했어", "수집이 안 됐어", "미수집", "이중집계", "0원으로 나와",
  "특정 매장/날짜 데이터가 없어", "파이프라인 버그 고쳐줘", "prd_codex 태스크 처리" 등을
  언급할 때 반드시 사용한다.

  핵심 역할: 증상만 보고 코드를 바로 고치지 않는다. 실측(로그·마커·parquet)으로 원인을
  확정한 뒤 최소 재현→수정→재검증 루프를 돈다.
  이 절차 없이 바로 고치면 반드시 아래 문제가 발생한다:
  - 증상(빈 결과)만 보고 엉뚱한 곳 수정 → 재발
  - 봇탐지/타임아웃 같은 수집단 문제를 변환단 버그로 오진
  - 재발방지 마커/가드 없이 임시 처치 → 다음 실행에 또 실패
---

## 언제 쓰나
DB_*/Sales_*/Strategy_* DAG가 실패하거나, 수집은 됐는데 값이 비거나(0원/미수집), 소스 간
집계가 어긋날 때. prd_codex(`C:\airflow\prd_codex\`)에 쌓인 수정 태스크를 처리할 때도 사용.

## 사전 확인 (코드 보기 전)
1. `harness/common.md` — 구현/검증 기본 원칙
2. 도메인 지식: `harness/baemin_macro.md`, `harness/coupang_macro.md`, `harness/unified_sales_grp.md`, `harness/fin_product.md`, `harness/review_checklist.md`
3. `docs/codex/dag-change-checklist.md`, 수집 문제면 `docs/codex/crawling-gotchas.md`
4. 관련 MEMORY 디버깅 노트: coupang-macro-failures / baemin-now-store-select-fix / unified-toorder-double-count / coupang-orders-f5-resume

## 도메인별 상세 (필요 시 로드)
- **UnifiedSales/Review 정합성**(0원·이중집계·교차합계·qty타입·미수집 lookback) → `references/unified.md`
- **배민·쿠팡 수집 함정** → `collect-ops` 스킬(references/baemin.md, coupang.md)
- **LLM 분류** → `ollama-classify` 스킬

## 표준 절차 (LOOP)
1. **증상·범위 확정** — 어느 dag_id, 어느 매장(store)·날짜(sale_date), 언제부터. conf 관례는 `sale_date`, `backfill`.
2. **실측** — Airflow 로그, NoData/복구 마커, parquet/csv 실제 파일 존재·행수·컬럼을 눈으로 확인.
   - 진단 스크립트 패턴 재사용: `scripts/_diag_okpos_state.py`, `scripts/_check_*.py`, `scripts/_verify_*.py` (`scripts/_base.py` 베이스: sys.path·summary.json·logging 제공)
   - 새 진단 스크립트는 루트 금지 → `scripts/` 또는 `.tmp/`
3. **원인 분류** — 수집단(봇탐지·타임아웃·shell-ready 오판·not interactable) vs 변환단(집계·타입·조인·중복) vs 스케줄/센서.
4. **최소 재현** — 전체 DAG 돌리기 전에:
   - `python -X utf8 -c "from dags.<domain>.<DAG> import dag; print(dag.dag_id)"` (import 무결성)
   - 파이프라인 함수 단위 좁은 테스트
5. **수정** — 비즈니스 로직은 `modules/transform/pipelines/`에. DAG는 오케스트레이션만. print 금지·logging 사용·하드코딩 금지(paths.py/schedule.py 상수).
6. **재검증** — 4번 재실행 + 실제 산출물 재확인. 인코딩은 UTF-8 확인.
7. **재발방지** — NoData 마커/스케줄 가드/lookback 등 안전장치 추가. `harness/`에 배운 점 반영.

## 종료 조건
증상 재현 불가(정상) + 실제 산출물 정합 + Constraints 위반 없음.

## Constraints
- OneDrive 경로 수정은 사용자 승인 필요(AGENTS.md 절대규칙).
- git commit/push는 지시 없으면 하지 않는다.
- 응답은 한글.
- 관련 지식은 `[[coupang-macro-failures]]`, `[[unified-toorder-double-count]]` 등 memory와 `harness/*.md` 참조.
