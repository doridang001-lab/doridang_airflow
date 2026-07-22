# Unified Sales Group Harness

## 대상
- DAG: `dags/db/DB_UnifiedSales_Dags.py`
- 주요 pipeline: `modules/transform/pipelines/db/DB_UnifiedSales*.py`
- 공통 스키마: `modules/transform/pipelines/db/DB_UnifiedSales_common.py`

## 작업 기준
- unified_sales 표준 컬럼, 파티션 경로, PK 생성 규칙은 공통 모듈을 우선한다.
- OKPOS, EasyPOS, Posfeed, UnionPOS, Baemin, Coupang 채널별 보정은 해당 채널 모듈에 둔다.
- DAG에는 채널 실행 순서, task dependency, trigger/conf 처리만 둔다.
- 상품 마스터나 `fin_product_grp.csv`에 영향을 주는 변경은 `fin_product.md`도 함께 참조한다.

## 검증
- 최소 검증:
  ```powershell
  python -c "import importlib; importlib.import_module('dags.db.DB_UnifiedSales_Dags'); print('import ok')"
  ```
- 채널별 함수 변경 시 관련 단위 테스트가 있으면 해당 테스트만 먼저 실행한다.
- 날짜 재처리 변경은 `sale_date` 단일 날짜와 `backfill` 범위를 분리해 검토한다.

## 금지사항
- 채널별 원천 수집 로직을 `DB_UnifiedSales_Dags.py`에 직접 추가하지 않는다.
- 공통 스키마 변경 없이 채널별 출력 컬럼만 임의로 늘리지 않는다.
