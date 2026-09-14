# Fin Product Harness

## 대상
- DAG: `dags/db/DB_FinProduct_Dags.py`
- 매핑 DAG: `dags/db/DB_FinProduct_Map_Dags.py`
- 주요 pipeline: `modules/transform/pipelines/db/DB_FinProduct.py`, `DB_FinProduct_Map.py`, `DB_ItemIdAllocator.py`
- 연동 영역: UnifiedSales 상품 매핑, Posfeed/UnionPOS 신규 상품 반영

## 작업 기준
- 상품 식별자, 최신 여부, 그룹 매핑 규칙은 기존 `fin_product_grp.csv` 처리 흐름을 우선한다.
- OKPOS/EasyPOS 상품조회 원천 컬럼 변경은 로더와 정규화 단계를 분리해 처리한다.
- UnifiedSales에서 상품 마스터를 참조하는 변경은 `unified_sales_grp.md`도 함께 참조한다.
- 수동 검토가 필요한 LLM 또는 매핑 보정은 자동 확정하지 않고 검토 플래그를 유지한다.

## 검증
- 최소 import 검증:
  ```powershell
  python -c "import importlib; importlib.import_module('dags.db.DB_FinProduct_Dags'); print('import ok')"
  ```
- 상품 매핑 변경은 샘플 입력과 결과 row count, 신규/기존 item_id 변화를 함께 확인한다.

## 금지사항
- 기존 상품 ID를 근거 없이 재할당하지 않는다.
- 원천 엑셀 부재를 정상 성공으로 처리하지 않는다.
