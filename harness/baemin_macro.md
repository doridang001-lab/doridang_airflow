# Baemin Macro Harness

## 대상
- DAG: `dags/db/DB_Beamin_Macro_Dags.py`
- 보조 DAG: `dags/db/DB_Beamin_Macro_Dags_Retry.py`, `dags/db/DB_Beamin_Macro_Pc2_Dags.py`
- 주요 pipeline: `modules/transform/pipelines/db/DB_Beamin_*.py`, `DB_Beamin_combined.py`, `DB_Beamin_retry.py`, `DB_Beamin_Macro_validate.py`

## 작업 기준
- 계정/매장 순회, retry, validation, manual load 경계가 섞이지 않게 유지한다.
- Selenium/브라우저 안정화 변경은 crawler 문서와 기존 recovery 테스트를 확인한다.
- 배민 수동 CSV 적재와 자동 수집 보정은 `DB_BaeminManual_load.py`와 validation 모듈의 기존 규칙을 따른다.
- UnifiedSales 후속 반영이 필요하면 `unified_sales_grp.md`도 함께 참조한다.

## 검증
- 최소 import 검증:
  ```powershell
  python -c "import importlib; importlib.import_module('dags.db.DB_Beamin_Macro_Dags'); print('import ok')"
  ```
- 관련 테스트 우선:
  ```powershell
  python -m pytest tests/test_baemin_macro_driver_recovery.py tests/test_baemin_macro_validation.py
  ```

## 금지사항
- 운영 계정, 비밀번호, 세션 값을 문서나 코드에 새로 기록하지 않는다.
- 재시도 횟수나 sleep을 근거 없이 크게 늘려 장애를 숨기지 않는다.
