# Coupang Macro Harness

## 대상
- DAG: `dags/db/DB_CoupangMacro_Load_Dags.py`
- 관련 DAG: `dags/db/DB_Coupang_Notify_Dags.py`, `dags/sales/Sales_CoupangEats_CMG_Partition_Dags.py`
- 주요 pipeline: `modules/transform/pipelines/db/DB_CoupangMacro_load.py`, `DB_Coupang_*.py`, `DB_UnifiedSales_coupang.py`
- 운영 문서: `docs/coupang-boot-automation.md`

## 작업 기준
- 수집 자동화, raw CSV 적재, UnifiedSales 보정의 책임을 분리한다.
- host Chrome, extension, boot automation 변경은 운영 문서를 함께 갱신한다.
- DAG trigger 또는 REST API 호출 변경은 중복 실행 방지와 idempotency를 확인한다.
- UnifiedSales 반영 변경은 `unified_sales_grp.md`도 함께 참조한다.

## 검증
- 최소 import 검증:
  ```powershell
  python -c "import importlib; importlib.import_module('dags.db.DB_CoupangMacro_Load_Dags'); print('import ok')"
  ```
- 적재 로직 변경 시 좁은 테스트나 단일 파일 dry-run 스크립트를 우선 사용한다.

## 금지사항
- 로컬 브라우저 프로필, 인증 쿠키, 다운로드 산출물을 git 대상 경로에 추가하지 않는다.
- 수집 실패를 빈 성공 결과로 삼지 않는다.
