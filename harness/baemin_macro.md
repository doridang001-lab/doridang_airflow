# Baemin Macro Harness

## 대상
- DAG: `dags/db/DB_Beamin_Macro_Dags.py`
- 보조 DAG: `dags/db/DB_Beamin_Macro_Dags_Retry.py`, `dags/db/DB_Beamin_Macro_Pc2_Dags.py`, `dags/db/DB_Beamin_Macro_Upload_Pc2_Dags.py`
- 주요 pipeline: `modules/transform/pipelines/db/DB_Beamin_*.py`, `DB_Beamin_combined.py`, `DB_Beamin_retry.py`, `DB_Beamin_Macro_validate.py`

## 작업 기준
- 계정/매장 순회, retry, validation, manual load 경계가 섞이지 않게 유지한다.
- 상위 정기 수집은 단일 `DB_Beamin_Macro_Dags` 안에서 `collect_batch_1`~`collect_batch_4`를 2레인으로 실행하고, 네 배치 실패를 합친 뒤 `retry_failed`를 한 번 수행한다.
- 네 배치는 같은 staging을 누적 사용하되 `_collect_progress_batch_*` 체크포인트는 각각 분리한다. 중간 배치 실패·타임아웃도 다음 배치와 최종 retry를 막지 않는다.
- `DB_Beamin_Macro_Dags_B2`·`B3`·`B4` legacy DAG는 제거되었고, 신규 수집은 단일 `DB_Beamin_Macro_Dags`의 4개 배치 task만 사용한다.
- NOW·우리가게 단계 실패는 조용히 성공 처리하지 않고 계정 전체 최종 retry 대상으로 기록한다.
- `DB_Beamin_Macro_Upload_Pc2_Dags`는 중앙 PC에서만 unpause하고 bottom 폴더를 기존 upload DAG으로 직렬 전달한다.
- PC2 watcher에 적재·검증 task를 복제하지 않고 기존 upload DAG의 `max_active_runs=1` 직렬화를 유지한다.
- PC2(`JSFNE-K`)는 OneDrive 선택적 동기화에서 `data/mart/`를 제외한다. PC2에 필요한 경로는 `Collect_Data/영업관리부_수집/_baemin_pc2_inbox/`뿐이며, mart를 동기화하면 `unified_sales_YYMMDD-JSFNE-K.parquet` 충돌본으로 매출이 이중 집계된다.
- 중앙 상위 수집은 export 완료 직후 해당 top 폴더만 기존 upload DAG으로 트리거해 07:40 이전 미완료분도 당일 적재한다.
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
