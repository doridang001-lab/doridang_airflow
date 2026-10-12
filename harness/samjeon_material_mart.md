# 송파삼전점 원재료 mart

## 대상과 실행

- DAG: `dags/db/DB_Samjeon_Material_Mart_Dags.py`
- 변환: `modules/transform/pipelines/db/samjeon_material_agent/mart.py`
- 신규 DAG는 일시중지 상태로 등록한다. 일정은 한국시간 09:15~11:45에 30분 간격이다.
- `publish` 태스크는 Airflow Variable `SAMJEON_MATERIAL_MART_PUBLISH_ENABLED=true`일 때만 OneDrive에 쓴다. 기본값은 `false`이며 미설정 시 게시를 건너뛴다. 승인 전에는 DAG 일시중지도 유지한다.
- 기본 기준일은 한국시간 전일이다. 수동 실행은 `{"sale_date":"YYYY-MM-DD"}` 또는 `{"date_from":"YYYY-MM-DD","date_to":"YYYY-MM-DD"}`로 지정한다. 혼용·역전·당일 이후 날짜·지원하지 않는 conf는 실패시킨다.
- 정기 실행은 기준일까지 57일, 기간 지정은 요청 기간과 종료일 월 누적·8주 분석 범위를 재계산한다. XCom에는 로컬 `plan.json` 경로·해시·스냅샷 ID만 보낸다. 실행별 작업 폴더를 분리한다.

## 입력과 분류

- 매출: `MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet`, `store=송파삼전점`.
- 송파삼전점 `source=okpos`는 홀 매출이다. 일별·월 누적 `hall_sales_won`에 순매출을 부호 그대로 더한다. 다른 원천은 홀로 합치지 않으며 전체 매출에서 홀 소계를 뺀 금액을 배달 매출로 확정하지 않는다.
- 구매: `ANALYTICS_DB/Food_Guide_Raw/food_guide_orders_YYYYMM.parquet`와 `ANALYTICS_DB/Logistics_Dam/Dam_order_details_YYMMDD.parquet`.
- `도리당 송파삼전점(부식)`은 품목명에 관계없이 `현장 직원식대`로 분리한다.
- `LOCAL_DB/samjeon_material_agent/classification.json`의 `items` 배열에 `source, product_code, item_name, spec, unit, category`를 검수해 입력한다. 허용 분류는 `원재료`, `포장재`, `기타`다. 미등록 품목은 `미분류`이며 `review_candidates.csv`에 나온다.
- `review_candidates.csv`는 물류 품목 마스터의 정확한 5개 키와 푸드가이드 대분류로 추천한다. 추천만으로는 확정하지 않는다. 직원식대 우선 분류는 검수표와 무관하게 유지한다.
- 원천별 날짜 마감은 `LOCAL_DB/samjeon_material_agent/coverage.json`의 `days/YYYY-MM-DD/<sales|food_guide|logistics_dam>`에 범위·날짜별 `slice_hash`·확인 근거·확인자를 저장한다. 파일이 있어도 확인 기록이 없으면 잠정이다. 월별 파일의 다른 날짜 변경은 마감을 해제하지 않으며 해당 날짜 변경은 오래됨으로 판정한다.
- 원천 파일이 없으면 누락이다. 행이 없는 날짜를 정상 0원으로 취급하려면 해당 날짜·계정 전체의 수집 완료 근거와 명시적인 무거래 확인이 필요하다. 물류담 개별 행의 `마감됨`만으로 날짜 전체 완료를 인정하지 않는다.
- 운영 설정은 `LOCAL_DB/samjeon_material_agent/config.json`이다. 기본값은 목표율 `0.37`, 적용일 `null`, 금액 기준 확인 `false`, 증가율 `0.30`, 증가액 `100000`, 요일 최소 표본 `4`다. 적용일과 금액 기준 미확인 시 참고 비교만 제공한다.

## 출력과 검증

- 스테이징: `LOCAL_DB/temp/samjeon_material_mart/<snapshot_id>/<실행ID>`.
- 게시: `MART_DB/CEO_DB/samjeon_agent_DB/snapshots/<snapshot_id>`.
- `purchase_lines`, `sales_lines`, `sales_daily`, `purchase_daily`, `item_daily`, `quality_daily`는 Parquet이다. `agent_context.json`, `agent_guide.md`, `manifest.json`, `issues.json`, `review_candidates.csv`, `coverage_candidates.csv`도 함께 게시한다. LLM은 같은 스냅샷의 가이드와 문맥을 함께 읽어야 한다.
- 가이드 원본은 변환 패키지의 `agent_guide.md`다. 내용 해시를 스냅샷 ID와 manifest·agent_context에 기록하므로 가이드 변경도 새 판본으로 게시한다.
- v3는 boolean·정수 원·비율 문자열 계약을 검사하고, 원천 행 → 날짜·공급처·계정·분류 → 품목 → 결과 문맥을 대사한다. 근거 ID 중복·기간 밖 날짜·원천/설정/추천표/계산 코드 해시 변경 시 게시하지 않는다.
- 미분류는 순금액이 0이어도 행수로 미완료 판정한다. 필요한 원천이 있고 분류 완료·마감 미확인인 경우에만 잠정 비율을 제공한다. 월말 전망은 기준일 이전 56일의 표본을 고정하고, 매출·매입 각각 요일별 4개 이상의 완료 표본을 요구한다.
- 검증 후 버전 폴더를 게시한다. 기준일이 전일인 단일 날짜 실행만 `latest.json`을 마지막에 교체한다. 과거 날짜 및 기간 지정 실행은 최신 포인터를 바꾸지 않는다. 게시 실패 시 기존 포인터를 보존하고 이번 실행의 임시 게시 폴더를 제거한다.
- OneDrive 게시를 실제 실행하려면 저장소의 OneDrive 변경 승인 규칙을 지킨다.
- 2026-10-12에 v1 DAG가 정기·수동 실행돼 v1 스냅샷 1개를 OneDrive에 게시한 사실을 확인했다. DAG를 일시중지했으며 기존 산출물은 별도 승인 없이 수정·삭제하지 않는다.

## 로컬 검증

```powershell
python -X utf8 -m pytest tests/test_samjeon_material_mart.py tests/test_samjeon_material_mart_v3.py -q -p no:cacheprovider
python scripts/harness_cli.py validate --target .
```

## 로컬 계산·검수·조회

아래 명령은 OneDrive에 게시하지 않는다. `build --dry-run`과 `doctor`는 계산 결과만 반환하고 파일을 저장하지 않는다.

```powershell
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent doctor --date 2026-10-11
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent build --date 2026-10-11 --dry-run
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent build --date-from 2026-07-01 --date-to 2026-10-11
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent query --snapshot '<로컬 스냅샷 폴더>' --intent summary
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent query --snapshot '<로컬 스냅샷 폴더>' --intent evidence --date-from 2026-10-01 --limit 100
```

- 조회 종류: `summary`, `compare`, `anomalies`, `forecast`, `staff_meal`, `evidence`, `status`. 근거는 `--evidence-id`, `--product-key`, `--source`, `--offset`, `--limit`로 제한한다. 범위 밖 조회는 필요한 재계산 기간을 반환한다.
- 스냅샷은 불변이다. 검수할 때 `review_candidates.csv`와 `coverage_candidates.csv`를 별도 로컬 경로로 복사하고 그 사본을 편집한다.
- 품목 검수표는 `approved_category`에 `원재료/포장재/기타`, `approved`에 `Y`를 입력한다. 승인된 행만 기존 설정에 병합하고 나머지 분류는 보존한다.
- 마감 검수표는 해당 계정 전체의 실제 수집 결과를 확인하고 `confirmation_evidence`, `confirmed_by`, `approved=Y`를 입력한다. 관측 행수가 0이면 `no_transactions=Y`도 필수다. 가져오기 시 최신 원천의 날짜별 해시를 재검사한다.

```powershell
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent review-import --review '<검수한 로컬 분류표>' --version reviewed-20261012
python -X utf8 -m modules.transform.pipelines.db.samjeon_material_agent coverage-import --date 2026-10-11 --review '<검수한 로컬 마감표>'
```

설정 반영 후 `build`를 다시 실행해야 새 판본에 검수가 반영된다. 분류·마감·목표 설정은 자동 승인하지 않는다. v1/v2 게시본은 v3 조회에서 명시적으로 거부하며 승인 후 새 스냅샷을 게시한다.
