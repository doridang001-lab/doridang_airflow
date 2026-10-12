# 송파삼전점 원재료 mart

## 대상과 실행

- DAG: `dags/db/DB_Samjeon_Material_Mart_Dags.py`
- 변환: `modules/transform/pipelines/db/samjeon_material_agent/mart.py`
- 신규 DAG는 일시중지 상태로 등록한다. 일정은 한국시간 09:15~11:45에 30분 간격이다.
- 기본 기준일은 한국시간 전일이다. 수동 실행은 `{"sale_date":"YYYY-MM-DD"}`로 지정한다.
- 매 실행마다 기준일까지 57일을 재계산한다. XCom에는 파일 경로와 해시가 담긴 작은 계획만 보낸다.

## 입력과 분류

- 매출: `MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet`, `store=송파삼전점`.
- 송파삼전점 `source=okpos`는 홀 매출이다. 일별·월 누적 `hall_sales_won`에 순매출을 부호 그대로 더한다. 다른 원천은 홀로 합치지 않으며 전체 매출에서 홀 소계를 뺀 금액을 배달 매출로 확정하지 않는다.
- 구매: `ANALYTICS_DB/Food_Guide_Raw/food_guide_orders_YYYYMM.parquet`와 `ANALYTICS_DB/Logistics_Dam/Dam_order_details_YYMMDD.parquet`.
- `도리당 송파삼전점(부식)`은 품목명에 관계없이 `현장 직원식대`로 분리한다.
- `LOCAL_DB/samjeon_material_agent/classification.json`의 `items` 배열에 `source, product_code, item_name, spec, unit, category`를 검수해 입력한다. 허용 분류는 `원재료`, `포장재`, `기타`다. 미등록 품목은 `미분류`이며 `review_candidates.csv`에 나온다.
- 원천별 날짜 마감은 `LOCAL_DB/samjeon_material_agent/coverage.json`의 `days/YYYY-MM-DD/<sales|food_guide|logistics_dam>`에 `source_hash`와 `evidence`를 입력한다. 파일이 있어도 확인 기록이 없으면 잠정이다. 마감 후 원천 해시가 달라지면 오래됨이다.

## 출력과 검증

- 스테이징: `LOCAL_DB/temp/samjeon_material_mart/<snapshot_id>`.
- 게시: `MART_DB/CEO_DB/samjeon_agent_DB/snapshots/<snapshot_id>`.
- `purchase_lines`, `sales_daily`, `purchase_daily`, `item_daily`, `quality_daily`는 Parquet이며, `agent_context.json`, `agent_guide.md`, `manifest.json`, `review_candidates.csv`도 함께 게시한다. LLM은 같은 스냅샷의 가이드와 문맥을 함께 읽어야 한다.
- 가이드 원본은 변환 패키지의 `agent_guide.md`다. 내용 해시를 스냅샷 ID와 manifest·agent_context에 기록하므로 가이드 변경도 새 판본으로 게시한다.
- 구매 행 합계, 일별 합계, 품목별 합계가 일치해야 한다. 근거 ID 중복·미래 날짜·원천 해시 변경 시 게시하지 않는다.
- 완전한 날짜만 구매율·7일 비교에 사용한다. 월말 전망은 완료된 동일 요일의 최근 8주 매출·매입 표본이 잔여일마다 4개 이상일 때만 낸다.
- 검증 후 버전 폴더를 게시하고, 기준일이 전일이면 `latest.json`을 마지막에 교체한다. 과거 날짜 수동 실행은 최신 포인터를 바꾸지 않는다.
- OneDrive 게시를 실제 실행하려면 저장소의 OneDrive 변경 승인 규칙을 지킨다.

## 로컬 검증

```powershell
python -X utf8 -m pytest tests/test_samjeon_material_mart.py -q
python scripts/harness_cli.py validate --target .
```
