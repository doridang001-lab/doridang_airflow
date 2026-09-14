# DAG 참조

핵심: DAG는 orchestration만 둡니다. 비즈니스 로직은 `modules/transform/pipelines/`로 보냅니다.

## 먼저 볼 곳
- strategy DAG: @C:\airflow\dags\strategy\AGENTS.md
- sales DAG: @C:\airflow\dags\sales\AGENTS.md
- DB 수집 DAG: @C:\airflow\dags\db\AGENTS.md
- DAG 변경 체크: @C:\airflow\docs\codex\dag-change-checklist.md
- Airflow 실행/로그: @C:\airflow\docs\codex\airflow-workflow.md

## 규칙
- `dag_id=Path(__file__).stem`, `catchup=False`, `max_active_runs=1` 기본.
- `dagrun_timeout`은 필수. `modules.transform.utility.dag_defaults`의
  `DEFAULT_DAGRUN_TIMEOUT`(일반) / `COLLECT_DAGRUN_TIMEOUT`(Selenium·확장 수집) /
  `BACKFILL_DAGRUN_TIMEOUT`(백필)을 씁니다. 없으면 멈춘 런이 `max_active_runs=1`에
  걸려 다음 스케줄을 영구히 막습니다(2026-09-11 장애).
- schedule은 `modules.transform.utility.schedule` 상수 우선.
- conf 관례: `sale_date`, `backfill`, conf 없음은 lookback/append.
- Selenium/crawler DAG면 @C:\airflow\docs\codex\crawling-gotchas.md 도 확인.
