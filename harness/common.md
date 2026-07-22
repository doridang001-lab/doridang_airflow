# Airflow Harness Common

## 기본 원칙
- DAG는 orchestration만 둔다. 비즈니스 로직은 `modules/transform/pipelines/` 또는 기존 utility로 이동한다.
- DAG 변경 전 `dags/CLAUDE.md`, 관련 `dags/*/AGENTS.md`, `docs/codex/dag-change-checklist.md`를 확인한다.
- DB pipeline 변경 전 `modules/transform/pipelines/db/AGENTS.md`를 확인한다.
- Selenium, Playwright, crawler 변경은 `docs/codex/crawling-gotchas.md`를 함께 확인한다.

## 구현 기준
- 기존 DAG의 `dag_id`, schedule 상수, `catchup`, `max_active_runs`, conf 처리 방식을 유지한다.
- 날짜 conf는 기존 관례인 `sale_date`, `backfill`을 우선한다.
- 새 임시 파일은 루트가 아니라 `.tmp/` 또는 해당 운영 스크립트가 지정한 경로에 둔다.
- OneDrive 경로는 사용자 승인 없이 수정하지 않는다.

## 검증 기본값
- 문서/소스 인코딩 확인:
  ```powershell
  python -X utf8 -c "from pathlib import Path; Path(r'<path>').read_text(encoding='utf-8'); print('utf8 ok')"
  ```
- DAG import 또는 좁은 테스트를 우선 실행한다.
- Docker/Airflow 런타임 확인이 필요하면 먼저 `docker compose ps`로 컨테이너 상태를 확인한다.
