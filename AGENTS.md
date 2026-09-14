# 절대규칙(critical)
 - 항상 한글로만 대답할것
 - onedrive를 수정할때는 승인을 받고 진행할것


# 참조
- `AGENTS.md` is the operating-rule source of truth for this repository.
- `CODEX.md` and `CLAUDE.md` are compatibility pointers only.
- 토큰 절약을 위해 필요한 주제만 @경로로 읽으세요.
- DAG: @C:\airflow\dags\CLAUDE.md
- 모듈/utility/API: @C:\airflow\modules\CLAUDE.md
- transform/pipeline: @C:\airflow\modules\transform\CLAUDE.md
- script: @C:\airflow\scripts\CLAUDE.md
- 환경/키배치: @C:\airflow\docs\team-clone-setup.md
- Airflow harness 가이드: @C:\airflow\docs\codex\harness-airflow.md

## Harness 실행
- phase/step 기반 작업은 `phases/`와 `scripts/execute.py`를 사용한다.
- DAG별 반복 운영 지식은 `harness/`에 두고, step의 `harness_docs`에는 필요한 문서만 지정한다.
- `harness/registry.json`은 증가하는 DAG를 경로 group과 정확한 override로 분류한다.
- DAG 변경 step은 `dag_targets`에 `dags/` 기준 상대 경로를 기록하고, 실행기는 registry에서 관련 guidance와 harness 문서를 자동 주입한다.
- 신규 DAG 추가 후 `python scripts/harness_cli.py validate --target .`로 미분류·중복 분류·문서 누락을 확인한다.
- 기본 실행은 git을 건드리지 않는다. 브랜치/커밋/push 자동화를 원할 때만 `--git` 또는 `--git --push`를 명시한다.

## Harness 모니터링
- `Strategy_HarnessMonitoring_01_Dashboard_Dags`는 `phases/`, `harness/`, `scripts/execute.py`, dashboard 산출물 상태를 점검한다.
- 컨테이너에서는 `docker-compose.yaml`의 `./phases`, `./harness`, `./scripts`, `./AGENTS.md` 마운트가 있어야 대시보드가 정상 판정한다.
- 대시보드 판단 순서: `하네스 설정 점검`의 ERROR/WARN 확인 → KPI의 오류/차단/지연 확인 → 진행 중 step이 있으면 `MD 보기`로 작업 지시서 확인.
- `$codex-md-improver`는 Airflow DAG가 아니라 Windows 작업스케줄러의 주기적 guidance 감사에 사용한다. 자동 수정은 하지 않고 리포트 후 승인받는다.

## Fast Start (권장 검증)
- `docker compose ps` — Airflow 컨테이너 상태 확인
- `python -c "from pathlib import Path; print(Path('dags').exists())"` — 저장소 경로 기본 가시성 점검
- `python -c "import importlib.util, pathlib; print('ok' if importlib.util.find_spec('modules.transform') else 'missing')"` — 모듈 임포트 준비 점검

## 작업 산출물 규칙
- 임시/디버그/일회성 스크립트·파일은 저장소 루트에 만들지 말 것.
- 루트 임시 산출물은 `.tmp/`(gitignore됨) 또는 운영 스크립트가 지정한 경로에서만 생성하고, `git add` 하지 말 것.
- 백업 파일은 기본적으로 생성하거나 보존하지 않는다.
- 롤백을 위해 백업이 꼭 필요하면 원본·운영 데이터 폴더 옆에 `*.bak` 파일을 만들지 말고, `LOCAL_DB/temp/backups/<작업명>/<실행ID>/` 전용 폴더에 격리한다.
- 검증 성공 또는 정상 롤백 후 임시 백업을 삭제한다. 롤백 실패로 보존할 때는 경로·사유·정리 조건을 결과에 명시한다.
- OneDrive에 영구 백업이 꼭 필요하면 별도 승인을 받은 뒤 승인된 루트의 `_backup/<작업명>/<실행ID>/`에만 저장하며, 수집·변환 glob 대상에서 제외한다.

## 업데이트 기록
- `prd_codex/` 작업 계획, 원인 수정, 운영 규칙 변경을 수행한 뒤에는 `C:\airflow\prd_codex\update_log.md`에 간략히 누적 기록한다.
- 기록 형식은 날짜, 대상, 변경 요약, 검증 결과, 남은 위험을 3~6줄로 유지한다.
- 비밀값, 계정 정보, 원본 로그 전문은 기록하지 않는다.
- OneDrive 경로에 기록하거나 동기화 파일을 수정해야 할 때는 먼저 승인을 받는다.

## 텍스트 인코딩 규칙
- 문서/파이썬 소스(`*.md`, `*.py`, `*.txt`, `*.json`)는 UTF-8(가능하면 BOM 미포함)로 유지한다.
- PowerShell에서 텍스트를 읽거나 쓸 때는 UTF-8을 명시한다.
  - 읽기: `Get-Content -Path <path> -Encoding utf8`
  - 쓰기: `Set-Content -Path <path> -Encoding utf8 -Value ...`
  - 실행 후 확인: `python -c "from pathlib import Path; Path(r'<path>').read_text(encoding='utf-8'); print('utf8 ok')"`
- 깔끔한 재확인:
  - `python -c "import pathlib; f=pathlib.Path(r'<path>'); f.read_text(encoding='utf-8'); print('ok')"`
