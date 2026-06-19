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

## Fast Start (권장 검증)
- `docker compose ps` — Airflow 컨테이너 상태 확인
- `python -c "from pathlib import Path; print(Path('dags').exists())"` — 저장소 경로 기본 가시성 점검
- `python -c "import importlib.util, pathlib; print('ok' if importlib.util.find_spec('modules.transform') else 'missing')"` — 모듈 임포트 준비 점검

## 작업 산출물 규칙
- 임시/디버그/일회성 스크립트·파일은 저장소 루트에 만들지 말 것.
- 루트 임시 산출물은 `.tmp/`(gitignore됨) 또는 운영 스크립트가 지정한 경로에서만 생성하고, `git add` 하지 말 것.

## 텍스트 인코딩 규칙
- 문서/파이썬 소스(`*.md`, `*.py`, `*.txt`, `*.json`)는 UTF-8(가능하면 BOM 미포함)로 유지한다.
- PowerShell에서 텍스트를 읽거나 쓸 때는 UTF-8을 명시한다.
  - 읽기: `Get-Content -Path <path> -Encoding utf8`
  - 쓰기: `Set-Content -Path <path> -Encoding utf8 -Value ...`
  - 실행 후 확인: `python -c "from pathlib import Path; Path(r'<path>').read_text(encoding='utf-8'); print('utf8 ok')"`
- 깔끔한 재확인:
  - `python -c "import pathlib; f=pathlib.Path(r'<path>'); f.read_text(encoding='utf-8'); print('ok')"`
