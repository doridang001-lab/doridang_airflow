# script 참조

## 어디를 볼지
- script runner: @C:\airflow\scripts\_base.py
- 환경/키배치: @C:\airflow\docs\team-clone-setup.md
- 경로 상수: @C:\airflow\modules\transform\utility\paths.py
- 모듈/API 설정: @C:\airflow\modules\CLAUDE.md

## 규칙
- `scripts/`는 분석/검증용입니다. DAG runtime 로직을 넣지 않습니다.
- 가능하면 `main()`이 meta/summary/stats dict를 반환하게 합니다.
- 표준 runner를 쓰면 결과는 `scripts/output/` JSON으로 저장합니다.
- 파라미터는 `argparse`를 사용합니다.
