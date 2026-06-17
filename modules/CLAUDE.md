# 모듈 참조

`modules/transform/utility`나 API/key/env 확인이 필요할 때 이 파일을 먼저 봅니다.

## 어디를 볼지
- 경로/OneDrive: @C:\airflow\modules\transform\utility\paths.py
- Airflow API 계정/env: @C:\airflow\modules\transform\utility\airflow_api.py
- Telegram 알림 키: @C:\airflow\modules\transform\utility\notifier.py
- DB/GSheet 공통 설정: @C:\airflow\modules\common\config.py
- 개인 env 예시: @C:\airflow\.env.example
- 키 파일 배치: @C:\airflow\docs\team-clone-setup.md
- transform/pipeline: @C:\airflow\modules\transform\CLAUDE.md

## 규칙
- 공통 기능은 기존 `modules/transform/utility/` 재사용.
- DAG runtime 로직은 `dags/`가 아니라 `modules/`에 둡니다.
- DB 연결은 `modules/common/config.py`, 경로는 `paths.py`를 먼저 확인합니다.
