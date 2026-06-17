# transform 참조

## 어디를 볼지
- strategy pipeline: @C:\airflow\modules\transform\pipelines\strategy\AGENTS.md
- sales pipeline: @C:\airflow\modules\transform\pipelines\sales\AGENTS.md
- DB pipeline: @C:\airflow\modules\transform\pipelines\db\AGENTS.md
- utility/API/key/env: @C:\airflow\modules\CLAUDE.md
- architecture: @C:\airflow\docs\architecture.md
- DB/path schema: @C:\airflow\docs\db-schema.md

## 규칙
- pipeline 함수는 문자열 XCom 메시지나 DataFrame 반환을 우선합니다.
- 내부 로직은 private helper로 분리하고 utility를 먼저 재사용합니다.
- DB_UnifiedSales 공통 스키마/저장은 `DB_UnifiedSales_common.py`에 둡니다.
