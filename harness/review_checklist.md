# Harness Review Checklist

## 완료 전 확인
- 변경 범위가 step 또는 사용자 요청 범위를 넘지 않았는가.
- DAG는 orchestration만 담당하고, 무거운 로직은 pipeline/utility에 있는가.
- 날짜 conf, schedule, retry, trigger 규칙이 기존 관례와 맞는가.
- downstream 산출물 경로, parquet/csv schema, 알림/리포트 영향이 확인됐는가.
- 한글 문서와 소스가 UTF-8로 읽히는가.

## 운영 확인
- 필요한 최소 import/test/smoke 검증을 실행했는가.
- Airflow 컨테이너나 런타임 로그 확인이 필요한 변경이면 확인 방법을 남겼는가.
- OneDrive, 인증 정보, 로컬 프로필, 다운로드 산출물을 승인 없이 수정하거나 커밋하지 않았는가.
