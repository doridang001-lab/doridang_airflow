# 2026-09-09 Airflow 메모리 장애 재발 방지 적용

## 적용 결과
- 일반 Celery 워커 5개(default), 과거 처리 워커 1개(history). 실제 프로세스 수와 prefetch를 각각 5/1로 확인했다.
- 스케줄러 parallelism은 12. 기존 작업은 종료하지 않고 일반 워커의 유휴 슬롯을 줄였다.
- `DoridangDockerAirflowWatchdog`를 1분 주기로 적용했다. 메모리 정책은 Airflow Variable `airflow_resource_policy_v1`에 보존한다.
- 가용 메모리 4GiB 미만에서는 과거 작업이 양보하고, 2GiB 미만에서는 모든 pool의 새 배정을 보류한다. 6GiB 이상 5회 연속 확인 후 원래 pool 슬롯을 복원한다. 이미 실행 중인 작업은 종료하지 않는다.
- 일반 배민 수집이 실행 중이면 과거 작업은 양보한다. 브라우저 계정 잠금과 통합매출 쓰기 잠금을 추가했다.

## 과거 요청 및 야간 계산
- 수집·재시도·업로드·검증을 전용 DAG 4개로 분리했다. 각 DAG는 동시 실행 1개, 과거 전용 큐를 사용한다.
- 시작하지 않은 기존 요청 33건을 전용 DAG로 이전했다. 새 요청 생성과 기존 요청 취소는 같은 DB 트랜잭션이다. 33건 모두 대응하는 새 run과 기존 취소 이력을 검증했다.
- 이전 요청에는 `history_migrated_to`를 기록하고 failed 상태로 남겼다. 실제 수집 실패와 구분해서 확인해야 한다.
- 신규 수집·재시도 요청은 활성 요청 6건, 전체 트리거 경로에서 분당 2건 상한을 적용한다. 기존 33건은 보존하여 순차 소진하며, 상한 이상에서는 신규 요청을 보류한다.
- 초과 요청은 `history_request_*` Variable에 원래 conf와 함께 보존한다. 동일 범위의 활성·보류 요청은 중복 등록하지 않으며, 재등록 시 최근 날짜부터 배정한다.
- 일반 통합매출은 명시적인 `include_full_recalc` 요청이 없으면 전체기간 추가 계산을 하지 않는다. 전체기간 계산은 `DB_UnifiedSales_Nightly_Dags`의 22시 일정으로 분리했다.
- 야간 계산은 날짜 완료 후 체크포인트를 저장하고 07시부터 다음 날짜를 시작하지 않는다. 실행 중 날짜는 마무리한 뒤 다음 야간에 재개한다. 현재 전체기간 대상 매장 설정은 빈 목록이다.

## 복구와 관찰
- 메모리 오류 로그 또는 메모리 압박 중 워커 중단 증거가 있고, 원래 입력과 안전한 재처리 조건이 확인되는 허용 태스크만 1회 재개한다.
- `dag_id/run_id/task_id/map_index`별 영구 claim을 기존 자동 복구기와 공유한다. 성공 태스크·알림 태스크를 함께 초기화하지 않는다. 복구 후 다시 실패하면 추가 재개를 중지하고 알림을 남긴다.
- 기존 자동 복구기는 유휴 상태에서 정상 종료 후 새 코드로 시작했다. 새 PID의 heartbeat와 pending 0건을 확인했다.
- 관찰 파일: `C:/Local_DB/airflow_ops/resource_observation.json`. 최소 가용 메모리, 메모리 오류, 위험 진입, 감시 간격 누락을 기록한다. 24시간 경과 시 별도 요약 알림을 보낸다.

## 검증 및 남은 확인
- 관련 회귀 테스트 178건 통과(검증 묶음별 실행 및 실패 수정 후 재검증). 신규 DAG 5개는 운영 Airflow에서 파일별 단독 등록 및 직렬화 왕복 검증을 통과했다.
- 신규 과거 DAG의 우선순위 문자열 직접 대입 때문에 발생한 import 오류 4건을 등록된 전략 객체 사용으로 수정했다. 스케줄러의 import 오류는 0건이다.
- harness registry 검증: 95개 DAG 분류 정상.
- 10:12 기준 실제 워커 5+1, 큐 default/history 분리, 스케줄러 parallelism 12 확인. 10:14 기준 가용 메모리 약 9.6GiB, 실행 중 태스크 3개.
- **24시간 안정성 관찰은 진행 중이다.** 초기 배포 중 감시 간격 누락 1회가 기록되어 있으며, 24시간 경과 자체를 무오류 검증 완료로 간주하지 않는다. 미소진 과거 수집과 야간 전체기간 실데이터 완주도 아직 완료 결과로 보고하지 않는다.

## 운영 확인
```powershell
docker compose ps
docker compose exec -T airflow-scheduler airflow dags list-import-errors -o json
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/airflow_resource_control.py
python scripts/harness_cli.py validate --target .
```

정책 스크립트는 기본 읽기 전용이다. `--apply`는 운영 watchdog이 사용한다. 이관 스크립트도 기본 읽기 전용이며 `--apply`는 미실행 요청만 이전한다. 데이터 전체 삭제·무차별 clear·전체 워커 재시작으로 대기열을 줄이지 않는다.
