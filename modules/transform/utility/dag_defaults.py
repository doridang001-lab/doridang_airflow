"""DAG 공통 기본값.

2026-09-11 장애: Docker 백엔드가 무너지면서 43개 DagRun이 이틀간 running 상태로 남았고,
대부분의 DAG가 max_active_runs=1 이라 다음 스케줄이 아예 생성되지 않았다.
당시 DAG 98개 중 dagrun_timeout을 설정한 곳은 0개였다 - 한 번 멈춘 런을 스스로
끝낼 방법이 없었다는 뜻이다.

dagrun_timeout을 두면 스케줄러가 초과한 DagRun을 failed로 마감하므로 봉쇄가 자동으로 풀린다.
Strategy_ScheduleGuard_01_Overdue_Dags 의 좀비 런 마감과 함께 이중 방어선을 이룬다.
"""

from datetime import timedelta

# 일반 변환/적재 DAG. 정상이면 수십 분 안에 끝나므로 6시간이면 충분히 넉넉하다.
DEFAULT_DAGRUN_TIMEOUT = timedelta(hours=6)

# Selenium/크롬확장 수집 DAG. 매장 수십 곳을 순회하고 재시도까지 도는 경우가 있어
# 일반 DAG보다 길게 잡는다.
COLLECT_DAGRUN_TIMEOUT = timedelta(hours=12)

# 백필/재수집 DAG. 여러 날짜를 한 런에서 순차 처리한다.
BACKFILL_DAGRUN_TIMEOUT = timedelta(hours=24)
