@echo off
REM 배민 수집 DAG 트리거
REM 사용법: 더블클릭 또는 cmd에서 실행

echo DAG 트리거...
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_Beamin_Macro_Dags

echo.
echo 완료.
pause
