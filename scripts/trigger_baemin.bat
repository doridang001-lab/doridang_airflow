@echo off
REM 배민 수집 DAG 트리거 + 대시보드 자동 열기
REM 사용법: 더블클릭 또는 cmd에서 실행

echo [1/2] 대시보드 열기...
start http://localhost:8787/db-beamin-macro

echo [2/2] DAG 트리거...
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_Beamin_Macro_Dags

echo.
echo 완료. 대시보드: http://localhost:8787/db-beamin-macro
pause
