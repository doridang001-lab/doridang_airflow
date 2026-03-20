# DAG 규칙

## 구조
- extract → transform → load 순서 엄수
- dag_id: 파일명 자동 생성 (`__file__`)
- task_id: 동사형 (load_data, aggregate)
- schedule: io.py 상수 사용 (SMD_ORDERS_TIME 등)

## 금지
- DAG 파일에 비즈니스 로직 금지 → modules/transform/pipelines/ 에 작성
- catchup=True 금지
- 하드코딩 스케줄 금지 → io.py 상수

## 네이밍
- 주문: `SMD_##_설명_Dags.py` (dags/sales/)
- 전략: `SMP_##_설명_Dags.py` (dags/strategy/)
- 기타: dags/etl/

## 필수 설정
- pendulum timezone "Asia/Seoul"
- max_active_runs=1
- XCom으로 task간 parquet 경로 전달

## 참조
- `modules/CLAUDE.md` - 모듈 개발 규칙
- `modules/transform/utility/io.py` - 스케줄 상수, 공통 함수
