# doridang_airflow

Airflow ETL Pipeline for Sales Data Management

## 프로젝트 설명
배달 플랫폼(배민, 쿠팡이츠, 투어더 등)의 주문 데이터를 수집, 변환, 적재하는 Airflow 기반 ETL 파이프라인

## 주요 기능
- 배달 플랫폼 크롤링 자동화
- 데이터 전처리 및 병합
- Google Sheets 연동
- PostgreSQL 데이터베이스 저장
- 일일 알림 및 리포트 자동화

## 디렉토리 구조
```
airflow/
├── dags/              # Airflow DAG 정의 파일
│   ├── etl/          # ETL 파이프라인 DAG
│   └── test/         # 테스트 DAG
├── modules/          # 커스텀 모듈
│   ├── extract/      # 데이터 추출 (크롤링, DB, API)
│   ├── transform/    # 데이터 변환 및 파이프라인
│   ├── load/         # 데이터 적재
│   └── notification/ # 알림 및 이메일
├── notebooks/        # Jupyter 노트북 (분석 및 개발)
├── config/          # 설정 파일
└── logs/            # Airflow 로그
```

## 설치 및 실행
```bash
# 의존성 설치
pip install -r requirements.txt

# Airflow 초기화
airflow db init

# Airflow 웹서버 실행
airflow webserver -p 8080

# Airflow 스케줄러 실행
airflow scheduler
```

## 환경 설정
- Python 3.12+
- Apache Airflow
- PostgreSQL
- Docker (선택사항)
