# F&B 데이터 파이프라인 (Airflow ETL)

## 공통 규칙
- 모든 응답은 한국어로 작성
- 하드코딩 금지 → paths.py 상수 사용
- 날짜 기준 partition
- 코드 중심 출력, 설명 최소화

## 금지
- 폴더 구조/파일명 변경 금지
- 의존성/Docker 설정 무단 변경 금지
- print 사용 금지 → logging 사용
- 추상적 지시 금지 (함수 단위, 입출력 타입 명시)

## 기술 스택
- Airflow(LocalExecutor) + Pandas + Parquet
- Google Sheets(gspread) + Selenium + PostgreSQL
- Docker + SMTP 이메일

## 참조
- `dags/CLAUDE.md` - DAG 작성 규칙
- `modules/CLAUDE.md` - 모듈 개발 규칙
