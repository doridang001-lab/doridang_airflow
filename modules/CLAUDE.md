# 모듈 규칙

## 코드 규칙
- 함수형 작성, 재사용 가능하게
- 입력/출력 타입 힌팅 필수
- logging 필수 (logger = logging.getLogger(__name__))
- 경로는 paths.py 상수 사용 (COLLECT_DB, LOCAL_DB)

## 금지
- 하드코딩 경로/키 금지 → 환경변수 또는 paths.py
- print 금지 → logger 사용
- DAG 로직을 모듈에 혼재 금지

## 폴더 역할
- transform/utility/ - 공통 함수 (io.py, paths.py, schedule.py, mailer.py, gsheet.py, db.py)
- transform/pipelines/sales/ - 주문 처리 (SMD_*)
- transform/pipelines/strategy/ - 전략 처리 (SMP_*)
- extract/ - 크롤링 전용 (croling_*.py) + GSheet/DB 래퍼
- load/ - 적재 래퍼 (GSheet, DB, OneDrive)

## 참조
- `transform/utility/schedule.py` - 스케줄 상수 (SMD_ORDERS_TIME 등)
- `transform/utility/mailer.py` - send_email, text_to_html
- `transform/utility/gsheet.py` - GSheet 통합 (extract_gsheet + save_to_gsheet)
- `transform/utility/db.py` - DB 통합 (read_table + postgre_db_save)
- `transform/utility/io.py` - ETL 공통 (load_files, save_to_csv 등)
- `transform/utility/paths.py` - 경로 상수
