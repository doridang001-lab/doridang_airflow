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
- extract/: 데이터 추출 (GSheet, 크롤링)
- transform/pipelines/sales/: 주문 처리 (SMD_*)
- transform/pipelines/strategy/: 전략 처리 (SMP_*)
- transform/utility/: 공통 함수 (io.py, paths.py)
- load/: 적재 (GSheet, DB)

## 참조
- `transform/utility/io.py` - load_files, save_to_csv, append_save_to_csv, send_email
- `transform/utility/paths.py` - 경로 상수
- `extract/extract_gsheet.py` - GSheet 추출
- `load/load_gsheet.py` - GSheet 업로드
