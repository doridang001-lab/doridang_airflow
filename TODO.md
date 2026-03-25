# Airflow ETL TODO

## ✅ 완료
- [x] modules/notification/ 삭제
- [x] io.py 분리 → schedule.py, mailer.py 생성
- [x] gsheet.py, db.py 통합 허브 생성
- [x] extract/__init__.py optional import 처리 (크롤링 의존성)
- [x] modules/__init__.py lazy import 정리

## 🔄 진행 중
- [ ] 모든 DAG 실제 실행 테스트 (Airflow 환경)
- [ ] CLAUDE.md Lazy Loading 구조 적용


## 📁 핵심 파일 맵
- 스케줄: modules/transform/utility/schedule.py
- 이메일: modules/transform/utility/mailer.py
- GSheet: modules/transform/utility/gsheet.py
- DB: modules/transform/utility/db.py
- 경로: modules/transform/utility/paths.py
- ETL공통: modules/transform/utility/io.py
