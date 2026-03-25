# 모듈 규칙

## 폴더 역할
- `transform/utility/` - 공통 함수 (paths, io, schedule, mailer, gsheet, db, onedrive)
- `transform/pipelines/sales/` - 주문 처리 (SMD_*)
- `transform/pipelines/strategy/` - 전략 처리 (SMP_*)
- `extract/` - 크롤링 + GSheet/DB 래퍼
- `load/` - 적재 래퍼

## 새 파이프라인 추가
1. `pipelines/sales/` 또는 `strategy/`에 생성
2. 반환: str(XCom 메시지) 또는 DataFrame
3. 내부 로직은 `_접두사` private 함수로 분리

## 참조
- `docs/architecture.md` - 아키텍처/utility 선택 기준표
- `docs/db-schema.md` - DB/경로 참조
