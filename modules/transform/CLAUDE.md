# 파이프라인 개발 규칙

## 작성 규칙
- 반환: str(XCom 메시지) 또는 DataFrame
- 내부 로직은 `_접두사` private 함수로 분리
- utility 함수 우선 사용, 새 공통 함수는 `utility/`에 추가

## 참조
- `docs/architecture.md` - utility 선택 기준표
