---
name: collect-ops
description: |
  배민·쿠팡 등 플랫폼 수집(Selenium/크롬확장) 운영·복구 도메인 지식 스킬.
  사용자가 "배민 수집", "쿠팡 수집", "매크로 실패", "NOW 매장선택", "Akamai 차단",
  "확장 watchdog", "F5 재개", "드라이버 오류", "수집 복구" 등을 언급할 때 반드시 사용한다.

  핵심 역할: 플랫폼별 함정(배민=API불가·NOW선택·렌더러타임아웃 / 쿠팡=Akamai·shell-ready·reload-resume)을
  아는 상태로 다룬다. 일반 크롤링 가정으로 접근하면 반드시 실패한다.
  플랫폼별 상세는 아래 references를 필요할 때 읽는다.
---

## 대상 판별 → 해당 reference 로드
- **배민(배달의민족)** → `references/baemin.md` (Selenium 전용)
- **쿠팡이츠** → `references/coupang.md` (크롬확장 배치)

## 공통 원칙
- 수집단 문제(로그인/선택/타임아웃/드라이버/봇탐지)인지 변환단 버그인지 **먼저 분류**한다. 무작정 재시도 금지.
- 드라이버 복구는 공통화·**fail-fast**(복구 실패 시 즉시 중단, 좀비 세션 방지).
- 소수 대상(단일 계정·소수 매장)으로 재현·검증 후 확대.
- 진행 실측: 로그, 다운로드 폴더(DOWN_DIR), NoData/복구 마커, parquet/csv 실제 파일.
- 설계·URL 조사는 `crawl` 스킬, 실행·복구는 `crawl-runner` 에이전트가 담당. 변환단 버그로 이어지면 `dag-debug`로 넘긴다.

## Constraints
- OneDrive 수정 승인 필요. git push 지시 시에만. 응답 한글. print 금지·logging 사용.
- 확장 js 수정 시 사용자 복사/적용 절차 안내(빌드는 사용자 복사 방식).
