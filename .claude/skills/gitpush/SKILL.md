---
name: gitpush
description: 변경사항을 자동으로 add, commit, push까지 수행
---

## instructions

1. `git status`를 실행하여 현재 변경사항 확인
2. 변경된 파일들을 분석하고 요약
3. `git add .` 실행하여 모든 변경사항 스테이징
4. 변경 내용을 기반으로 커밋 메시지 작성
   - 형식: `<type>: <subject>`
   - type: feat/fix/chore/docs/refactor 등
   - 예: `feat: add gitpush skill`, `fix: resolve sync issue`
5. `git commit -m "<message>"` 실행
6. `git push` 실행하여 원격 저장소에 반영

## behavior

- git status 결과와 파일 변경 요약을 표시
- 커밋 메시지 자동 생성 후 사용자에게 표시
- 각 단계별 실행 결과 보고
- push 완료 후 성공 메시지 출력
