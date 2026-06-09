---
name: gitpush
description: 코드 변경을 감지하고 안전한 브랜치 푸시를 유도하는 Git hook형 스킬
---

## instructions

1. `git status --porcelain`로 현재 변경사항 확인
2. 변경사항이 없으면 `No changes` 메시지 출력 후 종료
3. `.claude/scripts/gitpush.sh` 스크립트를 실행해 브랜치 생성 + push + PR 오픈
4. 스크립트 출력에서 PR URL을 추출해 사용자에게 보여줌
5. 사용자가 PR을 확인 후 "머지해줘" / "승인" / "merge" 등으로 응답하면
   → `gh pr merge --merge --delete-branch <PR번호>` 실행해 main 병합 + 브랜치 삭제

## behavior

- 커밋 메시지 인수가 있으면 그대로 사용, 없으면 변경 파일 경로로 자동 추론
- **항상** 새 feature 브랜치를 생성 (main 직접 push 불가)
- 브랜치명: `feat/메시지슬러그-MMDD` 형식 (예: `feat/dags업데이트-0609`)
- push 후 자동으로 GitHub PR 생성 (`gh pr create`)
- PR URL을 사용자에게 출력하고 승인 대기
- 승인 확인 후 `gh pr merge --merge --delete-branch`로 main 병합

## usage

```
/gitpush "커밋 메시지"   → 브랜치 생성 + push + PR 오픈 → 사용자 승인 대기
/gitpush                 → 메시지 자동 추론 + 위와 동일
```

## script execution

```bash
bash /mnt/c/airflow/.claude/scripts/gitpush.sh "커밋 메시지"
# 또는 메시지 없이
bash /mnt/c/airflow/.claude/scripts/gitpush.sh
```

## merge (사용자 승인 후)

```bash
gh pr merge --merge --delete-branch <PR_NUMBER_OR_URL>
```

## notes

- `gh` CLI가 설치되어 있고 인증된 상태여야 PR 생성 가능
- GitHub remote가 없으면 PR 생성 단계에서 실패 → push까지는 정상 완료됨
- 이미 feature 브랜치에 있는 경우 현재 브랜치를 그대로 사용 (새 브랜치 미생성)
