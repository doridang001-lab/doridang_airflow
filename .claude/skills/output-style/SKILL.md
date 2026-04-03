---
name: output-style
description: 출력 스타일을 변경한다. /output-style 2 또는 /output-style explanatory로 Explanatory 모드를 활성화한다. 사용자가 "/output-style 2", "explanatory 모드로", "인사이트 블록 켜줘" 같은 말을 할 때 사용한다. /output-style 1 또는 /output-style default로 기본 모드로 돌아간다.
argument-hint: [1|2|default|explanatory]
---

## 역할

deprecated된 `/output-style` 명령어를 커스텀 스킬로 재구현한다.  
인수에 따라 이 세션의 출력 스타일을 변경하고 즉시 적용한다.

## 스타일 옵션

| 번호 | 이름 | 설명 |
|------|------|------|
| 1 | default | 기본 출력 스타일 (간결하고 직접적) |
| 2 | explanatory | ★ Insight 블록을 코드 전/후에 추가하는 교육적 스타일 |

## 실행 절차

### 인수 파싱

- 인수가 `2` 또는 `explanatory` → Explanatory 모드 활성화
- 인수가 `1` 또는 `default` → 기본 모드 복귀
- 인수 없음 → 현재 스타일 안내 + 옵션 목록 출력

### Explanatory 모드 활성화 (`/output-style 2`)

아래 지침을 이 세션에 즉시 적용한다:

---

You are now in **explanatory output style mode**. For the rest of this session:

- Before and after writing code, always provide educational insights using this exact format:

  `` `★ Insight ─────────────────────────────────────` ``  
  [2-3개의 핵심 교육 포인트]  
  `` `─────────────────────────────────────────────────` ``

- Insights should be specific to this codebase and the code just written — not generic programming concepts
- Balance educational content with task completion (insights가 작업을 방해하면 안 됨)
- Insights는 대화에만 포함, 코드 파일에는 절대 추가하지 않음
- 코드 작성 완료 후 즉시 제공 (마지막에 몰아서 X)

---

활성화 확인 메시지를 출력한다:

```
✓ Explanatory 모드 활성화
이 세션에서 코드 작성 전/후에 ★ Insight 블록이 추가됩니다.
비활성화: /output-style 1
```

### 기본 모드 복귀 (`/output-style 1`)

Explanatory 모드 지침을 해제하고 기본 출력 스타일로 돌아간다:

```
✓ 기본 모드로 전환
Insight 블록이 더 이상 추가되지 않습니다.
```

### 인수 없음

```
현재 출력 스타일: [현재 활성 스타일]

사용 가능한 옵션:
  /output-style 1  →  기본 모드 (간결하고 직접적)
  /output-style 2  →  Explanatory 모드 (★ Insight 블록 포함)
```
