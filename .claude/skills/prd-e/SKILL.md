---
name: prd-e
description: 사용자가 수정한 features.md를 검토·재수정하고 features.json과 tree.json을 재생성한다. /prd-e [features.md 경로 또는 slug]
argument-hint: [features.md 경로 또는 slug]
allowed-tools: [Read, Write, Bash]
model: haiku
---

## 역할

사용자가 직접 수정한 features.md를 읽어 품질 검토 후 재수정하고,
features.json(기준 데이터)과 tree.json을 자동 재생성한다.

**원칙**: 사용자는 features.md를 수정한다. prd-e는 그걸 검토·보완한 뒤 features.json을 최신 상태로 동기화한다.

---

## 실행 순서

### 1. 입력 처리

`$ARGUMENTS`가 파일 경로면 Read로 읽는다.
slug만 전달되면 `C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\features.md` 로 조합.
없으면 planning 폴더에서 가장 최근 수정된 폴더의 features.md 자동 선택.

slug는 폴더명에서 추출. summary.json도 같은 폴더에서 읽는다 (title/goal/schedule 참조용).

---

### 2. features.md 검토 기준

사용자가 수정한 features.md를 아래 기준으로 점검한다:

- `### 목적`이 1줄 이내인가
- `### 상세 기능` 항목이 구체적인가 (추상 표현 금지: "처리", "관리" → 구체적 동작)
- `### 입력`, `### 출력`, `### 예외`가 모두 채워져 있는가 ("확인 필요" 허용)
- 기능명(`## N.`)이 동사+목적어 형태인가
- 번호 체계가 올바른가 (1, 1.1, 1.2, 2, 2.1 …)
- 기능 간 중복 설명이 있는가
- `### 구현 힌트`에 재사용 모듈/경로 상수가 명시되어 있는가

---

### 3. features.md 재수정

문제 항목을 수정한 features.md를 덮어쓴다.
수정 없으면 파일 그대로 유지.

형식 고정 (반드시 준수):

```markdown
# 기능명세서: {title}

> 목적: {goal}
> 스케줄: {schedule}

---

## {id}. {name}

### 목적
{purpose — 1줄}

### 상세 기능
- {구체적 동작 1}
- {구체적 동작 2}

### 입력
- 소스: {source}
- 주요 컬럼: {columns}

### 출력
- 대상: {target}
- 형태: {format}

### 예외
- {상황}: {처리 방법}

### 완료 기준
- {측정 가능한 기준}

### 구현 힌트
- 재사용: `{reuse_module}`
- 참고 패턴: `{ref_dag}`
- 경로 상수: `{path_const}`
```

---

### 4. features.json 재생성 (features.md 기반)

수정 완료된 features.md를 파싱하여 features.json을 덮어쓴다.
features.md → features.json 방향으로 동기화.

스키마 고정:

```json
{
  "features": [
    {
      "id": "1",
      "name": "",
      "purpose": "",
      "details": [],
      "inputs": { "source": "", "columns": [] },
      "outputs": { "target": "", "format": "" },
      "exceptions": [],
      "dependencies": [],
      "done_criteria": [],
      "hints": {
        "reuse_module": "",
        "ref_dag": "",
        "path_const": ""
      }
    }
  ]
}
```

---

### 5. tree.json 재생성

features.md의 `## N.` 섹션 기준으로 tree.json 재생성.
저장 경로: features.md와 같은 폴더.

```json
{
  "id": "root",
  "label": "{title}",
  "depth": 0,
  "children": [
    {
      "id": "f{id}",
      "label": "{name}",
      "depth": 1,
      "purpose": "{purpose}",
      "io": "{inputs.source} → {outputs.target}",
      "children": [
        { "id": "f{id}_detail_{n}", "label": "{detail항목}", "depth": 2 }
      ]
    }
  ]
}
```

---

### 6. VS Code 열기 + 보고

```bash
code "C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\features.md"
```

```
검토 완료: {slug}

[features.md 수정 내역]
- ## {id}. {name}: {수정 내용 한 줄}
(없으면 "수정 사항 없음")

features.json  동기화 → ...planning/{slug}/features.json
tree.json      재생성 → ...planning/{slug}/tree.json

확정 시 /prd-c 로 PRD/TRD를 생성하세요.
```
