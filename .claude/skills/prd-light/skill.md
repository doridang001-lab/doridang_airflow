---
name: prd-light
description: Use when the user runs /prd-light or wants to create/manage a feature list as a tree. Creates features.md as single source of truth with Mermaid diagram + interactive button-driven UI (no chat). Also triggers for "기능목록 만들어", "feature tree 만들어", "기능 트리 생성" requests.
argument-hint: [product-name] [description]
allowed-tools: [Bash, Read, Write]
mode: bypassPermissions
---

## /prd-light 실행 절차

### 0. 제품명 결정
`$ARGUMENTS`에서 첫 단어(영문 kebab-case)를 제품명으로 사용. 나머지는 description.
비어있으면 `product`. 한글 제품명은 영문 kebab-case로 변환.

### 1. 의존성 확인 + 서버 시작 (background)
```bash
py -3.12 -c "import flask" 2>/dev/null || py -3.12 -m pip install --quiet flask
```
서버 시작 (background):
```
PYTHONUNBUFFERED=1 py -3.12 "C:/airflow/.claude/skills/prd-light/scripts/server.py" --name "<이름>" --description "<설명>"
```

### 2. PORT 확인 후 링크 출력
서버 stdout에서 `PRD_PORT:` 줄을 읽어 PORT 파싱. 사용자에게 **http://localhost:{PORT}** 출력.

### 3. runner.py 실행 (background)
```
PYTHONUNBUFFERED=1 py -3.12 "C:/airflow/.claude/skills/prd-light/scripts/runner.py" {PORT}
```

### 4. 즉시 완료 — 여기서 멈춤
background 프로세스(server + runner)가 시작되면 즉시 아래 형식으로 보고 후 종료.

```
PRD Light가 시작되었습니다: http://localhost:{PORT}
브라우저에서 기능 목록을 생성하고 관리하세요.
모든 수정은 features.md 기준으로 이루어집니다.
```

**이 단계 이후 추가 tool 호출 금지.** 모든 LLM 작업은 runner.py가 독립적으로 처리한다.
