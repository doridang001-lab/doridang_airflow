---
name: prd
description: Use when the user runs /prd or wants to create product planning documents (PRD, feature specification, TRD). Launches an interactive browser UI for questionnaire → PRD → feature spec → TRD generation. Also triggers for "제품기획", "PRD 만들어", "기능명세서 만들어" requests.
argument-hint: [product-name]
allowed-tools: [Bash, Read, Write]
mode: bypassPermissions
---

## /prd 실행 절차

### 0. 제품명 결정
`$ARGUMENTS`가 비어있으면 `product`, 영문 kebab-case면 그대로, 한글이면 영문 kebab-case로 변환. 원본 설명은 `--description`으로 전달.

### 1. 의존성 확인 + 서버 시작 (background)
```bash
py -3.12 -c "import flask" 2>/dev/null || py -3.12 -m pip install --quiet flask
```
서버 시작 (background):
```
PYTHONUNBUFFERED=1 py -3.12 "C:/airflow/.claude/skills/prd/scripts/server.py" --name "<이름>" --description "<설명>"
```

### 2. PORT 확인 후 링크 출력
서버 output에서 `PRD_PORT:` 줄을 읽어 PORT를 파싱. 사용자에게 **http://localhost:{PORT}** 링크 출력.

### 3. runner.py 실행 (background)
이 스크립트가 요청 파일을 감시하고, 각 요청마다 `claude -p`를 **직접 1회 호출**한다. Claude Code는 **1회만 실행**하면 된다.
```
PYTHONUNBUFFERED=1 py -3.12 "C:/airflow/.claude/skills/prd/scripts/runner.py" {PORT}
```
runner.py가 `export_complete` 신호를 감지하면 자동 종료한다.

### 4. 즉시 완료 — 여기서 멈춤
**background 프로세스(server + runner)가 시작되면 즉시 사용자에게 URL만 보고하고 종료한다.**
절대로 runner.py/server.py 종료를 기다리거나 출력을 polling하지 않는다.

보고 형식:
```
PRD Generator가 시작되었습니다: http://localhost:{PORT}
브라우저에서 설문 → PRD → 기능명세 → TRD 순서로 진행하세요.
완료되면 자동으로 파일이 저장됩니다.
```

**이 단계 이후 추가 tool 호출 금지.** 모든 LLM 작업은 runner.py가 독립적으로 처리한다.
