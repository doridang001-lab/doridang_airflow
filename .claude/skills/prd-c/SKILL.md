---
name: prd-c
description: summary.json + features.json + dev_design.json을 읽어 PRD.md와 TRD.md를 생성한다. /prd-c [폴더경로 또는 slug]
argument-hint: [폴더경로 또는 slug]
allowed-tools: [Read, Write, Bash]
model: haiku
---

## 역할

확정된 JSON 기준 데이터(summary.json, features.json, dev_design.json, tree.json)를 읽어
{slug}-prd.md와 {slug}-trd.md를 생성한다.

---

## 실행 순서

### 1. 입력 처리

`$ARGUMENTS`가 전체 경로면 그대로 사용.
slug만 전달되면 `C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\` 로 조합.
없으면 planning 폴더에서 가장 최근 수정된 폴더 자동 선택.

Read: `summary.json`, `features.json`, `dev_design.json`, `tree.json`

---

### 2. PRD 생성

파일명: `{slug}-prd.md`
대상: 비개발자 (기획자, 운영팀)
기반: summary.json + features.json

```markdown
# {summary.title} — PRD

## 1. 개요

### 1.1. 한 줄 정의
{summary.goal}

### 1.2. 제품 목표
{summary.scope.in 항목 기반 — 무엇을 자동화/개선하는지, 기대 효과}

### 1.3. 배경
{summary.problem — 현재 방식의 문제점, 왜 지금 만드는지}

---

## 2. 핵심 가치

### 2.1. 사용자 문제
{features.json에서 추출한 담당자 불편 요약}

### 2.2. 해결 방식
{summary.data_flow 또는 features 흐름 기반 — 자동화 흐름 요약}

### 2.3. 차별점
{재사용 모듈(hints.reuse_module) 기반 — 기존 인프라 활용 포인트}

---

## 3. 대상 및 시나리오

### 3.1. 타겟 사용자
{운영팀 / 데이터 담당자 / 자동화 수혜자}

### 3.2. 사용 시나리오
{dev_design.data_flow를 단계별 자연어로}

---

## 4. 속성

### 4.1. 카테고리
{summary.stack.orchestration} 기반 자동화 파이프라인

### 4.2. 실행 환경
- Orchestration: {summary.stack.orchestration}
- Storage: {summary.stack.storage}
- Notify: {summary.stack.notify}

### 4.3. 스케줄
{summary.schedule}
```

---

### 3. TRD 생성

파일명: `{slug}-trd.md`
대상: 개발자
기반: dev_design.json + features.json

```markdown
# {summary.title} — TRD

## 1. 시스템 아키텍처

{dev_design.data_flow를 ASCII 다이어그램으로 표현}

```
{소스} → [Task1: extract] → [Task2: transform] → [Task3: load] → {저장대상}
```

---

## 2. 기술 스택

| 역할 | 기술 |
|------|------|
| Orchestration | {summary.stack.orchestration} |
| Data Processing | Pandas / Parquet |
| Storage | {summary.stack.storage 항목별} |
| Notify | {summary.stack.notify} |

---

## 3. 파일 구조

```
{dev_design.files.dag}
{dev_design.files.pipeline}
```

---

## 4. 데이터 흐름

### 4.1. 입력
{features.json의 inputs.source 항목 정리}

### 4.2. 출력
{features.json의 outputs.target 항목 정리}

---

## 5. 데이터 모델

{features.json의 inputs.columns + outputs.format 기반 주요 컬럼/테이블 정리}

---

## 6. 보안

- Google 서비스 계정: `/opt/airflow/config/*.json`
- DB 접속: Airflow Connection 사용
- 파일 경로: `paths.py` 상수 참조

---

## 7. 성능 고려사항

{features.json의 exceptions 중 타임아웃/데이터 규모 관련 항목}

---

## 8. 구현 단계

### Phase 1: Pipeline 모듈 ({dev_design.files.pipeline})
{dev_design.functions 항목별 — 함수명 + 역할}

### Phase 2: DAG 파일 ({dev_design.files.dag})
{dev_design.tasks 항목별 — task_id + operator + calls}

---

## 9. Task 흐름

{dev_design.tasks를 순서대로 나열: task_id → depends_on → calls}

{task1_id} >> {task2_id} >> {task3_id}
```

---

### 4. VS Code 열기 + 보고

```bash
code "C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\{slug}-prd.md"
```

```
생성 완료: {slug}

{slug}-prd.md → ...planning/{slug}/{slug}-prd.md
{slug}-trd.md → ...planning/{slug}/{slug}-trd.md

기반 데이터: summary.json + features.json + dev_design.json
```
