---
name: prd
description: 기획서를 받아 JSON 기반 실행 구조로 변환. context.json(통합) + prd.md + features.md(Mermaid) + trd.md 생성. /prd [기획서 내용 또는 파일경로]
argument-hint: [기획서 내용 또는 파일경로]
allowed-tools: [Read, Write, Bash]
model: haiku
---

## 목적
기획서 → 4개 파일 생성. `context.json`(Claude 이해용 통합 JSON) + `prd.md` + `features.md` + `trd.md`

**원칙**: context.json = Single Source of Truth / MD = JSON 파생 / 기능명 동사+목적어 / id 체계: 1 / 1.1 / 1.2

## 저장 경로
`C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\`
슬러그: 제목→공백`-`→특수문자제거→소문자→최대20자 / 실패시 `prd_YYYYMMDD_HHMM`

---

## STEP 0: 입력 + Q&A
`$ARGUMENTS`가 파일 경로면 Read. 비어있으면 기획서 요청.
기획서 분석 후 구현 모호 항목 5~7개 질문. **파일 생성 전 답변 대기.**
질문 기준(미명시 항목만): 기능범위/입력소스(DB·GSheet·크롤링)/출력대상(OneDrive·GSheet·DB·이메일)/스케줄(cron)/컬럼명·파일경로/예외처리

---

## STEP 1: context.json
summary + features + dev_design을 하나의 JSON으로 저장.

```
{
  "summary": { title, slug, problem(한줄), goal(한줄), scope:{in[], out[]}, schedule, stack:{orchestration:"Airflow", storage[], notify}, created_at },
  "features": [{
    id, name(동사+목적어), purpose(한줄), details[](구체적 동작),
    inputs:{source, columns[]}, outputs:{target, format},
    exceptions[]("{상황}: {처리}"), dependencies[], done_criteria[], hints:{reuse_module, ref_dag, path_const}
  }],
  "dev_design": {
    dag: { id, file("dags/{sales|strategy}/{ID}_Dags.py"), schedule, tags[] },
    tasks: [{ id, operator("PythonOperator"), calls("module.function"), xcom_push, depends_on[] }],
    functions: [{ name, file("modules/transform/pipelines/{sales|strategy}/{slug}_pipeline.py"), purpose, inputs, outputs }],
    data_flow: ["{소스} → {task} → {저장대상}"]
  }
}
```

rules: inputs.columns·outputs.target 최대한 구체적으로(모르면 "확인 필요") / 추상표현 금지("데이터 처리" → "dag_run + task_instance JOIN 후 집계")

---

## STEP 2: prd.md
```
# {title}
> {problem} → {goal}

## 개요
{scope.in 항목들}

## 실행 흐름
{data_flow 자연어}

## 스케줄
{schedule}

## 범위 외
{scope.out 항목들}
```

---

## STEP 3: features.md
최상단에 Mermaid 다이어그램, 이후 기능 상세.

```
# 기능 명세서
> {purpose 한줄} | 스케줄: {schedule} | DAG: {dag.id}

## 기능 구조
```mermaid
flowchart LR
    DAG[{dag.id}] --> F1[{feature1.name}]
    DAG --> F2[{feature2.name}]
    F1 --> F1a[{detail}]
    F1 --> F1b[{detail}]
    ...
```

## {id}. {name}
**목적** {purpose}
**상세 기능** {details[] 번호 목록}
**입력** {inputs.source} | 컬럼: {inputs.columns}
**출력** {outputs.target} ({outputs.format})
**예외** {exceptions[]}
**완료 기준** {done_criteria[]}
**구현 힌트** {hints}
```

---

## STEP 4: trd.md
```
# 기술 설계

## DAG
- ID: {dag.id}
- 파일: {dag.file}
- 스케줄: {dag.schedule}
- 태그: {dag.tags}

## Task 흐름
{task1} >> {task2} >> [{task3}, {task4}]
(각 task: operator / calls / xcom_push / depends_on)

## 데이터 흐름
{data_flow[] 목록}

## 신규/수정 파일
- DAG: {dag.file}
- Pipeline: {functions[].file}

## 재사용 모듈
{hints.reuse_module 취합 목록}
```

DAG 규칙: `from modules.transform.utility.schedule import {CONST}` / `start_date=pendulum.datetime(..., tz="Asia/Seoul")` / `catchup=False`
Pipeline 규칙: `logger = logging.getLogger(__name__)`

---

## STEP 5: VS Code + 완료 보고
```bash
code "C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\features.md"
```

```
생성 완료: {slug}
[JSON] context.json (summary + features + dev_design)
[MD]   prd.md / features.md / trd.md
→ C:\Users\민준\OneDrive - 주식회사 도리당\data\planning\{slug}\

수정 시  /prd-e  → features 재검토
확정 시  /prd-c  → PRD/TRD 재생성
```

---

## 공통 규칙
- 중간 결과 출력 금지 — 완료 보고만
- context.json은 반드시 valid JSON
- features.md는 Claude가 받아서 바로 코딩 가능한 수준으로 작성
