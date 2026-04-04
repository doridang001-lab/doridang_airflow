"""Prompts for PRD-Light: description → questions → PRD → feature spec."""
import json


def build_questions_prompt(name: str, description: str) -> str:
    desc_line = f"\n설명: {description}" if description else ""
    return f"""프로젝트명: {name}{desc_line}

이 프로젝트의 PRD를 작성하기 위한 핵심 질문 5~7개를 JSON 배열로 반환하세요.

다음 항목을 기반으로 프로젝트에 맞는 질문을 구성하세요:
- 타겟 사용자 및 핵심 페인 포인트
- 핵심 기능 범위 (MVP vs 전체)
- 기술/플랫폼 환경 (웹/앱/내부 도구 등)
- 경쟁사 혹은 현재 대안
- 성공 지표 또는 출시 기준
- 제약사항 (일정, 예산, 기술 등)

JSON 형식 (id, title, hint 필드만):
[
  {{"id": "q1", "title": "질문 내용", "hint": "답변 예시나 유도 힌트"}},
  ...
]

JSON 배열만 출력하세요. 마크다운 코드블록 없이."""


def build_prd_prompt(name: str, description: str, answers: dict) -> str:
    qa_lines = "\n\n".join(
        f"Q: {q}\nA: {a if a else '(미입력)'}" for q, a in answers.items()
    )
    return f"""프로젝트명: {name}
설명: {description}

설문 답변:
{qa_lines}

위 정보를 바탕으로 PRD(제품 요구사항 정의서)를 작성하세요.

## 출력 형식:

# {name} PRD

## 개요
(프로젝트 한 줄 요약)

## 배경 및 목적
(왜 이 제품이 필요한가)

## 타겟 사용자
(누가 쓰는가, 주요 페르소나)

## 핵심 기능
(MVP 범위의 핵심 기능 목록)

## 비기능 요구사항
(성능, 보안, 접근성 등)

## 성공 지표
(KPI, 측정 방법)

## 제약사항 및 가정
(기술, 일정, 예산 등)

마크다운 형식으로 PRD만 출력하세요."""


def build_spec_from_prd_prompt(name: str, prd_content: str) -> str:
    return f"""다음 PRD를 기반으로 기능 명세서(features.md)를 작성하세요.

PRD 내용:
{prd_content}

## 반드시 따를 출력 형식:

# 기능 구조 다이어그램

```mermaid
flowchart TD
    (각 최상위 기능을 노드로, 하위기능을 자식 노드로 표현)
```

# 기능 목록

## 1. 기능명
**설명:** 기능 설명
**우선순위:** High

### 1.1 하위기능명
**설명:** 하위기능 설명
**우선순위:** Medium

## 2. 기능명
...

## 규칙:
- ```mermaid``` 다이어그램은 반드시 최상단에 위치
- 최상위 기능 3~8개 (PRD 핵심 기능 기반)
- 각 기능은 하위기능 0~3개
- 우선순위: High / Medium / Low
- 번호 체계: ## 1., ### 1.1, #### 1.1.1

features.md 전체 내용만 출력하세요. 다른 설명 없이."""


def build_patch_prompt(features_md: str, action: str, target: str, content: str) -> str:
    return f"""현재 features.md:

{features_md}

---
작업 유형: {action}
대상 기능: {target}
변경 내용: {content}

## 반드시 따를 규칙:
1. features.md 상단의 ```mermaid``` 다이어그램을 수정 내용에 맞게 재생성
2. 기능 번호 체계 유지 (## 1., ### 1.1)
3. 수정되지 않은 기능들은 그대로 유지
4. 전체 features.md를 출력 (부분 출력 금지)
5. 설명 없이 수정된 features.md만 출력

수정된 features.md:"""


def build_section_prompt(feature_name: str, feature_desc: str, context: str) -> str:
    return f"""기능명: {feature_name}
기능 설명: {feature_desc}

프로젝트 맥락:
{context}

이 기능의 상세 명세를 마크다운으로 작성하세요.

## 포함 항목:
- ## 기능 개요
- ## 사용자 스토리
- ## 기능 요구사항
- ## 비기능 요구사항
- ## UI/UX 고려사항
- ## 기술 고려사항
- ## 엣지 케이스

마크다운만 출력하세요."""
