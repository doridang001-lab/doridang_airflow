"""PRD generation prompt builder."""


def build_prd_prompt(answers: dict, project_name: str) -> str:
    lines = []
    if answers.get("target_user"):
        lines.append(f"- 주요 대상 사용자: {answers['target_user']}")
    if answers.get("core_goals"):
        v = answers["core_goals"]
        lines.append(f"- 핵심 목표/문제: {', '.join(v) if isinstance(v, list) else v}")
    if answers.get("core_features"):
        v = answers["core_features"]
        lines.append(f"- 핵심 기능: {', '.join(v) if isinstance(v, list) else v}")
    if answers.get("platforms"):
        v = answers["platforms"]
        lines.append(f"- 지원 플랫폼: {', '.join(v) if isinstance(v, list) else v}")
    if answers.get("priority"):
        lines.append(f"- 개발 우선순위: {answers['priority']}")
    if answers.get("content_types"):
        v = answers["content_types"]
        lines.append(f"- 콘텐츠 유형: {', '.join(v) if isinstance(v, list) else v}")
    if answers.get("additional"):
        lines.append(f"- 추가 정보: {answers['additional']}")

    answers_str = "\n".join(lines)

    return f"""당신은 제품 기획 전문가입니다. 아래 설문 답변을 바탕으로 전문적인 PRD를 작성해주세요.

제품명: {project_name}

설문 답변:
{answers_str}

반드시 아래 정확한 구조(## 섹션, ### 서브섹션)로 한국어 PRD를 작성하세요.
각 서브섹션은 구체적이고 실용적인 내용으로 충실하게 채워주세요.

## Overview

### 한 줄 정의
[제품을 한 문장으로 정의]

### 제품 목표
[제품이 달성하려는 목표 2-3개를 구체적으로]

### 배경
[개발 배경 및 필요성, 현재 문제 상황]

## Core Value

### 사용자 문제
[사용자가 현재 겪는 핵심 문제점 2-3가지]

### 해결 방식
[우리 제품이 해당 문제를 해결하는 구체적인 방법]

### 차별점
[기존 방식 또는 경쟁 제품 대비 차별화되는 점]

## Targets & Scenarios

### 타겟 사용자
[주요 타겟 사용자 프로필과 특성]

### 사용 시나리오
[핵심 사용 시나리오 2-3개를 단계별로 서술]

## Success Metrics

### 핵심 KPI
[측정 가능한 핵심 KPI 3-5개를 수치 목표와 함께 글머리 목록으로 작성. 표 형식 사용 금지]

### 리스크/이슈
[주요 리스크와 대응 방안을 글머리 목록으로 작성. 표 형식 사용 금지]

## Attribute Settings

### 카테고리
[제품 카테고리 (예: 데이터 자동화, SaaS, 모바일 앱 등)]

### 사용자 역할
[역할을 쉼표로 구분하여 나열. 예: 일반 사용자, 관리자]

### 기기
[지원 기기를 쉼표로 구분하여 나열. 예: 웹 브라우저, macOS 데스크톱]"""
