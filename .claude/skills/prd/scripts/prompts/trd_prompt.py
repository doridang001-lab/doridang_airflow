"""TRD generation prompt builder."""
import json


def build_trd_prompt(prd: str, feature_spec: dict) -> str:
    spec_str = json.dumps(feature_spec, ensure_ascii=False, indent=2)[:3000]

    return f"""당신은 시니어 소프트웨어 아키텍트입니다. 아래 PRD와 기능명세서를 바탕으로 상세한 TRD(Technical Requirements Document)를 작성해주세요.

PRD:
{prd[:2000]}

기능명세서 요약:
{spec_str}

다음 섹션으로 구성된 TRD를 한국어로 작성해주세요:

## 시스템 아키텍처
(전체 시스템 구조, 컴포넌트 구성, 데이터 흐름)

## 기술 스택
(프론트엔드, 백엔드, 데이터베이스, 인프라, 외부 서비스)

## API 설계
(주요 API 엔드포인트 목록, 요청/응답 형식, 인증 방식)

## 데이터 모델
(핵심 엔티티, 테이블 구조, 관계)

## 보안 요구사항
(인증/인가, 데이터 암호화, 취약점 대응)

## 성능 요구사항
(응답시간 목표, 동시접속 처리, 확장성 계획)

## 구현 단계
(Phase 1~3 구분, 각 단계별 목표와 산출물)

## 기능별 기술 노트
(각 주요 기능의 구현 방법, 사용할 라이브러리, 주의사항)"""
