"""Feature specification prompt builder."""
import json


def build_spec_prompt(prd: str, answers: dict) -> str:
    return f"""당신은 제품 기획 전문가입니다. 아래 PRD를 분석하여 상세한 기능명세서를 JSON 형식으로 작성해주세요.

PRD 내용:
{prd}

다음 JSON 스키마에 맞게 기능명세서를 작성해주세요.
ID 형식: R-XXXXXX (요구사항), F-XXXXXX (기능), D-XXXXXX (상세기능) — 영대문자 6자리 랜덤
최상위 요구사항 3~6개, 각 2~5개 기능, 각 1~4개 상세기능

```json
{{
  "requirements": [
    {{
      "id": "R-ABCDEF",
      "title": "요구사항 제목",
      "description": "상세 설명",
      "status": "신규",
      "priority": 1,
      "features": [
        {{
          "id": "F-ABCDEF",
          "title": "기능 제목",
          "description": "상세 설명",
          "status": "신규",
          "priority": 1,
          "detail_features": [
            {{
              "id": "D-ABCDEF",
              "title": "상세기능 제목",
              "description": "상세 설명",
              "status": "신규",
              "priority": 1
            }}
          ]
        }}
      ]
    }}
  ]
}}
```

반드시 유효한 JSON만 출력하세요. 다른 텍스트나 마크다운 코드블록 없이 JSON만 반환하세요."""


def build_child_prompt(parent: dict, depth: int, prd: str) -> str:
    depth_name = {1: "기능(Feature)", 2: "상세기능(Detail Feature)"}.get(depth, "하위 항목")
    id_prefix = {1: "F", 2: "D"}.get(depth, "D")

    return f"""당신은 제품 기획 전문가입니다. 아래 항목의 하위 {depth_name}을 생성해주세요.

상위 항목:
- ID: {parent.get('id', '')}
- 제목: {parent.get('title', '')}
- 설명: {parent.get('description', '')}

PRD 맥락:
{prd[:600]}

2~4개의 하위 항목을 JSON 배열로 반환해주세요. JSON 배열만 출력하세요:
[
  {{
    "id": "{id_prefix}-ABCDEF",
    "title": "제목",
    "description": "상세 설명",
    "status": "신규",
    "priority": 1
  }}
]"""


def build_chat_prompt(message: str, spec: dict, prd: str) -> str:
    spec_str = json.dumps(spec, ensure_ascii=False, indent=2)[:2000]
    return f"""당신은 Manny입니다. 기능명세서를 수정하는 AI 어시스턴트입니다.

현재 기능명세서:
{spec_str}

사용자 요청: {message}

요청에 따라 기능명세서를 수정하고 설명과 함께 수정된 전체 JSON을 반환하세요.
JSON은 ```json ... ``` 코드블록으로 감싸주세요."""
