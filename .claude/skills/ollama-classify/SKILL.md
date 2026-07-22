---
name: ollama-classify
description: |
  로컬 Ollama(qwen) LLM 분류/추출 파이프라인을 작성·디버깅하는 스킬.
  사용자가 "ollama 연결", "LLM 분류", "qwen으로 분류", "메뉴/상품 자동 분류",
  "fin_product 분류", "LLM 파이프라인", "AI 분류" 등을 언급할 때 반드시 사용한다.

  핵심 역할: 코드 작성 전에 실제 Ollama 환경(호스트·모델 후보)을 확인하고 소량 검증한다.
  이 절차 없이 바로 작성하면 반드시 아래 문제가 발생한다:
  - 모델명 형식 오류(gpt-oss-20b → 실제 gpt-oss:20b) → 404, 전량 폴백 → 쓸모없는 데이터 적재
  - 모델 존재 확인 없이 수백 건 호출 → 전량 실패 후 재분류
  - Docker 내부에서 localhost 사용 → 연결 불가
  - fallback 모델 없이 단일 모델 의존 → 모델 교체 시 전체 장애
---

## 절대 규칙 — 신규 코드 금지, 기존 유틸 재사용
LLM 호출은 반드시 `modules/transform/utility/qwen_client.py`를 쓴다. 직접 requests/openai 클라이언트를 새로 만들지 않는다.
```python
from modules.transform.utility.qwen_client import (
    get_ollama_client_with_candidates,  # (client, model_candidates) 반환 — 호스트/모델 후보 자동 결정
    query_qwen_json,                     # query_qwen_json(prompt, client=client, model_candidates=model_list) → dict
)
client, model_list = get_ollama_client_with_candidates()
result = query_qwen_json(prompt, client=client, model_candidates=model_list)
```

## 참조 파이프라인 (동일 패턴 따라가기)
- `modules/transform/pipelines/db/DB_ToOrderMenu_LLM.py` — 옵션/스토어/토탈 단계별 LLM 분류
- `modules/transform/pipelines/db/DB_FinProduct.py`, `DB_FinProduct_Map.py` — 상품 마스터 분류
- `harness/fin_product.md` — fin_product 분류 도메인 지식

## 절차
1. **환경 확인** — `get_ollama_client_with_candidates()`가 돌려주는 client/model_list를 실제로 출력해 호스트·모델 후보 확인.
2. **모델 검증** — 후보 모델이 실존하는지(형식 `name:tag`), fallback 후보가 2개 이상인지.
3. **소량 검증** — 대표 샘플 3~5건만 `query_qwen_json`으로 호출해 JSON 스키마·키가 기대대로 나오는지 확인.
4. **스키마 고정** — 프롬프트에 출력 JSON 키를 명시하고, 파싱 실패 시 fallback 값 정의.
5. **배치 dry-run** — 전량 호출 전 소량 배치로 실패율 확인 → 통과 후 전체.

## 종료 조건
샘플 검증 JSON 정상 + fallback 동작 확인 + 배치 실패율 허용치 이내.

## Constraints
- print 금지·logging 사용. 응답 한글. OneDrive 승인·git push 지시 시에만.
- LLM 결과로 DB/파티션 적재 시 원본 백업·복구 경로 유지(오염 시 복구 가능하게).
