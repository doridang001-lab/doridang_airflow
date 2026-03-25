# Report Skillup - 병렬 에이전트 기반 리포트 설계 시스템

**로컬 Ollama qwen 모델**을 이용한 PDF 리포트 자동 설계 및 영업 액션 제안 시스템

## 🚀 빠른 시작

```bash
# 1. Ollama 설치 및 qwen 모델 다운로드
ollama pull qwen2.5:7b
ollama serve

# 2. PDF 리포트 설계 및 액션 제안
python -c "
import sys; sys.path.insert(0, '.')
from agents.report_skillup import orchestrator
result = orchestrator.main('/path/to/pdf')
"

# 또는 직접 마크다운 파일에서 코드 실행
# 결과: C:\airflow\scripts\output\report-skillup_{store}_{timestamp}.json
```

## 📋 구조

```
C:\airflow\agents\report-skillup\
├── README.md                          ← 이 파일
├── orchestrator.md                    ← 메인 (3개 에이전트 조율)
├── qwen_prompter_agent.md             ← 에이전트 1️⃣: PDF → qwen 프롬프트
├── prompt_validator_agent.md          ← 에이전트 2️⃣: 검증 & 품질 평가
└── executive_reviewer_agent.md        ← 에이전트 3️⃣: 영업 관점 최종 검토

C:\airflow\modules\transform\utility\
└── qwen_client.py                     ← Ollama 래퍼 (의존성)
```

## 🤖 3가지 에이전트

| 파일 | 역할 | 입력 | 출력 |
|------|------|------|------|
| **qwen_prompter_agent.md** | PDF 분석 → qwen으로 3개 프롬프트 생성 | PDF 경로 | `{judgment, actions, causes}` |
| **prompt_validator_agent.md** | 생성된 응답 검증 (명확성/정확성/실행성) | qwen 결과 JSON | `{validation scores, status, issues}` |
| **executive_reviewer_agent.md** | 영업 관점 최종 검토 + 의사결정 트리 | validator 결과 JSON | `{action_breakdown, decision_tree}` |
| **orchestrator.md** | 3개 에이전트 순차 실행 + 결과 병합 | PDF 경로 | `{통합 JSON 리포트}` |

## 📊 실행 흐름

```
orchestrator.py 실행
  ↓
[1️⃣  qwen_prompter] PDF → 3개 qwen 응답 생성
  ↓
[2️⃣  prompt_validator] 응답 검증 (pass/caution/hold)
  ↓
[3️⃣  executive_reviewer] 액션 분류 (Type A/B/C) + 의사결정 트리
  ↓
result 병합 → JSON 저장
```

## 💻 사용법

### 오케스트레이터 (전체 파이프라인)
```bash
# 마크다운 파일에서 메인 함수 추출 후 실행
python -c "
import sys, json
sys.path.insert(0, 'C:/airflow')

# orchestrator.md에서 main 함수 실행
from agents.report_skillup.orchestrator import main
result = main('C:/path/to/shop_trend_상도점_20260321.pdf')

print('✨ 최종 결과 요약')
print(f'🎯 판단: {result[\"summary\"][\"qwen_judgment\"]}')
print(f'✅ 검증: {result[\"summary\"][\"validator_status\"]}')
print(f'👔 권고: {result[\"summary\"][\"executive_recommendation\"]}')
"
```

### 개별 에이전트 실행
```bash
# 1단계: qwen 프롬프트 생성
python -c "
import sys
sys.path.insert(0, 'C:/airflow')
from agents.report_skillup.qwen_prompter_agent import main
result = main('test.pdf')
"

# 2단계: 검증
python -c "
import sys, json
sys.path.insert(0, 'C:/airflow')
from agents.report_skillup.prompt_validator_agent import main
with open('qwen_output.json') as f:
    qwen_output = json.load(f)
result = main(qwen_output)
"

# 3단계: 영업 검토
python -c "
import sys, json
sys.path.insert(0, 'C:/airflow')
from agents.report_skillup.executive_reviewer_agent import main
with open('qwen_output.json') as f:
    qwen_output = json.load(f)
with open('validator_output.json') as f:
    validator_output = json.load(f)
result = main(qwen_output, validator_output)
"
```

## 📤 출력 예시

```json
{
  "meta": {
    "orchestrator": "report-skillup",
    "timestamp": "2026-03-21T10:30:45+09:00",
    "total_duration_sec": 12.3
  },
  "summary": {
    "qwen_judgment": "효율은 유지\n볼륨 부족",
    "validator_status": "PASS",
    "executive_recommendation": "즉시 실행 추천"
  },
  "action_items": [
    {
      "action": "전주 대비 광고비 축소 원인 확인",
      "action_type": "Type A (즉시)",
      "execution_time": "당일",
      "feasibility_score": 5,
      "next_if_success": "액션 2로 진행",
      "next_if_failure": "운영팀에 매장 상태 확인 요청"
    },
    {
      "action": "전주 수준 기준 광고비 10~20% 복원 테스트",
      "action_type": "Type A (당일)",
      "execution_time": "1일",
      "feasibility_score": 5,
      "test_period": "3일",
      "success_criteria": "주문수 10% 이상 회복 OR ROAS 5배 이상 유지"
    }
  ],
  "decision_tree": {
    "start": "액션 1: 광고비 축소 원인 확인",
    "branch_1": {
      "condition": "의도적 광고비 축소 확인",
      "true_path": "액션 2: 광고비 복원 테스트",
      "false_path": "액션 3: 운영 점검"
    }
  }
}
```

## 🔌 Ollama 설정

### 준비
```bash
# Windows: ollama.com 에서 설치
# 또는
# Docker: docker run -d -p 11434:11434 ollama/ollama

# 모델 다운로드
ollama pull qwen2.5:7b

# 서버 실행 (기본: localhost:11434)
ollama serve
```

### Docker Airflow에서
```python
# modules/transform/utility/qwen_client.py
OLLAMA_HOST = "http://host.docker.internal:11434"
```

### qwen 클라이언트 사용
```python
from modules.transform.utility.qwen_client import query_qwen, query_qwen_json

# 텍스트 응답
response = query_qwen("당신은 영업 전문가입니다. 주문이 30% 감소했을 때...")

# JSON 응답
data = query_qwen_json("반드시 JSON 형식으로:\n{\"actions\": [...]}")
```

## ✅ 검증 기준

### prompt_validator
- **명확성** (0~5): 프롬프트가 명확한가?
- **정확성** (0~5): 수치 계산이 맞는가?
- **실행 가능성** (0~5): 배민 앱에서 직접 실행 가능한가?

### executive_reviewer
- **Type A** (즉시): 배민 앱 내 직접 클릭 가능 → 점수 5
- **Type B** (주간): 운영팀 협력 필요 → 점수 3~4
- **Type C** (장기): 본부 승인 필요 → 점수 1~2

## 🐛 트러블슈팅

| 문제 | 해결 |
|------|------|
| `Ollama 연결 실패` | `ollama serve` 실행 확인, 포트 11434 |
| `모델 없음` | `ollama pull qwen2.5:7b` |
| `JSON 파싱 실패` | 프롬프트에 "반드시 JSON 형식으로" 명시 |
| `타임아웃 (300초)` | Ollama 성능 확인, 모델 크기 축소 (gemma2:2b) |

## 📝 로그 출력

```
[INFO] 🚀 Report Skillup Orchestrator 시작
[INFO] 📄 PDF: /path/to/pdf
[INFO] ============================================================
[INFO] ▶️  qwen_prompter 시작...
[INFO] ✅ qwen_prompter 완료
[INFO] ▶️  prompt_validator 시작...
[INFO] ✅ prompt_validator 완료
[INFO] ▶️  executive_reviewer 시작...
[INFO] ✅ executive_reviewer 완료
[INFO] ============================================================
[INFO] 🎯 판단: 효율은 유지\n볼륨 부족
[INFO] ✅ 검증: PASS
[INFO] 👔 권고: 즉시 실행 추천
[INFO] ⏱️  소요 시간: 12.3초
[INFO] 📄 보고서: C:\airflow\scripts\output\report-skillup_상도점_20260321.json
```

---

**생성**: 2026-03-21
**버전**: 1.0.0
