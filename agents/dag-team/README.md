# dag-team

DAG 생성 전담 3인 에이전트 팀. 사용자의 자연어 요청을 받아 프로젝트 컨벤션에 맞는 DAG + 파이프라인 모듈 코드를 생성하고 검증한다.

## 팀 구성

| 에이전트 | 모델 | 역할 |
|---|---|---|
| `dag-team-planner` | Haiku | 요청 분석 → 기술적 코딩 계획 수립 |
| `dag-team-coder` | Sonnet | 계획 → 완성된 DAG/파이프라인 코드 생성 |
| `dag-team-tester` | Sonnet | 생성된 코드 정적 분석 및 컨벤션 검증 |

## 워크플로우

```
사용자 요청
    ↓
[planner] 기존 DAG 스캔 → 네이밍/스케줄/경로 상수 확인 → plan dict 출력
    ↓
[coder] plan dict + reference_dag 참조 → DAG 파일 + 파이프라인 모듈 코드 생성
    ↓
[tester] 정적 분석 → PASS / CAUTION / FAIL 판정
```

## 프로젝트 컨벤션 요약

- DAG 파일명: `Sales_XXX_Dags.py` (sales) / `Strategy_XXX_Dags.py` (strategy)
- `dag_id = Path(__file__).stem`
- `schedule=` → `schedule.py` 상수 사용 (하드코딩 금지)
- `start_date=pendulum.datetime(..., tz="Asia/Seoul")`
- XCom: `op_kwargs={'input_task_id':..., 'input_xcom_key':..., 'output_xcom_key':...}`
- 파이프라인 로깅: `logger = logging.getLogger(__name__)` (print 금지)
- 경로: `paths.py` 상수만 사용
