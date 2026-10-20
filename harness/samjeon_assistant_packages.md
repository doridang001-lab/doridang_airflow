# 대화 학습과 향후 배포 준비

현재 노트북에서 발전한 비서의 목적·업무 방식·판단 기준을 나중에 공유할 수 있도록 준비한다. 이번 변경은 직원 등록, 서버 이전, 운영 배포, 모델 추가 학습을 실행하지 않는다. 기존 app-server, Telegram, 개인 기억, 예약·발송 원장은 계속 사용한다.

## 저장 경계

- 실행 코드와 역할별 지시문은 Git에서 관리한다. `samjeon_material_service/prompts/`에는 대화·자료 분석·보고·학습·공유 검토·개발 인계·검증 지시문이 있다.
- 학습은 기존 `state.sqlite3`의 버전 있는 meta 키에 사용자·대화·비서별로 저장한다. 별도 DB 마이그레이션이나 초기화는 없다. 상태 폴더는 운영 저장소 밖의 로컬 폴더를 사용한다.
- 공유 패키지는 UTF-8 JSON 한 파일이다. 목적, 지시문, 절차, 지식, 예시, 회귀 질문, 내용 해시, 엔진·도구 계약만 담는다.
- 원문 대화, 이력, 토큰, 사용자 ID, 개인 예약, 원천 데이터는 패키지 내보내기에 포함하지 않는다. OneDrive는 읽기 전용이다.

## 대화로 발전시키는 방법

자연어로 업무 목적이나 잘못된 판단을 설명하면 비서는 `propose_learning`으로 목적·규칙·절차·사례·기능 개선 후보를 만든다. 도구 결과의 ID와 판본을 사용한다.

1. `/학습목록`으로 후보를 찾고 `/학습보기 ID`로 전체 내용을 검토한다.
2. `/학습확정 ID 판본`으로 개인 기준 또는 개발 요청을 확정한다.
3. 수정은 비서에게 요청한다. 새 후보를 검토하는 동안에는 이전 확정 기준을 적용한다. 수정된 판본을 확정하면 새 기준으로 바뀐다.
4. `/학습삭제 ID 판본`은 기준을 비활성화한다. 삭제된 판본은 다시 확정할 수 없다. 수정 이력은 개인 기록에 남는다.
5. 공유할 때만 대표가 `/학습공유 ID 판본`을 입력한다. 공유 확정 이후 수정하면 공유 승인이 해제된다.

`/기억 이름=기준`, `기억해: 기준 문장`은 명시적인 개인 저장 요청으로 바로 반영한다. 새로 저장하는 기준은 학습 목록에도 나타난다. 기존 기억은 개인 상태로 보존하며 자동으로 공유하지 않는다. `/기억삭제`와 `/학습삭제`는 같은 이름의 기존 기억도 함께 비활성화한다.

모델은 확정·공유 명령을 실행할 수 없다. 텔레그램으로 수신한 실제 사용자 명령만 처리한다. 학습 저장은 발송 성공과 별도이므로 응답 수신이 불명확하면 `/학습목록`으로 확인한다. 같은 요청의 중복 저장은 방지한다.

## 선택 설정

기존 로컬 `service.json`은 덮어쓰지 않는다. 다음 키를 생략하면 기존 비서 ID와 기존 지시문으로 동작한다.

| 키 | 기본값 | 의미 |
| --- | --- | --- |
| `assistant_id` | `samjeon` | 개인 학습을 구분하는 실행 비서 ID |
| `assistant_package` | 빈 문자열 | 명시적으로 선택한 검증 패키지 경로. 상대 경로는 설정 파일 기준 |
| `package_owner_user` | `null` | 공유를 확정할 대표의 Telegram 사용자 ID. 허용 사용자여야 함 |

대표 ID는 운영자가 로컬 설정에 명시한다. 허용 목록의 첫 사용자를 대표로 추정하지 않는다. 지금 패키지를 선택하거나 직원 계정을 추가할 필요는 없다.

## 패키지 명령

명령은 저장소 루트에서 실행한다. 예시 출력 폴더 `.tmp`는 미리 준비하고, 실제 인계물은 승인된 로컬 폴더에 보관한다. 기존 출력 파일은 덮어쓰지 않는다. 명령의 경로·ID 자리표시자는 실제 로컬 값으로 바꾼다.

```powershell
python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service package export --id staff-helper --version 1.0.0 --output .tmp/staff-helper.json
python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service package inspect --input .tmp/staff-helper.json
python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service package validate --input .tmp/staff-helper.json
python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service package import --input .tmp/staff-helper.json --dry-run
```

기본 내보내기는 공통 시작 패키지만 만든다. 개인 학습을 포함하려면 `--config <로컬 설정> --user <대표 ID> --chat <해당 개인 대화 ID> --learning-id <확정 ID>`를 추가한다. 여러 항목은 `--learning-id`를 반복한다. 명시적으로 선택하고 현재 판본의 공유가 확정된 항목만 포함한다. 기능 개선 요청은 패키지에 넣을 수 없다.

검사는 내용 해시, 스키마, 엔진 계약, 도구 계약, 도구 존재 여부, 알려진 민감 정보·컴퓨터 경로를 확인한다. 실제 연결까지 확인하려면 검사에 `--config`를 전달한다. 파일은 읽기만 하고 CSV 본문·모델·Telegram은 호출하지 않는다.

`valid=true`는 구조 검사 통과다. `missing_connections`는 준비할 연결이고 `model_quality=not_run`, `deployment_ready=false`는 실제 모델 평가와 운영 배포가 아직 수행되지 않았음을 뜻한다. 가져오기는 `--dry-run`만 지원하며 설정·상태·자동 실행을 활성화하지 않는다.

민감 정보 검사는 알려진 패턴에 한정된다. 대표는 공유 확정 전에 사람 이름, 내부 비밀, 식별 가능한 사례까지 검토하고 일반화해야 한다. 패키지 해시는 손상을 검사하며 배포자의 신원을 인증하는 전자서명이 아니다. 실제 배포 단계에서 신뢰할 배포본 전달·직원별 권한·점진 배포를 추가한다.

## 기능 개발 인계

업무 대화에서는 실행 코드 쓰기 도구를 제공하지 않는다. 기존 파일 조회 도구는 유지하고 개선 요청은 로컬 학습 초안으로 저장한다. 이 방식으로 대화 때문에 운영 저장소에 수정 사항이 생겨 업데이트가 중단되는 문제를 방지한다.

기능 후보에는 `feature_details`의 `current`, `expected`, `example`, `acceptance`, `tools`를 채운다. 불명확한 내용은 후보로 보관하고 확인한다. 필수 내용 없이 개발 인계물을 내보낼 수 없다.

```powershell
python -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service package export-change --config '<로컬 설정>' --user '<사용자 ID>' --chat '<개인 대화 ID>' --learning-id '<확정된 기능 요청 ID>' --output .tmp/change-request.md
```

인계물은 개발 프롬프트, 현재·기대 동작, 사용 예시, 완료 조건, 관련 도구, 요청 판본, 엔진 계약을 포함한다. 코딩 에이전트가 별도 작업 공간에서 구현·검증하도록 사용한다. 인계물 생성 자체는 코드 구현·커밋·배포가 아니다.

## 도구와 검증

`tool_registry`는 전송 방식과 분리된 이름·입출력 계약을 제공한다. 실행 주체와 데이터 범위는 서비스가 주입하며 모델 인자로 바꾸지 못한다. 기존 app-server 동적 도구를 이용하고 실제 MCP 서버는 이번에 추가하지 않는다. 직원별 데이터 권한·다중 사용자 상시 운영은 후속 작업이다.

Runtime 부분 복제는 서비스 디렉터리 전체를 포함하므로 새 프롬프트와 패키지 모듈도 전달된다. `doctor`는 선택 패키지와 대표 설정 여부를 보고한다. 패키지 변경 또는 학습 변경 시 새 모델 스레드로 현재 문맥을 전달한다. 기존 대화·offset·발송 이력을 초기화하지 않는다.

```powershell
python -B -X utf8 -m pytest tests/test_samjeon_assistant_packages.py tests/test_samjeon_appserver.py tests/test_ceo_assistant.py tests/test_samjeon_report_schedules.py -q -p no:cacheprovider
```

자동 시험은 사용자 격리·판본·확정·삭제·공유 범위·미구현 기능·해시·호환성·무저장 사전 검사·가짜 모델 대화를 검증한다. 패키지의 `evals.jsonl`과 `prompts/validation.md`로 실제 답변도 평가한다. 실제 평가 시 질문별 모델·엔진·패키지 버전, 기대 행동, 결과와 근거를 로컬 평가 기록에 남긴다. 자동 시험 성공을 실제 모델 품질 합격으로 기록하지 않는다.
