# 송파삼전점 대화형 관리 에이전트

## 기존 잠정 자료 조회

검증된 `samjeon-mart-v1` 조회는 [기존 자료 조회 계약과 배포 절차](samjeon_legacy_v1.md)를 따른다. 매출·구매 관측액, 품목 순위, 기간 비교, 직원식대와 구매 근거를 지원하며 PDF·고급 이상 탐지·예측은 v3 전용이다. 아래 알림·PDF·마감·전체 준비 설명은 v3에 적용한다.

`doctor --require-data-query-ready`는 기본 대화와 자료 조회 준비를 함께 검사한다. v1에서는 `data_query_ready=true`, `data_ready=false`, `ready=false`다. `/items`는 구매액 순위이고 `/compare`는 기간 비교다. 오류 안내는 게시본 없음·형식 미지원·파일 검증 실패·기간 부족으로 구분한다. 자료 형식이 v1이라는 이유만으로 포괄적인 자료 미준비 안내를 보내지 않는다.

## WSL 백그라운드 연결

2026-10-14 이후 신규 운영은 [WSL 연결 TRD](samjeon_telegram_wsl_trd.md)를 따른다. 운영 노트북의 기존 Windows Telegram 서비스가 같은 노트북의 Linux Codex를 호출한다. 모델은 Astra, 추론 수준은 `medium`이다. 기존 Windows 직접 실행 설정은 호환용으로 유지하며 WSL 설정 실패 시 자동 전환하지 않는다.

### 2026-10-15 Telegram Codex 분석 세션

운영 설정의 `conversation_engine=appserver`는 기존 임시 JSON 조회 봇을 지속 Codex 대화로 전환한다. WSL Codex app-server의 `thread_id`를 사용자·개인 대화의 72시간 논리 세션에 연결하고, 모델이 검증된 잠정 자료의 구조 조회·읽기 전용 SQL 집계·거래 근거·전달 완료 과거 대화 검색 도구를 직접 사용한다. Codex 코드 실행 호스트는 도구 호출에 필요하며 앱의 별도 쓰기·셸 도구는 비활성이다. `sandbox=read-only`, `approvalPolicy=never`와 OneDrive 읽기 전용을 유지한다.

질문은 자연어 그대로 처리한다. `/help`와 기존 버튼은 호환용이며, 파일 업로드·고정 조회 종류 선택은 필요하지 않다. 최신 스냅샷의 manifest와 Parquet를 먼저 검증하고 OneDrive 밖 `state/analysis-cache`의 불변 SQLite 사본으로 집계한다. 운영 `state.sqlite3`와 분리된다. `query_data`는 SELECT/WITH, 최장 15초, 최대 1000행 및 128 KiB 응답을 허용한다. 72시간 후 첫 메시지에서 새 Codex thread를 만들고 기존 요약·분석을 승계한다. 서비스 재시작은 thread/resume으로 이어간다. 미전달 결과가 있는 thread는 재사용하지 않고 전달 완료 문맥으로 새 thread를 만든다.

실행 확인은 `doctor --require-data-query-ready`와 `state/health.json`의 `conversation_engine=appserver`, `agent_connected=true`, `conversation_ready=true`, `data_query_ready=true`를 함께 본다. `verify-ai`는 기존 임시 호출 계약 검사로 남아 있고 새 대화 품질 합격 근거가 아니다. 새 방식은 실제 app-server에서 데이터 카탈로그→다중 SQL 조회→자연어 답변→프로세스 재연결 뒤 후속 조언까지 검증한다. 2026-10-15 시험에서는 이 동작과 가짜 Telegram 전송기의 두 턴 전체 흐름을 통과했다. 실제 사용자 화면 수신은 별도 확인한다. 기존 이상 알림 `auto_send=false`를 유지하고, 오전 브리핑은 별도 설정으로 운영한다.

### 오전 9시 브리핑과 답변 표시

`daily_briefing_enabled=true`, `daily_briefing_time=09:00`, `recipient=허용된 개인 대화 ID`를 설정하면 KST 오전 9시 이후 정오 전까지 하루 한 번 발송한다. 9시에 노트북이 꺼져 있으면 정오 전 로그인 시 한 번 보충하고, 그 이후에는 건너뛴다. 안정적인 정시 수신에는 Windows 로그인이 유지돼야 한다.

Windows 예약 작업은 콘솔 창이 없는 `.venv/Scripts/pythonw.exe`를 직접 실행한다. 로그인 시작과 5분 반복 트리거를 함께 사용하고, 실행 중에는 중복 실행하지 않는다. WSL 호출도 창 없는 프로세스로 시작한다. 작업이 `Ready`인데 오래된 Python 자식이 `service.lock`을 잡은 상태는 비정상이며 해당 서비스 프로세스만 확인 후 정리해야 한다.

`DoridangSamjeonMaterialWatchdog`는 별도 숨김 작업으로 1분마다 서비스 작업 상태와 `health.json`의 `checked_at`, `telegram_poll_at`을 확인한다. 작업이 꺼졌으면 즉시 시작하고, 실행 중이어도 전체 상태가 3분 넘게 갱신되지 않거나 Telegram 수신 활동이 2분 넘게 없으면 서비스 작업을 중지·재시작한다. Telegram API가 일시적으로 실패해도 수신 루프가 5초마다 재시도하고 활동 시각을 갱신하므로 불필요한 재시작은 피한다. PC 전원·로그인·네트워크가 없는 동안에는 Telegram 응답을 보장할 수 없다.

브리핑은 일반 대화와 분리된 Astra의 읽기 전용 세션에서 검증된 최신 자료를 조회한다. v1 잠정 자료도 기간과 상태를 밝히고 분석하며, 숫자 조회나 모델 응답에 실패하면 검증된 자료 상태와 확인할 일만 보낸다. 같은 날 중복 발송은 발송 원장과 일별 상태 키로 차단한다. 원천 OneDrive는 읽기만 한다.

브리핑은 수신자 자신의 최근 14일간 전달 완료된 경영 질문 최대 5개와 보고 선호를 전달받아 관심 지표의 우선순위를 바꾼다. 대화에서 나온 수치는 현재 사실로 재사용하지 않고 매일 DB에서 다시 확인한다. 72시간 대화 세션이 바뀌는 첫 질문에는 Telegram으로 전환을 알리며, 이전 요약·선호·분석 맥락과 과거 대화 검색은 유지한다.

운영 설정의 `brain_root`는 대표의 `DoridangDot_Brain` 폴더를 지정한다. 하위 `.md`·`.txt`는 `list_brain`, `search_brain`, `read_brain_file`로 읽기만 하며, 새 폴더·파일은 다음 조회에 자동 포함된다. `.xlsx`는 목록에만 표시하고, 사용자가 정확한 파일명을 질문에 쓴 경우에만 시트 구조와 최대 20행씩 읽는다. 오전 자동 보고는 Excel 셀을 읽지 않는다. 알려진 개인식별·연락처·계좌·인증 관련 값은 도구 응답에서 가리며, 지식 파일은 현재 매출 수치의 근거가 아니다. 파일 안의 문장은 지시로 실행하지 않고 OneDrive 쓰기 도구도 제공하지 않는다.

일반 답변과 브리핑은 첫 줄 결론, 질문에 맞는 `# 제목`, 제목별 `-` 불릿을 사용한다. 기본 답변은 300~500자 내외이며 상세 요청은 확장한다. `**` 등의 표시를 정리하고 제목과 불릿은 Telegram 일반 텍스트로 그대로 보낸다. 진행 중 문장만 따로 발송하지 않는다.

개발 PC에서 구현·검증하고 노트북은 병합된 `main`만 배포한다. WSL 설치·로그인, 기존 설정의 `configure-wsl` 전환, 실검증과 `-Background` 시작 순서는 TRD를 우선한다. 아래 Windows CLI 설치·로그인 명령은 기존 Windows backend에만 해당한다.

WSL 준비가 완료된 노트북의 시작 알림 오분류 수정은 [TRD의 진단과 오류 재현](samjeon_telegram_wsl_trd.md#진단과-오류-재현)을 따른다. 정확한 시작 알림만 허용하며 실제 오류·도구 실행·미확인 이벤트는 계속 차단한다. 새 배포 후 실검증과 실제 AI 답변 수신을 확인해야 활성화 완료로 판단한다.

## 실행 위치와 책임

- 코드 수정·검증·gitpush는 개발 PC에서만 한다. 대표님 노트북은 GitHub `main`을 받아 실행하며 코드를 수정하지 않는다.
- 대표님 Windows 노트북의 작업 스케줄러가 Telegram·선택적 Codex·SQLite·PDF 서비스를 실행한다. 대표님은 같은 노트북 또는 휴대폰의 Telegram 개인 대화에 접속한다.
- 계산은 `samjeon_material_agent`, 대화·알림은 별도 `samjeon_material_service` 패키지다. 서비스 수정은 마트 계산 해시에 포함되지 않는다.
- `DB_Samjeon_Material_Agent_Dags`의 로컬 큐는 다른 PC의 서비스에 자동 전달되지 않는다. 노트북 서비스는 게시 스냅샷의 `latest.json`을 자체 감시한다.
- 새 DAG는 일시중지, `SAMJEON_MATERIAL_AGENT_ENABLED=false`, 서비스 `enabled=false`, `auto_send=false`, AI 비활성이 기본이다.

## 새 노트북 준비

1. Windows에서 Git과 Python 3.12를 설치한다. Telegram Desktop은 휴대폰 이용 시 선택 사항이다. WSL backend는 같은 운영 계정의 WSL 배포판과 Linux Codex 로그인이 필요하다. Windows 앱·CLI 로그인은 WSL 인증을 대신하지 않는다.
2. 개선 코드가 GitHub `main`에 반영된 후 아래 부분 복제를 사용한다. 노트북은 실행 파일만, 현재 PC는 개발 프로필의 관련 테스트·문서까지 받는다. 갱신은 아래 업데이트 스크립트로 수행한다.
3. 저장소 루트에서 `scripts/setup_samjeon_material_agent.ps1`을 실행한다. 이 스크립트는 Python 3.12 전용 `.venv`와 최소 의존성을 준비하고 `doctor`를 실행한다. 설정·토큰·스냅샷이 없는 첫 결과는 미준비로 표시되는 것이 정상이다.
4. 실제 서비스 설정은 저장소 밖의 `C:\Local_DB\samjeon_material_agent\service.json`에 둔다. `state_root`는 같은 로컬 폴더 아래로, `snapshot_root`는 `latest.json`과 `snapshots/`가 있는 게시 루트로 지정한다. 기존 v1과 승인된 v3를 형식별로 검증하며 원본 자료와 게시본은 읽기 전용으로 사용한다.
5. 전용 Telegram 봇 토큰은 노트북의 서비스 실행 계정 환경변수 `SAMJEON_TELEGRAM_BOT_TOKEN`에 보관한다. 대표님 Telegram 계정의 숫자 사용자·개인 대화 ID만 허용 목록에 넣는다. 토큰과 실제 ID는 Git에 저장하지 않는다.
6. 추가 사용 정책 확인과 실계정 대화 검증 후 `enabled=true`, `ai_enabled=true`, `ai_verified=true`, `auto_send=false`로 대화를 시험한다. 기존 잠정 자료는 전용 조회 검증 후 사용하고 PDF는 v3 검증 후 시험한다. 전체 `doctor --require-ready` 통과 후에만 작업 스케줄러를 등록한다. `auto_send=true`는 대표님의 별도 요청이 있을 때만 적용한다. 노트북은 로그인 상태·전원·네트워크를 유지하고 절전을 해제해야 응답한다.

```powershell
winget install --id Git.Git --exact --source winget
# Git 설치 후 새 PowerShell에서 계속한다.
git clone --filter=blob:none --no-checkout --single-branch --branch main https://github.com/doridang001-lab/doridang_airflow.git C:\airflow_samjeon_agent
cd C:\airflow_samjeon_agent
git show HEAD:config/samjeon-runtime.paths | git sparse-checkout set --no-cone --stdin
git checkout main
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_samjeon_material_agent.ps1 -InstallPrerequisites
```

현재 OneDrive의 `latest.json`은 v3가 아니다. v3 분류·마감 검수와 게시 승인이 끝나기 전에는 실자료 자동 알림을 켜지 않는다. OneDrive의 파일·동기화 설정 변경은 승인 후 진행한다.

## 준비와 명령

Python 3.12 전용 환경을 만들고 `requirements-samjeon.txt`를 설치한다. PDF 검증 도구는 개발 환경에만 `pypdf`, `pymupdf`, `pytest`를 추가한다.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-samjeon.txt
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config config/samjeon_material_agent.example.json
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service report --snapshot '<v3 로컬 스냅샷>' --dry-run
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service service --config '<로컬 service.json>' --dry-run
```

- 예시를 로컬 운영 설정으로 복사한 후 실제 경로·사용자 ID·개인 대화 ID를 등록한다. 상대 경로는 설정 파일의 부모 기준이다.
- 개발 `state_root`는 작업 폴더 `.tmp/samjeon-service`다. 운영에서는 `C:\Local_DB\samjeon_material_agent`를 지정한다.
- `snapshot`은 검증할 로컬 불변 v3 폴더다. 운영은 `snapshot`을 비우고 `snapshot_root`에 승인된 v3 게시 루트를 지정한다.
- `SAMJEON_TELEGRAM_BOT_TOKEN`은 서비스 계정 환경변수로 설정한다. 기존 운영 봇의 polling을 공유하지 않는다.
- `doctor`는 모델·Telegram 호출 없이 설정·스냅샷·글꼴·의존성과 CLI 로그인 상태를 확인한다. `token_present`나 `ai_configured`는 실대화 검증 성공을 뜻하지 않는다.
- dry-run은 `-B`와 함께 실행한다. 디렉터리·SQLite·모델 작업·발송이 발생하지 않아야 한다.

## 대화와 판단

- 아침에는 확인할 점 최대 3개를 대화식으로 설명하고, 새 스냅샷의 새 이상을 추가 안내한다. 자료 기준일과 상태를 항상 표시한다.
- 관측 사실과 선매입·배송일 변경 같은 확인 가설을 구분한다. 재고·폐기 근거 없이 낭비·손실을 확정하지 않는다.
- `/help`, `/status`, `/cancel`, `/summary`, `/compare`, `/anomalies`, `/forecast`, `/staff_meal`, `/evidence`, `/report`를 지원한다.
- 기간은 `YYYY-MM-DD YYYY-MM-DD`; 근거 필터는 `evidence_id=`, `product_key=`, `source=`, `offset=`, `limit=`다. 근거는 최대 500행, 채팅은 앞 20행을 표시한다.
- 자연어·버튼은 같은 조회 계약에 연결한다. 비슷한 품목이 여러 개면 선택 버튼을 제공한다. 선택한 품목은 짧은 후속 질문에서도 유지한다.
- 개인 대화별 논리 세션은 시작 시각부터 72시간이다. 경계 뒤 첫 새 질문에서 자동 전환하며 이전 요약·선호·분석을 승계한다. 전달 완료 원문은 24시간이 지나도 검색할 수 있다. 버튼은 원래 스냅샷과 사용자 소유권에 고정돼 다른 알림의 근거와 섞이지 않는다.
- 사용자 설명은 분석 기간·품목에 연결해 보존한다. 데이터·품목 분류·마감 승인을 변경하지 않는다.
- 상세 PDF는 버튼 또는 요청 시 생성한다. 대화와 같은 판본·기간·선택 품목을 사용하며 캐시 키에 사용자 설명도 포함한다.
- PDF는 한국어 글꼴을 포함한다. 기본은 Windows 맑은 고딕이며 글꼴 바이너리는 Git에 넣지 않는다. 원천 물리 경로 대신 근거 ID를 표시한다.

## 일정과 중복 방지

- 오전 슬롯은 09:30, 10:00, 10:30, 11:00, 11:30, 12:00 KST다. 미준비 안내와 준비 완료 안내를 각각 하루 한 번 처리한다.
- 새 이상은 기본 09:30~21:00에 전달한다. 시간 밖에서는 다음 허용 시간까지 보류한다. 질문 응답은 서비스 실행 중 항상 가능하다.
- 서비스는 60초마다 최신 포인터를 확인한다. 원천 수집을 실행하지 않으며 마트가 갱신돼야 새 자료를 알 수 있다. 현재 마트 정기 일정은 오전뿐이다.
- 큐는 **자료 확인 요청**이다. 서비스가 처리 시점의 최신 스냅샷·파일 해시를 검증한다. 오래된 날짜·정오 이후 만료된 큐는 `queue/expired`로 옮긴다.
- 수신 업데이트와 offset을 함께 저장한다. 처리 중 재시작된 요청은 중단으로 남기고 자동 재실행하지 않는다.
- 발송 전 `sending`, 성공 시 Telegram 메시지 ID와 `sent`를 기록한다. 타임아웃 또는 재시작으로 결과가 불명확하면 `uncertain`으로 두고 자동 재발송하지 않는다.
- 불명확하거나 실패한 발송은 DB 이력을 운영자가 대조한다. DB를 초기화해서 해결하지 않는다.

## 자동 세션과 과거 기록

- 세션은 서비스가 관리하며 Codex 호출은 계속 임시 대화 방식이다. 봇 프로세스를 72시간마다 다시 시작하지 않는다. UTC 수신 시각으로 경계를 판단하고 한국시간으로 날짜를 해석한다. /new 입력이나 전환 안내 메시지는 필요 없다.
- SQLite 판본 1을 유지하고 버전 있는 meta 기록으로 활성 세션, 요청, 전달 완료 턴, 분석 연결을 저장한다. 기존 저장 기록은 전달 완료와 소유자를 확인해 한 번만 “이전 대화”로 연결한다.
- 전환 요약 호출은 최대 한 번이다. 요약 실패 때도 저장된 기본 요약으로 새 질문을 처리한다. 모델에는 공통 선호, 2,000자 이내 승계 요약, 미해결 질문, 활성 분석, 현재 세션 최근 10문답을 전달한다.
- “지난번 10월 분석 가져와”처럼 이전 기록을 찾으면 당시 자료 기준일을 표시한다. 같은 조건의 후보가 여러 개면 번호를 확인한다. 최신 수치 요청은 현재 게시본을 다시 검증한다.
- 원래 세션의 버튼은 해당 소유자와 판본에서만 동작한다. 오래된 버튼을 눌러도 현재 세션의 분석 선택은 변경하지 않는다. 취소·발송 실패·결과 불명 답변은 정상 기억에 넣지 않는다.
- verify-ai --require-ready는 조회 계획, 독립 가상 사실 설명, 일반 대화, 후속 기억의 네 단계를 모두 검사한다. 단계별 실행·결과·안전한 오류 분류를 확인한다. v1 자료 준비는 doctor --require-data-query-ready로 따로 확인한다.

## Codex 운영 조건

- 모델은 `gpt-6-astra`, 지원 CLI는 `0.158.0`이다. 자유 대화는 최대 1회, 자료 분석은 조회 계획과 근거 설명으로 최대 2회, 호출당 최대 90초다. 단순 매출 질문·명령·버튼·자동 알림은 정형 조회를 사용한다.
- 사용자 설정·규칙 상속을 끄고 별도 작업 폴더, 읽기 전용 sandbox, 도구·플러그인·브라우저 비활성 옵션을 적용한다. API 키는 자식 환경에 전달하지 않는다.
- 첫 출력 계약은 허용 조회 종류·기간·품목의 최대 세 개 조회 또는 확인 질문이다. Python 계산 후 두 번째 계약으로 사실 ID·근거 있는 가설·추가 질문을 받는다. 수치는 검증된 결과에서 삽입하며 임의 수치·근거·셸 요청은 거부한다. 미확정 자료의 원인 판단은 보류한다.
- `ai_verified`는 운영 계정의 인증·무인 실행·도구 차단·추가 사용 통제를 실검증한 뒤 설정한다. CLI 설치 확인이나 자동 테스트만으로 true로 바꾸지 않는다.
- AI 사용 불가·한도·시간 초과·취소 시 정형 대화와 명령 조회를 유지한다. 유료 API로 자동 전환하지 않는다.

## 활성화·복구

1. 품목·마감 검수, 목표 적용일·금액 기준, 승인된 v3 게시 여부를 확인한다. OneDrive 게시 승인은 별도다.
2. 등록된 전용 봇·사용자·대화 ID로 승인된 시험을 수행한다. 기본 설정 예시에는 실제 ID·토큰을 넣지 않는다.
3. 승인된 실행 폴더에서 시작 스크립트를 실행한다. 작업스케줄러 등록 스크립트는 기본 비활성이며 `-Enable`을 명시해야 활성 등록된다.
4. 작업 이름은 `DoridangSamjeonMaterialAgent`, 로그인 시 숨김 시작·단일 인스턴스·실패 재시작이다. 배터리에서도 시작·계속 실행한다. PC 종료·절전·로그아웃 중에는 응답하지 못한다. 활성 등록 후 즉시 시작하려면 `Start-ScheduledTask -TaskName DoridangSamjeonMaterialAgent`를 실행한다. appserver 방식은 v3 전체 마감 대신 현재 대화와 자료 조회 준비 상태를 검사해 등록한다.
   - appserver 대화는 Astra가 답한다. Telegram 답변의 Markdown 표시는 일반 텍스트로 바꾸고 긴 답변은 문단·문장 기준으로 나눈다.
   - 대화 파일 도구는 현재 로컬 코드 저장소의 UTF-8 텍스트를 읽고 생성·수정한다. 삭제 도구와 임의 명령 실행은 없고, OneDrive와 숨김 경로는 대상에서 제외한다. 기존 파일 수정은 읽은 SHA-256이 일치해야 한다.
5. 복구 시 해당 서비스·DAG만 중지하고 승인된 기능 변경만 되돌린다. SQLite는 이전 판본을 조용히 초기화하지 않는다.
6. DB 복원 후 자동 발송을 끄고 발송 이력을 대조한다. 원천과 기존 스냅샷을 복구 대상으로 수정하지 않는다.

## 검증

```powershell
.\.venv\Scripts\python.exe -B -X utf8 -m pytest tests/test_samjeon_material_mart.py tests/test_samjeon_material_mart_v3.py tests/test_samjeon_material_service.py tests/test_samjeon_material_codex.py tests/test_samjeon_material_pdf.py tests/test_samjeon_material_agent_dag.py tests/test_samjeon_grounded.py tests/test_samjeon_laptop_runtime.py tests/test_samjeon_conversation.py -q -p no:cacheprovider
python scripts/harness_cli.py validate --target .
```

대화→품목→설명 기록→PDF 흐름, 미등록 사용자, 오래된 버튼, 취소, 재시작, 미준비·신규 이상 중복 방지, 메시지 분할, AI 계약·실패, PDF 한글·수치·판본, import부터 dry-run 무저장을 검증한다. 실제 발송·구독 인증·시범 운영 통과는 별도로 기록한다.

## 최초 설정과 Codex 연결 검증

`-InstallPrerequisites`는 Git·Python 3.12·Node.js LTS를 준비하고 npm으로 `@openai/codex@0.158.0`을 설치한다. PATH가 갱신되지 않으면 새 PowerShell을 연다. 다른 버전의 Python launcher만 있으면 Python 3.12를 별도 설치한 뒤 재실행한다. Telegram Desktop은 휴대폰으로 대화할 경우 선택 사항이다.

최초 설치는 저장소 밖 `C:\Local_DB\samjeon_material_agent\service.json`을 비활성 상태로 생성한다. 상태 폴더 기본값은 그 아래 `state`다. 기존 설정은 덮어쓰지 않는다. `-CheckOnly`는 설치·설정 생성을 수행하지 않는다. BotFather에서 전용 봇을 만들고 개인 대화에서 `/start`를 보낸 뒤 숫자 사용자·개인 대화 ID를 준비한다. 봇 토큰은 실행 계정의 `SAMJEON_TELEGRAM_BOT_TOKEN` 환경변수에 보관한다. 설정 후 새 터미널이나 Windows 재로그인으로 환경변수를 반영한다.

서비스를 실행할 노트북 Windows 계정으로 다음 절차를 진행한다. 앱 로그인만으로 CLI 인증을 가정하지 않는다.

```powershell
codex login
codex login status
$config = 'C:\Local_DB\samjeon_material_agent\service.json'
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $config
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service verify-ai --config $config --require-ready
```

- `doctor`는 로컬 CLI·인증 방식·설정·게시 파일·글꼴을 점검하며 모델·Telegram을 호출하지 않는다. `--require-ready`는 미준비일 때 종료코드 2를 반환한다.
- `verify-ai`는 가상 자료 계획·설명과 자유 대화 두 턴을 독립 실행한다. 확인된 일시적 연결·서버 오류에 한해 단계당 한 번 재시도할 수 있다. 후속 질문의 기억 단어도 확인한다. 추가 사용 정책 확인 후에만 실행하며 `ai_verified`는 자동 변경하지 않는다. `--dry-run`은 모델을 호출하지 않는다.
- API 키 인증은 차단한다. Business의 추가 크레딧·자동 충전·워크스페이스 사용 정책은 운영 전 관리 화면에서 확인한다. 실연결 성공은 추가 비용 차단 검증을 뜻하지 않는다.
- 지원 CLI 버전이 달라지면 진단이 실패한다. 개발 PC에서 인자·권한·출력 계약을 재검증하고 지원 버전을 게시한 후 갱신한다.

## 동기화 지연과 분석 이력

승인된 OneDrive 게시 폴더는 노트북에서 실제 파일을 읽을 수 있도록 준비한다. OneDrive 동기화 설정 변경도 승인 후 진행한다. 최신 포인터·매니페스트·파일 해시 검증이 실패하면 이전 정상 자료가 있어도 수치·PDF 조회를 중단하고 자료 준비 안내를 보낸다. 정상 v3가 도착하면 다음 조회부터 이용할 수 있다. 기존 분석과 버튼은 최신 게시 상태 확인에 더해 원래 판본을 검증해 사용한다.

현재 세션의 최근 10문답을 모델에 전달하고, 전달 완료 원문은 버전 있는 `meta` 기록으로 장기 보존한다. 기존 24시간 문맥 키는 호환용으로 남긴다. 사용자 설명·범위가 바뀌면 이전 AI 설명을 재사용하지 않는다. PDF는 같은 판본의 계산 결과와 저장된 분석 설명을 포함한다.

일반 대화와 자료 분석은 하나의 논리 세션 문맥에서 이어진다. 취소·실패·전송 결과 불명 답변은 전달된 대화로 기억하지 않는다. 명령과 업무 키워드, 마지막 대화 주제로 먼저 분기하고 모호한 질문은 되묻는다. `어제`는 Telegram 메시지 수신일의 한국 날짜 기준이며 스냅샷 날짜를 대신 사용하지 않는다. `지난주`는 월~일, `최근 7일`은 어제까지, `이번 달`은 월초~어제(월초 당일이면 당일)다.

`/start`·`/help`·`/status`는 일반 대화·자료 조회·전체 준비를 구분한다. v1은 전용 계약으로 잠정 관측값을 답한다. 게시본 없음·미지원 형식·검증 실패는 안전한 오류별 문구를 사용하고, 요청 기간이 없으면 보유 범위를 안내한다.

`state/logs/service.log`와 `state/health.json`에서 AI 실패 단계·자료 동기화 지연·발송 상태를 확인한다. 토큰과 원본 CLI 오류 전문은 저장하지 않는다. 노트북 실계정·전용 봇·승인된 v3 연결 후 연속 3일 운영 확인을 수행한다.

## 업데이트와 폴더 정리

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\update_samjeon_material_agent.ps1
# 실패 시 이전 검증 버전으로 복구
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\update_samjeon_material_agent.ps1 -Rollback
```

깨끗한 `main`에서 작업 중지·잠금 확인 → fast-forward pull → 부분 설치 목록 재적용 → 의존성 설치 → 준비 진단 → 기존 활성 작업 재시작 순서로 실행한다. 직접 실행한 서비스도 먼저 종료한다. 실패하면 자동 시작을 끄고 이전 커밋을 로컬 `deployment.json`에 보존한다. 복구는 `reset --keep`으로 코드만 되돌리고 설정·대화·발송 이력을 유지한다.

### v3 준비 전 대화 전용 갱신

`doctor`의 `conversation_deployable`은 토큰·허용 목록·의존성·지원 CLI/ChatGPT 로그인 및 `auto_send=false`를 검사한다. `enabled`·`ai_enabled`·`ai_verified`가 꺼져 있어도 코드 배포는 가능하다. 이 세 설정까지 켜져야 `conversation_ready=true`이며, `data_ready`는 검증된 v3 존재 여부다. 기존 `ready`와 `--require-ready`는 전체 운영 준비 조건을 유지한다.

```powershell
.\scripts\update_samjeon_material_agent.ps1 -ConversationOnly
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config 'C:\Local_DB\samjeon_material_agent\service.json' --require-conversation-deployable
```

대화 전용 갱신은 기존 자동 실행 작업을 중지·비활성화하고 재시작하지 않는다. 새 작업도 등록하지 않는다. 전체 준비 전에는 비활성 작업 등록도 차단한다. 복구는 기록된 배포 모드를 따르며, 복구한 구버전이 새 점검 옵션을 지원하지 않으면 코드 복구 후 서비스 중지를 유지한다.

**구버전에서 최초 전환:** 구버전 스크립트에는 `-ConversationOnly`가 없으므로 서비스를 중지하고 기존 자동 실행 작업도 비활성화한 뒤 아래 순서로 실행한다. 토큰·실제 ID·설정 파일 내용을 명령 출력에 넣지 않는다.

```powershell
Set-Location -LiteralPath 'C:\airflow_samjeon_agent'
if ((git branch --show-current) -ne 'main') { throw 'main 체크아웃이 필요합니다.' }
if (@(git status --porcelain).Count) { throw '로컬 변경을 먼저 확인하세요.' }
$previousCommit = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw '이전 커밋 확인 실패' }
$config = 'C:\Local_DB\samjeon_material_agent\service.json'
$stateRoot = & .\.venv\Scripts\python.exe -B -X utf8 -c 'import sys; from modules.transform.pipelines.db.samjeon_material_service.config import Settings; print(Settings.load(sys.argv[1]).state_root)' $config
if ($LASTEXITCODE -ne 0) { throw '설정 확인 실패' }
& .\.venv\Scripts\python.exe -B -X utf8 -c 'import sys; from pathlib import Path; from modules.transform.pipelines.db.samjeon_material_service.__main__ import instance_lock; lock=instance_lock(Path(sys.argv[1])); lock.__enter__(); lock.__exit__(None,None,None)' $stateRoot
if ($LASTEXITCODE -ne 0) { throw '서비스를 먼저 중지하세요.' }
git pull --ff-only origin main
if ($LASTEXITCODE -ne 0) { throw '갱신 실패' }
.\scripts\update_samjeon_material_agent.ps1 -Config $config -ConversationOnly -BootstrapPreviousCommit $previousCommit
```

최초 전환에서도 원래 커밋을 배포 기록에 보존한다. 설정·SQLite는 덮어쓰지 않는다. 추가 크레딧·자동 충전 정책 확인 → `verify-ai --require-ready` → 세 활성화 설정 수동 변경 → 시작 스크립트 실행 → Telegram 실제 화면에서 인사·후속 질문·매출 미준비 안내 확인 순서로 진행한다. 승인된 v3 동기화와 `doctor --require-ready`, 근거·후속 질문·PDF 검증 후에만 로그인 자동 실행을 등록한다. `auto_send=false`를 유지한다.

개발 PC의 worktree는 변경을 게시한 뒤 다음 명령으로 정리한다. 에이전트 코드·관련 테스트·운영 문서만 표시하고 다른 worktree의 범위는 유지한다. 저장소에서 다른 프로젝트의 파일을 삭제하지 않는다.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\set_samjeon_sparse.ps1 -Profile Development
```
