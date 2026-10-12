# 송파삼전점 대화형 관리 에이전트

## 실행 위치와 책임

- 코드 수정·검증·gitpush는 개발 PC에서만 한다. 대표님 노트북은 GitHub `main`을 받아 실행하며 코드를 수정하지 않는다.
- 대표님 Windows 노트북의 작업 스케줄러가 Telegram·선택적 Codex·SQLite·PDF 서비스를 실행한다. 대표님은 같은 노트북 또는 휴대폰의 Telegram 개인 대화에 접속한다.
- 계산은 `samjeon_material_agent`, 대화·알림은 별도 `samjeon_material_service` 패키지다. 서비스 수정은 마트 계산 해시에 포함되지 않는다.
- `DB_Samjeon_Material_Agent_Dags`의 로컬 큐는 다른 PC의 서비스에 자동 전달되지 않는다. 노트북 서비스는 게시 스냅샷의 `latest.json`을 자체 감시한다.
- 새 DAG는 일시중지, `SAMJEON_MATERIAL_AGENT_ENABLED=false`, 서비스 `enabled=false`, `auto_send=false`, AI 비활성이 기본이다.

## 새 노트북 준비

1. Windows에서 Git, Python 3.12, Telegram Desktop을 설치한다. WSL은 이 서비스의 실행 조건이 아니다. ChatGPT Business Standard 로그인은 설치 작업에 사용할 수 있으나 Telegram 봇의 Codex CLI 인증을 자동으로 대신하지 않는다.
2. GitHub의 `main`을 `C:\airflow_samjeon_agent`에 복제한다. 이후 코드 갱신은 변경 파일이 없는지 확인하고 `git pull --ff-only origin main`만 사용한다. 노트북에서 코드 수정·커밋·push를 하지 않는다.
3. 저장소 루트에서 `scripts/setup_samjeon_material_agent.ps1`을 실행한다. 이 스크립트는 Python 3.12 전용 `.venv`와 최소 의존성을 준비하고 `doctor`를 실행한다. 설정·토큰·스냅샷이 없는 첫 결과는 미준비로 표시되는 것이 정상이다.
4. 실제 서비스 설정은 저장소 밖의 `C:\Local_DB\samjeon_material_agent\service.json`에 둔다. `state_root`는 같은 로컬 폴더 아래로, `snapshot_root`는 `latest.json`과 `snapshots/`가 있는 승인된 v3 게시 루트로 지정한다. 원본 자료와 게시본은 서비스가 읽기 전용으로 사용한다.
5. 전용 Telegram 봇 토큰은 노트북의 서비스 실행 계정 환경변수 `SAMJEON_TELEGRAM_BOT_TOKEN`에 보관한다. 대표님 Telegram 계정의 숫자 사용자·개인 대화 ID만 허용 목록에 넣는다. 토큰과 실제 ID는 Git에 저장하지 않는다.
6. 설정의 `enabled=true`, `auto_send=false`로 대화·PDF를 먼저 시험한다. `doctor`에서 v3 판본과 준비 상태를 확인한 후 `auto_send=true`와 작업 스케줄러 `-Enable`을 적용한다. 노트북은 로그인 상태·전원·네트워크를 유지하고 절전을 해제해야 응답한다.

```powershell
winget install --id Git.Git --exact --source winget
winget install --id Python.Python.3.12 --exact --source winget
winget install --id Telegram.TelegramDesktop --exact --source winget
git clone --branch main https://github.com/doridang001-lab/doridang_airflow.git C:\airflow_samjeon_agent
cd C:\airflow_samjeon_agent
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_samjeon_material_agent.ps1
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
- `doctor`는 외부 연결 없이 설정·스냅샷·글꼴·의존성을 확인한다. `token_present`나 `ai_configured`는 실인증 성공을 뜻하지 않는다.
- dry-run은 `-B`와 함께 실행한다. 디렉터리·SQLite·모델 작업·발송이 발생하지 않아야 한다.

## 대화와 판단

- 아침에는 확인할 점 최대 3개를 대화식으로 설명하고, 새 스냅샷의 새 이상을 추가 안내한다. 자료 기준일과 상태를 항상 표시한다.
- 관측 사실과 선매입·배송일 변경 같은 확인 가설을 구분한다. 재고·폐기 근거 없이 낭비·손실을 확정하지 않는다.
- `/help`, `/status`, `/cancel`, `/summary`, `/compare`, `/anomalies`, `/forecast`, `/staff_meal`, `/evidence`, `/report`를 지원한다.
- 기간은 `YYYY-MM-DD YYYY-MM-DD`; 근거 필터는 `evidence_id=`, `product_key=`, `source=`, `offset=`, `limit=`다. 근거는 최대 500행, 채팅은 앞 20행을 표시한다.
- 자연어·버튼은 같은 조회 계약에 연결한다. 비슷한 품목이 여러 개면 선택 버튼을 제공한다. 선택한 품목은 짧은 후속 질문에서도 유지한다.
- 대화 문맥은 24시간이다. 버튼은 원래 스냅샷과 사용자 소유권에 고정돼 다른 알림의 근거와 섞이지 않는다.
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

## Codex 운영 조건

- 모델은 `gpt-6-astra`, 현재 어댑터는 질문당 1회(상한 2회 이내), 최대 90초다.
- 사용자 설정·규칙 상속을 끄고 별도 작업 폴더, 읽기 전용 sandbox, 도구·플러그인·브라우저 비활성 옵션을 적용한다. API 키는 자식 환경에 전달하지 않는다.
- 출력 스키마는 허용 조회 종류와 날짜만 받는다. 임의 금액·근거·셸 요청은 거부한다. 대화 숫자와 설명은 검증된 계산 결과로 구성한다.
- `ai_verified`는 운영 계정의 인증·무인 실행·도구 차단·추가 사용 통제를 실검증한 뒤 설정한다. CLI 설치 확인이나 자동 테스트만으로 true로 바꾸지 않는다.
- AI 사용 불가·한도·시간 초과·취소 시 정형 대화와 명령 조회를 유지한다. 유료 API로 자동 전환하지 않는다.

## 활성화·복구

1. 품목·마감 검수, 목표 적용일·금액 기준, 승인된 v3 게시 여부를 확인한다. OneDrive 게시 승인은 별도다.
2. 등록된 전용 봇·사용자·대화 ID로 승인된 시험을 수행한다. 기본 설정 예시에는 실제 ID·토큰을 넣지 않는다.
3. 승인된 실행 폴더에서 시작 스크립트를 실행한다. 작업스케줄러 등록 스크립트는 기본 비활성이며 `-Enable`을 명시해야 활성 등록된다.
4. 작업 이름은 `DoridangSamjeonMaterialAgent`, 로그인 시 숨김 시작·단일 인스턴스·실패 재시작이다. PC 종료·절전·로그아웃 중에는 서비스가 중단될 수 있다.
5. 복구 시 해당 서비스·DAG만 중지하고 승인된 기능 변경만 되돌린다. SQLite는 이전 판본을 조용히 초기화하지 않는다.
6. DB 복원 후 자동 발송을 끄고 발송 이력을 대조한다. 원천과 기존 스냅샷을 복구 대상으로 수정하지 않는다.

## 검증

```powershell
.\.venv\Scripts\python.exe -B -X utf8 -m pytest tests/test_samjeon_material_mart.py tests/test_samjeon_material_mart_v3.py tests/test_samjeon_material_service.py tests/test_samjeon_material_codex.py tests/test_samjeon_material_pdf.py tests/test_samjeon_material_agent_dag.py -q -p no:cacheprovider
python scripts/harness_cli.py validate --target .
```

대화→품목→설명 기록→PDF 흐름, 미등록 사용자, 오래된 버튼, 취소, 재시작, 미준비·신규 이상 중복 방지, 메시지 분할, AI 계약·실패, PDF 한글·수치·판본, import부터 dry-run 무저장을 검증한다. 실제 발송·구독 인증·시범 운영 통과는 별도로 기록한다.
