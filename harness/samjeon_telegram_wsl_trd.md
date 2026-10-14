# 삼전점 Telegram–WSL 백그라운드 Codex 연결

## 운영 구조와 책임

- 개발 PC의 `C:\airflow_samjeon_agent`에서 수정·테스트하고 PR로 배포한다. 운영 노트북에서는 병합된 `main`만 받는다.
- 운영 노트북의 Windows 서비스가 기존 Telegram 봇을 polling한다. 같은 노트북의 WSL에 로그인한 Linux Codex를 요청마다 숨김 실행하고 답변을 기존 개인 대화에 반환한다.
- 모델은 `gpt-6-astra`, 추론 옵션은 `model_reasoning_effort="medium"`, 검증 대상 CLI는 `0.158.0`이다. `middle`이라는 옵션은 사용하지 않는다.
- 서비스가 백그라운드로 상주하고 모델 프로세스는 요청마다 종료한다. 최근 10턴·24시간 문맥은 기존 Windows SQLite에서 전달한다. 터미널 화면이나 대화 세션 ID를 공유하지 않는다.
- 원격 개발 PC 중계, 새 봇, API 키 인증과 유료 API 자동 전환을 추가하지 않는다. WSL 실패 시 Windows Codex로 자동 전환하지 않는다.

## 실행 환경 준비

운영 Windows 계정에서 WSL 배포판 목록을 확인한다. WSL이 없다면 관리자 PowerShell에서 설치하고 필요한 재부팅을 완료한다. 재부팅과 로그인은 운영자가 수행한다.

```powershell
wsl --list --verbose
# WSL이 없는 경우에만 관리자 PowerShell에서 수행
wsl --install -d Ubuntu
```

운영에 사용할 배포판과 Linux 사용자를 정한다. 아래 `Ubuntu`, `samjeon`, `/usr/local/bin/codex`는 예시이며 실제 설치와 일치시켜야 한다. 해당 사용자에게 Linux Python 3와 Linux Node.js LTS 및 `@openai/codex@0.158.0`을 준비한다. Linux 사용자에게 설치한 Node와 Codex가 비로그인 셸에서도 실행되어야 한다. NVM 설치라면 Codex와 Node가 있는 실제 절대경로를 사용한다.

```powershell
wsl -d Ubuntu -u samjeon --exec /usr/bin/python3 --version
wsl -d Ubuntu -u samjeon --exec /usr/local/bin/codex --version
wsl -d Ubuntu -u samjeon --exec /usr/local/bin/codex login
wsl -d Ubuntu -u samjeon --exec /usr/local/bin/codex login status
```

로그인 상태는 `ChatGPT`여야 한다. Windows CLI 로그인은 WSL 인증을 증명하지 않는다. 실행기는 Linux 사용자의 홈 아래 `.codex` 인증만 사용하며 Windows `CODEX_HOME`과 `WSLENV`를 전달하지 않는다. `/mnt/c/...`의 Windows npm shim, `.exe`, `.cmd`를 Linux Codex로 지정하지 않는다. 배포판 전체를 종료하거나 다른 봇을 변경하지 않는다.

신규 설치는 `setup_samjeon_material_agent.ps1 -CodexBackend wsl`을 사용한다. Windows Python 환경과 비활성 예시 설정을 준비하며 WSL이나 Linux 패키지·로그인은 자동 설치하지 않는다. 기존 Windows 방식을 명시하려면 `-CodexBackend windows`를 사용한다. 기존 설정은 어느 모드에서도 덮어쓰지 않는다.

## 기존 운영 설정 전환

운영 설정은 `C:\Local_DB\samjeon_material_agent\service.json`을 유지한다. 먼저 삼전점 서비스와 해당 자동 실행 작업을 중지·비활성화한다. 기존 코드 갱신은 하네스의 `-ConversationOnly` 절차로 진행한다. 새 코드 배포 후 아래 명령을 수행한다.

```powershell
$config = 'C:\Local_DB\samjeon_material_agent\service.json'
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service configure-wsl --config $config --distribution Ubuntu --user samjeon --codex-executable /usr/local/bin/codex --dry-run
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service configure-wsl --config $config --distribution Ubuntu --user samjeon --codex-executable /usr/local/bin/codex
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $config --require-conversation-deployable
```

`configure-wsl`은 서비스 파일 잠금을 확인하고 다음 항목만 원자적으로 변경한다. dry-run은 파일·디렉터리를 생성하지 않는다. OneDrive 설정 파일은 거부한다.

| 항목 | 의미 |
| --- | --- |
| `codex_backend` | `wsl`; 필드가 없는 구버전 설정은 `windows`로 읽는다 |
| `wsl_distribution` | WSL에 등록된 배포판 이름 |
| `wsl_user` | 해당 배포판의 실행·로그인 사용자 |
| `wsl_codex_executable` | WSL 내부 Linux Codex 절대경로 |
| `model_reasoning_effort` | `medium` |
| `enabled`, `ai_enabled`, `ai_verified`, `auto_send` | 전환 시 모두 `false`; 실검증 후 별도 활성화 |

봇 토큰 환경변수 이름, 실제 사용자·대화 ID, 수신자, 자료 경로, 상태 폴더, SQLite와 배포 기록은 보존한다. 토큰·실제 ID·설정 전문을 출력하지 않는다. 예시는 `config/samjeon_material_agent.wsl.example.json`을 참고한다.

## 실행·출력·취소 계약

- Windows `wsl_transport`가 지정 배포판·사용자의 Python으로 `wsl_runner`를 실행한다. 셸 문자열을 조립하지 않으며 저장소 경로는 `wslpath`로 변환한다.
- 표준입력 첫 줄은 요청 ID, 동작(`diagnose` 또는 `invoke`), CLI 경로, 출력 스키마, 지시문, 질문, 문맥을 담은 UTF-8 JSON이다. 최대 256 KB다. 질문·비밀값을 프로세스 인자에 넣지 않는다.
- 응답은 동일 요청 ID의 JSON 한 건이며 최대 64 KiB다. 진단 결과 또는 검증할 답변과 안전한 오류 분류·이벤트 종류만 반환한다. CLI stdout/stderr 원문은 전달하지 않는다.
- Linux 임시 작업 폴더에서 기존 Codex 어댑터를 사용한다. 사용자 설정·규칙 상속을 끄고 읽기 전용 sandbox와 기존 도구 비활성 옵션을 유지한다. Linux 실행기에는 별도 pandas/PDF 패키지가 필요 없다.
- 모델 실행은 최대 90초다. 입력 쓰기도 별도 스레드에서 수행하므로 CLI가 stdin을 읽지 않아도 취소·시간 제한이 작동한다.
- Windows는 요청 동안 stdin을 열어 둔다. `/cancel` 또는 부모 종료로 EOF가 발생하면 Linux 실행기가 해당 요청을 취소하고 Codex의 프로세스 그룹을 종료한다. 다른 WSL 프로세스나 배포판은 종료하지 않는다.
- Windows는 취소 후 실행기 정리를 최대 15초 기다린다. WSL 제어 자체가 멈추면 호스트 프로세스를 종료한다. Linux 실행기의 자체 시간 제한은 남은 모델 실행을 제한한다.
- 일반 대화 실패 시 기존 이용 불가 안내를 반환한다. 실패·취소·전송 불명 답변은 성공한 대화 기억에 추가하지 않는다.

## 진단과 오류 재현

`doctor`는 모델과 Telegram을 호출하지 않는다. WSL 존재·배포판·사용자/Python·경로 변환·Linux CLI·지원 버전·ChatGPT 로그인을 검사한다. 결과의 `codex.backend`는 `wsl`, `reasoning_effort`는 `medium`이어야 한다. Linux에 실제 모델 접근 권한이 있는지는 실검증으로 확인한다.

CLI 호출에는 `-c suppress_unstable_features_warning=true`를 명시한다. CLI 0.158.0의 Code Mode 비활성 시작 알림은 첫 `turn.started` 이전의 정확한 `item.completed`/`error` 형식과 전체 문구가 일치할 때만 허용한다. 시스템 설정 파일의 알림 억제에 의존하지 않는다. 도구 비활성 옵션은 유지한다.

최상위 `error`, `turn.failed`, 다른 오류 항목은 `model_failed` 또는 인증·사용량 오류로 반환한다. 실제 도구 실행은 `unexpected_tool`, 미확인 이벤트는 `unexpected_event`로 차단한다. 종료 코드 0·정상 턴 완료·유효한 최종 JSON을 모두 요구하므로 답변 파일이 있어도 실패를 성공으로 바꾸지 않는다. 알려진 이벤트 종류만 `event_type`으로 기록하고 그 외 값은 `unknown`으로 바꾼다. `verify-ai`의 단계별 `checks`는 유지하며 원본 CLI 오류 전문은 저장하지 않는다.

### 2026-10-14 운영 노트북 인계 반영

운영자가 Ubuntu 26.04.1 LTS, 사용자 `samjeon`, Python 3.14.4, Node.js 24.21.0, `/opt/samjeon-node/bin/codex` 0.158.0과 ChatGPT 구독 로그인을 확인했다. WSL 전환과 `doctor --require-conversation-deployable`도 통과했다. 추가 크레딧·자동 충전 모두 비활성이라고 확인했으며 계정별 사용량 변화는 운영자가 확인한다.

허용된 개인 대화의 질문 수신과 정형 연결 시험 답변 발송은 확인됐다. 이는 AI 자동 문답 성공이나 사용자 화면 수신 확인을 의미하지 않는다. 시작 알림 때문에 `verify-ai`가 실패하여 세 활성화 설정과 `auto_send`는 모두 false인 상태로 인계됐다.

이 노트북은 환경 설치와 `configure-wsl`을 반복하지 않는다. 새 변경이 main에 병합되면 노트북의 `gitpull-agent` 대화 전용 절차(저장소의 `update_samjeon_material_agent.ps1 -ConversationOnly`)로 갱신하고 아래 실검증·활성화·수신 확인을 이어간다. 기존 설정·SQLite·인증·배포 이력과 OneDrive 자료는 보존한다.

## 실검증과 활성화

1. Business 관리 화면에서 추가 크레딧·자동 충전 비활성을 확인한다. CLI 옵션만으로 추가 과금 차단이 증명되지 않는다. 확인 전에는 아래 실모델 명령을 실행하지 않는다.
2. 운영 계정에서 `verify-ai --config $config --require-ready`를 실행한다. 가상 자료 계획·설명·일반 대화·후속 기억이 모두 통과해야 한다. 설정은 자동 변경하지 않는다.
3. `enabled`, `ai_enabled`, `ai_verified`만 `true`로 설정한다. `auto_send=false`를 유지하고 `doctor`의 `conversation_ready=true`를 확인한다.
4. 아래 시작 명령으로 숨김 서비스를 실행한다. 시작 요청 성공은 서비스 정상 동작의 증명이 아니므로 `doctor`, 상태 파일과 실제 수신을 확인한다.

```powershell
.\scripts\start_samjeon_material_agent.ps1 -Config $config -Background
```

5. 허용된 개인 대화에서 `안녕` → `아까 말한 내용 다시 설명해줘` → `어제 매출은?`을 보낸다. Bot API 전송 성공과 대표님 화면 수신을 각각 기록한다.
6. v3가 없으면 마지막 답변은 아래 문장과 정확히 같아야 한다. 일반 대화는 v3 없이 가능해야 한다.

> 현재 검증된 삼전점 자료가 준비되지 않아 매출·원재료 수치를 답할 수 없습니다. 준비되면 다시 질문해 주세요.

승인된 v3 동기화와 전체 `doctor --require-ready`, 매출·근거·후속 질문·PDF 판본 검증 전에는 로그인 자동 실행 작업을 등록하지 않는다. 노트북 종료·절전·로그아웃 중에는 응답할 수 없다.

## 개발 검증과 복구

기존 삼전점 전체 테스트와 `tests/test_samjeon_wsl.py`를 실행한다. 실제 WSL 통합 테스트는 `SAMJEON_TEST_WSL_DISTRIBUTION`에 개발용 배포판을 명시하면 실행되며, 임시 가짜 CLI만 사용한다. 실모델·API·Telegram 호출은 없다.

```powershell
$env:SAMJEON_TEST_WSL_DISTRIBUTION = 'UbuntuCodex'
.\.venv\Scripts\python.exe -B -X utf8 -m pytest tests/test_samjeon_wsl.py -q -p no:cacheprovider
```

실패 시 서비스와 자동 실행을 중지하고 기존 배포 기록으로 코드만 복구한다. WSL 필드를 모르는 구버전은 보존된 설정을 읽지 못할 수 있으므로 복구 후 서비스 중지를 유지한다. DB 초기화, 인증 초기화, Windows 자동 전환, OneDrive 수정으로 해결하지 않는다. 재배포 시 설정을 읽을 수 있는 새 버전에서 진단부터 다시 수행한다.
