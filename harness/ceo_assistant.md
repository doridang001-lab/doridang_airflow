# 대표님 데이터 비서 운영

## 동작과 검증 범위

`assistant_profile=ceo_data`는 별도 대표님 봇을 위한 선택 모드다. 기존 설정의 기본값은 `legacy`다.
기동·대기·기본 진단·사용법 안내에서 업무 CSV를 읽지 않는다. WSL app-server 연결은 유지하고 모델 추론은 질문 때만 수행한다.
주기적인 데이터 확인, Airflow 작업 큐 수신, 자동 브리핑은 이 모드에서 실행하지 않는다.

데이터 질문은 목록 → 관련 가이드 → 필요한 월·테이블 준비 → SQL 집계 순서로 처리한다.
전체 추세는 요약부터, 특정 메뉴 질문은 해당 테이블부터 읽는다. 모델의 의미 판단이 포함되므로 실제 질문 품질 검증도 필요하다.

2026-10-19 개발 검증에서 기존 서비스 포함 202개 테스트가 통과했고 실제 WSL 통합 9개는 실행하지 않았다.
실제 CEO_DB의 9월·10월 요약과 홀·배달 채널 CSV 6개만 읽어 동일 기간 합계가 일치함을 확인했다.
OneDrive 파일은 변경하지 않았다. 운영 노트북의 실제 로그인·봇 수신·모델 답변은 별도 확인이 필요하다.

## 새 노트북 준비

운영 전 이 기능이 포함된 코드를 별도 노트북 작업 폴더에 배포한다. 기존 second_computer의 봇 토큰을 복사하지 않는다.
아래는 코드가 배포된 폴더에서 실행하는 명령이다. `DataRoot`에는 해당 노트북에 동기화된 실제 경로를 사용한다.

```powershell
.\scripts\setup_ceo_assistant.ps1 -DataRoot '<노트북의 CEO_DB 경로>' -CheckOnly
.\scripts\setup_ceo_assistant.ps1 -DataRoot '<노트북의 CEO_DB 경로>'
```

Python 3.12가 필요하며 설치 스크립트는 전용 `.venv`와 비활성 설정을 준비한다.
기존 설정 파일은 덮어쓰지 않는다. 기본 설정은 `C:\Local_DB\ceo_assistant\service.json`이며 상태·기억·캐시는 같은 폴더 아래 `state`에 둔다.

운영자가 설정할 항목:

- `ceo_data_root`: 실제 CEO_DB 경로. OneDrive 원천은 읽기 전용이다.
- `allowed_users`, `allowed_chats`: 대표님 개인 대화의 숫자 ID.
- `CEO_TELEGRAM_BOT_TOKEN`: 대표님 전용 봇 토큰을 담는 실행 계정의 환경변수. JSON·Git·보고서에 토큰을 저장하지 않는다.
- `wsl_distribution`, `wsl_user`, `wsl_codex_executable`: 노트북에서 확인한 실제 WSL 실행 환경.
- 기존 검증된 Codex 0.158.0, Astra, medium 설정을 사용한다. 실제 Linux 실행 계정의 구독 로그인을 확인한다.
- `enabled`, `ai_enabled`, `ai_verified`: 실연결 검증 뒤 활성화한다. `auto_send`, `daily_briefing_enabled`는 false를 유지한다.

```powershell
$configPath = 'C:\Local_DB\ceo_assistant\service.json'
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $configPath
.\scripts\start_samjeon_material_agent.ps1 -Config $configPath -Background
```

기본 doctor는 자료를 검증하지 않는다. `data_status=미조회`는 정상이며 연결 준비와 자료 준비를 구분한다.
운영 계정으로 대화를 시작해 인사, 이번 달 매출 비교, 원인 후속 질문, 기억 저장·삭제를 실제로 확인한다.
노트북 종료·절전·로그아웃·네트워크 단절 중에는 응답하지 않는다.

## 자동 실행과 복구

```powershell
.\scripts\register_ceo_assistant_task.ps1 -Config $configPath -Enable
Start-ScheduledTask -TaskName 'DoridangCeoAssistant'
```

전용 작업은 `DoridangCeoAssistant`, `DoridangCeoAssistantWatchdog`다. 등록은 실제 연결 진단을 요구한다.
시작·감시는 창 없는 Python으로 동작하며 기존 삼전점 작업을 변경하지 않는다.
watchdog는 로컬 심박과 Telegram 수신 활동만 검사한다.

```powershell
.\scripts\update_ceo_assistant.ps1 -Config $configPath
.\scripts\update_ceo_assistant.ps1 -Config $configPath -Rollback
```

업데이트는 깨끗한 운영 `main`에서 전용 작업들을 중지하고 fast-forward한다. 설정·SQLite·OneDrive는 보존한다.
실패하면 전용 작업을 중지한 상태로 남긴다. 복구는 `ceo-deployment.json`의 이전 커밋으로 코드만 되돌리고 진단 통과 후 기존 활성 작업을 재개한다.
CEO 설정을 지원하지 않는 이전 코드로 복구했을 때에는 서비스를 중지한 상태로 유지한다.

## 새 자료와 토큰 관리

- 데이터 폴더마다 `AGENTS.md`를 둔다. `config/ceo_dataset_guide.example.md`를 사용한다. OneDrive 문서 반영은 승인 후 수행한다.
- CSV 파일명은 `YYYY_MM.csv`, 날짜 열은 `날짜`와 `YYYY-MM-DD`, 인코딩은 UTF-8/BOM이다. 같은 테이블의 월별 열 구성은 동일해야 한다.
- manifest가 있으면 등록된 CSV만 해시 검증 후 읽는다. 없으면 원천 검증 정보가 없다고 표시한다. 선택하지 않은 자료의 오류는 해당 질문을 막지 않는다.
- 파일 변경·해시 불일치·열 변경·기간 부재는 안내하고 숫자를 추측하지 않는다. 실패한 준비 후 이전 자료로 조용히 조회하지 않는다.
- 기본 SQL 출력은 20행·16KiB, 상세는 100행·64KiB다. 집계는 전체 선택 행에서 수행하며 출력 제한과 구분한다.
- 가이드는 관련 부분 3개·6,000자씩 조회한다. 동일 요청의 동일 SQL 결과와 파일 해시별 로컬 SQLite를 재사용한다.
- 12회 전달 완료 응답 후 새 모델 스레드로 이어간다. 최근 3문답의 짧은 맥락과 관련 확정 기준만 승계한다.
- `/기억 이름=기준`, `기억해: 기준 문장`, `/기억목록`, `/기억삭제 이름`을 지원한다. 수정·삭제 후 다음 질문은 새 스레드로 시작해 예전 기준을 제거한다.
- 상태 DB의 `ceo-usage-v1-<update_id>`에는 선택한 CSV 수, SQL 횟수, 캐시 재사용, 전달 문자 수, 응답 시간과 CLI가 제공한 사용량을 기록한다. 사용량이 없으면 추정 토큰 수를 만들지 않는다.

## 개발 검증

```powershell
python -B -X utf8 -m pytest tests/test_ceo_assistant.py tests/test_samjeon_appserver.py tests/test_samjeon_session_memory.py tests/test_samjeon_conversation.py tests/test_samjeon_material_service.py tests/test_samjeon_legacy.py tests/test_samjeon_watchdog.py tests/test_samjeon_wsl.py -q -p no:cacheprovider
```

기동·대기 CSV 미조회, 기간·테이블 선택, 파일 판본, SQL 격리, 출력 제한, 재시작 기억, 12턴 문맥 교체 및 기존 모드 회귀를 검사한다.
실제 WSL·Telegram 시험은 운영 환경이 준비된 뒤 수행한다. 테스트용 가짜 전송기의 성공을 실제 사용자 수신으로 기록하지 않는다.
