# 기존 텔레그램 봇의 데이터 확장과 노트북 이전

대화 학습·향후 공유 패키지 준비는 [비서 패키지 운영 안내](samjeon_assistant_packages.md)를 따른다. 실행 코드 쓰기는 개선 초안 저장으로 전환하며 기존 파일 조회는 유지한다.

## 하나의 봇과 기존 설정

기존 저장소의 `modules/transform/pipelines/db` 서비스를 계속 사용한다.
기존 봇 토큰·대화방·허용 사용자·로컬 상태 DB·WSL 연결을 이어받는다. 새 코드 폴더나 별도 봇을 만들지 않는다.
파일명에 남은 `ceo`는 데이터 조회 기능의 내부 이름이다.

기존 로컬 `service.json`에 `ceo_data_root`를 추가한다. 값은 노트북에 동기화된 실제 CEO_DB 경로다.
JSON에서는 `C:/Users/.../data/mart/CEO_DB`처럼 슬래시를 사용한다.
`state_root`, `token_env`, `allowed_users`, `allowed_chats`, WSL 설정은 유지한다.
`conversation_engine=appserver`, `auto_send=false`, `daily_briefing_enabled=false`가 필요하다.
`assistant_profile`은 추가할 필요 없다. 이전 `ceo_data` 값은 호환을 위해 허용한다.
루트가 없는 기존 설정은 기존 데이터 연결을 사용한다.

## 기존 폴더에서 설치와 진단

아래 설정 경로는 기본 예시다. 실제 사용 중인 설정 경로가 다르면 그 경로를 지정한다.

```powershell
$configPath = 'C:\Local_DB\samjeon_material_agent\service.json'
.\scripts\setup_samjeon_material_agent.ps1 -Config $configPath -CheckOnly
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $configPath --require-ready
```

처음 노트북을 준비할 때는 기존 설정을 먼저 옮긴 뒤 설치 스크립트를 `-CheckOnly` 없이 실행한다.
설치 스크립트는 기존 설정을 덮어쓰지 않는다. WSL Codex 로그인은 노트북 실행 계정에서 확인한다.
기본 진단은 CSV를 열지 않는다. `data_status=미조회`는 정상이며 `data_root_exists`로 경로를 확인한다.
실제 데이터 접근과 질문 품질은 전환 후 대화에서 확인한다.

## second_computer에서 노트북으로 이전

1. 노트북에 코드를 받고 기존 설정·WSL·Python 환경을 준비한다. 아직 봇을 실행하지 않는다.
2. second_computer에서 `DoridangSamjeonMaterialWatchdog`를 먼저 비활성화·중지하고 `DoridangSamjeonMaterialAgent`도 비활성화·중지한다. 직접 실행한 프로세스도 종료하고 서비스 잠금 해제를 확인한다.
3. 중지된 기존 설정의 `state_root` 전체를 노트북 상태 위치로 복사한다. `state.sqlite3`와 남아 있는 `-wal`, `-shm`도 함께 보존한다. 노트북에 기존 상태가 있으면 덮어쓰지 말고 사용할 기록을 먼저 확인한다.
4. 기존 봇 토큰을 노트북 실행 계정의 기존 `token_env`에 설정한다. Git·문서에 토큰을 기록하지 않는다. 데이터 경로·WSL 사용자 등 컴퓨터별 설정만 노트북에 맞춘다.
5. 연결 진단 후 기존 실행 스크립트로 서비스 하나를 시작한다. 대화·기억·수신 offset·발송 이력을 같은 상태 DB에서 이어간다. 모델 스레드는 최초 질문에 새로 열고 전달 완료 기록을 제한적으로 승계한다.
6. 기존 텔레그램 대화방에서 인사, 이번 달/전월 동일 기간 비교, 후속 질문, 기억 조회·삭제를 확인한다. 동일 메시지에 중복 응답이 없어야 한다.
7. 검증 후 기존 등록 스크립트로 자동 실행을 활성화한다. 이전 컴퓨터의 해당 작업은 계속 비활성 상태로 둔다.

```powershell
.\scripts\start_samjeon_material_agent.ps1 -Config $configPath -Background
# 수동 실행을 종료한 뒤 자동 실행으로 전환
.\scripts\register_samjeon_material_agent_task.ps1 -Config $configPath -Enable
Start-ScheduledTask -TaskName 'DoridangSamjeonMaterialAgent'
```

노트북 검증이 끝날 때까지 이전 컴퓨터의 상태 원본을 보존한다. 이후 이전 컴퓨터로 복귀하려면 노트북 서비스를 먼저 중지하고 최신 상태를 역으로 이전한다.
PR #30의 별도 CEO 작업을 이미 등록했다면 그 작업도 중지·비활성화하고 기존 작업만 실행한다. 사용 중인 설정이나 기록을 자동 삭제하지 않는다.

## 데이터와 코드 갱신

- CEO_DB 아래 데이터 폴더·월별 CSV·가이드를 추가/수정한다. 다음 관련 질문에서 새 목록과 판본을 확인한다. 재시작·재설치는 필요 없다.
- 시작·대기·일반 대화·도움말·상태 확인은 업무 CSV를 읽지 않는다. 목록은 요청 시 메타데이터만 확인한다.
- 데이터 질문은 목록 → 관련 가이드 → 선택 기간 요약 → 필요한 상세 자료 순서다. 집계는 SQL에서 수행한다.
- CSV는 `YYYY_MM.csv`, 날짜 열은 `날짜`, 값은 `YYYY-MM-DD`, 인코딩은 UTF-8/BOM이다. 가이드 양식은 `config/ceo_dataset_guide.example.md`를 참고한다.
- manifest가 있으면 선택 파일의 해시를 검증한다. 없으면 원천 검증 정보가 없다고 표시한다. 무관한 자료 오류로 다른 질문을 막지 않는다.
- 선택 파일·가이드 변경 시 캐시를 갱신한다. 동기화 중 변경·스키마 오류·누락을 안내하고 과거 수치를 최신 결과로 답하지 않는다.
- SQL 출력은 기본 20행·16KiB, 상세 100행·64KiB다. 가이드는 관련 부분 최대 3개·6,000자다. 전체 선택 행의 집계와 출력 제한은 구분한다.
- 전달 완료 응답 12회 후 모델 문맥을 교체한다. 최근 3문답·관련 확정 기준·제한된 기존 선호와 요약을 승계한다.
- `/기억 이름=기준`, `기억해: 기준 문장`, `/기억목록`, `/기억삭제 이름`을 지원한다. 기존 지식 조회·허용된 작업 공간 기능도 같은 봇에서 사용한다.
- 원천 데이터와 OneDrive는 서비스가 수정하지 않는다. 토큰 사용량은 CLI가 제공한 경우만 기록한다.

코드는 같은 저장소의 깨끗한 `main`에서 갱신한다. 기존 스크립트가 서비스와 감시 작업을 함께 중지하고 `git pull --ff-only` 후 진단한다.

```powershell
.\scripts\update_samjeon_material_agent.ps1 -Config $configPath
.\scripts\update_samjeon_material_agent.ps1 -Config $configPath -Rollback
```

성공하면 이전에 활성화돼 있던 작업을 복원한다. 실패하면 둘 다 중지 상태로 남기며 설정·대화·OneDrive를 보존한다.
`-ConversationOnly`는 기존 의미대로 코드만 갱신하고 자동 실행을 재개하지 않는다.
이 기능을 지원하지 않는 코드로 복구해 진단이 실패하면 봇을 재개하지 않는다.

## 검증 범위

자동 검증은 기존 연결 회귀, 시작·대기 미조회, 기존 설정에 루트만 추가한 전환, 자료·가이드 갱신, 기억·수신 이력 유지와 도구 변경 후 맥락 승계를 확인한다.
실제 노트북 이전·WSL 로그인·Telegram 수신은 해당 컴퓨터에서 확인해야 한다. 가짜 전송기 검증을 실제 수신으로 보고하지 않는다.
