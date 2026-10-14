# 삼전점 기존 v1 자료 조회 복구

기준일: 2026-10-14 KST. 운영 책임은 `AGENTS.md`와 [서비스 안내](samjeon_material_agent.md), 인증·실행은 [WSL TRD](samjeon_telegram_wsl_trd.md)를 따른다.

## 목적과 확인 범위

기존 `samjeon-mart-v1` 게시본을 검증해 잠정 관측값으로 조회한다. WSL·Ubuntu·Node·Codex 재설치나 재로그인, snapshot_root 변경은 하지 않는다. v3 마트 생성 코드·검증·가이드는 변경하지 않는다.

개발 PC에서 실제 게시본을 읽기 전용으로 확인했다. 자료 기간은 2026-08-16~2026-10-11이고 일곱 파일 해시와 판본·기간이 일치한다. 구매 분류는 미분류와 현장 직원식대다. 이 값을 원재료·포장재로 추정 분류하지 않는다. 매출·구매를 원표에서 직접 합산한 값과 새 조회 결과를 대조하며, 전후 파일 해시 일치도 확인한다.

운영자가 보고한 PR #26 배포 상태는 `enabled=true`, `ai_enabled=true`, `ai_verified=true`, `auto_send=false`이며 WSL 구독 인증 실검증을 통과했다. 이 개발 작업은 운영 노트북에 접속하거나 서비스·설정·Telegram을 변경하지 않는다. 새 코드의 실제 배포·실모델·화면 수신 완료는 별도로 기록한다.

## 게시 형식과 검증 계약

- 포인터와 manifest가 모두 `schema_version=3`이면 기존 v3 로더를 사용한다. 한쪽에만 버전 표시가 있거나 값이 다르면 차단한다.
- 두 파일에 schema_version이 없고 manifest.rule_version이 정확히 `samjeon-mart-v1`인 경우만 v1 후보로 인정한다. context의 판본·날짜·기간·rule_version도 일치해야 한다.
- 필수 파일은 agent_context.json, sales_daily.parquet, purchase_daily.parquet, purchase_lines.parquet, item_daily.parquet, quality_daily.parquet, review_candidates.csv다. 선언된 모든 파일의 해시를 검사하며 검사한 바이트로 표를 읽는다.
- 포인터 경로는 등록 루트의 snapshots 하위 manifest.json으로, 파일 경로는 해당 스냅샷 안으로 제한한다. 알 수 없는 형식·해시 불일치·필수 컬럼 누락·날짜 충돌은 차단한다.
- 계산 금액은 정수 원 단위이며 null·비수치·소수 금액은 오류다. 반품·차감의 음수는 보존한다. 거래가 없으면 관측 거래 없음으로 표시하고 확정 0원으로 바꾸지 않는다.
- 근거 ID 중복·품목 식별자 충돌·집계 키 중복을 차단하고 구매 상세와 일별·품목별 금액·행수를 대사한다.
- 내부 형식은 `legacy_v1`, 상태는 잠정이다. 원래 context의 원가율·목표·이상·예측을 가져오거나 없는 v3 필드를 만들지 않는다.

## 조회와 대화 계약

| 조회 | 계약 |
|---|---|
| summary | 매출·분류별 구매·직원식대·미분류·주요 구매 품목·품질 상태 |
| items | 해당 기간 구매 관측액 순 상위 10개; 규격·원천을 구분 |
| compare | 지정 기간과 같은 길이의 바로 앞 기간; 관측액 차이만 표시 |
| staff_meal | 명시된 현장 직원식대 분류만 집계 |
| evidence | 원래 근거 ID가 있는 구매 상세; 매출 상세 근거는 없음 |
| status | 일반 대화·잠정 자료 조회·v3 전체 준비를 구분 |

전체 구매액에는 직원식대가 포함된다. 별도로 표시한 직원식대를 다시 더하지 않는다. 미분류를 원재료·포장재로 취급하지 않는다. 구매액 순위와 증가 비교를 구분하며 수량 순위·비율·목표 초과·손실 판단은 제공하지 않는다.

숫자 월·연월은 원래 Telegram 수신일의 한국 연도로 해석한다. 월 요청만 월초~월말 요청 범위와 실제 보유 기간의 교집합을 분리해 저장한다. 겹치지 않는 월은 기간 부족이다. 오늘·어제·명시 날짜 범위·최근 7일은 축소하지 않는다. 기존 `이번 달`은 월초~어제이며 명시적인 월 전체 요청만 월 전체 규칙을 사용한다. 잘못된 월·기간 충돌은 확인 질문으로 처리한다.

자동 세션 전환 뒤에도 검증된 분석 기간·품목·직전 조회가 이어진다. “쉽게 다시 설명해줘”는 직전 조회를 같은 판본과 기간에서 다시 계산해 짧은 잠정 관측 문장으로 안내한다. 과거 답변 검색은 당시 자료 기준일을 표시하고 최신 재계산과 구분한다.

현재 10월 분석 첫 답변에는 다음 문장을 포함한다.

> 2026년 10월 자료는 현재 10월 1일~11일까지 있습니다. 해당 기간의 잠정 관측값으로 분석하며, 10월 전체 실적은 아닙니다.

실제 기간은 기존 분석 레코드에, 요청 기간·자료 형식·지원 기능·품목과 페이지 문맥은 `legacy-view-v1-*` meta JSON에 저장한다. SQLite 테이블 구조를 바꾸지 않는다. 후속 품목과 근거는 같은 판본·실제 기간을 이어받고 새 날짜 질문은 수신일로 다시 계산한다.

근거 API는 최대 500행, 채팅은 한 페이지 20행이다. 이전·다음 버튼은 표시한 행 기준으로 이동하며 서버에 저장된 필터와 개인 대화 소유권을 검사한다. 품목 선택 뒤에는 원래 요청한 조회를 이어간다. “그 구매 근거”는 선택 품목, 또는 직전 목록에 표시한 품목들을 사용하며 문맥이 없으면 기간 전체 구매임을 명시한다.

v1의 PDF·고급 이상 탐지·예측은 명령·모델 계획·기존 버튼·직접 PDF 호출에서 모두 차단한다. 모델 계획에는 지원 기능만 제공하며 Python에서도 검사한다. 모델에는 계산한 사실과 허용된 근거 필드만 전달한다. 계획·설명이 실패하면 단순 월 분석은 정형 요약으로 답하고 재시도하지 않는다. 도구 실행·임의 수치·물리 경로·원본 오류 노출 차단과 구독 인증을 유지한다.

## 진단과 오류

`data_format`, `data_query_ready`, `available_date_from`, `available_date_to`, `supported_queries`를 추가한다. v1이면 data_query_ready=true이지만 data_ready=false, ready=false다. doctor와 /help·/status·health.json에서 대화·자료 조회·전체 자료 준비를 구분한다.

- `doctor --require-data-query-ready`: conversation_ready와 data_query_ready가 모두 true여야 성공.
- `doctor --require-ready`: 기존 v3 전체 운영 조건. v1로 통과시키지 않음.
- `verify-ai --require-ready`: 가상 자료와 대화의 모델 실검증. 자료 전체 준비와 별개.
- 게시본 없음, 미지원 형식, 파일·판본 검증 실패, 기간 부족, 기능 미지원을 구분해 안내한다. 검증을 생략하거나 이전 정상 게시본으로 조용히 대체하지 않는다.
- 자동 자료 알림은 v3에서만 실행한다. v1 조회 복구만으로 로그인 작업을 등록하지 않는다.

## 검증과 노트북 적용

개발 테스트는 실제 v1 구조를 재현한 임시 자료만 생성한다. 정상 합산·음수·미분류·직원식대·기간 비교·다중 규격·원래 수신일·월 부분 기간·근거 21/101/501행·해시/컬럼/판본/경로 오류·미지원 기능·취소·중복·소유권·정형 대체를 검증한다. 기존 v3·Windows/WSL·구독 인증·무도구·PDF·배포/복구 회귀도 실행한다.

개발 PC에서 feature PR로 게시하고 해당 PR 승인 후 병합한다. 노트북에서는 다음 순서를 따른다.

1. 현재와 대상 커밋·배포 기록을 확인하고 기존 문서를 보존한다. 삼전점 서비스 트리만 종료하고 해당 자동 시작 작업은 비활성화한다. 다른 Python 작업은 종료하지 않는다.
2. 인스턴스와 SQLite 잠금 해제를 확인하고 설정·SQLite 보존 대조를 준비한다. 깨끗한 main에서 아래 대화 전용 갱신을 수행한다.

```powershell
$config = 'C:\Local_DB\samjeon_material_agent\service.json'
.\scripts\update_samjeon_material_agent.ps1 -Config $config -ConversationOnly
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $config --require-conversation-deployable
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service doctor --config $config --require-data-query-ready
.\.venv\Scripts\python.exe -B -X utf8 -m modules.transform.pipelines.db.samjeon_material_service verify-ai --config $config --require-ready
.\scripts\start_samjeon_material_agent.ps1 -Config $config -Background
```

3. 각 명령의 성공을 확인한 뒤 다음 단계로 진행한다. 모델 호출은 앞서 사용자가 확인한 추가 크레딧·자동 충전 비활성 조건과 구독 인증을 유지한다. 활성화 세 설정은 true, auto_send=false인지 확인한다. 명령에 토큰·실제 ID·설정 파일 전문을 출력하지 않는다.
4. health 갱신과 단일 서비스 트리를 확인한다. “송파삼전점 10월을 분석해줘” → “어떤 품목을 많이 샀어?” → “그 구매 근거 보여줘” → “어제 매출은?” → “10월 전체 확정 실적은?” → “안녕”을 새 메시지로 시험한다.
5. 계산값·부분 기간·잠정 표시·기간 부족 안내를 확인하고 전송 원장의 sent와 사용자 화면 수신을 각각 기록한다. 과거 실패 질문은 자동 재실행하지 않는다.

실패 시 서비스를 중지하고 deployment.json을 사용해 업데이트 스크립트 -Rollback으로 코드만 복구한다. DB·설정·인증·OneDrive는 초기화하지 않는다. 이전 코드는 v1을 읽지 못하므로 롤백 자체가 조회 복구를 뜻하지 않는다. 로그인 자동 실행은 등록하지 않으며 재부팅 후 수동 시작 조건은 유지한다. 10월 12일 이후 자료와 v3 전체 준비는 별도 작업이다.
