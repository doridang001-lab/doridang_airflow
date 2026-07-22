# Airflow Harness Guide

## 목적

`harness/`는 Codex가 Airflow DAG 작업을 phase/step 단위로 실행할 때 필요한 도메인별 운영 지침입니다.

DAG 파일 옆에 긴 작업 지침을 두지 않고, 루트 `harness/`에 공통 규칙과 DAG별 규칙을 분리합니다.

`scripts/execute.py`는 `phases/{phase}/index.json`의 pending step을 순차 실행하고, Codex 프롬프트에 공통 규칙과 선택된 harness 문서를 주입합니다.

## 권장 구조

```text
C:\airflow
├── AGENTS.md
├── phases
│   ├── index.json
│   └── <phase-name>
│       ├── index.json
│       └── step0.md
├── scripts
│   ├── execute.py
│   └── test_execute.py
├── harness
│   ├── common.md
│   ├── unified_sales_grp.md
│   ├── baemin_macro.md
│   ├── coupang_macro.md
│   ├── fin_product.md
│   └── review_checklist.md
└── dags
    └── db
        ├── DB_UnifiedSales_Dags.py
        ├── DB_Beamin_Macro_Dags.py
        ├── DB_CoupangMacro_Load_Dags.py
        └── DB_FinProduct_Dags.py
```

## 실행

기본 실행은 git checkout, commit, push를 하지 않습니다.

```powershell
python scripts/execute.py <phase-name>
```

깨끗한 작업트리에서 하네스가 브랜치와 커밋까지 관리하게 하려면 `--git`을 명시합니다.

```powershell
python scripts/execute.py <phase-name> --git
```

push까지 수행하려면 `--git --push`를 함께 사용합니다. `--push`만 단독으로 쓰면 실행기는 에러로 중단합니다.

```powershell
python scripts/execute.py <phase-name> --git --push
```

현재 저장소처럼 작업트리가 더러운 상태에서는 `--git` 실행이 중단됩니다. 이 경우 기본 실행으로 step 상태만 갱신하거나, 기존 변경사항을 먼저 정리한 뒤 `--git`을 사용합니다.

## step에서 사용하는 법

DAG 변경 step은 우선 `dag_targets`에 정확한 DAG 상대 경로를 지정합니다. 실행기는
`harness/registry.json`에서 group guidance와 특수 DAG override 문서를 자동으로 찾습니다.

```json
{
  "step": 0,
  "name": "fix-unified-sales-reprocess",
  "status": "pending",
  "dag_targets": ["dags/db/DB_UnifiedSales_Dags.py"]
}
```

`docs`와 `harness_docs`는 registry가 해결하지 않는 교차 영역 문서를 명시적으로 더할 때 사용합니다.

`phases/{phase}/index.json`의 step에 `harness_docs`를 지정합니다.

```json
{
  "step": 0,
  "name": "fix-unified-sales-reprocess",
  "status": "pending",
  "harness_docs": ["unified_sales_grp"]
}
```

여러 영역을 건드리면 필요한 문서만 추가합니다.

```json
{
  "step": 1,
  "name": "repair-baemin-to-unified-sales",
  "status": "pending",
  "harness_docs": ["baemin_macro", "unified_sales_grp"]
}
```

실행기는 항상 `harness/common.md`와 `harness/review_checklist.md`를 포함하고, DAG별 문서는 `harness_docs`에 지정된 파일만 포함합니다.

`harness_docs`는 문자열 또는 배열을 허용합니다. 값은 `harness/{name}.md`로 해석하며, 경로 traversal을 막기 위해 `/`, `\`, `.`로 시작하는 이름은 사용할 수 없습니다. 지정 문서가 없으면 해당 step은 `blocked`로 기록됩니다.

## phase 파일 규칙

Top-level index:

```json
{
  "phases": [
    {
      "dir": "fix-example-dag",
      "status": "pending"
    }
  ]
}
```

Task index:

```json
{
  "project": "airflow",
  "phase": "fix-example-dag",
  "steps": [
    {
      "step": 0,
      "name": "harness-smoke",
      "status": "pending",
      "harness_docs": []
    }
  ]
}
```

상태값은 `pending`, `completed`, `error`, `blocked`를 사용합니다.

각 step 파일은 다음 섹션을 둡니다.

- `읽어야 할 파일`
- `작업`
- `Acceptance Criteria`
- `검증 절차`
- `금지사항`

## 복구

- `error`: 실패 원인을 확인하고 `error_message`를 해결한 뒤 해당 step의 `status`를 `pending`으로 되돌려 재실행합니다.
- `blocked`: `blocked_reason`의 사용자 개입 사항을 해결한 뒤 해당 step의 `status`를 `pending`으로 되돌려 재실행합니다.
- 기본 실행 중 생성된 `step{N}-output.json`은 실행 결과 추적용입니다.

## 감시 대시보드

하네스 상태는 `Strategy_HarnessMonitoring_01_Dashboard_Dags`가 매일 13:00 KST에 점검합니다.

- 스냅샷: `DASHBOARD_DB/harness_monitoring_snapshot.json`
- HTML: `DASHBOARD_DB/harness_monitoring_dashboard.html`
- 서버 라우트: `/harness-monitoring`
- API 라우트: `/api/harness-monitoring/snapshot`

대시보드는 phase/step 흐름, `error`, `blocked`, 24시간 이상 `pending` 상태인 step과 하네스 설정 진단을 보여줍니다. Telegram 알림은 문제가 있을 때만 발송합니다.

### 감시 대시보드 판정법

1. `하네스 설정 점검`에서 `phases/index.json`, `phase/step 파일`, `harness 문서`, `scripts/execute.py`, `Airflow DAG`, `대시보드 산출물`을 먼저 확인합니다.
2. `ERROR`는 즉시 보완 대상, `WARN`은 운영 참고, `UNAVAILABLE`은 현재 실행 환경에서 조회 불가를 뜻합니다.
3. KPI에서 `오류`, `차단`, `지연`이 0인지 확인합니다. `지연`은 pending 24시간 초과입니다.
4. step 카드의 `MD 보기`를 눌러 해당 `stepN.md`와 연결된 harness 문서를 확인합니다.
5. `phases`가 0개면 `/opt/airflow/phases` 마운트 또는 `phases/index.json` 등록 문제를 먼저 의심합니다.

### Docker 마운트 요구사항

하네스 대시보드 DAG가 컨테이너에서 정상 진단하려면 다음 마운트가 필요합니다.

- `./phases:/opt/airflow/phases`
- `./harness:/opt/airflow/harness`
- `./scripts:/opt/airflow/scripts`
- `./AGENTS.md:/opt/airflow/AGENTS.md:ro`

### 작업스케줄러 기준

`$codex-md-improver`는 Airflow DAG가 아니라 Windows 작업스케줄러의 주기적 guidance 감사에 사용합니다. 작업스케줄러는 읽기 전용 감사 리포트 용도이며, `AGENTS.md`, `CODEX.md`, `CLAUDE.md`를 자동 수정하지 않습니다. 문서 수정은 리포트와 승인 후 별도 작업으로 진행합니다.

주의: `DASHBOARD_DB`가 OneDrive 경로일 수 있으므로, 운영 DAG 실행 또는 산출물 직접 생성은 OneDrive 수정 승인 후 진행합니다.

## 검증

DAG inventory와 registry 검증:

```powershell
python scripts/harness_cli.py inventory --target .
python scripts/harness_cli.py validate --target .
```

새 group, 특수 DAG override, phase 골격은 `python scripts/harness_cli.py scaffold --help`의
하위 명령을 사용하며 기존 파일은 덮어쓰지 않습니다.

실행기 변경 후 최소 검증:

```powershell
python -X utf8 -m pytest scripts/test_execute.py --basetemp C:\tmp\harness_pytest
```

문서 인코딩 검증:

```powershell
python -X utf8 -c "from pathlib import Path; [p.read_text(encoding='utf-8') for p in [Path('AGENTS.md'), Path('docs/codex/harness-airflow.md')]]; print('utf8 ok')"
```

## 문서 추가 기준

- DAG 또는 DAG 그룹마다 반복되는 운영 지식이 있으면 `harness/{domain}.md`를 추가합니다.
- 단발성 버그 분석이나 구현 계획은 `prd_codex/` 또는 phase step에 둡니다.
- 문서에는 대상 DAG, 관련 pipeline, 작업 기준, 검증 명령, 금지사항을 짧게 적습니다.
