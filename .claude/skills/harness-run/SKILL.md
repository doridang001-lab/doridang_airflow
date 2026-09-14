---
name: harness-run
description: |
  Airflow harness의 phase/step 작업을 실행·운영하는 스킬.
  사용자가 "harness 실행", "phase 돌려줘", "step 실행", "execute.py", "하네스 작업",
  "작업 지시서 실행", "하네스 모니터링" 등을 언급할 때 반드시 사용한다.

  핵심 역할: phases/index.json의 pending step을 harness 문서 주입과 함께 순차 실행하고,
  git은 기본적으로 건드리지 않는다.
---

## 구조
- `scripts/execute.py` — phase/index.json의 pending step 순차 실행 + `harness_docs` 문서를 프롬프트에 주입
- `phases/` — phase별 index.json(step 정의, harness_docs 지정)
- `harness/` — DAG별 반복 운영 지식(baemin_macro, coupang_macro, unified_sales_grp, fin_product, review_checklist, common)
- `docs/codex/harness-airflow.md` — harness 가이드

## 실행
```bash
python scripts/execute.py            # pending step 실행 (기본: git checkout/commit/push 안 함)
python scripts/execute.py --git      # 브랜치/커밋까지
python scripts/execute.py --git --push  # push까지 (명시할 때만)
```
- 기본 실행은 git 미변경이 원칙. `--git`/`--push`는 사용자가 명시할 때만 붙인다.

## 절차
1. 대상 phase의 `index.json`에서 pending step과 `harness_docs`(주입 문서) 확인.
2. step의 작업 지시(MD)와 필요한 `harness/*.md`를 먼저 읽는다.
3. `python scripts/execute.py` 실행. 임시 산출물은 루트 금지 → `.tmp/` 또는 지정 경로.
4. 결과를 `Strategy_HarnessMonitoring_01_Dashboard_Dags` 대시보드로 점검:
   - `하네스 설정 점검`의 ERROR/WARN → KPI 오류/차단/지연 → step `MD 보기`로 지시서 확인
   - 컨테이너는 `docker-compose.yaml`에 `./phases`, `./harness`, `./scripts`, `./AGENTS.md` 마운트 필요

## Constraints
- git 자동화는 명시적 플래그 있을 때만. OneDrive 수정은 승인 필요. 응답 한글.
- `$codex-md-improver`는 Airflow DAG가 아니라 작업스케줄러 guidance 감사용 — 자동수정 금지, 리포트 후 승인.
