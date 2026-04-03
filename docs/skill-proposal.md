# 스킬 신규 제안서 (10개)

> 기존 스킬: data-report, gitpush, output-style, to-compact, prd
> 기존 에이전트: dag-team (planner/coder/tester), web-scraper-auto-scroll
> 분석 기반: DAG 27개, 파이프라인 26개, Extract 15개, Load 8개, Utility 13개

---

## 전체 요약

| # | 스킬명 | 트리거 예시 | 핵심 가치 |
|---|--------|------------|----------|
| 1 | `crawl-builder` | "배민 크롤러 만들어" | extract/ 크롤러 보일러플레이트 자동 생성 |
| 2 | `pipeline-debug` | "SMD_03 에러 확인해" | XCom/Parquet 추적 + 에러 진단 |
| 3 | `dag-log` | "07 Alert 로그 봐줘" | logs/ 최근 실행 분석 + 실패 원인 |
| 4 | `data-check` | "주문 CSV 검증해" | 스키마/중복/누락 자동 검증 |
| 5 | `db-query` | "배민 테이블 조회해" | PostgreSQL 쿼리 생성 + 실행 |
| 6 | `docker-ops` | "도커 재시작" | docker-compose 빌드/재시작/로그 |
| 7 | `onedrive-browse` | "OneDrive 파일 찾아줘" | Repository 폴더 탐색/검색 |
| 8 | `etl-status` | "ETL 현황 알려줘" | DAG 실행 상태 종합 대시보드 |
| 9 | `script-gen` | "매출 분석 스크립트 만들어" | scripts/ 표준 구조 스캐폴딩 |
| 10 | `llm-classify` | "리뷰 분류 파이프라인 만들어" | Ollama/Qwen 텍스트 분류 구조화 |

---

## 1. crawl-builder

### 필요 근거
- `modules/extract/`에 15개 크롤러 존재, Selenium 셋업/로그인/네비게이트/파싱 패턴이 반복
- 새 크롤러 생성 시 매번 undetected_chromedriver, Docker 호환 옵션, 에러 핸들링을 수동 작성

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/crawl-builder/SKILL.md 스킬을 만들어줘.

이름: crawl-builder
트리거: "크롤러 만들어", "크롤링 모듈 생성", "새 사이트 수집"
역할: modules/extract/ 에 새 크롤링 모듈을 프로젝트 표준에 맞게 생성

## 절차:
1. 사용자에게 질문: 대상 사이트 URL, 로그인 필요 여부, 수집 데이터 종류
2. 기존 크롤러 참조 (croling_beamin.py, posfeed_order_download.py 등)
3. 표준 구조로 modules/extract/{name}.py 생성:
   - undetected_chromedriver (Docker: --headless --no-sandbox)
   - CHROME_BIN, CHROMEDRIVER_PATH 환경변수 대응
   - account.py의 account_df 활용 (로그인 필요 시)
   - try/finally로 driver.quit() 보장
   - logger = logging.getLogger(__name__)
   - 반환: DataFrame
4. Docker 환경 호환 확인 (Dockerfile의 Chrome 경로)
5. 생성된 파일 경로와 함수 시그니처 요약 출력

참조 파일:
- modules/extract/croling_beamin.py (Selenium 패턴)
- modules/extract/posfeed_order_download.py (로그인+다운로드)
- modules/extract/croling_toorder.py (undetected_chromedriver)
- modules/transform/utility/account.py (계정 관리)
- Dockerfile (Chrome 설치 경로)
```

---

## 2. pipeline-debug

### 필요 근거
- XCom 체인이 최대 9단계 (01_Extract → 09_FlowReport)
- 중간 Parquet 파일 확인, XCom 키 추적, 에러 원인 파악을 수동으로 하면 시간 소모

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/pipeline-debug/SKILL.md 스킬을 만들어줘.

이름: pipeline-debug
트리거: "파이프라인 디버깅", "SMD 에러", "XCom 확인", "파이프라인 오류 분석"
역할: SMD_*/SMP_* 파이프라인 에러를 진단하고 XCom 데이터를 추적

## 절차:
1. 사용자가 지정한 파이프라인 파일 (SMD_XX 또는 SMP_XX) 읽기
2. 해당 파이프라인의 XCom 키 체인 추적
   - xcom_push/xcom_pull 호출 매핑
   - 입력 task_id → output_xcom_key 의존성 그래프 출력
3. TEMP_DIR (paths.py) 에서 관련 Parquet 파일 탐색
   - 최근 파일 찾아서 df.info(), df.head() 출력
4. logs/ 폴더에서 해당 DAG의 최근 로그 확인
   - 에러 트레이스백 추출 + 원인 분석
5. 진단 결과 요약:
   - 실패 지점 (어떤 task에서 실패)
   - 데이터 상태 (입력 데이터 존재 여부, 컬럼 누락)
   - 수정 제안

참조:
- modules/transform/utility/paths.py (TEMP_DIR, ONEDRIVE_DB 경로)
- modules/transform/utility/io.py (load_files, preprocess_df)
- dags/ 폴더의 DAG 정의 (task 의존성)
- logs/ 폴더 구조 (dag_id=XXX/)
```

---

## 3. dag-log

### 필요 근거
- logs/ 폴더에 30개 이상 DAG 로그 존재
- 특정 DAG 실패 시 로그 파일을 수동으로 찾아 읽는 과정이 반복

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/dag-log/SKILL.md 스킬을 만들어줘.

이름: dag-log
트리거: "로그 확인", "DAG 로그", "실패 로그 봐줘", "에러 로그"
역할: logs/ 폴더에서 특정 DAG의 최근 실행 로그를 분석

## 절차:
1. 사용자 입력 파싱: DAG명 또는 키워드 (예: "07 Alert", "Orders 03")
2. logs/ 하위 폴더에서 매칭되는 dag_id= 디렉토리 탐색
3. 해당 DAG의 가장 최근 실행 로그 파일 찾기 (날짜 기준 정렬)
4. 로그 분석:
   - SUCCESS/FAILED 상태 확인
   - ERROR/CRITICAL 레벨 로그 추출
   - 트레이스백이 있으면 전체 출력
   - 실행 시간 (시작~종료) 계산
5. 요약 출력:
   - 상태: SUCCESS/FAILED
   - 실행 시간: X분 Y초
   - 에러 메시지 (있으면)
   - 마지막 성공 실행 일시

로그 경로 규칙: logs/dag_id={DAG_ID}/run_id=.../task_id=.../attempt=N.log
```

---

## 4. data-check

### 필요 근거
- CSV/Excel 파일 스키마 불일치, 중복, 날짜 형식 혼재가 빈번
- SMD_02 (review), SMD_03 (transform)에서 반복되는 검증 로직을 범용 스킬로 독립

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/data-check/SKILL.md 스킬을 만들어줘.

이름: data-check
트리거: "데이터 검증", "CSV 확인", "파일 검증", "데이터 품질", "스키마 체크"
역할: CSV/Excel/Parquet 파일의 품질을 자동 검증하고 이슈 보고

## 절차:
1. 대상 파일 경로 확인 (사용자 지정 또는 OneDrive/로컬 경로)
2. 파일 로드 (pandas: csv/xlsx/parquet 자동 감지)
3. 기본 검증:
   - df.shape, df.dtypes, df.info()
   - 컬럼별 null 비율
   - 중복 행 수 (전체 / dedup_key 기준)
4. 심화 검증:
   - 날짜 컬럼: Excel 직렬값(45000) 혼재 여부 (parse_mixed_date 참조)
   - 금액 컬럼: 음수/0/이상치 탐지
   - 문자열 컬럼: 공백/특수문자 비율
   - PK 컬럼 유니크 여부
5. 결과 출력 (테이블 형식):
   | 항목 | 결과 | 비고 |
   |------|------|------|
   | 행 수 | 1,234 | |
   | 중복 | 5건 | store_name+주문번호 기준 |
   | null | 주문일시 2건 | |
   | 날짜 형식 | 혼재 | Excel serial 3건 발견 |

참조:
- modules/transform/utility/io.py (load_files, preprocess_df)
- modules/transform/pipelines/sales/SMD_03_sales_orders_transform.py (parse_mixed_date)
- modules/transform/utility/paths.py (경로 상수)
```

---

## 5. db-query

### 필요 근거
- PostgreSQL (doridangdb) 조회/저장이 빈번하지만 매번 연결 정보와 쿼리를 수동 작성
- db.py, extract_db.py의 래퍼 함수 활용을 표준화

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/db-query/SKILL.md 스킬을 만들어줘.

이름: db-query
트리거: "DB 조회", "테이블 확인", "PostgreSQL", "쿼리 실행", "DB 데이터"
역할: PostgreSQL 테이블 조회, 스키마 확인, 간단한 분석 쿼리 실행

## 절차:
1. 사용자 요청 파싱: 테이블명, 조건, 원하는 데이터
2. docs/db-schema.md 참조하여 테이블/컬럼 정보 확인
3. Windows 환경에서 Python 스크립트로 실행:
   - modules/transform/utility/db.py의 read_table() 활용
   - 또는 modules/extract/extract_db.py의 래퍼 활용
4. 쿼리 생성:
   - read_table(table, schema, columns, where, order_by) 형태
   - 복잡한 쿼리는 raw SQL + sqlalchemy engine 직접 사용
5. 결과 출력:
   - df.shape, df.head(10), 요약 통계
   - 대량 데이터는 샘플만 출력하고 전체는 Parquet/CSV로 저장

DB 연결 정보: docs/db-schema.md 참조
주의: SELECT만 허용, DROP/DELETE/UPDATE는 사용자 확인 필수

참조:
- modules/transform/utility/db.py
- modules/extract/extract_db.py
- docs/db-schema.md
```

---

## 6. docker-ops

### 필요 근거
- Docker 기반 Airflow 운영 (docker-compose.yaml, Dockerfile)
- 빌드/재시작/로그 확인을 WSL에서 수행하는 패턴이 반복

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/docker-ops/SKILL.md 스킬을 만들어줘.

이름: docker-ops
트리거: "도커 재시작", "docker 빌드", "컨테이너 로그", "도커 상태", "airflow 재시작"
역할: Docker Compose 기반 Airflow 환경의 빌드/재시작/로그 확인

## 절차:
1. 사용자 요청 분류:
   - status: docker compose ps
   - build: docker compose build --no-cache
   - restart: docker compose down && docker compose up -d
   - logs: docker compose logs --tail=100 {service}
   - exec: docker compose exec {service} {command}
2. Windows 환경에서는 WSL 경유 실행 안내
   - wsl -e bash -c "cd /mnt/c/airflow && docker compose ..."
3. 실행 후 결과 요약:
   - 컨테이너 상태 (Up/Exit)
   - 에러 로그 추출 (있으면)
   - 헬스체크 결과

파일 참조:
- docker-compose.yaml (서비스 정의)
- Dockerfile (이미지 빌드 설정)
- requirements.txt (Python 패키지)

주의사항:
- build 시 Chrome/ChromeDriver 설치 확인
- volume 마운트 경로 확인 (Windows ↔ Docker)
- 기존 데이터 보존 확인 후 down 실행
```

---

## 7. onedrive-browse

### 필요 근거
- OneDrive Repository가 ETL의 주요 데이터 소스/싱크
- 파일 경로가 Windows/Docker/WSL에서 다르며, 파일 검색이 빈번

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/onedrive-browse/SKILL.md 스킬을 만들어줘.

이름: onedrive-browse
트리거: "OneDrive 파일", "Repository 확인", "수집 데이터 목록", "파일 찾아줘"
역할: OneDrive Repository 폴더의 파일 탐색, 검색, 요약

## 절차:
1. paths.py에서 현재 환경의 OneDrive 경로 확인
   - ONEDRIVE_DB (Repository 루트)
   - COLLECT_DB (수집 데이터)
   - ANALYTICS_DB (분석 데이터)
   - REPORT_SALES_DB (리포트)
2. 사용자 요청에 따라:
   - 목록: 특정 폴더의 파일/폴더 트리 출력
   - 검색: 파일명 패턴으로 검색 (glob)
   - 요약: CSV/Excel 파일의 행 수, 컬럼 수, 최종 수정일
   - 최근: 최근 N일 내 수정된 파일 목록
3. 경로 변환 안내:
   - Windows: C:\Users\민준\OneDrive - 주식회사 도리당\Repository
   - Docker: /opt/airflow/Repository
   - WSL: /mnt/c/Users/민준/OneDrive\ -\ 주식회사\ 도리당/Repository

참조:
- modules/transform/utility/paths.py (경로 상수 + resolve 함수)
```

---

## 8. etl-status

### 필요 근거
- 27개 DAG의 실행 상태를 한눈에 파악할 방법이 없음
- 아침에 전체 ETL 현황을 빠르게 확인하는 시나리오가 빈번

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/etl-status/SKILL.md 스킬을 만들어줘.

이름: etl-status
트리거: "ETL 현황", "DAG 상태", "파이프라인 현황", "오늘 실행 결과"
역할: 전체 DAG의 최근 실행 상태를 종합 대시보드로 출력

## 절차:
1. logs/ 폴더 스캔: 모든 dag_id= 디렉토리 목록
2. 각 DAG별 최근 실행 확인:
   - 최근 run_id (날짜 추출)
   - 마지막 task의 성공/실패 여부
   - 실행 소요 시간
3. 대시보드 테이블 출력:
   | DAG | 최근 실행 | 상태 | 소요시간 |
   |-----|----------|------|---------|
   | Sales_Orders_01_Extract | 2026-04-03 | ✅ | 3m 20s |
   | Sales_Orders_07_Alert | 2026-04-03 | ❌ | 1m 05s |
   | Strategy_ToOrderVoc_01 | 2026-04-02 | ✅ | 5m 10s |
4. 실패 DAG가 있으면 에러 요약 추가
5. 선택적으로 schedule.py의 스케줄과 비교하여 미실행 DAG 표시

참조:
- logs/ 폴더 구조
- modules/transform/utility/schedule.py (스케줄 상수)
- dags/ 폴더 (DAG 목록)
```

---

## 9. script-gen

### 필요 근거
- scripts/ 에 표준 구조 (_base.py의 run_script, save_summary)가 있지만 매번 보일러플레이트 작성
- 분석 스크립트 생성 시 paths.py, io.py 등 utility 연결을 자동화

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/script-gen/SKILL.md 스킬을 만들어줘.

이름: script-gen
트리거: "분석 스크립트 만들어", "스크립트 생성", "데이터 검증 스크립트"
역할: scripts/ 표준 구조에 맞는 분석/검증 스크립트를 자동 생성

## 절차:
1. 사용자에게 질문: 분석 목적, 대상 데이터, 필요한 필터
2. scripts/ 표준 구조로 파일 생성:
   ```python
   import sys, logging, argparse
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).parent.parent))
   from scripts._base import run_script, save_summary

   logger = logging.getLogger(__name__)

   def main(args) -> dict:
       # paths.py 상수 활용
       # 비즈니스 로직
       return {"meta": {...}, "summary": {...}, "stats": {...}}

   if __name__ == "__main__":
       parser = argparse.ArgumentParser()
       # 필터 옵션 추가
       args = parser.parse_args()
       run_script(lambda: main(args), "script_name")
   ```
3. 필요한 utility import 자동 추가 (paths, io, db 등)
4. 결과 저장 경로 확인: scripts/output/{name}_{timestamp}.json
5. 실행 명령어 안내

참조:
- scripts/_base.py (run_script, save_summary)
- modules/transform/utility/paths.py
- modules/transform/utility/io.py
```

---

## 10. llm-classify

### 필요 근거
- Strategy 파이프라인에서 Ollama(SMP_crawling_toorder_voc_02) + Qwen(qwen_client.py) 기반 텍스트 분류가 반복
- 새 분류 파이프라인 생성 시 카테고리 정의, 프롬프트 설계, 배치 처리 패턴을 표준화

### 프롬프트 가이드

```
아래 내용으로 .claude/skills/llm-classify/SKILL.md 스킬을 만들어줘.

이름: llm-classify
트리거: "리뷰 분류", "LLM 분류", "텍스트 분류 파이프라인", "Ollama 분류"
역할: Ollama/Qwen 기반 텍스트 분류 파이프라인을 표준 구조로 생성

## 절차:
1. 사용자에게 질문:
   - 분류 대상 (리뷰, VOC, CS 문의 등)
   - 카테고리 목록 (SECONDARY_CATEGORIES 형태)
   - 사용할 모델 (ollama 로컬 / qwen_client)
2. 기존 분류 파이프라인 참조:
   - SMP_crawling_toorder_voc_02_trans.py 패턴
   - SECONDARY_CATEGORIES 상수 정의
   - ollama.generate() 호출 구조
3. 파이프라인 모듈 생성:
   - 카테고리 상수 정의
   - 분류 프롬프트 설계 (few-shot 포함)
   - 배치 처리 (tqdm 진행률)
   - 분류 실패 시 "기타" 폴백
   - 결과 DataFrame 반환
4. 선택적으로 이메일 알림 연동 (mailer.py)
5. 테스트 데이터 샘플로 분류 정확도 확인

참조:
- modules/transform/pipelines/strategy/SMP_crawling_toorder_voc_02_trans.py
- modules/transform/pipelines/strategy/SMP_crawling_toorder_voc_03_trans_alert.py
- modules/transform/utility/qwen_client.py
- modules/transform/utility/mailer.py
```

---

## 우선순위 권장

| 순위 | 스킬 | 이유 |
|------|------|------|
| 🔴 1 | `pipeline-debug` | 매일 운영 중 가장 빈번한 디버깅 작업 |
| 🔴 2 | `dag-log` | 실패 원인 파악이 즉시 필요 |
| 🟡 3 | `data-check` | 데이터 품질 이슈가 파이프라인 실패의 주원인 |
| 🟡 4 | `etl-status` | 아침 모니터링 루틴에 필수 |
| 🟡 5 | `crawl-builder` | 새 크롤러 추가 빈도 높음 |
| 🟢 6 | `db-query` | DB 조회 작업 표준화 |
| 🟢 7 | `script-gen` | 분석 요청 시 빠른 스캐폴딩 |
| 🟢 8 | `docker-ops` | 배포/재시작 자동화 |
| 🟢 9 | `onedrive-browse` | 파일 탐색 편의 |
| 🟢 10 | `llm-classify` | 새 분류 파이프라인은 비정기적 |
