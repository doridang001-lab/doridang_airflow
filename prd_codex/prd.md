# fin_product 상품 표준화 마트 최종 기획서

## 요약

- `unified_sales`의 POS별 상품명 편차를 줄이기 위해 신규 표준화 마스터 `fin_product_map.csv`를 만든다.
- v1은 전체 매장이 아니라 `송파삼전점`만 대상으로 제한한다.
- 대상 매장은 스크립트 맨 위 상수 `TARGET_STORES = ["송파삼전점"]`로 관리한다.
- 대상 행이 없으면 전체 매장으로 fallback하지 않는다.
- LLM은 Claude/Anthropic이 아니라 현재 운영 코드와 동일하게 내부 Ollama `gpt-oss` 계열을 사용한다.
- 신규 마스터는 `approved` 행만 downstream LEFT JOIN에 사용하고, LLM 결과는 항상 `pending`으로 적재해 사람 검토를 거친다.

## 주요 변경

- `modules/transform/utility/paths.py`에 신규 상수 2개를 추가한다.
  - `FIN_PRODUCT_MAP_CSV_PATH = MART_DB / "fin_product" / "fin_product_map.csv"`
  - `FIN_PRODUCT_MAP_JSON_PATH = MART_DB / "fin_product" / "fin_product_map.json"`
- 신규 스크립트 2개를 추가한다.
  - `scripts/product_map_migrate.py`: 송파삼전점 판매 항목 기준으로 초기 map 후보 생성
  - `scripts/product_map_llm.py`: 송파삼전점 미매핑 상품명을 내부 LLM으로 표준명 후보 분류
- `fin_product_map.csv` 컬럼은 아래 순서로 고정한다.
  - `store`, `source`, `item_name`, `표준_메뉴명`, `수동분류`, `review_status`, `classified_by`, `updated_at`
- v1 중복 기준은 `store + source + item_name`이다.
- `수동분류` 허용값은 기존 운영과 호환되도록 `메인`, `1인`, `사이드`, `기타`만 사용한다.

## 구현 방침

- `product_map_migrate.py`
  - 파일 상단에 `TARGET_STORES = ["송파삼전점"]`를 둔다.
- `unified_sales_grp` parquet에서 `store`, `source`, `item_name`을 읽고 대상 매장만 필터한다.
- 기존 `fin_product_grp.csv`, `fin_product_mart.csv`, `fin_product_posfeed_whitelist.csv`는 참고하지 않는다.
- 초기 `표준_메뉴명`은 `item_name`과 동일하게 두고, `수동분류`는 빈값/pending으로 둔다.
  - `--dry-run`은 CSV를 쓰지 않고 summary만 저장한다.
- `product_map_llm.py`
  - 파일 상단에 `TARGET_STORES = ["송파삼전점"]`를 둔다.
  - parquet에서 `store`, `source`, `item_name`을 읽고 대상 매장만 필터한다.
  - 대상 행이 0건이면 summary를 남기고 종료한다.
  - 전체 매장 fallback, store 필터 무시, store 컬럼 누락 상태의 계속 진행은 금지한다.
  - 내부 LLM은 `ollama.Client(host="http://host.docker.internal:11434")`를 사용한다.
  - 모델 후보는 `gpt-oss:20b`, `gpt-oss:latest`, `gpt-oss`, `qwen2.5:7b`, `qwen2.5:latest` 순서로 둔다.
  - LLM 결과는 `review_status=pending`, `classified_by=llm`으로 append한다.
  - `--dry-run`은 LLM 호출과 CSV/JSON 저장을 하지 않는다.
- downstream JOIN 패턴
- `review_status == "approved"` 행만 사용한다.
  - 조인 키는 v1 기준 `store + source + item_name`이다.
- DAG는 `DB_FinProduct_Map_Dags`로 자동 스케줄 연결한다.
- 스케줄은 `DB_FIN_PRODUCT_MAP_TIME = "50 8 * * *"`이며, 매일 08:50 KST에 실행한다.
- 운영 기본값은 `dry_run=False`로 실제 `fin_product_map.csv/json`을 갱신한다.
- 수동 테스트 실행은 DAG conf에 `{"dry_run": true}`를 넣어 파일 쓰기를 막는다.

## 테스트 계획

- 경로 상수 import 확인
  - `python -c "from modules.transform.utility.paths import FIN_PRODUCT_MAP_CSV_PATH, FIN_PRODUCT_MAP_JSON_PATH; print(FIN_PRODUCT_MAP_CSV_PATH)"`
- 문법 확인
  - `python -m py_compile scripts\product_map_migrate.py scripts\product_map_llm.py modules\transform\utility\paths.py`
- 마이그레이션 dry-run
  - `python scripts\product_map_migrate.py --dry-run`
  - 기대: CSV 저장 없음, `target_rows`, `approved`, `pending`, `duplicate_keys` summary 저장
- LLM dry-run
  - `python scripts\product_map_llm.py --dry-run --limit 20`
  - 기대: LLM 호출 없음, CSV/JSON 저장 없음, 대상/미매핑 summary 저장
- 대상 매장 필터 확인
  - `scan_target_items()` 결과의 `store` 고유값이 `["송파삼전점"]`만 포함돼야 한다.

## 인수 조건

- 스크립트 상단에서 `TARGET_STORES = ["송파삼전점"]`를 바로 확인하고 수정할 수 있다.
- `store` 필터가 적용되지 않는 경우 실행이 실패하거나 대상 없음으로 종료된다.
- 대상 행이 0건이면 전체 매장으로 확대하지 않는다.
- `fin_product_map.csv`는 `store + source + item_name` 기준 중복이 없어야 한다.
- LLM은 Claude/Anthropic이 아니라 내부 Ollama `gpt-oss` 계열을 사용한다.
- `--dry-run`은 OneDrive CSV/JSON을 쓰지 않는다.
- OneDrive 쓰기 작업은 별도 승인 후 수행한다.
- DAG 자동 실행 시 OneDrive 저장 대상은 `fin_product_map.csv`, `fin_product_map.json`이다.

## 현재 검증 결과

- `송파삼전점` 대상 고유 `store + source + item_name` 항목은 385건이다.
- `product_map_migrate.py --dry-run` 기준 385건 모두 기존 상품테이블 참고 없이 `pending`이다.
- `product_map_llm.py --dry-run --limit 20`은 LLM 호출과 CSV/JSON 저장 없이 정상 종료했다.
