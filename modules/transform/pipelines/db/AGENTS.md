# DB pipeline

먼저 @C:\airflow\modules\transform\CLAUDE.md
DB DAG는 @C:\airflow\dags\db\AGENTS.md

`DB_UnifiedSales_common.py`의 공통 스키마/저장 규칙을 우선합니다.

## 메뉴계층 전용 검수표

- `new_fin_product_map_review_input.csv`는 송파삼전점 `DB_MenuHierarchy_Test` 전용 검수표입니다.
- 기존 `fin_product_map_review_input.csv`는 `DB_FinProduct_Map_Dags` 운영 파일이므로 메뉴계층 정규화 때문에 직접 바꾸지 않습니다.
- 메뉴계층 lookup은 기존 review input을 먼저 읽고, `new_fin_product_map_review_input.csv`가 있으면 같은 `store, source, brand, item_id` 키를 새 파일 값으로 덮습니다.
- `build_new_fin_product_map_review_input(..., dry_run=False)`는 mart의 새 검수표를 저장하므로 OneDrive 수정 승인 후에만 실행합니다.

## 수기 전용 컬럼

`fin_product_map_review_input.csv`와 `new_fin_product_map_review_input.csv`의 닭 사용량 수기 컬럼은 사람이 엑셀에서 직접 입력하는 값이며, LLM/DAG가 자동으로 쓰거나 보정하지 않습니다.

| 컬럼 | 허용값 | 의미 |
|---|---|---|
| `닭유형_manual` | `뼈닭`, `순살`, 빈칸 | 빈칸은 닭 미사용 |
| `사이즈_manual` | `소`, `중`, `대`, `1인`, `2인`, 빈칸 | 메뉴/옵션 사이즈 |
| `닭사용량_manual` | 숫자 소수점, 빈칸 | 주문 1개당 닭 마리수 |
| `수익률_manual` | `20%` 형태, 빈칸 | 주문/메뉴 그룹 수익률 |

입력 위치:
- 사이즈가 옵션 행으로 분리된 메뉴는 옵션 행에만 입력하고 메인 행은 빈칸으로 둡니다.
- 사이즈가 메뉴명에 고정된 메뉴는 해당 메인/1인 행에 입력합니다.
- 리뷰, 음료, 주류, 토핑 등 닭 미사용 행은 3개 컬럼을 모두 빈칸으로 둡니다.
- `13_manager_input.csv`의 `옵션조합=(메뉴기본값)` 행은 메뉴 전체 fallback입니다. 옵션조합별 행이 비어 있을 때만 이 행의 수기값을 적용합니다.
- `line_role=side`는 음료·사이드·리뷰·주류가 메인처럼 잡힌 경우의 강등값입니다. 닭 계산에서는 제외하지만, 수익률 수기 입력과 `추정수익` 계산 대상에는 남깁니다.
- 음료·사이드 수익률은 `13_manager_input.csv`에서 해당 `std_menu_name`의 옵션조합별 행 또는 `(메뉴기본값)` 행 `수익률_manual`에 입력합니다.

환산 기준:
- 뼈닭: 소 0.5, 중 1.0, 대 1.5
- 순살: 소 0.4, 중 0.8, 대 1.2, 1인 0.3, 2인 0.6

금지:
- `MAP_COLUMNS`, `JOIN_COLUMNS`, `RECENTLY_COLUMNS`, few-shot `required`에 수기 컬럼을 추가하지 않습니다.
- LLM 분류 루프와 DAG는 이 컬럼에 쓰지 않으며, 검증은 경고만 남기고 사람 입력값을 자동 수정하지 않습니다.
