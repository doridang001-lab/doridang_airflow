# 송파삼전점 MenuHierarchy 채널별 검증 보강 기록

- 날짜: 2026-08-07
- 대상 DAG: `dags/db/DB_MenuHierarchy_Test_Dags.py`
- 대상 파이프라인: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`
- 산출 폴더: `new_classification_orders`

## 목적

`DB_MenuHierarchy_Test_Dags` 실행 후 채널별 전체 주문서에서 다음 문제가 없는지 확인하고, 실제 문제 주문건수를 반영해 정규화 룰을 보강했다.

- 사이드/옵션/맛 선택값이 `main`으로 올라가는 문제
- `순살` 메뉴가 `뼈닭`으로 정규화되는 문제
- `1인 순살` 메뉴가 `중/대` 사이즈로 남는 문제
- 비닭 메뉴에 `[중]`, `뼈` 같은 옵션 신호가 붙어 닭사용량이 계산되는 문제
- 채널별 주문번호 충돌로 다른 날짜/채널 주문이 같은 주문그룹처럼 묶이는 문제

## 적용한 룰

### 1. 옵션형 main 차단

`기본`, `기본맛`, `순한맛`, `중간맛`, `매운맛`, `아주매운맛`, `보통맛`, `맛 선택`, `[중] 2인`, `뼈`, `순살`, `추가/변경/선택`류 단독 항목은 상품표에 `메인`으로 남아 있어도 주문 계층에서는 `main`으로 승격하지 않도록 했다.

이 룰로 `기본` 또는 맛 선택값이 대표 메뉴처럼 잡히는 케이스를 차단한다.

### 2. 채널별 주문그룹 키 보강

계층 조정과 parent lookup을 단순 `order_id`가 아니라 가능한 경우 `source + sale_date + order_id` 기준으로 처리하도록 보강했다.

이유:

- 채널별로 같은 `order_id`가 재사용될 수 있다.
- 같은 날짜가 아닌 주문끼리 섞이면 main/option 부모가 잘못 연결된다.
- 주문 적재방식 자체를 바꾸지 않고도 계층 분류 안정성을 확보할 수 있다.

결론: 현재 문제는 주문서 적재방식 변경이 아니라 계층/정규화 룰 보강으로 해결했다.

### 3. 고정 닭속성 메뉴명 우선 보정

메인 상품명 또는 표준메뉴명 자체가 닭유형과 사이즈를 고정하면 채널과 무관하게 메뉴명 기준을 우선한다.

- `1인 순살`, `한그릇 순살` -> `순살 / 1인 / 0.3`
- `2인 순살`, `순살 ... 2인이상` -> `순살 / 2인 / 0.6`
- `1인 백숙`, `1인 닭한마리` -> `뼈닭 / 1인 / 0.5`

적용 이유:

- 쿠팡수동 `300004060`, `300004063`, `300004064`, `300004065`, `300004066` 계열에서 `[1인] 순살 닭도리탕 (밥포함) 1인분`이 과거 수기 기본값 때문에 `순살/중/0.8`로 남는 문제가 있었다.
- OKPOS `10030814`처럼 상품 자체가 `1인 순살 닭도리탕(공깃밥 포함)`으로 고정돼 있는데, 주문 옵션의 `[중]`, `뼈` 신호가 덮어써서 `뼈닭/중`으로 흔들리는 문제가 있었다.

### 4. 옵션형 가변 메뉴는 주문 옵션 우선 유지

`도리당 닭도리탕`, `묵은지 닭도리탕`, `백도리탕`처럼 상품명 자체가 특정 닭유형/사이즈를 고정하지 않는 메뉴는 `[중] 2인`, `[대] 3인`, `뼈`, `순살` 같은 주문 옵션 조합을 우선한다.

이유:

- 같은 메인명 아래에서 사이즈/닭유형 옵션이 실제 사용량을 결정하는 상품이 있다.
- 모든 메뉴를 고정 메뉴명 룰로 강제하면 가변형 닭도리탕 사용량이 오히려 틀어진다.

### 5. 비닭 메뉴 닭사용량 제거

`미나리 비빔칼국수`, `흑미 공기밥`처럼 닭 메뉴 토큰이 없는 주문그룹은 `[중]`, `뼈` 같은 옵션명이 붙어도 `닭유형`, `사이즈`, `사용용량`을 비우도록 했다.

이유:

- OKPOS에서 비닭 메뉴에 다른 옵션이 붙은 주문그룹이 닭사용량으로 집계될 수 있었다.
- 닭사용량 분석에서는 닭 메뉴 그룹만 포함해야 한다.
- 분석용 `사용용량`, `재료사용량`, `표준중량사용량`은 main 행에만 남긴다. option/side 행은 단순 합산 시 닭사용량이 과대계산되지 않도록 닭 사용량을 비운다.

### 6. 닭 메뉴 토큰 및 사이즈 추론 보강

다음 패턴을 닭 메뉴/사이즈 추론에 추가했다.

- `닭도리`
- `1인 순살 닭도리 정식` -> `순살 / 1인 / 0.3`
- `순살 닭도리 정식(2인이상)` -> `순살 / 2인 / 0.6`

## 수동 입력 파일 사용법

### 상품 자체 분류가 잘못된 경우

수정 파일:

- `new_fin_product_map_review_input.csv`

수정 컬럼:

- `수동분류_edit`: `메인`, `사이드`, `옵션`, `리뷰`, `음료`, `제외` 등
- `표준_메뉴명_edit`: 표준 메뉴명
- `닭유형_manual`: `뼈닭` 또는 `순살`
- `사이즈_manual`: `1인`, `2인`, `소`, `중`, `대`
- `닭사용량_manual`: 예: `0.3`, `0.5`, `0.8`, `1`, `1.2`, `1.5`
- `검수유무`: 반영하려면 `1`

수정 후 `DB_MenuHierarchy_Test_Dags`를 다시 실행한다.

### 주문 옵션조합별 정규화가 잘못된 경우

수정 파일:

- `13_manager_input.csv`

키 컬럼:

- `source`
- `brand`
- `store`
- `std_menu_name`
- `옵션조합`

수정 컬럼:

- `닭유형_manual`
- `사이즈_manual`
- `닭사용량_manual`
- `수익률_manual`
- 필요한 경우 `미나리사용량_manual`, `우거지사용량_manual` 같은 `*사용량_manual` 컬럼

수정 후 `DB_MenuHierarchy_Test_Dags`를 다시 실행한다.

### 옵션 라인 자체의 재료사용량이 필요한 경우

수정 파일:

- `16_option_material_input.csv`

예:

- `미나리사용량_manual`
- `우거지사용량_manual`
- `분모자사용량_manual`

수정 후 재실행하면 `12_orders_left.csv`의 `옵션재료사용량`에 반영된다.

### 표준중량/재고 loss 비교가 필요한 경우

수정 파일:

- `17_menu_weight_input.csv`

수정 컬럼:

- `닭사용량_manual`
- `우거지사용량_manual`
- `순살추가사용량_manual`
- 기타 `*사용량_manual`

수정 후 재실행하면 `18_material_usage_summary.csv`에 메뉴별 예상 사용량이 집계된다.

## 검증 결과

기존 실제 산출 기준:

- `12_orders_left.csv`: 30,842행
- 주문 수: 5,519건
- `19_validation_issues.csv`: 0행
- `04_product_gap.csv`: 0행

2026-08-07 재계획 반영 dry-run 기준:

- `12_orders_left.csv`: 30,873행
- `19_validation_issues.csv`: 0행
- `20_classification_audit.csv`: 120행
- audit 구성: `main_attr_unstable` 56행, `review_chicken_attr_blank` 53행, `profit_zero_placeholder` 11행
- OneDrive 산출물은 승인 전이라 실제 덮어쓰지 않았다.

채널별 계층 분포:

| source | main | option | side | fee |
| --- | ---: | ---: | ---: | ---: |
| okpos | 3,926 | 16,211 | 260 | 0 |
| 배민수동 | 834 | 4,661 | 22 | 0 |
| 쿠팡수동 | 855 | 3,855 | 146 | 0 |
| posfeed | 10 | 49 | 4 | 9 |

의미 오류 점검:

| 점검 항목 | 결과 |
| --- | ---: |
| 옵션형 항목이 main으로 남은 건 | 0 |
| 순살 메뉴가 뼈닭으로 정규화된 건 | 0 |
| 1인 순살 메뉴가 1인 외 사이즈로 남은 건 | 0 |
| 확인된 비닭 main에 닭사용량이 붙은 건 | 0 |

## 검증 명령

```powershell
python -m pytest tests/test_menu_hierarchy_no_model_classification.py tests/test_menu_hierarchy_dag_defaults.py -q --basetemp .tmp\pytest-menu-hierarchy
python -X utf8 -m py_compile modules/transform/pipelines/db/DB_MenuHierarchy_Test.py tests/test_menu_hierarchy_no_model_classification.py
python -X utf8 -c "from modules.transform.pipelines.db.DB_MenuHierarchy_Test import build_orders; print(build_orders(None))"
docker compose exec -T airflow-scheduler airflow tasks test DB_MenuHierarchy_Test_Dags build_orders 2026-08-07
```

실행 결과:

- pytest: 58건 통과
- py_compile: 통과
- 파일 쓰기 캡처 dry-run `build_orders(None)`: 성공, `19_validation_issues.csv` 0행
- 실제 `build_orders(None)`와 Airflow task test는 OneDrive 산출물 덮어쓰기 승인 후 재실행 필요

## 남은 주의사항

닭사용량 분석은 main 행 기준으로 가능하다. `12_orders_left.csv`를 단순 합산할 때는 option/side 행의 닭사용량이 비어 있어야 중복 집계가 나지 않는다.

수익분석은 `수익률_manual` 입력값에 의존한다. 현재 일부 `수익률_manual=0` 값은 검증 통과용 임시값이므로, 실제 수익분석에 쓰려면 `13_manager_input.csv` 또는 `new_fin_product_map_review_input.csv`에서 실제 마진율로 교체해야 한다.

모든 채널의 주문서 규칙이 완전히 같지 않으므로, 상품명 자체가 속성을 고정하는 메뉴와 주문 옵션으로 속성이 바뀌는 메뉴를 분리해서 봐야 한다.
