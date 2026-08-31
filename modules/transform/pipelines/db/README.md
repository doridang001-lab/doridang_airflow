# DB_MenuHierarchy_Test 작업 가이드

이 문서는 다음 에이전트가 `DB_MenuHierarchy_Test.py`의 홀 주문 닭유형/사이즈/수익률 문제를 빠르게 진단하고 수정하기 위한 실행 가이드다.

## 기본 원칙

- OneDrive 산출물(`data/mart/new_classification_orders`)을 수정하거나 재생성할 때는 먼저 사용자 승인을 받는다.
- 단건 오분류는 전체 `build_orders()`부터 돌리지 않는다. 전체 재생성은 약 10분 이상 걸릴 수 있으므로 `_debug/latest/12_orders_left.csv`와 함수 단위 재현으로 먼저 원인을 고정한다.
- 한글 값 필터링은 PowerShell 문자열 인자에 직접 싣지 말고 `python -X utf8` 내부에서 읽고 처리한다.
- 수기 입력값은 자동 삭제하지 않는다. 새 산출물에 현재 데이터 기준 행이 없어도 수기값이 있는 기존 행은 workbook에 보존해야 한다.

## 주요 파일

- 핵심 로직: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`
- 회귀 테스트: `tests/test_menu_hierarchy_no_model_classification.py`
- DAG: `dags/db/DB_MenuHierarchy_Test_Dags.py`
- 운영 산출물: OneDrive `data/mart/new_classification_orders`
- 빠른 확인용 산출물: OneDrive `data/mart/new_classification_orders/_debug/latest`
- 운영 변경 기록: `prd_codex/update_log.md`

## 주문그룹 키 주의

OKPOS 홀 주문은 `order_id`가 날짜를 넘어서 재사용될 수 있다. 따라서 닭유형/사이즈를 주문 묶음 단위로 계산할 때는 반드시 `sale_date`를 포함해야 한다.

현재 기준:

```python
ORDER_GROUP_COLUMNS = ["source", "brand", "store", "sale_date", "order_id", "menu_seq"]
```

`sale_date`가 빠지면 다음 같은 오분류가 생긴다.

- 2026-06-16 `송파삼전점_1-18_19:28:32`: `1인 순살 닭도리탕`
- 2026-07-02 `송파삼전점_1-18_19:28:32`: `한우순살곱도리탕` + `[중] 2인`
- 날짜 없이 묶으면 두 주문이 같은 그룹이 되어 한우 곱도리탕이 `1인`으로 집계된다.

## 빠른 재현 절차

전체 재생성 전에 아래 방식으로 문제 주문만 확인한다.

```powershell
python -X utf8 -c "from pathlib import Path; import pandas as pd; base=next(Path.home().glob('OneDrive - *'))/'data'/'mart'/'new_classification_orders'/'_debug'/'latest'; df=pd.read_csv(base/'12_orders_left.csv', dtype=str, encoding='utf-8-sig').fillna(''); oid='송파삼전점_1-18_19:28:32'; print(df[df['order_id'].eq(oid)][['sale_date','order_id','menu_seq','item_seq','line_role','item_name','std_menu_name','닭옵션키','닭유형','사이즈']].to_string(index=False))"
```

PowerShell에서 한글이 깨지면 한글 리터럴을 유니코드 이스케이프로 바꾸거나, Python 안에서 데이터에서 후보값을 뽑아 필터링한다.

## 함수 단위 검증

`12_orders_left.csv`를 입력으로 새 로직만 재계산하면 전체 빌드 없이 결과를 빠르게 확인할 수 있다.

```powershell
python -X utf8 -c "from pathlib import Path; import pandas as pd; from modules.transform.pipelines.db import DB_MenuHierarchy_Test as mh; base=next(Path.home().glob('OneDrive - *'))/'data'/'mart'/'new_classification_orders'/'_debug'/'latest'; left=pd.read_csv(base/'12_orders_left.csv', dtype=str, encoding='utf-8-sig').fillna(''); profile=mh._build_menu_chicken_profile_master(left); groups=mh._build_order_group_attrs(left, menu_chicken_profile_master=profile); attached=mh._attach_manual_chicken_columns(left, group_attrs=groups); mh._manual_profit_rate_attrs=lambda: pd.DataFrame(columns=mh.MANUAL_PROFIT_RATE_MASTER_COLUMNS); profit=mh._build_manual_profit_rate_master(attached); menu='한우 순살 곱도리탕'; print(profit[(profit['대표품목명'].eq(menu)) & (profit['수익채널'].eq('홀'))][['수익키','사이즈','닭유형','대표옵션조합','계산사이즈','주문건수','판매수량','매출합계']].to_string(index=False))"
```

정상 기대값:

- `메뉴|홀|한우 순살 곱도리탕|중|순살` 존재
- `메뉴|홀|한우 순살 곱도리탕|1인|순살` 없음
- `[중] 2인` 옵션이 있는 주문은 `사이즈=중`
- `[대] 3인` 옵션이 있는 주문은 `사이즈=대`

## 수익률 absorbed option 주의

`닭유형`, `사이즈`, `맛선택` 같은 옵션은 메뉴 속성을 결정하는 보조 행이다. 유료 옵션이어도 별도 메뉴 수익키를 만들지 않고 부모 main 메뉴 수익키에 매출을 흡수해야 한다.

예:

- `갈비찜닭 반반세트 [3~6인]` main이 `혼합/중`
- 유료 옵션 `닭도리탕 [순살]` 700~1000원
- 잘못된 결과: `메뉴|홀|갈비찜닭 반반세트|중|순살` 별도 생성, `대표품목원문/원본품목명목록/닭유형_신호` 공란
- 정상 결과: `메뉴|홀|갈비찜닭 반반세트|중|혼합`에 옵션 매출 흡수, 판매수량은 main 기준 유지

재료추가, 사이드, 음료, 공기밥은 이 규칙 대상이 아니며 기존처럼 `품목|...` 수익키를 유지한다.

## 수기값 보존 체크

`01_수기입력.xlsx` 쓰기 전 가드가 `*_manual` 감소를 감지하면 저장을 중단한다. 이때 무조건 가드를 끄지 말고, 어떤 시트/컬럼의 어떤 키가 빠졌는지 비교한다.

특히 `메뉴중량`은 workbook의 최종 마스터 시트이고, 기존 시트가 레거시 컬럼명 `사이즈`, `닭유형`을 쓸 수 있다. 읽기 로직은 이를 `사이즈키`, `닭유형키`로 정규화하고, 수기값이 있는 기존 행은 새 데이터에 없어도 보존해야 한다.

`판정옵션`은 자동생성 행이 많다. `메모`가 `자동생성 `으로 시작하는 행은 수기 유실 가드 카운트에서 제외하고, 사람이 직접 적은 행만 보호한다.

## 검증 명령

코드 수정 후 먼저 단위 검증을 실행한다.

```powershell
python -X utf8 -m py_compile modules/transform/pipelines/db/DB_MenuHierarchy_Test.py
$env:PYTHONPATH='C:\airflow'; pytest tests/test_menu_hierarchy_no_model_classification.py -q --basetemp .tmp/pytest-menu-hierarchy -p no:cacheprovider
```

OneDrive 재생성 승인이 있으면 마지막에만 전체 빌드를 실행한다.

```powershell
docker compose exec -T airflow-scheduler python -X utf8 -c "from modules.transform.pipelines.db.DB_MenuHierarchy_Test import build_orders; build_orders(debug_outputs=True)"
```

재생성 후 `_debug/latest/27_profit_rate_master.csv`와 `01_수기입력.xlsx`의 `수익률` 시트를 확인한다. workbook이 Excel에서 열려 있으면 `PermissionError`가 날 수 있으므로, 확인은 가능하면 `_debug/latest` CSV로 먼저 한다.

## 최근 수정 맥락

- 홀 명시 사이즈 옵션이 순살 전용 메뉴프로필 기본 사이즈보다 우선되도록 보정했다.
- Codex 자동채움 수기 사이즈가 명시 옵션과 충돌하면 자동 판정값을 사용하도록 보정했다.
- 주문그룹 키에 `sale_date`를 추가해 날짜가 다른 동일 `order_id/menu_seq`가 섞이지 않도록 했다.
- `메뉴중량`과 `예외보정` 등 수기 입력값이 재생성 중 사라지지 않도록 보존 가드를 강화했다.
- 유료 닭유형/사이즈/맛선택 옵션 매출이 별도 메뉴 수익키를 만들지 않고 부모 main 수익키로 흡수되도록 보정했다.
