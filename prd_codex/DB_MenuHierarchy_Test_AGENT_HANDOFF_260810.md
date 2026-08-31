# DB_MenuHierarchy_Test Agent Handoff

## 목적

이 문서는 다음 에이전트가 송파삼전점 메뉴계층 완전분류 파이프라인을 바로 이해하고 수정할 수 있게 만든 인계 문서다.

대상 DAG:

- `dags/db/DB_MenuHierarchy_Test_Dags.py`

핵심 로직:

- `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`

운영 산출물:

- OneDrive `data/mart/new_classification_orders`

히스토리/하네스 문서:

- `prd_codex/DB_MenuHierarchy_Test_하네스히스토리_260810.md`

## 현재 운영 구조

루트 산출물은 4개만 둔다.

- `01_수기입력.xlsx`: 사람이 입력하는 유일한 파일
- `02_최종주문.csv`: 최종 주문별 완전분류 결과
- `03_요약.xlsx`: 메뉴별 닭사용량, 수익, source별 분류 검증
- `04_사용법.md`: 현장 사용법

기존 `10_orders.csv`부터 `28_manual_profit_summary.csv`까지의 CSV는 루트에 두지 않는다.
필요할 때만 `_debug/latest/`에 생성한다.

## 완전분류 기준

최종 목표는 신뢰도 표기가 아니라 완전분류다.

`02_최종주문.csv`에는 `판정상태`를 노출하지 않는다.
최종 분석자는 아래 컬럼만 본다.

- `닭유형`
- `사이즈`
- `사용용량`
- `사용용량_합계`
- `분류룰`

`03_요약.xlsx > source별_분류검증`에서 `미분류행=0`이어야 완전분류다.
`미분류_TOP`에 1행이라도 남으면 아직 완성된 상태가 아니다.

## 판정옵션 원칙

원천 주문에 실제 0원 옵션 행을 추가하지 않는다.

이유:

- `_pk`가 늘어난다.
- `item_seq`가 원천과 달라진다.
- source별 주문행 검증이 깨진다.
- 매출은 0이어도 주문 구조가 바뀌어 후속 검증이 어려워진다.

대신 `01_수기입력.xlsx > 판정옵션` 시트에 분류 전용 룰을 넣는다.

판정옵션 필수 컬럼:

- `source`
- `brand`
- `store`
- `std_menu_name`
- `조건`
- `닭옵션키`
- `닭유형`
- `사이즈`
- `사용용량`
- `메모`

`조건` 값:

- `옵션없음`
- `사이즈옵션없음`
- `닭유형충돌`
- `사이즈충돌`
- `자동보정`

판정옵션이 적용되면 주문행은 그대로 유지되고 `닭유형`, `사이즈`, `사용용량`, `분류룰`만 확정된다.

## 분류 우선순위

분류 우선순위는 아래 순서다.

1. 실제 주문 옵션/메뉴명 명시값
2. `예외보정` 시트의 주문그룹 직접 입력
3. `판정옵션` 시트의 source별 메뉴 기본값
4. 기존 메뉴 고정 규칙
5. 남으면 미분류로 보고 DAG 실패

`분류룰` 예시:

- `확정:수기`
- `확정:메뉴명`
- `확정:판정옵션`
- `확정:닭미사용`

## 닭사용량 계산 방식

`사용용량`은 메뉴 1개 기준 닭 사용량이다.

예:

- 메뉴 1개당 0.6마리를 쓰면 `사용용량=0.6`
- 같은 주문 라인의 `qty=2`이면 `사용용량_합계=1.2`
- 같은 주문 라인의 `qty=3`이면 `사용용량_합계=1.8`

계산식:

```text
사용용량_합계 = qty * 사용용량
```

따라서 사람이 `01_수기입력.xlsx`에서 `사용용량` 또는 `닭사용량_manual`에 `0.6`을 입력하면, DAG 재실행 때 주문 수량을 곱해 `02_최종주문.csv`의 `사용용량_합계`가 자동 계산되어야 한다.

월별/메뉴별 닭 사용량 집계는 반드시 `사용용량`이 아니라 `사용용량_합계`를 합산해야 한다.

반반 메뉴는 별도 분해 컬럼을 같이 본다.

```text
사용용량_합계 = 뼈사용용량_합계 + 순살사용용량_합계
```

반반 조합:

- `뼈+뼈`: 뼈 사용량만 계산
- `뼈+순살`: 뼈 기준 50% + 순살 기준 50%
- `순살+순살`: 순살 사용량만 계산

반반 자동 판정은 주문/메뉴 컨텍스트에 `반반`이 있을 때만 적용한다.

잘못된 집계:

```text
sum(사용용량)
```

올바른 집계:

```text
sum(사용용량_합계)
```

## 닭수량 체크 방법

1. `02_최종주문.csv`에서 `line_role=main`만 본다.
2. `닭유형 != 닭미사용`인 행만 닭 사용 대상이다.
3. `사용용량` 공백이 있으면 분류 실패다.
4. `사용용량_합계`는 `qty * 사용용량`과 같아야 한다.
5. 메뉴별 총 닭사용량은 `std_menu_name`, `사이즈`, `닭유형` 기준으로 `사용용량_합계`를 합산한다.

검증 예시:

```python
import pandas as pd
from modules.transform.pipelines.db.DB_MenuHierarchy_Test import FINAL_ORDERS_OUTPUT_PATH

df = pd.read_csv(FINAL_ORDERS_OUTPUT_PATH, dtype=str, encoding="utf-8-sig").fillna("")
main = df[df["line_role"].eq("main")].copy()

qty = pd.to_numeric(main["qty"], errors="coerce").fillna(0)
usage = pd.to_numeric(main["사용용량"], errors="coerce").fillna(0)
usage_total = pd.to_numeric(main["사용용량_합계"], errors="coerce").fillna(0)

bad = main[(usage_total - (qty * usage)).abs() > 0.0001]
print("사용용량_합계 오류행:", len(bad))

summary = (
    main[main["닭유형"].ne("닭미사용")]
    .assign(_usage_total=usage_total)
    .groupby(["std_menu_name", "사이즈", "닭유형"], dropna=False)["_usage_total"]
    .sum()
    .reset_index(name="닭사용량합계")
)
print(summary.sort_values("닭사용량합계", ascending=False).head(20))
```

## 수기입력 위치

닭 사용량을 입력할 수 있는 주요 위치:

- `01_수기입력.xlsx > 메뉴중량`
  - 메뉴, 사이즈, 닭유형별 표준 닭 사용량
  - 재료/원가 분석까지 연결되는 기준

- `01_수기입력.xlsx > 판정옵션`
  - 옵션 없음/충돌/사이즈 없음 주문을 source별 메뉴 기본값으로 확정
  - `사용용량`에 `0.6`처럼 메뉴 1개 기준값 입력

- `01_수기입력.xlsx > 예외보정`
  - 특정 주문그룹 단위로 직접 보정
  - `닭사용량_manual`에 `0.6`처럼 입력

## 성공 검증 기준

정상 상태:

- `02_최종주문.csv` 행 수와 `_pk` unique가 같다.
- source별 매출 합계가 변하지 않는다.
- `03_요약.xlsx > source별_분류검증`의 `미분류행`이 전부 0이다.
- `03_요약.xlsx > 미분류_TOP`이 0행이다.
- `02_최종주문.csv`에 `판정상태` 컬럼이 없다.
- `02_최종주문.csv`에 `분류룰` 컬럼이 있다.
- `사용용량_합계 = qty * 사용용량` 검증이 0건이다.

2026-08-10 기준 검증값:

- `02_최종주문.csv`: 31,663행
- `_pk` unique: 31,663
- 매출합계: 177,571,719
- source별 완전분류율: 100%
- `미분류_TOP`: 0행
- blocking issue: 0건

## 남은 작업의 의미

`profit_missing`, `manual_profit`, `material_price`, `menu_weight` WARN은 분류 실패가 아니다.

이 WARN은 아래 입력이 아직 안 끝났다는 뜻이다.

- 품목별 수익률
- 재료 단가
- 메뉴별 표준중량
- 옵션 재료 사용량

메뉴/닭유형/사이즈/사용량 완전분류와 수익/원가 입력 대기는 분리해서 봐야 한다.
