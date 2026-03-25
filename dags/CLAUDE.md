# DAG 규칙

## 구조
- extract → transform → load 순서
- `dag_id=Path(__file__).stem`, `catchup=False`, `max_active_runs=1`
- schedule: `schedule.py` 상수 사용
- 비즈니스 로직은 `modules/transform/pipelines/`에 작성
- XCom에 parquet 경로만 전달

## 네이밍
- 영업관리부: `Sales_{DOMAIN}_{##}_{STAGE}_Dags.py` → `dags/sales/`
- 전략기획부: `Strategy_{DOMAIN}_{##}_{STAGE}_Dags.py` → `dags/strategy/`
- 독립 DAG (순서 없음): `Sales_{DOMAIN}_{STAGE}_Dags.py`

### DOMAIN 예시
- `Orders` (주문 데이터), `VisitLog` (방문일지), `Employee` (직원 정보)
- `ToOrderVoc` (투오더 VOC), `FdamCS` (프담 CS), `CoupangCoupon` (쿠팡 쿠폰)

### STAGE 예시
- `Extract` → `Transform` → `Gsheet` / `Load` / `Alert` / `Report`

### 현재 DAG 순서
```
Sales_Orders_01_Extract → 02_Review → 03_Transform → 04_Join
  → 05_Marketing → 06_Gsheet → 07_Alert → 08_Report → 09_FlowReport

Sales_VisitLog_01_Crawl → 02_Transform → 03_Gsheet

Strategy_ToOrderVoc_01_Crawl → 02_Transform
Strategy_FdamCS_01_Process → 02_FlowMacro
Strategy_CoupangCoupon_01_Extract / 02_Reupload
```

## 참조
- `docs/architecture.md` - 아키텍처/모듈 구조도
