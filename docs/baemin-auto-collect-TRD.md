# TRD: 배민 자동 수집 DAG 기술 설계서

> 작성일: 2026-05-08  
> 작성자: 민준  
> 참조 PRD: `docs/baemin-auto-collect-PRD.md`  
> 참조 익스텐션: `doridang_collector_1.3/content/02_baemin.js`

---

## 1. 시스템 아키텍처

```
sales_employee.csv
        │  플랫폼='배달의 민족' 필터
        ▼
Sales_Baemin_01_Collect_Dags.py  (Airflow DAG)
        │
        ├─ Task 1: load_accounts
        │         CSV 로드 → XCom
        │
        └─ Task 2: collect_all_stores
                  SMD_baemin_01_collect.collect_all()
                  │
                  └─ For each store (순차):
                      ① launch_browser()         undetected_chromedriver
                      ② login_baemin()           human_type + 8~12초 대기
                      ③ get_store_id()           DOM 파싱
                      ④ collect_metrics()        우리가게NOW 6개 지표
                      ⑤ collect_marketing()      우가클 테이블 파싱
                      ⑥ collect_orders()         JS 주입 → 전 페이지 수집
                      ⑦ logout_baemin()          settings 페이지 → 버튼 클릭
                      ⑧ random sleep 30~90s
                      ⑨ driver.quit()
                      │
                      ▼
              OneDrive/data/analytics/baemin_orders_detail/
```

---

## 2. 파일 구조

### 신규 생성
```
dags/sales/
  Sales_Baemin_01_Collect_Dags.py        # DAG 정의

modules/extract/
  baemin_02_orders.py                    # 주문내역 수집 (JS 주입)
  baemin_03_marketing.py                 # 우가클 수집 (DOM 파싱)

modules/transform/pipelines/sales/
  SMD_baemin_01_collect.py              # 전체 플로우 오케스트레이션
```

### 기존 확장
```
modules/extract/croling_beamin.py
  + get_store_id(driver) -> str          # DOM에서 store_id 추출
  + logout_baemin(driver, account_id)    # settings → 로그아웃
```

---

## 3. DAG 명세

```python
# dags/sales/Sales_Baemin_01_Collect_Dags.py

dag_id    = "Sales_Baemin_01_Collect_Dags"
schedule  = "50 23 * * *"   # KST 08:50 (UTC 23:50 전날)
catchup   = False
max_active_runs = 1
```

### Task 1: load_accounts
- `sales_employee.csv` 로드
- `플랫폼 == '배달의 민족'` 필터
- `TARGET_STORES = ["송파", "역삼"]` 테스트 필터 (운영 시 `[]`)
- XCom 반환: `List[Dict]` (매장명, ID, PW)

### Task 2: collect_all_stores
- XCom에서 account_list pull
- `dag_run.conf`에서 `start_date`, `end_date` 읽기 (없으면 KST 전일)
- `SMD_baemin_01_collect.collect_all()` 호출

---

## 4. 계정 로드 로직

```python
CSV_PATH = "C:/Users/민준/OneDrive - 주식회사 도리당/Repository/sales_employee.csv"
TARGET_STORES = ["송파", "역삼"]  # 테스트용

df = pd.read_csv(CSV_PATH)
df = df[df["플랫폼"] == "배달의 민족"]
if TARGET_STORES:
    mask = df["매장명"].str.contains("|".join(TARGET_STORES))
    df = df[mask]
```

### 매핑 테이블
| 사용자 표현 | CSV 매장명 | 배민 계정 ID |
|------------|------------|--------------|
| 송파삼전점 | 도리당 송파점 | songdori |
| 역삼점 | 도리당 역삼점 | doriys |

---

## 5. 브라우저 설정 (기존 `croling_beamin.py` 상속)

```python
# undetected_chromedriver 설정
options.add_argument('--headless=new')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--window-size=1920,1080')
options.add_argument(f'--user-data-dir={CHROME_PROFILE_DIR}/{account_id}')
```

---

## 6. 로그인 (`croling_beamin.login_baemin` 기존)

```
URL: https://biz-member.baemin.com/login?returnUrl=https%3A%2F%2Fself.baemin.com%2F

셀렉터:
  ID 필드:  input[name="id"]
  PW 필드:  input[name="password"]
  버튼:     button[type="submit"]

타이밍:
  타이핑: 문자당 0.05~0.15초
  버튼 클릭 전: 0.5~1.0초
  로그인 완료 대기: 8~12초
```

---

## 7. Store ID 추출 (`get_store_id` 신규)

```python
def get_store_id(driver) -> str:
    """로그인 후 현재 선택된 매장 ID 추출"""
    # 방법 1: DOM 요소 (.ShopSelect-module__j4Qm)
    elem = driver.find_element(By.CSS_SELECTOR, ".ShopSelect-module__j4Qm")
    match = re.search(r'(\d+)', elem.text)
    if match:
        return match.group(1)
    # 방법 2: URL 패턴 /shops/{id}/
    url_match = re.search(r'/shops/(\d+)/', driver.current_url)
    return url_match.group(1) if url_match else ""
```

---

## 8. 메인 지표 수집 (`collect_metrics` 기존)

수집 항목:
```
조리소요시간, 조리소요시간_순위구분, 조리소요시간_순위비율
주문접수시간, 주문접수시간_순위구분, 주문접수시간_순위비율
최근재주문율
조리시간준수율, 조리시간준수율_순위구분, 조리시간준수율_순위비율
주문접수율, 주문접수율_순위구분, 주문접수율_순위비율
최근별점
```

저장: `ANALYTICS_DB/baemin_orders_detail/metrics/{매장명}_{date}.csv`

---

## 9. 우가클 수집 (`baemin_03_marketing.py` 신규)

```
URL: https://self.baemin.com/shops/{store_id}/stat/marketing/woori-shop-click
     ?initialDateOption=MONTHLY&initialMonth={YYYY-MM}

수집 범위: 당월 (DAG 실행 기준)

처리 순서:
  1. URL 이동 후 페이지 로드 대기
  2. "전체보기" 버튼 클릭 (table[data-atelier-component="Table"])
  3. tbody tr.Table_b_r4ax_1dwbr4on 행 순회
  4. 날짜, 광고지출, 노출수, 클릭수, 주문수, 주문금액, 광고효과 파싱

컬럼:
  collected_at, store_id, store_name, 날짜,
  광고지출, 노출수, 클릭수, 주문수, 주문금액, 광고효과

저장: ANALYTICS_DB/baemin_orders_detail/marketing/{매장명}_{YYYY-MM}.csv
```

---

## 10. 주문내역 수집 (`baemin_02_orders.py` 신규)

### 전략: JS 주입 방식

익스텐션 `02_baemin.js`의 검증된 셀렉터를 `execute_script()`로 직접 실행.
Python DOM 재구현보다 정확도가 높고 익스텐션과 동일한 결과 보장.

```
URL: https://self.baemin.com/orders/history
     ?startDate={YYYY-MM-DD}&endDate={YYYY-MM-DD}

날짜 범위:
  dag_run.conf: {"start_date": "2026-05-07", "end_date": "2026-05-07"}
  기본값: KST 전일 (start = end = yesterday)

처리 순서:
  1. URL 이동
  2. execute_script("return _expandAllOrders()")
  3. execute_script("return _collectCurrentPageData()")  → JSON 반환
  4. 다음 페이지 있으면 반복 (마지막 페이지까지)
  5. 전체 rows를 DataFrame 변환
```

### 수집 컬럼 (익스텐션 동일)
```
collected_at, store_name,
주문상태, 주문번호, 주문시각, 광고상품, 캠페인ID,
주문내역, 주문수량, 결제타입, 수령방법,
결제금액, 상품금액, 즉시할인, 즉시할인_파트너부담, 즉시할인_배민지원,
배민부담_쿠폰할인, 총결제금액,
주문옵션상세, 주문옵션금액,
주문중개, 고객할인비용, 배달, 그외, 부가세, 만나서결제금액, 입금예정금액
```

저장: `ANALYTICS_DB/baemin_orders_detail/orders/{매장명}_{start}_{end}.csv`

---

## 11. 로그아웃 (`logout_baemin` 신규)

```python
def logout_baemin(driver, account_id: str):
    driver.get("https://self.baemin.com/settings")
    # 페이지 로드 대기 4~6초
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button.LandingPage-module__mLoG")
        )
    )
    # 클릭 전 0.5~1.0초 대기 (human-like)
    btn.click()
    # 로그아웃 처리 대기 2~3초
```

---

## 12. Anti-Detection 전략

| 기법 | 구현 |
|------|------|
| undetected_chromedriver | uc.Chrome() 사용 (기존 동일) |
| WebDriver 플래그 제거 | `--disable-blink-features=AutomationControlled` |
| Chrome 프로필 격리 | 계정별 `CHROME_PROFILE_DIR/{account_id}` |
| 사람처럼 타이핑 | `human_type()` - 문자당 50~150ms 랜덤 |
| 계정 간 대기 | 로그아웃 후 **30~90초** 랜덤 슬립 |
| 배치 간 휴식 | 배치 완료 후 **45~90초** 슬립 (기존) |

---

## 13. 저장 경로 구조

```
ANALYTICS_DB = C:/Users/민준/OneDrive - 주식회사 도리당/data/analytics

ANALYTICS_DB/
  baemin_orders_detail/
    orders/
      도리당 송파점_2026-05-07_2026-05-07.csv
      도리당 역삼점_2026-05-07_2026-05-07.csv
    marketing/
      도리당 송파점_2026-05.csv
      도리당 역삼점_2026-05.csv
    metrics/
      도리당 송파점_20260508.csv
      도리당 역삼점_20260508.csv
```

---

## 14. 에러 처리

| 상황 | 처리 방식 |
|------|----------|
| 로그인 실패 | 해당 매장 skip, 다음 매장 진행 |
| store_id 추출 실패 | 우가클 수집 skip, 주문내역만 수집 |
| 페이지 로드 타임아웃 | 최대 15초 대기 후 중단 |
| JS 주입 오류 | Python 폴백 파서로 재시도 |
| 드라이버 연결 끊김 | driver.quit() 후 다음 계정 진행 |

---

## 15. 타이밍 상수 (TIMING)

```python
TIMING = {
    "login_wait":       (8.0, 12.0),   # 로그인 완료 대기
    "page_load":        (4.0, 6.0),    # 페이지 로드 대기
    "typing_char":      (0.05, 0.15),  # 문자당 타이핑 속도
    "typing_pause":     (0.3, 0.6),    # 타이핑 전후 멈춤
    "before_click":     (0.5, 1.0),    # 클릭 전 멈춤
    "store_switch":     (4.0, 6.0),    # 매장 전환 대기
    "stat_extract":     (0.2, 0.4),    # 통계 추출 간격
    "account_stagger":  (3.0, 7.0),    # 배치 내 계정 간격
    "batch_rest":       (45.0, 90.0),  # 배치 간 휴식
    "logout_wait":      (30.0, 90.0),  # 로그아웃 후 대기 (NEW)
}
```

---

## 16. 검증 방법

1. `airflow dags trigger Sales_Baemin_01_Collect_Dags` 수동 실행
2. `ANALYTICS_DB/baemin_orders_detail/orders/` CSV 생성 확인
3. 주문번호 건수 비교 (익스텐션 수동 수집 vs 자동 수집)
4. Airflow 로그: `로그인 성공`, `수집 완료`, `로그아웃 완료` 3개 체크포인트 확인
5. 우가클 클릭수 랜덤 3행 사이트 직접 비교

---

## 17. 구현 순서

```
Step 1  croling_beamin.py       get_store_id(), logout_baemin() 추가
Step 2  baemin_03_marketing.py  우가클 테이블 파싱 구현
Step 3  baemin_02_orders.py     JS 주입 방식 주문내역 수집 구현
Step 4  SMD_baemin_01_collect.py  플로우 오케스트레이션
Step 5  Sales_Baemin_01_Collect_Dags.py  DAG 정의
```
