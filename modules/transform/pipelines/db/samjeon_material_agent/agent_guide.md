# 송파삼전점 원재료 mart — LLM 데이터 해석 가이드

이 문서는 `schema_version=3`인 같은 스냅샷의 `agent_context.json`과 Parquet 파일을 해석하는 기준이다. 답변에는 `as_of_date`, 조회 기간, 자료 상태, 사용한 지표와 근거를 함께 제시한다. 숫자는 파일에 있는 값만 사용하고 원인을 추측하지 않는다. v1/v2 산출물을 v3로 간주하지 않는다.

## 파일과 판본

- `latest.json`은 최신 스냅샷의 `manifest.json` 위치를 가리킨다. 과거 날짜 수동 실행은 최신 포인터를 바꾸지 않는다.
- `manifest.json`의 `snapshot_id`, `rule_version`, `sources`, `config_hashes`, `files`는 계산 판본과 원천·출력 파일의 해시다. 다른 스냅샷의 숫자와 가이드를 섞지 않는다.
- `agent_context.json`은 기준일 보고용 계산 결과다. `guide.file`과 `guide.sha256`이 이 문서를 가리킨다.
- `complete`와 각 `*_complete`, `target_applicable`은 JSON boolean이다. 금액은 정수 원, 표시 비율은 소수점 한 자리 문자열이며 `null`은 계산 또는 판정 보류를 뜻한다. 문자열 `"False"`를 boolean으로 해석하지 않는다.
- `sales_daily.parquet`은 `sale_date × source`의 할인 후 순매출과 원천 행수다. `purchase_lines.parquet`은 구매 원천 행과 `evidence_id`, `source_file`, `source_sha256`, `source_row`를 보존한다. `purchase_daily.parquet`은 날짜·원천·비용 분류별 합계, `item_daily.parquet`은 품목별 합계다. `quality_daily.parquet`은 날짜·원천별 상태와 이유다. `review_candidates.csv`는 사람이 검수할 미분류 품목 목록이다.
- `sales_lines.parquet`은 중복 검증 후의 매출 행과 `_pk`, 근거를 보존한다. 구매 일별 합계는 계정도 구분한다. `source_ref`는 운영체제와 무관한 `원천/파일명` 참조이며 `source_file`은 생성 당시 물리 경로다. `source_row`는 필터 전 0부터 시작하는 행 위치다.
- `review_candidates.csv`의 추천은 확정 분류가 아니다. 검수 입력은 별도 로컬 사본에서 수행한다. `coverage_candidates.csv`는 수집·마감 확인용 초안이며 빈 승인란을 자동으로 채우지 않는다. 스냅샷 파일을 수정하면 해시 검증이 실패한다.

## 매출과 홀 소계

- `observed_sales_won`은 송파삼전점의 **모든** 매출 원천에서 `total_price`를 부호 그대로 더한 금액이다. 취소·반품의 음수와 0원 행도 유지한다.
- `hall_sales_won`은 그중 `source=okpos`의 합계다. 송파삼전점에서는 OKPOS를 홀로 취급하며 `홀_포장` 주문도 이 소계에 포함한다.
- `source=posfeed`, `배민수동`, `쿠팡수동`은 홀 소계에 포함하지 않는다. 이 문서는 그 밖의 원천을 일괄적으로 배달로 확정하지 않는다. **전체 매출에서 홀 소계를 뺀 값을 배달 매출이라고 부르지 않는다.**
- 홀 소계는 `daily`와 `month_to_date`에 있다. 매입률의 분모는 홀 소계가 아니라 `observed_sales_won`이다.

## 구매와 비용

- `observed_purchase_won`은 검수된 `원재료 + 포장재` 매입액이다. 실제 재고 소모 원가나 확정 손익이 아니다.
- 푸드가이드 `도리당 송파삼전점(부식)` 계정의 전액은 `현장 직원식대`다. `staff_meal_won`으로 따로 보고하며 매입률과 목표 37% 비교에서 제외한다. 같은 상품이 본 계정에도 있어도 계정별 분류를 유지한다.
- `other_won`은 기타 운영비, `unclassified_won`은 분류 검수가 끝나지 않은 구매다. 미분류 금액을 임의로 원재료·포장재에 합치지 않는다. 반품·환불의 음수도 원천 부호대로 반영한다.
- `unclassified_row_count`가 1 이상이면 순미분류 금액이 0원이어도 검수 미완료다. `unclassified_absolute_amount_won`은 반품과 상쇄되기 전 절대금액이다. 분류가 없는 자료에서 `observed_purchase_won=0`을 전체 구매 0원으로 표현하지 않는다.
- `purchase_rate_pct`는 `observed_purchase_won ÷ observed_sales_won × 100`, `target_gap_pp`는 해당 비율과 37%의 차이다. `target_excess_purchase_won`은 매입액에서 매출액의 37%를 뺀 금액이다. 모두 재고 소모율이 아닌 **식재료·포장재 매입률** 지표다.

## 기간과 자료 상태

- `daily`는 `as_of_date` 하루, `month_to_date`는 해당 월 1일부터 기준일까지다. `requested_period`는 지정 조회 기간이다. `seven_day`는 기준일 포함 최근 7일과 그 직전 7일의 매입액 비교이며 `staff_meal_seven_day`는 직원식대 비교다. 각 결과의 시작·종료일을 함께 확인한다.
- `quality_daily.parquet`에서 `정상`은 날짜별 원천 판본과 마감 근거가 확인된 상태다. `잠정`은 확인이 남은 상태, `누락`은 필요한 원천이 없는 상태, `오래됨`은 확인 이후 판본이 바뀐 상태다.
- 파일에 구매 행이 없다는 사실만으로 그날 구매가 0원으로 확정되지는 않는다. 마감 근거가 없는 0원·소계는 관측값으로만 설명한다.
- 기간 내 필요한 원천이 `정상`이고 미분류 구매 행이 없을 때만 해당 지표가 완료다. 매입률은 매출·매입 모두 완료되어야 확정된다. 직원식대는 푸드가이드의 완료 여부를 별도로 사용한다.
- 분류가 완료되고 필요한 원천이 있으나 마감 확인만 남은 경우 `provisional_purchase_rate_pct`를 제공한다. 반드시 잠정 비율이라고 표시한다. 누락·오래됨·미분류가 있거나 매출이 0 이하이면 비율을 보류한다.
- 목표는 `settings.target_rate`, `target_start_date`, `amount_basis_confirmed`를 따른다. `target_comparison_status=참고`는 공식 목표 초과 판정이 아니다. `target_exceeded=true`일 때만 확정 초과로 설명한다. 표시된 차이가 0.0%p여도 반올림 전 계산상 초과일 수 있다.
- `forecast.status=보류`는 유효한 월말 전망을 낼 수 없다는 뜻이다. `정상` 전망에도 `최근 패턴 유지` 가정이 붙으며, 재고·행사·발주 계획을 반영하지 않는다.
- 전망은 기준일 이전 최근 56일의 동일 요일 표본을 고정한다. 매출·매입 각각의 표본 날짜와 수를 확인한다. 확인된 무거래일은 0원이며 미확인일은 제외한다. 월말 마감이 완료되면 `status=실적`, 미완료이면 `보류`다. 잔여 매입 여력의 `capacity_status=참고`도 공식 예산으로 해석하지 않는다.

## 답변 기준

- 먼저 기준일과 `status` 및 `complete`를 확인한다. 잠정·누락·오래됨 자료는 관측값과 확인할 항목을 설명하고 목표 초과를 확정하지 않는다.
- 이상 증가나 상위 품목은 금액 변화와 근거 행을 제시한다. 선매입·폐기·낭비 같은 원인은 추가 증거 없이 단정하지 않는다.
- `issues`는 우선순위가 높은 최대 3개이며 전체 문제는 `issues.json`, 건수는 `issues_total`에 있다. 목록이 짧다는 이유로 그 외 기간이 정상이라고 추정하지 않는다.
- 질문에 필요한 수치가 스냅샷에 없으면 모른다고 말하고 필요한 원천 또는 검수 항목을 알려준다. 여러 날짜·스냅샷을 비교할 때는 각각의 판본과 자료 상태를 밝힌다.
- 조회 함수가 `status=범위 밖`을 반환하면 `required_date_from`~`required_date_to`로 별도 스냅샷을 계산해야 한다. 과거 자료를 오늘의 결과로 표현하지 않는다. 생성 시점의 정상 여부는 이후 원천의 최신 수집 완료를 보증하지 않는다.
- `summary`·`staff_meal`·`evidence`·`status`는 지정한 시작~종료 기간을 조회한다. `compare`는 종료일 기준 최근 7일 대 직전 7일, `anomalies`는 종료일의 일별·7일·월 누적 규칙, `forecast`는 종료일 기준 월말 전망이다. 별도 기간의 전체 날짜별 이상을 조회한 것으로 표현하지 않는다.
