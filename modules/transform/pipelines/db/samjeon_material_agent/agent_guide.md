# 송파삼전점 원재료 mart — LLM 데이터 해석 가이드

이 문서는 같은 스냅샷의 `agent_context.json`과 Parquet 파일을 해석하는 기준이다. 답변에는 `as_of_date`, 자료 상태, 사용한 지표와 근거를 함께 제시한다. 숫자는 파일에 있는 값만 사용하고 원인을 추측하지 않는다.

## 파일과 판본

- `latest.json`은 최신 스냅샷의 `manifest.json` 위치를 가리킨다. 과거 날짜 수동 실행은 최신 포인터를 바꾸지 않는다.
- `manifest.json`의 `snapshot_id`, `rule_version`, `sources`, `config_hashes`, `files`는 계산 판본과 원천·출력 파일의 해시다. 다른 스냅샷의 숫자와 가이드를 섞지 않는다.
- `agent_context.json`은 기준일 보고용 계산 결과다. `guide.file`과 `guide.sha256`이 이 문서를 가리킨다.
- `sales_daily.parquet`은 `sale_date × source`의 할인 후 순매출과 원천 행수다. `purchase_lines.parquet`은 구매 원천 행과 `evidence_id`, `source_file`, `source_sha256`, `source_row`를 보존한다. `purchase_daily.parquet`은 날짜·원천·비용 분류별 합계, `item_daily.parquet`은 품목별 합계다. `quality_daily.parquet`은 날짜·원천별 상태와 이유다. `review_candidates.csv`는 사람이 검수할 미분류 품목 목록이다.

## 매출과 홀 소계

- `observed_sales_won`은 송파삼전점의 **모든** 매출 원천에서 `total_price`를 부호 그대로 더한 금액이다. 취소·반품의 음수와 0원 행도 유지한다.
- `hall_sales_won`은 그중 `source=okpos`의 합계다. 송파삼전점에서는 OKPOS를 홀로 취급하며 `홀_포장` 주문도 이 소계에 포함한다.
- `source=posfeed`, `배민수동`, `쿠팡수동`은 홀 소계에 포함하지 않는다. 이 문서는 그 밖의 원천을 일괄적으로 배달로 확정하지 않는다. **전체 매출에서 홀 소계를 뺀 값을 배달 매출이라고 부르지 않는다.**
- 홀 소계는 `daily`와 `month_to_date`에 있다. 매입률의 분모는 홀 소계가 아니라 `observed_sales_won`이다.

## 구매와 비용

- `observed_purchase_won`은 검수된 `원재료 + 포장재` 매입액이다. 실제 재고 소모 원가나 확정 손익이 아니다.
- 푸드가이드 `도리당 송파삼전점(부식)` 계정의 전액은 `현장 직원식대`다. `staff_meal_won`으로 따로 보고하며 매입률과 목표 37% 비교에서 제외한다. 같은 상품이 본 계정에도 있어도 계정별 분류를 유지한다.
- `other_won`은 기타 운영비, `unclassified_won`은 분류 검수가 끝나지 않은 구매다. 미분류 금액을 임의로 원재료·포장재에 합치지 않는다. 반품·환불의 음수도 원천 부호대로 반영한다.
- `purchase_rate_pct`는 `observed_purchase_won ÷ observed_sales_won × 100`, `target_gap_pp`는 해당 비율과 37%의 차이다. `target_excess_purchase_won`은 매입액에서 매출액의 37%를 뺀 금액이다. 모두 재고 소모율이 아닌 **식재료·포장재 매입률** 지표다.

## 기간과 자료 상태

- `daily`는 `as_of_date` 하루, `month_to_date`는 해당 월 1일부터 기준일까지다. `seven_day`는 기준일 포함 최근 7일과 그 직전 7일의 매입액 비교다.
- `quality_daily.parquet`에서 `정상`은 날짜별 원천 판본과 마감 근거가 확인된 상태다. `잠정`은 확인이 남은 상태, `누락`은 필요한 원천이 없는 상태, `오래됨`은 확인 이후 판본이 바뀐 상태다.
- 파일에 구매 행이 없다는 사실만으로 그날 구매가 0원으로 확정되지는 않는다. 마감 근거가 없는 0원·소계는 관측값으로만 설명한다.
- 기간 내 모든 원천이 `정상`이고 미분류 구매가 없을 때만 `complete=true`다. 미완료 또는 매출액 0 이하일 때 비율·목표 판정은 `null`이며 임의로 계산하거나 확정 표현을 하지 않는다.
- `forecast.status=보류`는 유효한 월말 전망을 낼 수 없다는 뜻이다. `정상` 전망에도 `최근 패턴 유지` 가정이 붙으며, 재고·행사·발주 계획을 반영하지 않는다.

## 답변 기준

- 먼저 기준일과 `status` 및 `complete`를 확인한다. 잠정·누락·오래됨 자료는 관측값과 확인할 항목을 설명하고 목표 초과를 확정하지 않는다.
- 이상 증가나 상위 품목은 금액 변화와 근거 행을 제시한다. 선매입·폐기·낭비 같은 원인은 추가 증거 없이 단정하지 않는다.
- 질문에 필요한 수치가 스냅샷에 없으면 모른다고 말하고 필요한 원천 또는 검수 항목을 알려준다. 여러 날짜·스냅샷을 비교할 때는 각각의 판본과 자료 상태를 밝힌다.
