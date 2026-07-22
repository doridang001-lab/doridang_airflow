# 쿠팡이츠 Extension 안정화 작업 기록

## 요약

쿠팡이츠 크롬 익스텐션 수집기에서 다음 문제를 해결했다.

- runner.html 재시작 또는 익스텐션 리로드 후 당일 완료 매장을 다시 수집할 수 있는 문제
- 재시도 라운드의 "건너뜀" 배지가 회색 대기 상태로 보여 미완료처럼 보이는 문제
- 수동 orders 수집 중 중간 실패 시 사용자가 직접 "이어수집"을 눌러야 하는 문제
- 자동 orders 수집은 "어제" 단일 날짜를 조회하지만, 동일한 반복 요청 제한과 빈 페이지 오판 위험이 있는 문제
- 쿠팡 반복 요청 제한 `10056: 단시간 내 반복 요청을 제한하고 있습니다. 잠시 후 다시 시도해 주세요.` 발생 후 `데이터 없음`으로 잘못 종료되는 문제
- orders 조회가 최신일자부터 역순으로 표시되는데, 이어수집 범위가 `06.13~06.13`처럼 잘못 축소되는 문제
- 수동 orders 수집 속도와 클릭 패턴이 너무 빠르고 고정적이라 페이지 안정화와 요청 제한에 취약한 문제

## 대상 프로젝트

- 프로젝트: Chrome Extension, Manifest V3, Vanilla JS
- 경로: `C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_개발용\`
- 주요 파일:
  - `content/01_utils.js`
  - `content/03_coupangeats.js`
  - `background.js`
  - `runner.js`

## 구현 완료 내역

### 1. Stateless Download Beacon

Chrome storage 또는 인메모리 checkpoint가 사라져도 이미 다운로드된 CSV를 기준으로 완료 여부를 확인하도록 했다.

- `content/01_utils.js`
  - `Utils.checkDownloadBeacon(storeName, targetDateStr, purpose)` 추가
  - 파일명 prefix는 `coupangeats_${purpose}_${cleanStr(storeName)}` 기준
  - content script에서 `chrome.downloads.search`가 직접 불가한 경우 background message로 위임
  - 실패 시 `false`를 반환해 기존 수집 흐름을 계속 진행

- `background.js`
  - `CHECK_DOWNLOAD_BEACON` message 처리 추가
  - `chrome.downloads.search({ query:[prefix], state:'complete', orderBy:['-startTime'], limit:10 })`로 최근 완료 다운로드 확인

- `content/03_coupangeats.js`
  - CMG 다중매장 checkpoint 스킵 이후 beacon 2차 확인 추가
  - orders 다중매장 checkpoint 스킵 이후 beacon 2차 확인 추가
  - beacon 확인 시 해당 매장은 완료/건너뜀으로 처리

### 2. Runner 배지 수정

`runner.js`의 skip 배지를 완료 색상으로 변경했다.

```js
skip: ['b-ok', '건너뜀']
```

이제 이미 완료되어 건너뛴 항목이 회색 대기가 아니라 초록 완료 상태로 보인다.

### 3. 수동 Orders 자동 이어수집

수동 orders 수집에서 검증 불일치 또는 중간 실패가 발생하면 사용자가 버튼을 누르지 않아도 자동 이어수집한다.

- `_resumeOrdersRange()` 추가
- 검증 불일치 시 잔여 범위를 계산해 자동 재조회
- ESC로 사용자가 의도적으로 중단한 경우에는 기존처럼 수동 이어수집 모달 유지
- 자동 이어수집 최대 횟수는 3회

### 4. 역순 Orders 이어수집 범위 수정

쿠팡 orders 화면은 최신 날짜부터 역순으로 표시된다.

예시:

- 사용자가 `2026.06.01~2026.06.27` 조회
- 화면 표시 순서: `06.27, 06.26, ...`
- 수집 중 `06.13` 근처에서 실패

수정 전에는 `06.13~06.13` 또는 `06.01~06.13`처럼 잘못 재조회될 수 있었다.

수정 후 기준:

```text
막힌 날짜 ~ 원래 종료일
```

따라서 위 예시는 다음처럼 이어수집한다.

```text
2026.06.13 ~ 2026.06.27
```

로그 예시:

```text
⚠️ 에러 발생 → 역순 이어수집 2026.6.13 ~ 2026.6.27 재조회 시작
```

원래 시작일/종료일은 `_originalStartDateMs`, `_originalEndDateMs`로 seed에 보존해 재귀 수집 중에도 종료일이 사라지지 않게 했다.

### 5. 10056 반복 요청 제한 대응

`10056` 또는 `단시간 내 반복 요청` 문구가 감지되면 수집 실패나 `데이터 없음`으로 종료하지 않고 대기 후 재조회한다.

대기 정책:

- 1회: `55~75초` 랜덤
- 2회: `90~130초` 랜덤
- 3회 이상: `150~210초` 랜덤

관련 동작:

- 조회 버튼 클릭 전 `_waitBeforeOrdersSearch()`에서 제한 문구 감지
- 제한 감지 시 `_ordersSearchCooldownUntil` 설정
- 안정화 타임아웃, 로딩 타임아웃, 오류성 빈 페이지는 transient error로 기록
- 오류 직후 `allRows.length === 0`이어도 즉시 `데이터 없음` 팝업을 띄우지 않고 같은 범위를 자동 재조회
- 자동 orders에서 어제 조회 결과가 0건이어도 제한/불안정 상태가 감지되면 "어제 주문 없음"으로 확정하지 않고 재조회 경로로 보낸다.

### 6. 사람같은 Orders 수집 패턴

수동 orders와 자동 orders 모두 더 느리고 불규칙하게 조정했다. 자동 orders는 "어제" 단일 날짜 수집이지만, 조회 버튼 반복과 상세 펼치기 패턴은 동일하게 제한 대상이 될 수 있어 같은 안전 페이스를 적용한다.

추가된 패턴:

- 상세 펼치기 전 `scrollIntoView`
- 버튼 `mouseover`, `mousemove`, `focus` 이벤트 후 클릭
- 상세 펼치기 후 `900~2300ms` 랜덤 대기
- 주문 8~15건마다 `3.5~9초` 짧은 휴식
- 페이지 전환 후 `1.5~4.5초` 랜덤 대기
- 일부 페이지 전환 후 `6~12초` 추가 안정화 대기
- 자동/수동 orders 조회 요청 간 최소 간격 `15~25초` 랜덤 적용

목적:

- 고정 간격 반복 클릭 완화
- 페이지 안정화 실패 감소
- 단시간 반복 요청 제한 가능성 감소

## 중요 동작 기준

- 기존 checkpoint 스킵 로직은 유지한다.
- beacon은 checkpoint가 사라졌을 때를 위한 2차 fallback이다.
- beacon 실패 시에는 수집을 막지 않는다.
- `10056` 감지 직후의 빈 결과는 진짜 데이터 없음으로 보지 않는다.
- 진짜 `데이터 없음`은 제한 문구가 없고, 최근 transient error도 없을 때만 표시한다.
- 자동수집의 "어제 0건" 스킵도 제한 문구와 최근 transient error가 없을 때만 정상 처리한다.
- 수동 orders에서 오류로 자동 이어수집할 때는 `막힌 날짜~원래 종료일`을 재조회한다.
- 사용자가 ESC로 중단한 경우는 자동 재시작하지 않고 사용자 선택 흐름을 유지한다.

## 검증 완료

실행한 검증:

```powershell
node --check content/01_utils.js
node --check content/03_coupangeats.js
node --check background.js
node --check runner.js
```

추가 확인:

- UTF-8 읽기 정상
- Unicode replacement 문자 `0`
- JS nullish coalescing 연산자는 정상 코드로 확인

## 운영 확인 시나리오

1. runner.html 재시작 후 이미 다운로드된 CMG/orders 매장이 beacon으로 건너뛰어지는지 확인
2. "건너뜀" 배지가 초록 완료 상태로 표시되는지 확인
3. 수동 orders에서 `06.01~06.27` 조회 중 `06.13`에서 실패 시 `06.13~06.27`로 재조회되는지 확인
4. `10056` 발생 시 `데이터 없음` 팝업 없이 랜덤 대기 후 재조회되는지 확인
5. 자동 orders 어제 수집에서 `10056` 또는 빈 페이지가 발생해도 "0건"으로 체크포인트 저장하지 않고 재조회하는지 확인
6. 주문 상세 펼치기와 페이지 이동 간격이 고정값처럼 반복되지 않는지 로그 시간으로 확인

## 남은 주의사항

- 쿠팡의 제한 정책은 외부 서비스 정책이므로 완전 회피를 보장할 수 없다.
- `10056`이 반복되면 자동 대기 시간이 길어지므로 전체 수집 시간은 늘어난다.
- beacon은 Chrome 다운로드 기록을 기준으로 하므로, 사용자가 다운로드 기록을 지우면 beacon 스킵은 동작하지 않을 수 있다.
- 다운로드 파일명 규칙이 바뀌면 `checkDownloadBeacon()`의 prefix/date 매칭도 함께 수정해야 한다.
