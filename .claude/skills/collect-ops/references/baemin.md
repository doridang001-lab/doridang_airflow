# 배민(배달의민족) Selenium 수집

## 대전제
- 배민은 **API 직수집 불가** → Selenium 안정화가 유일한 방향. requests/REST 시도 금지.
- 관련 MEMORY: `[[baemin-collection-direction]]`, `[[baemin-now-store-select-fix]]`

## 알려진 함정과 해법
1. **NOW 매장선택 not interactable** — 드롭다운 클릭이 불안정 → **URL 직접 이동 방식으로 전환**. 매장선택 후 복구마커 등록.
2. **렌더러 타임아웃 누적** — 장시간 세션에서 크롬 렌더러가 느려지며 누적 실패 → 주기적 드라이버 재시작/복구.
3. **드라이버 복구** — 복구 로직 공통화 + **fail-fast**(복구 실패 시 즉시 중단).
4. **60계정 확장** — 계정별 격리·순차 처리, 계정 단위 실패가 전체를 막지 않도록.

## 참조
- `modules/extract/croling_beamin.py` — 배민 크롤러 본체
- `harness/baemin_macro.md` — 운영 지식
- DAG: `dags/db/DB_Beamin_Macro_Dags.py`, `DB_Beamin_Macro_Dags_Retry.py`, `DB_Beamin_Macro_Pc2_Dags.py`
- `docs/codex/crawling-gotchas.md`
