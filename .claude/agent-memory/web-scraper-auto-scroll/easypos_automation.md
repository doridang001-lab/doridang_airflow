---
name: EasyPOS NexacroN 자동화 패턴
description: EasyPOS(NexacroN 기반) 로그인부터 당일매출내역 조회, 영수증 팝업까지 검증된 Playwright 자동화 패턴
type: project
---

## EasyPOS 자동화 검증된 패턴 (2026-04-11)

**URL**: https://smart.easypos.net/index.jsp

### 핵심 원칙: NexacroN pointer events 우회
NexacroN 컴포넌트는 `element.click()`이 타임아웃됨 (다른 div가 pointer events 가로챔).
**반드시 `bounding_box()` + `page.mouse.click()` 절대좌표 방식 사용.**

```python
def mouse_click_el(page, el):
    box = el.bounding_box()
    if box:
        page.mouse.click(box['x'] + box['width']/2, box['y'] + box['height']/2)
        return True
    return False
```

### 팝업 처리 순서 (로그인 직후 2개 팝업)
1. **비밀번호 변경 안내 팝업** - 닫기 버튼 ID:
   `#mainframe_childframe_popupChangePasswd_form_div_popup_bottom_btnClose`
   - visible=True지만 element.click() 실패 → mouse.click(bounding_box) 성공
   - 타이틀바 X 버튼도 사용 가능: `#mainframe_childframe_popupChangePasswd_titlebar_closebutton`

2. **광고 팝업 (placeAdPopup)** - 닫기 버튼 ID:
   `#mainframe_childframe_placeAdPopup_titlebar_closebutton`

### 메뉴 네비게이션
- 탑메뉴 "영업속보": `#mainframe_childframe_form_divTop_img_TA_top_menu2`
- 좌측 메뉴 row0 (영업속보 헤더 - 클릭 시 하위 펼침):
  `#mainframe_childframe_form_divLeftMenu_divLeftMainList_grdLeft_body_gridrow_0`
- 당일매출내역 = row1 (영업속보 펼친 후):
  `#mainframe_childframe_form_divLeftMenu_divLeftMainList_grdLeft_body_gridrow_1`

### 전일/조회 버튼
- 전일: `#mainframe_childframe_form_divMain_divWork_divSalesDate_btnBeforeDay`
- 조회: `#mainframe_childframe_form_divMain_divMainNavi_divCommonBtn_btnCommSearch`

### 영수증 버튼 ID 패턴
그리드: `grdSalePerDayList`, 2번 컬럼에 영수증 버튼 위치
```
mainframe_childframe_form_divMain_divWork_grdSalePerDayList_body_gridrow_{N}_cell_{N}_2_controlbutton
```
- `endswith("_controlbutton")` 필터링 필요 (TextBoxElement 제외)
- `"_2_controlbutton"` 포함 여부로 2번 컬럼(영수증) 필터

### 영수증 팝업
- 팝업 타이틀: "영수증"
- 내용: 정상영수증 (매장명, 테이블, 날짜, 품목, 결제수단, 카드정보)
- 닫기 버튼 텍스트: "닫기"

### Why:
NexacroN은 자체 렌더링 레이어 위에 투명 div를 얹어서 pointer events를 캡처함.
Playwright/Selenium의 일반 click()은 이 레이어에 막힘.
bounding_box로 절대좌표 계산 후 page.mouse.click()으로 OS 레벨 클릭 필요.

### How to apply:
EasyPOS 자동화 코드 작성 시 모든 click() 호출을 mouse_click_el() 헬퍼로 교체.
팝업 2개 닫기 → 탑메뉴 → row0(헤더) → row1(당일매출) 순서 고정.
