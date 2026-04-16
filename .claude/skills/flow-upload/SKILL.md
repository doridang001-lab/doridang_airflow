---
name: flow-upload
description: |
  Flow(협업툴) 프로젝트에 리포트를 자동 게시하는 DAG/파이프라인 설계 전용 skill.
  사용자가 "Flow에 올려줘", "Flow 리포트 자동 게시", "Flow 하위업무 생성", "Flow 게시물 자동화" 등을 언급할 때 반드시 사용한다.

  핵심 역할: Selenium Flow 자동화의 두 가지 모드(게시글/하위업무)를 검증된 코드로 제공한다.
  이 skill 없이 Flow 자동화를 작성하면 반드시 아래 문제 중 하나가 발생한다:
  - 프로젝트 목록 로딩 전에 클릭 → StaleElementException
  - CKEditor에 send_keys 사용 → ElementNotInteractable
  - 하위업무 input readonly 상태에서 입력 → 무반응
  - 등록 버튼이 스크롤 밖에 있어서 클릭 실패
  - CKEditor 인스턴스 이름 혼동 (postContents vs 마지막 인스턴스)
---

# Flow Upload Skill — Flow 자동화 DAG 설계 전용

## 레퍼런스 파일

| 모드 | 파일 |
|------|------|
| 하위업무 추가 | `dags/sales/Sales_Orders_09_FlowReport_Dags.py` |
| 게시글 작성 | `dags/strategy/Strategy_FdamCS_02_FlowMacro_Dags.py` |
| 브라우저 런처/로그인 | `modules/transform/pipelines/sales/SMD_sales_visit_log_01_crawling.py` |

```python
from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import launch_browser, do_login
```

---

## 모드 선택 기준

| 질문 | 선택 |
|------|------|
| 기존 프로젝트에 날짜별 업무 항목을 쌓는가? | **하위업무 모드** (SMD_09 패턴) |
| 프로젝트 게시판(글 탭)에 새 글을 올리는가? | **게시글 모드** (FdamCS_02 패턴) |

---

## 모드 A: 게시글 작성 (FdamCS_02 패턴)

프로젝트 > 글 탭 > 새 글 작성 (제목 input + CKEditor)

```python
driver = launch_browser()
driver.set_window_size(1920, 1080)
if not do_login(driver): raise RuntimeError("Flow 로그인 실패")
wait = WebDriverWait(driver, 20)

# ① 내 프로젝트 클릭
my_project = wait.until(EC.element_to_be_clickable(
    (By.CSS_SELECTOR, "li[data-code='project'].my-project")
))
driver.execute_script("arguments[0].click();", my_project)
time.sleep(2)

# ② 프로젝트 클릭 (presence + scrollIntoView — element_to_be_clickable은 스크롤 밖 타임아웃 발생)
project_item = wait.until(EC.presence_of_element_located(
    (By.CSS_SELECTOR, f"li#project-{FLOW_PROJECT_SRNO}")
))
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", project_item)
time.sleep(0.5)
driver.execute_script("arguments[0].click();", project_item)
time.sleep(2)

# ③ 글 탭 클릭 → 글쓰기 폼 진입
post_filter = wait.until(EC.element_to_be_clickable(
    (By.CSS_SELECTOR, "li.post-filter[data-post-code='91']")
))
driver.execute_script("arguments[0].click();", post_filter)
time.sleep(2.5)  # 에디터 렌더링 대기 (2.5초 미만 불안정)

# ④ 제목 입력 (send_keys 가능 — 일반 input)
title_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#postTitle")))
title_input.clear()
title_input.send_keys(title)
time.sleep(0.5)

# ⑤ CKEditor 본문 입력 (named instance: 'postContents')
ckeditor_ready = False
for _ in range(15):
    try:
        if driver.execute_script(
            "return !!(typeof CKEDITOR !== 'undefined'"
            " && CKEDITOR.instances"
            " && CKEDITOR.instances['postContents']"
            " && CKEDITOR.instances['postContents'].status === 'ready');"
        ):
            ckeditor_ready = True
            break
    except Exception:
        pass
    time.sleep(1)

if ckeditor_ready:
    driver.execute_script("CKEDITOR.instances['postContents'].setData(arguments[0]);", body_html)
else:
    # fallback: iframe 직접
    cke_iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")))
    driver.switch_to.frame(cke_iframe)
    driver.execute_script("document.body.innerHTML = arguments[0];", body_html)
    driver.switch_to.default_content()
time.sleep(0.5)

# ⑥ 등록 클릭
submit_btn = wait.until(EC.presence_of_element_located(
    (By.CSS_SELECTOR, "button.js-complete-btn.create-button.create-post-submit")
))
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
time.sleep(0.5)
driver.execute_script("arguments[0].click();", submit_btn)
time.sleep(3)
```

---

## 모드 B: 하위업무 추가 (SMD_09 패턴)

프로젝트 > 하위업무 목록 > + 하위업무 추가 (제목 입력 후 클릭 → 설정 → 수정 → CKEditor)

```python
driver = launch_browser()
driver.set_window_size(1920, 1080)
if not do_login(driver): raise RuntimeError("Flow 로그인 실패")
wait = WebDriverWait(driver, 20)

# ① 내 프로젝트 클릭
my_project = wait.until(EC.element_to_be_clickable(
    (By.CSS_SELECTOR, "li[data-code='project'].my-project")
))
driver.execute_script("arguments[0].click();", my_project)
time.sleep(3)

# 프로젝트 목록 로딩 대기 (필수 — 생략하면 StaleElement)
WebDriverWait(driver, 15).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "li[id^='project-']"))
)

# 사이드바 스크롤 (목록이 길면 대상이 뷰포트 밖)
driver.execute_script(f"""
    var target = document.querySelector('li#project-{FLOW_PROJECT_SRNO}');
    if (target) target.scrollIntoView({{block: 'center'}});
""")
time.sleep(1)

# ② 프로젝트 클릭
project_item = WebDriverWait(driver, 20).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, f"li#project-{FLOW_PROJECT_SRNO}"))
)
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", project_item)
driver.execute_script("arguments[0].click();", project_item)
time.sleep(5)  # 콘텐츠 렌더링 대기 (5초 — 4초 미만 불안정)

# 콘텐츠 로딩 확인
wait.until(EC.presence_of_element_located(
    (By.CSS_SELECTOR, ".subtask-list, .post-list, .task-list, .js-add-subtask-button")
))
time.sleep(2)

# ③ 페이지 하단 스크롤 → + 하위업무 추가 버튼 클릭
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(2)
add_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.js-add-subtask-button")))
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", add_btn)
driver.execute_script("arguments[0].click();", add_btn)
time.sleep(2)

# ④ 하위업무 제목 입력 (readonly가 아닌 input — JS 이벤트만 동작, send_keys 금지)
subtask_input = wait.until(lambda d: next(
    (el for el in d.find_elements(By.CSS_SELECTOR, "input.js-subtask-input")
     if not el.get_attribute("readonly")),
    None
))
driver.execute_script("""
    var el = arguments[0], title = arguments[1];
    el.scrollIntoView({block: 'center'});
    el.focus();
    el.value = title;
    el.dispatchEvent(new Event('input', {bubbles: true}));
    el.dispatchEvent(new Event('change', {bubbles: true}));
    el.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
    el.dispatchEvent(new KeyboardEvent('keypress',{key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
    el.dispatchEvent(new KeyboardEvent('keyup',  {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
""", subtask_input, today_title)
time.sleep(3)

# ⑤ 생성된 하위업무 클릭 (p.subtask__tit--display에서 제목으로 찾기)
target_subtask = next(
    (el for el in driver.find_elements(By.CSS_SELECTOR, "p.subtask__tit--display")
     if today_title in el.text),
    None
)
if not target_subtask:
    raise RuntimeError(f"하위업무 '{today_title}' 찾기 실패")
driver.execute_script("arguments[0].click();", target_subtask)
time.sleep(2)

# ⑥ 설정(점3개) → ⑦ 수정
setting_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.js-setting-button.set-btn")))
driver.execute_script("arguments[0].click();", setting_btn)
time.sleep(1)
modify_item = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li.js-setting-item[data-code='modify']")))
driver.execute_script("arguments[0].click();", modify_item)
time.sleep(2)

# ⑧ CKEditor 본문 입력 (unnamed — 마지막 인스턴스 사용)
ckeditor_ready = False
for _ in range(15):
    try:
        if driver.execute_script("""
            if (typeof CKEDITOR==='undefined' || !CKEDITOR.instances) return false;
            var keys = Object.keys(CKEDITOR.instances);
            return keys.length > 0 && CKEDITOR.instances[keys[keys.length-1]].status==='ready';
        """):
            ckeditor_ready = True
            break
    except Exception:
        pass
    time.sleep(1)

if ckeditor_ready:
    driver.execute_script("""
        var keys = Object.keys(CKEDITOR.instances);
        CKEDITOR.instances[keys[keys.length-1]].setData(arguments[0]);
    """, body_html)
else:
    cke_iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")))
    driver.switch_to.frame(cke_iframe)
    driver.execute_script("document.body.innerHTML = arguments[0];", body_html)
    driver.switch_to.default_content()
time.sleep(1)

# ⑨ 등록 클릭
submit_btn = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "button.js-complete-btn.confirm")))
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
time.sleep(0.5)
driver.execute_script("arguments[0].click();", submit_btn)
time.sleep(3)
```

---

## CKEditor 인스턴스 이름 차이

| 상황 | 인스턴스 접근 방식 |
|------|-------------------|
| 게시글 작성 (`postContents`) | `CKEDITOR.instances['postContents']` |
| 하위업무 수정 (이름 없음) | `CKEDITOR.instances[Object.keys(CKEDITOR.instances)[keys.length-1]]` |

---

## HTML 리포트 빌드 — 두 가지 구조

### 리스트형 (하위업무 본문, SMD_09)

```python
def _build_flow_report_html(df, now_kst) -> str:
    parts = ['<div class="post-editor-wrap">']
    parts.append('<h1>섹션명</h1>')
    for manager, grp in df.groupby('담당자', sort=True):
        parts.append(f'<h2>{manager}</h2><ul>')
        for _, row in grp.iterrows():
            parts.append(f'<li>{row["매장명"]}<ul><li>세부내용</li></ul></li>')
        parts.append('</ul><div>&nbsp;</div>')
    # 링크
    parts.append(f'<div><a target="_blank" class="blue js-hyper-button urllink" href="{URL}">{URL}</a>&nbsp;</div>')
    parts.append('</div>')
    return '\n'.join(parts)
```

**규칙**: 래퍼 `<div class="post-editor-wrap">`, 빈 줄 `<div>&nbsp;</div>`, 링크 class 필수

### 표형 (게시글 본문, FdamCS_02)

```python
_th = 'padding:8px;text-align:left;border:1px solid #dee2e6;background:#f8f9fa;'
_td = 'padding:8px;font-size:12px;border:1px solid #dee2e6;'

body_html = f"""
<p style="font-size:13px;">기준일: <strong>{today_str}</strong> | 총 <strong>{n}건</strong></p>
<table style="border-collapse:collapse;width:100%;border:1px solid #dee2e6;">
  <thead><tr>
    <th style="{_th}">컬럼1</th>
    <th style="{_th}">컬럼2</th>
  </tr></thead>
  <tbody>{row_html}</tbody>
</table>"""
```

**규칙**: `html_escape()` 필수, `white-space:nowrap` / `word-break:break-word` 적절히 사용

---

## 날짜 제목 헬퍼

```python
import pendulum
WEEKDAY_KR = {0:'월', 1:'화', 2:'수', 3:'목', 4:'금', 5:'토', 6:'일'}

def _get_today_title(now_kst) -> str:
    """예: 26.03.17(화)"""
    return f"{now_kst.strftime('%y.%m.%d')}({WEEKDAY_KR[now_kst.weekday()]})"

now_kst = pendulum.now("Asia/Seoul")
today_str = now_kst.to_date_string()    # "2026-03-17"
today_title = _get_today_title(now_kst) # "26.03.17(화)"
```

---

## GSheet 로드 패턴

```python
from modules.extract.extract_gsheet import extract_gsheet

CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

df = extract_gsheet(url=GSHEET_URL, sheet_name="시트1", credentials_path=CREDENTIALS_PATH)
if df is None or df.empty:
    return "스킵 (데이터 없음)"

# 빈 행 제거
df = df[df['매장명'].astype(str).str.strip().replace('nan', '').ne('')].copy()

# 최신 수집날짜만 추출
if '수집날짜' in df.columns:
    dates = df['수집날짜'].astype(str).str[:10]
    latest_date = dates[dates.str.match(r'^\d{4}-\d{2}-\d{2}$')].max()
    df = df[dates == latest_date].copy().reset_index(drop=True)
```

---

## DAG 구조

```python
with DAG(
    dag_id=filename.replace('.py', ''),
    schedule=SCHEDULE_CONST,   # schedule.py 상수 (하드코딩 금지)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False, max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5), ...},
    tags=['flow', 'report'],
) as dag:

    wait_sensor = ExternalTaskSensor(
        task_id='wait_for_prev',
        external_dag_id='선행_DAG_id',
        external_task_id='선행_task_id',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='reschedule',
        timeout=3600, poke_interval=60,
        check_existence=True,
        soft_fail=True,          # 센서 실패 → skipped 처리, downstream 계속 실행
    )

    t_post = PythonOperator(
        task_id='post_to_flow',
        python_callable=post_to_flow,
        execution_timeout=timedelta(minutes=15),
        trigger_rule=TriggerRule.NONE_FAILED,  # soft_fail=True와 반드시 세트
    )

    wait_sensor >> t_post
```

---

## FLOW_PROJECT_SRNO 확인

Flow 프로젝트 사이드바 `li#project-{번호}` 에서 확인.

| 프로젝트 | SRNO |
|---------|------|
| [영업관리부] 영업관리 DX/AX 전환 | `2833856` |
| [물류] 프담CS 알림 BOT | `2830044` |

---

## instructions

1. 어떤 데이터를 Flow에 올리는지 파악 (GSheet? XCom? DB?)
2. **게시글 vs 하위업무** 모드 결정 → 맞는 패턴 선택
3. HTML 빌드 함수 작성 (리스트형 or 표형)
4. Selenium 패턴 복사 → FLOW_PROJECT_SRNO만 교체
5. `try/finally: driver.quit()` 반드시 포함
6. DAG 파일 위치: 영업 → `dags/sales/`, 전략 → `dags/strategy/`

## 절대 금지

- 하위업무 input에 `send_keys` 사용 (JS 이벤트 디스패치만 동작)
- 게시글/하위업무 모드 CKEditor 인스턴스 이름 혼용
- `time.sleep(5)` 미만으로 프로젝트 페이지 로딩 대기 (하위업무 모드)
- `trigger_rule=TriggerRule.NONE_FAILED` 없이 `soft_fail=True` 단독 사용
