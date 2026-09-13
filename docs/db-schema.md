# DB 스키마 참조

## PostgreSQL
- 접속: `doridang:doridang@host.docker.internal:5434/doridangdb` (로컬: `127.0.0.1:5434`)
- 스키마: `public`
- 테이블: `baemin_sales` (order_id PK, order_number, order_date, order_time, order_amount)

## Flow analytics parquet
- 저장 루트: `ANALYTICS_DB / "flow"` (`C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\flow`)
- 프로젝트: `flow_project/flow_project.parquet`
- 게시글/하위업무: `flow_post/project_id=<project_id>/part.parquet`
- 댓글: `flow_comment/project_id=<project_id>/part.parquet`
- 첨부 metadata: `flow_attachment/project_id=<project_id>/part.parquet`
- 첨부 원본 파일: `flow_attachment_files/project_id=<project_id>/post_id=<post_id>/...`
- 증분 state: `LOCAL_DB / "flow_posts_index.json"`이며 analytics/OneDrive가 아니라 로컬 상태 파일로 유지한다.
- 첨부 다운로드 대상 프로젝트: Airflow Variable 또는 env `FLOW_ATTACHMENT_PROJECT_IDS`에 쉼표 구분 프로젝트 ID를 입력한다.
- PostgreSQL `public.flow_project`, `public.flow_post`, `public.flow_comment`는 사용하지 않는다.

### `flow_project.parquet`
| 컬럼 | 설명 |
|------|------|
| project_id | Flow 프로젝트 ID |
| project_name | Flow 프로젝트명 |
| project_url | Flow 프로젝트 URL |
| is_store | 매장 프로젝트 여부 |
| region | 프로젝트명에서 파생한 권역 |
| store_name | 프로젝트명에서 파생한 매장명 |
| status_tag | `폐업`, `양도`, `TF` 등 접두 상태 |
| collected_at | 수집 시각 |

### `flow_post`
| 컬럼 | 설명 |
|------|------|
| post_id | Flow 게시글 또는 하위업무 post ID |
| project_id, project_name, store_name | 소속 프로젝트/매장 |
| parent_post_id, depth | 하위업무 재귀 관계 |
| title, post_date, registered_at, edited_at | 게시글 기본 정보 |
| author_name, author_id | 작성자 |
| content_text, content_hash | `outContent` 원문과 해시 |
| post_type | `방문일지`, `매장정보`, `업무`, `기타` |
| task_nm, task_status, progress, worker, start_dt, end_dt | 업무형 게시글 정보. `worker`는 쉼표 구분 다중값 |
| task_status | Flow 사용자가 프로젝트마다 직접 만드는 자유 라벨이라 고정 열거가 아니다. 분류는 `modules/transform/utility/flow_task_status.py` 참조 — 진행 업무: `진행`,`대기`,`보류`,`피드백`,`보완` / 종료: `완료` / **관찰만**(집계 제외, 기한은 감시): `모니터링` / **묶음·기록**(집계·기한 모두 제외): `업무단위`,`회의록`,`액션`. 모르는 값은 열린 업무로 본다 |
| remark_cnt, child_cnt, image_cnt, attach_cnt | 댓글/자식/첨부 개수 |
| post_url, collected_at | Flow 링크와 수집 시각 |

### `flow_comment`
| 컬럼 | 설명 |
|------|------|
| comment_id | Flow 댓글 ID |
| post_id, project_id | 소속 게시글/프로젝트 |
| author_name, author_id, written_at | 댓글 작성 정보 |
| content_text | HTML/멘션 앵커를 제거한 댓글 평문 |
| mention_names | 댓글 멘션 이름 목록 |
| is_system, sys_code, reply_cnt | 시스템 댓글 여부와 Flow 내부 코드 |
| parent_comment_id, root_comment_id, comment_depth, comment_order | 대댓글 히스토리 복원용 부모 댓글, 최상위 댓글, 깊이, 같은 부모 내 순서 |
| collected_at | 수집 시각 |

### `flow_attachment`
| 컬럼 | 설명 |
|------|------|
| attachment_id | Flow 첨부 ID |
| post_id, project_id | 소속 게시글/프로젝트 |
| attachment_type | `file` 또는 `image` |
| file_name, file_size, extension | 파일명/크기/확장자 |
| download_url, thumbnail_url | Flow 첨부 URL |
| local_path | 다운로드된 원본 파일 경로 |
| downloaded, download_error | 다운로드 성공 여부와 실패 사유 |
| content_hash | 원본 파일 SHA-256 |
| width, height | 이미지 크기 |
| registered_at, author_name, collected_at | 등록/작성/수집 정보 |

## Flow visit mart parquet
- 저장 루트: `MART_DB / "Flow_mart" / "Flow_visit"` (`C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\Flow_mart\Flow_visit`)
- 운영 override: env `FLOW_VISIT_BASE_DIR`
- 방문 로그: `flow_visit_log/project_id=<project_id>/part.parquet`
- 방문 이슈: `flow_visit_issue/project_id=<project_id>/part.parquet`
- 본사 답변: `flow_visit_followup/project_id=<project_id>/part.parquet`
- 방문 할 일: `flow_visit_todo/project_id=<project_id>/part.parquet`
- 방문 하위업무 원천: `flow_visit_subtask/project_id=<project_id>/part.parquet`
- 방문별 점주 프로필 스냅샷: `flow_visit_profile_snapshot/project_id=<project_id>/part.parquet`
- 매장 프로필: `flow_store_profile/flow_store_profile.parquet`
- RAG JSONL: `flow_visit_corpus.jsonl`
- 이슈 LLM 캐시: `LOCAL_DB / "flow_visit_llm_cache.json"`이며 OneDrive 산출물이 아니라 로컬 재실행 캐시로 유지한다.
- 점주 히스토리 프로필 캐시: `LOCAL_DB / "flow_visit_profile_cache.json"`이며 매장별 누적 digest가 같으면 재분석하지 않는다.

### `flow_visit_log`
| 컬럼 | 설명 |
|------|------|
| project_id, store_name, store_key, post_id, post_url | Flow 방문일지 출처 |
| store_rel_key, visit_rel_key | Power BI 관계용 단일 키 |
| visit_date, visit_date_source, registered_date | 방문일자와 파싱 출처, 게시글 등록일 |
| visit_purpose, author_name, content_clean, topic_table_json | 방문 목적, 작성자, 정제 본문, 룰 파서 힌트 |
| task_status, progress, task_nm, worker, start_dt, end_dt | Flow 원천 업무 상태, 진행률, 업무명, 담당자, 시작/종료일 |
| store_status_summary, owner_sentiment | 누적 점주 특성과 최근 점주 인식·태도 상태 |
| issue_cnt, followup_cnt | 이슈/본사 답변 수 |
| image_cnt, attach_cnt, content_hash | 원천 게시글 첨부/해시 정보 |
| llm_model, prompt_version, generated_at | 생성 모델, 프롬프트 버전, 생성 시각 |

### `flow_visit_issue`
| 컬럼 | 설명 |
|------|------|
| project_id, store_name, store_key, post_id, visit_date | 이슈 출처 방문 |
| store_rel_key, visit_rel_key, issue_rel_key | Power BI 관계용 단일 키 |
| issue_seq, category, issue_key, issue_label | 방문 내 순번과 taxonomy 분류 |
| owner_voice, sv_action, opinion_source | 점주 의견, 담당자 조치, 의견 출처 |
| is_request, severity, status | 요청 여부, 심각도, 처리 상태 |
| evidence, evidence_ok | 원문 근거와 근거 검증 플래그 |

### `flow_visit_followup`
| 컬럼 | 설명 |
|------|------|
| project_id, post_id, visit_date, comment_id | 답변 출처 댓글 |
| responder, reply_text, written_at | 답변자, 답변 본문, 작성 시각 |
| linked_issue_key, resolution_status | 연결된 이슈와 처리 상태 |

### `flow_visit_todo`
| 컬럼 | 설명 |
|------|------|
| todo_id | 재실행해도 동일 업무면 동일하게 생성되는 결정적 ID |
| project_id, store_name, store_key, post_id, visit_date | 할 일 출처 방문 |
| issue_key, issue_seq, todo_seq | 연결 이슈와 할 일 순번 |
| store_rel_key, visit_rel_key, issue_rel_key | Power BI 관계용 단일 키 |
| todo_list | 담당자가 실제 수행해야 할 업무 |
| todo_due_date | 방문 대화에 명시된 기한만 저장, 없으면 NULL |
| todo_owner | 원천/라우팅 근거가 있으면 담당 역할, 없으면 `담당자 미정` |
| todo_status | `대기`, `진행`, `완료`, `보류` 중 하나. 기본값은 `대기` |
| manager_request | 점주의 원 요청을 정규화한 문구 |
| analysis_evidence | 판단 근거가 되는 방문일자·게시글·짧은 근거 JSON |

### `flow_visit_subtask`
| 컬럼 | 설명 |
|------|------|
| project_id, store_name, store_key | Flow 매장 프로젝트 |
| store_rel_key, visit_rel_key | Power BI 관계용 단일 키 |
| parent_post_id, subtask_post_id, subtask_url | 방문일지 부모 글과 하위업무 글 출처 |
| direct_parent_post_id, direct_parent_title | Flow 원천 기준 바로 위 부모 업무 |
| subtask_depth, subtask_path_titles | 방문일지 기준 하위업무 계층 깊이와 제목 경로 |
| visit_date | 부모 방문일지의 방문일자 |
| registered_date, registered_time, author_name | 하위업무 작성일자, 작성 시간, 작성자 |
| title, task_status, start_dt, end_dt, task_nm, worker | 하위업무 제목, 상태, 시작일, 마감일, 업무명, 담당자 |
| content_text | 하위업무 원문 |
| comment_group, comment_author, comment_text | 하위업무 댓글 구분, 댓글작성자, 댓글내용 |
| comment_history_json | 원댓글 작성자 기준 댓글 히스토리 그룹과 시스템 이벤트 JSON |
| generated_at | 마트 생성 시각 |

`comment_group`은 `일반댓글`, `상태변경`, `시작일변경`, `마감일변경`, `제목변경`, `시스템댓글`, `대댓글있음`을 댓글 원천 필드 기준으로 조합한다.
`comment_history_json`은 `groups`와 `system_events`를 담으며, `groups`는 최상위 일반 댓글 작성자별로 원댓글과 답글을 시간순 보존한다.

### `flow_visit_profile_snapshot`
| 컬럼 | 설명 |
|------|------|
| project_id, store_name, store_key, post_id, visit_date | 스냅샷 출처 방문 |
| store_rel_key, visit_rel_key | Power BI 관계용 단일 키 |
| visit_seq_in_month | 같은 월 내 방문 순번 |
| period_start, period_end | 해당 방문이 대표하는 월내 기간. `period_start=max(월초, 같은 달 이전 방문 다음날)`, `period_end=visit_date` |
| profile_as_of_date, visit_cnt_as_of | 해당 방문 시점 프로필 기준일과 누적 방문 수 |
| owner_status, store_status_summary | 해당 방문 시점의 점주 상태와 누적 점주 특성 |
| key_concerns_json, handling_points_json, manager_memo | 해당 방문 시점의 최근 화두, 현재 문제·고민(화두의 부분집합), 명시 요청사항 |
| open_issues_json, recurring_issues_json, category_counts_json | 해당 방문 시점까지의 미해결/반복/카테고리 집계 |
| next_visit_action, analysis_evidence_json | 다음 방문 액션과 근거 |
| llm_model, prompt_version, schema_version, generated_at | 생성 모델, 프롬프트/스키마 버전, 생성 시각 |

### 점주 히스토리 프로필 생성
- 기본값은 `FLOW_VISIT_PROFILE_PROVIDER=off`이며 기존 규칙 기반 프로필을 생성한다.
- `FLOW_VISIT_PROFILE_PROVIDER=local`이면 매장별 누적 `history_digest`를 로컬 Ollama `gpt-oss` JSON 프롬프트로 합성한다. 프로필 합성에서는 qwen 계열을 사용하지 않는다.
- `FLOW_VISIT_PROFILE_PROVIDER=openai`이면 `FLOW_VISIT_PROFILE_OPENAI_MODEL` 모델로 프로필 합성을 시도한다. `openai` 패키지와 API 키가 없거나 호출이 실패하면 규칙 기반 결과로 대체한다.
- 프로필 캐시 키는 `project_id + store_key + visit_history_hash + prompt_version + schema_version + provider + model` 기준이다.
- `owner_status`/`owner_sentiment`에는 점수제 표현을 저장하지 않는다.
- LLM이 반환한 todo도 `todo_id`, 관계키, 상태값, 기한 형식은 코드에서 다시 검증해 저장한다.
- local provider는 prompt 길이를 줄이기 위해 후보 ID 기반 압축 digest를 사용하며, 최근 화두·요청사항·todo는 코드 기준 산출을 우선한다.

#### 이슈 요약 생성
- `issue_key` 분류는 규칙(`_rule_class_result`)이 담당한다. alias 점수가 하나라도 걸리면 LLM을 부르지 않는다.
- **요약은 로컬 gpt-oss가 생성한다.** `build_summary_prompt`가 `owner_summary` / `sv_summary` / `issue_label` / `is_concern` / `next_action`을 한 번에 받는다. 세그먼트당 1회 호출이며 결과는 `flow_visit_llm_cache.json`에 `s1|...` 키로 캐시된다.
- 상한은 분류와 분리한다. `FLOW_VISIT_LLM_MAX_SEGMENTS`(기본 12)는 분류용, `FLOW_VISIT_SUMMARY_MAX_SEGMENTS`(기본 400)는 요약용이다.
- 요약 실패는 `MAX_FALLBACK_RATIO` 가드에 반영하지 않는다. 그 가드는 분류 실패 지표다.
- 요약 폴백 3단: LLM이 쓴 요약 → (구 캐시의) 문장 번호 선택 → 원문 첫 절 80자.
- `owner_summary` / `sv_summary`는 각 60자 이내다. 원문 전체가 필요하면 `owner_voice_raw` / `sv_action_raw`를 쓴다.

#### 화두와 문제·고민의 관계 (불변식)
`analysis_evidence`가 화두의 정본이며, 나머지 두 리스트는 여기서 파생한다.

- `key_concerns`는 `analysis_evidence` 각 행의 `concern`과 1:1이다. 행 수와 순서가 같다. 위치(index) 기반 정렬은 쓰지 않고 `issue_rel_key`로 연결한다.
- `handling_points`는 `key_concerns`의 **부분집합**이며 화두 문구를 그대로 쓴다. 액션문으로 바꾸지 않는다.
- **문제·고민 = 점주가 곤란해하는 것.** 판단 기준은 "점주가 곤란해하는가"이지 "본사가 할 일이 있는가"가 아니다. 요청·희망·관심(신메뉴 추가 희망)과 본사가 먼저 꺼낸 안내·교육·재공지 같은 내부 과제는 화두로만 남고 문제·고민에서 빠진다. 화두 10건 중 문제·고민이 5건일 수 있다.
- 판정 우선순위: ① `status`가 `해결`/`안내완료`면 무조건 아님 → ② 요약 LLM의 `is_concern` → ③ LLM이 안 돌았으면 신호 규칙. 신호 규칙은 불편·불만·우려·부담과 품질/납기 표현만 보며, `is_request`만으로는 고민으로 잡지 않는다. 결과는 `analysis_evidence.is_problem`에 남는다.
- 다음 방문 액션문은 `analysis_evidence.action_hint`와 `next_visit_action`/`handover_summary`에만 둔다.
- `action_hint`는 요약 LLM이 쓴 `next_action` → 매핑된 `_handling_text` → 공란 순이다. `_handling_text(issue, allow_generic=False)`는 매핑에 없는 이슈에 `"OO 처리 상태 확인"` 같은 라벨 반복 문구를 만들지 않는다.
- **없다는 사실을 글자로 채우지 않는다.** 본사 답변·담당자 조치·후속 댓글·남은 확인이 없으면 해당 컬럼은 빈 문자열이다. `"기록 없음"`, `"본사 답변 없음"`, `"후속 댓글 없음"` 같은 채움말을 만들지 않는다.
- 내용이 없는 세그먼트(`요청사항-없음`, `특이사항 없음`, `없음. ALL`)는 화두로 만들지 않는다. 화두가 아니면 문제·고민도 아니다.
- 화두 문구에는 목차 번호·제목 접두(`5. `, `1. 매장현황 - `)를 남기지 않는다.
- `flow_visit_viz`에서 문제·고민 여부는 `concern_is_problem`이 정본이다. `followup_summary`가 `NULL`인 행은 문제·고민이 아니며, `followup_summary.notna()`와 `concern_is_problem`은 **항상 일치해야 한다**(테스트로 고정). 대시보드가 `followup_summary IS NOT NULL`로 문제·고민을 거르므로 이 규약을 깨면 화두 전부가 고민으로 잡힌다.
- `issue_problem_detail`은 문제·고민이 아닌 화두 행에도 내용이 들어간다. **판정에 쓰지 않는다.** `key_concerns`의 어떤 화두에 연결된 이슈인지는 `concern_text`로 본다.
- 6개 핵심 표시 컬럼은 서로 대체하지 않는다. `owner_status`는 "지금 어떤 상태인가", `store_status_summary`는 "여러 번 만나보니 어떤 특징인가", `key_concerns`는 "이번 방문에서 무슨 이야기를 했는가", `handling_points`는 "그중 지금 해결해야 할 문제는 무엇인가", `handover_summary`는 "다른 담당자가 무엇을 알아야 하는가", `next_visit_action`은 "그래서 다음 방문에서 무엇을 해야 하는가"에 답한다.

#### 표시 필드 규약
같은 값을 `"문제: … / 점주 의견: … / 상태: …"` 처럼 한 문자열로 다시 이어붙이지 않는다. 이미 독립 컬럼이 있는 값은 컬럼으로 쓴다.

| 컬럼 | 내용 |
|------|------|
| `owner_summary` / `sv_summary` | 점주 의견 / 담당자 조치 요약. 각 60자 이내. 표의 "핵심 요약"은 이 둘을 나란히 쓴다 |
| `owner_voice_raw` / `sv_action_raw` | 원문 그대로 |
| `store_status_summary` / `owner_status` | 물리 컬럼명은 유지하되 화면에서는 각각 "가맹점 특징·성향", "현재 가맹점 상태"로 표시한다 |
| `issue_problem_detail` | 이슈 내용 한 줄(120자). 점주 발언 → 담당자 조치 → 원문 순으로 채운다. 문제·고민이 아닌 화두 행에도 내용이 들어간다 |
| `followup_reply` / `followup_state` / `followup_next` | 본사 답변 / 상태 / 남은 확인. 문제·고민이 아닌 행은 셋 다 공란 |
| `followup_summary` | **문제·고민 행에만** `issue_problem_detail`과 같은 값을 담고, 아니면 `NULL` |
| `concern_text` / `concern_is_problem` | 이 이슈에 대응하는 `key_concerns` 화두 문구와 문제·고민 여부. `concern_text`는 에이전트 최종 문장에 노출하지 않더라도 내부 연결용으로 유지한다 |
| `followup_cnt` | 해당 이슈에 연결된 후속답변 수 |

`concern_is_problem`은 프로필의 `analysis_evidence`에서만 온다. `issue_rel_key`로 잇고, 같은 화두로 병합된 형제 이슈는 `issue_key`로 물려받는다. **`status`로 폴백하지 않는다** — `status` 기본값이 `미해결`이라 폴백을 두면 전 행이 문제·고민이 되어 부분집합 관계가 다시 무너진다. 화두로 잡히지 않은 이슈(내용 없음 등)는 `concern_text`가 비고 문제·고민도 아니다. 대시보드에서 화두만 보고 싶으면 `concern_text`가 있는 행으로 거른다.

### `flow_store_profile`
| 컬럼 | 설명 |
|------|------|
| project_id, store_name, store_key, store_rel_key, last_visit_date, visit_cnt | 매장과 방문 이력 요약 |
| owner_status, manager_memo, handover_summary | 최근 점주 인식·태도 상태, 명시 요청사항, 방문 전 요약 |
| key_concerns_json, handling_points_json, open_issues_json | 최근 방문 화두, 현재 문제·고민(화두의 부분집합), 미해결 이슈 JSON |
| recurring_issues_json, category_counts_json | 반복 이슈와 카테고리 집계 JSON |
| next_visit_action, analysis_evidence_json | 다음 방문 응대 순서와 분석 근거 JSON |
| llm_model, prompt_version, schema_version, generated_at | 생성 모델, 프롬프트/스키마 버전, 생성 시각 |

### Flow visit 관계키
Power BI 복합키 관계 제약을 피하기 위해 코드에서 단일 문자열 키를 생성한다.

| 키 | 생성 규칙 |
|----|-----------|
| store_rel_key | `project_id + "|" + store_key` |
| visit_rel_key | `project_id + "|" + post_id` |
| issue_rel_key | `project_id + "|" + post_id + "|" + issue_seq` |

## 함수
```python
# DB 저장 (pk_col 기준 중복 제외)
from modules.transform.utility.db import postgre_db_save
postgre_db_save(df, table, schema="public", pk_col="order_id", if_exists="append")

# DB 조회
from modules.transform.utility.db import read_table
read_table(table, schema="public", columns=None, where=None, order_by=None)

# OneDrive CSV 저장/병합/백업
from modules.transform.utility.onedrive import save_to_onedrive_csv, merge_to_onedrive, backup_to_onedrive
```

## 경로 상수 (paths.py)
| 상수 | 용도 |
|------|------|
| COLLECT_DB | 수집 데이터 (OneDrive/Docker 자동감지) |
| LOCAL_DB | C:/Local_DB (충돌 방지) |
| ONEDRIVE_DB | OneDrive Repository |
| TEMP_DIR | 임시 parquet |
| ANALYTICS_DB | 분석/집계 데이터 |
| FLOW_VISIT_TODO_PARQUET | Flow 방문일지 할 일 fact parquet |
| FLOW_VISIT_PROFILE_SNAPSHOT_PARQUET | Flow 방문일지 방문별 점주 프로필 스냅샷 parquet |
| FLOW_VISIT_PROFILE_CACHE | Flow 방문일지 매장별 히스토리 프로필 LLM 캐시 JSON |
