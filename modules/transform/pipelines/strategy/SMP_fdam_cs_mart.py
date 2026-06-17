"""
CS Issue Mart 파이프라인

구글시트 원본 CS 데이터 (접수번호별 여러 행)를
접수번호 1개 = 1행의 Power BI용 마트 DataFrame으로 변환한다.

출력 컬럼:
  접수번호, 등록월, 등록월표시, 등록월정렬, 등록일, 처리완료일,
  최종진행상태, 처리완료여부, 처리소요기간, 매장명, 유형, 매입처,
  이슈유형, CS구분, issue_type, issue_summary, 담당자

저장:
  MART_DB / "fdam_cs_report" / "cs_issue_mart.csv"

Usage:
  from modules.transform.pipelines.strategy.SMP_fdam_cs_mart import (
      build_cs_issue_mart,
      validate_mart,
  )
"""

import datetime as _dt
import pandas as pd
import numpy as np
import re
from pathlib import Path

# 영문 월 매핑 (locale 의존 없이 직접 매핑)
_MONTH_EN = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
    5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec',
}


def _has_text(value) -> bool:
    return pd.notna(value) and str(value).strip() != ''


def _extract_memo_field(value, field_name: str) -> str:
    """비공개메모에서 '필드명: 값' 형식의 한 줄 값을 추출한다."""
    if not _has_text(value):
        return ''
    text = str(value).replace('\r\n', '\n').replace('\r', '\n')
    pattern = rf'(?m)^\s*{re.escape(field_name)}\s*[:：]\s*([^\n]*)'
    match = re.search(pattern, text)
    if not match:
        return ''
    return match.group(1).strip()


def _parse_legacy_memo_date(value, base_dt) -> pd.Timestamp:
    """과거 처리내용(내용.1)에 남은 날짜 마커를 비공개메모 입력일 fallback으로 사용."""
    if not _has_text(value):
        return pd.NaT
    text = str(value)
    m = re.search(r'(?<!\d)(20\d{2})[-./](\d{1,2})[-./](\d{1,2})(?!\d)', text)
    if m:
        return pd.to_datetime(f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", errors='coerce')
    m = re.search(r'(?<!\d)(\d{2})[.](\d{1,2})[.](\d{1,2})(?!\d)', text)
    if m:
        return pd.to_datetime(f"20{int(m.group(1)):02d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", errors='coerce')
    if pd.isna(base_dt):
        return pd.NaT
    m = re.search(r'(?<!\d)(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)', text)
    if m:
        return pd.to_datetime(f"{base_dt.year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}", errors='coerce')
    return pd.NaT


def _infer_registration_date(row) -> pd.Timestamp:
    """등록일 누락 시 원문에 있는 '14일자' 같은 표현을 수집월 기준으로 복원."""
    raw = pd.to_datetime(row.get('_reg_dt_raw'), errors='coerce')
    if pd.notna(raw):
        return raw
    base_dt = pd.to_datetime(row.get('_collect_dt'), errors='coerce')
    if pd.isna(base_dt) or not _has_text(row.get('내용')):
        return pd.NaT
    m = re.search(r'(?<!\d)(\d{1,2})\s*일자', str(row.get('내용')))
    if not m:
        return pd.NaT
    return pd.to_datetime(f"{base_dt.year}-{base_dt.month:02d}-{int(m.group(1)):02d}", errors='coerce')

# 마트 출력 컬럼 순서 (최종)
MART_COLUMNS = [
    '접수번호', '등록월', '등록월표시', '등록월정렬', '등록일',
    '처리완료일', '최종진행상태', '처리완료여부', '처리소요기간',
    '매장명', '유형', '매입처', '이슈유형', 'CS구분', 'issue_type', 'issue_summary', '담당자',
]


def build_cs_issue_mart(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    구글시트 원본 DataFrame → cs_issue_mart DataFrame (접수번호별 1행)

    Parameters
    ----------
    df_raw : pd.DataFrame
        구글시트 ws.get_all_records()로 로드한 원본 데이터.
        필수 컬럼: 접수번호, 등록일, 처리일자, 진행상태, 매장명, 유형, 매입처,
                   비공개메모, CS구분, issue_type, issue_summary, 담당자, uploaded_at

    Returns
    -------
    pd.DataFrame  (MART_COLUMNS 순서, 접수번호별 1행)
    """
    if df_raw is None or df_raw.empty:
        print("[cs_issue_mart] 원본 데이터 없음 → 빈 DataFrame 반환")
        return pd.DataFrame(columns=MART_COLUMNS)

    df = df_raw.copy()
    print(f"[cs_issue_mart] 원본: {len(df)}건, 접수번호 {df['접수번호'].nunique()}개 (중복 포함)")

    # ── 1. 타입 정규화 ─────────────────────────────────────────
    # 접수번호: 문자열 정규화 (공백 제거)
    df['접수번호'] = df['접수번호'].astype(str).str.strip()
    # 공백 접수번호 제거
    df = df[df['접수번호'].str.len() > 0].copy()

    # uploaded_at → datetime (최신 행 탐색용)
    df['_uploaded_at_dt'] = pd.to_datetime(df.get('uploaded_at', pd.NaT), errors='coerce')

    # 수집일 → datetime (처리소요기간 계산용). 없으면 uploaded_at 날짜로 대체.
    if '수집일' in df.columns:
        df['_collect_dt'] = pd.to_datetime(df['수집일'], errors='coerce')
    else:
        df['_collect_dt'] = pd.NaT
    df['_collect_dt'] = df['_collect_dt'].combine_first(df['_uploaded_at_dt']).dt.normalize()

    # 등록일 → date (MIN 계산용). 누락 시 원문 날짜 표현으로 보정.
    df['_reg_dt_raw'] = pd.to_datetime(df.get('등록일', pd.NaT), errors='coerce')
    df['_reg_dt'] = df.apply(_infer_registration_date, axis=1)

    # 처리일자: 빈 문자열('') → NaT 변환 필수
    df['_proc_dt'] = pd.to_datetime(
        df.get('처리일자', pd.NaT).replace({'': pd.NaT}),
        errors='coerce'
    )

    # 진행상태 정규화
    df['_status'] = df.get('진행상태', '').astype(str).str.strip()

    # ── 2. 접수번호별 최신 row (latest row) ───────────────────
    # uploaded_at 기준 내림차순 정렬 후 first()
    df_sorted = df.sort_values('_uploaded_at_dt', ascending=True)
    latest = (
        df_sorted
        .groupby('접수번호', sort=False)
        .last()
        .reset_index()
    )

    # ── 3. 접수번호별 집계값 계산 ──────────────────────────────

    # 3-1. 등록일 = MIN(등록일) per 접수번호
    reg_min = (
        df.groupby('접수번호')['_reg_dt']
        .min()
        .rename('_reg_min')
    )

    # 3-2. 처리완료일 계산 (완료 건만)
    df_done = df[df['_status'] == '완료'].copy()

    # 1순위: MAX(처리일자 where 완료) — NaT skipna
    done_proc_max = (
        df_done.groupby('접수번호')['_proc_dt']
        .max()
        .rename('_proc_max')
    )

    # 2순위: MIN(uploaded_at where 완료) → 날짜만
    done_upload_min = (
        df_done.groupby('접수번호')['_uploaded_at_dt']
        .min()
        .dt.normalize()  # 시각 → 자정 (날짜만)
        .rename('_upload_min')
    )

    # 3-3. 비공개메모 최초 등장 수집일
    if '비공개메모' in df.columns:
        has_memo = df['비공개메모'].apply(_has_text)
        first_memo_date = (
            df[has_memo]
            .groupby('접수번호')['_collect_dt']
            .min()
            .rename('_first_memo_dt')
        )
    else:
        df['비공개메모'] = ''
        first_memo_date = pd.Series(name='_first_memo_dt', dtype='datetime64[ns]')

    df['_memo_issue_type'] = df['비공개메모'].apply(lambda v: _extract_memo_field(v, '이슈유형'))
    memo_issue_type = (
        df[df['_memo_issue_type'].apply(_has_text)]
        .sort_values('_uploaded_at_dt', ascending=True)
        .groupby('접수번호')['_memo_issue_type']
        .last()
        .rename('이슈유형')
    )

    # 과거 데이터는 비공개메모 대신 내용.1에 날짜 마커가 남아 있음.
    if '내용.1' in df.columns:
        df['_legacy_memo_dt'] = df.apply(
            lambda r: _parse_legacy_memo_date(r.get('내용.1'), r.get('_collect_dt')),
            axis=1
        )
        legacy_memo_date = (
            df[df['_legacy_memo_dt'].notna()]
            .groupby('접수번호')['_legacy_memo_dt']
            .min()
            .rename('_legacy_memo_dt')
        )
        first_memo_date = first_memo_date.combine_first(legacy_memo_date).rename('_first_memo_dt')

    # 3-4. 최종진행상태 = latest row의 진행상태 (이미 latest에 있음)

    # ── 4. latest에 집계값 병합 ───────────────────────────────
    mart = latest.join(reg_min, on='접수번호', how='left')
    mart = mart.join(done_proc_max, on='접수번호', how='left')
    mart = mart.join(done_upload_min, on='접수번호', how='left')
    mart = mart.join(first_memo_date, on='접수번호', how='left')
    mart = mart.drop(columns=['이슈유형'], errors='ignore')
    mart = mart.join(memo_issue_type, on='접수번호', how='left')

    # ── 5. 최종진행상태 / 처리완료여부 ───────────────────────
    mart['최종진행상태'] = mart['_status']
    mart['처리완료여부'] = (mart['최종진행상태'] == '완료').astype(int)

    # ── 6. 처리완료일 결정 (1순위 → 2순위) ──────────────────
    # 1순위: 비공개메모 첫 수집일 — 처리소요기간과 동일 기준, 실제 완료 확인일
    # 2순위: 완료 상태 첫 uploaded_at — 메모 없이 완료된 케이스 대비
    # (처리일자 제거: 담당자 응답 등록일이라 실제 완료 확인일과 다를 수 있음)
    mart['_완료일_dt'] = mart['_first_memo_dt'].combine_first(mart['_upload_min'])
    # 미완료는 None (NaT → None)
    mart['처리완료일'] = mart.apply(
        lambda r: r['_완료일_dt'].strftime('%Y-%m-%d')
        if r['처리완료여부'] == 1 and pd.notna(r['_완료일_dt'])
        else None,
        axis=1
    )

    # ── 7. 등록일 확정 ────────────────────────────────────────
    mart['등록일'] = mart['_reg_min'].apply(
        lambda d: d.strftime('%Y-%m-%d') if pd.notna(d) else None
    )

    # ── 8. 처리소요기간 ─────────────────────────────────────
    # - 비공개메모 있음: 최초 비공개메모 수집일 - 등록일 (고정값)
    # - 비공개메모 없음: 오늘(KST) - 등록일 (매일 증가)
    def _calc_duration(row) -> object:
        try:
            try:
                if int(str(row.get('접수번호', '')).strip()) < 215:
                    return None
            except (ValueError, TypeError):
                pass

            reg = pd.to_datetime(row['등록일'], errors='coerce')
            if pd.isna(reg):
                return None
            first_memo = pd.to_datetime(row.get('_first_memo_dt'), errors='coerce')
            if pd.notna(first_memo):
                end = first_memo
            else:
                end = pd.Timestamp.now(tz='Asia/Seoul').normalize().tz_localize(None)
            if pd.isna(end):
                return None
            days = (end.normalize() - reg.normalize()).days
            return int(days) if days >= 0 else 0
        except Exception:
            return None

    mart['처리소요기간'] = mart.apply(_calc_duration, axis=1)

    # ── 9. 등록월 파생 컬럼 ──────────────────────────────────
    reg_dt_series = pd.to_datetime(mart['등록일'], errors='coerce')

    # 등록월: "YYYY-MM"
    mart['등록월'] = reg_dt_series.dt.strftime('%Y-%m').where(reg_dt_series.notna(), None)

    # 등록월표시: "Jan-25" 형식 (locale 독립)
    def _month_display(d) -> object:
        if pd.isna(d):
            return None
        return f"{_MONTH_EN[d.month]}-{str(d.year)[2:]}"

    mart['등록월표시'] = reg_dt_series.apply(_month_display)

    # 등록월정렬: year*100 + month (int), 정렬·슬라이서용
    mart['등록월정렬'] = reg_dt_series.apply(
        lambda d: int(d.year * 100 + d.month) if pd.notna(d) else None
    )

    # ── 10. 정적 컬럼 (latest row에서 직접) ──────────────────
    # 매장명, 유형, 매입처, 이슈유형, CS구분, issue_type, issue_summary, 담당자
    for col in ['매장명', '유형', '매입처', '이슈유형', 'CS구분', 'issue_type', 'issue_summary', '담당자']:
        if col not in mart.columns:
            mart[col] = None

    # ── 11. 최종 컬럼 선택 및 정렬 ───────────────────────────
    result = mart[MART_COLUMNS].copy()

    # None → pd-native None (PowerBI parquet 호환)
    result = result.where(result.notna(), None)

    done_cnt = int((result['처리완료여부'] == 1).sum())
    print(f"[cs_issue_mart] 마트 완성: 총 {len(result)}건 (완료 {done_cnt}건, 미완료 {len(result)-done_cnt}건)")
    return result


def validate_mart(df: pd.DataFrame) -> bool:
    """
    마트 품질 검증. 로그 출력 후 bool 반환 (False = 경고 있음, 실행은 중단 안 함).
    """
    ok = True

    # 1) 접수번호 중복 없어야 함
    dup = df['접수번호'].duplicated().sum()
    if dup > 0:
        print(f"[validate] ⚠️  접수번호 중복 {dup}건!")
        ok = False
    else:
        print(f"[validate] ✅ 접수번호 중복 없음 ({len(df)}건)")

    # 2) 완료 건 → 처리완료일 있어야 함
    done_mask = df['처리완료여부'] == 1
    done_no_date = done_mask & df['처리완료일'].isna()
    if done_no_date.sum() > 0:
        print(f"[validate] ⚠️  완료인데 처리완료일 없음: {done_no_date.sum()}건")
        ok = False
    else:
        print(f"[validate] ✅ 완료건 처리완료일 모두 있음 ({done_mask.sum()}건)")

    # 3) 처리소요기간은 등록일 기준으로 산출되어야 함
    reg_dt = pd.to_datetime(df['등록일'], errors='coerce')
    has_reg = reg_dt.notna()
    old_num = pd.to_numeric(df['접수번호'], errors='coerce')
    legacy_mask = old_num < 215
    no_dur_with_reg = has_reg & df['처리소요기간'].isna() & ~legacy_mask
    if no_dur_with_reg.sum() > 0:
        print(f"[validate] ⚠️  등록일 있는데 처리소요기간 없음: {no_dur_with_reg.sum()}건")
        ok = False
    else:
        print(f"[validate] ✅ 등록일 있는 건 처리소요기간 모두 있음 ({has_reg.sum()}건)")

    # 4) 미완료 건 → 처리완료일 없어야 함
    notdone_mask = df['처리완료여부'] != 1
    notdone_with_date = notdone_mask & df['처리완료일'].notna()
    if notdone_with_date.sum() > 0:
        print(f"[validate] ⚠️  미완료인데 처리완료일 있음: {notdone_with_date.sum()}건")
        ok = False

    # 5) 처리소요기간 음수 없어야 함
    if df['처리소요기간'].notna().any():
        min_dur = df['처리소요기간'].dropna().astype(int).min()
        if min_dur < 0:
            print(f"[validate] ⚠️  처리소요기간 < 0인 건 있음 (min={min_dur})")
            ok = False
        else:
            print(f"[validate] ✅ 처리소요기간 최솟값 {min_dur}일 (정상)")

    # 6) 등록월 파생 컬럼 일관성
    expected_ym = reg_dt.dt.strftime('%Y-%m').where(reg_dt.notna(), None)
    mismatch = (has_reg & (df['등록월'] != expected_ym)).sum()
    if mismatch > 0:
        print(f"[validate] ⚠️  등록월 불일치: {mismatch}건")
        ok = False
    else:
        print(f"[validate] ✅ 등록월 파생 컬럼 일관성 OK")

    missing_reg = (~has_reg).sum()
    if missing_reg > 0:
        print(f"[validate] ⚠️  등록일 없음: {missing_reg}건 (등록월/처리소요기간 산출 불가)")
        ok = False

    return ok


AI_COLUMNS = ['ai_현상요약', 'ai_예상문제', 'ai_제안액션']

_AI_MONTH_PROMPT_TMPL = """
너는 프랜차이즈 CS 월간 리포트를 위한 AI 해설자다.
이번 달과 지난 달 CS 통계를 비교해 3가지 종합 인사이트를 한 문장씩 작성해라.

[이번 달: {cur_month}]
- 총 접수건수: {cur_total}건
- 완료건수: {cur_done}건 (완료율: {cur_done_rate}%)
- 평균 처리소요일: {cur_avg_days}일
- 주요 이슈유형: {cur_issue_types}

[지난 달: {prev_month}]
- 총 접수건수: {prev_total}건
- 완료율: {prev_done_rate}%
- 평균 처리소요일: {prev_avg_days}일
- 주요 이슈유형: {prev_issue_types}

[변화]
- 접수건수: {delta_total:+d}건 ({delta_total_pct})
- 완료율: {delta_done_rate:+d}%p

작성 규칙:
* ai_현상요약: 이번 달 실적을 지난 달과 비교한 사실 요약 (수치 포함, 1문장)
* ai_예상문제: 현재 추세가 지속될 경우 운영 우려 사항 (1문장)
* ai_제안액션: 이번 달 실적 기반 실행 가능한 조치 (1문장)
* 과장 표현, 책임 추궁식 표현 금지
* "심각", "치명적", "반드시 문제" 같은 단정 표현 금지
* 각 문장 80자 이내
* 반드시 JSON 형식으로만 출력, 키 이름 변경 금지

출력 형식 (키 이름 반드시 동일하게):
{{"ai_현상요약": "", "ai_예상문제": "", "ai_제안액션": ""}}
""".strip()


_AI_RETRY_PROMPT_TMPL = """
아래 통계로 JSON 한 줄만 작성해라. 설명 금지.
키는 ai_현상요약, ai_예상문제, ai_제안액션 3개만 사용.
각 값은 60자 이내 한국어 문장.

이번 달 {cur_month}: 접수 {cur_total}건, 완료 {cur_done}건, 완료율 {cur_done_rate}%, 평균 {cur_avg_days}일, 주요 이슈 {cur_issue_types}
지난 달 {prev_month}: 접수 {prev_total}건, 완료율 {prev_done_rate}%, 평균 {prev_avg_days}일, 주요 이슈 {prev_issue_types}
변화: 접수 {delta_total:+d}건({delta_total_pct}), 완료율 {delta_done_rate:+d}%p

{{"ai_현상요약":"","ai_예상문제":"","ai_제안액션":""}}
""".strip()


def _row_val(row, key: str) -> str:
    """pandas Series row에서 값을 가져와 빈 문자열로 안전 변환."""
    v = row.get(key)
    if v is None or (isinstance(v, float) and v != v):  # NaN 체크
        return ''
    return str(v)


def _compute_month_stats(df_mart: pd.DataFrame, month: str) -> dict:
    """월별 CS 통계 계산."""
    df = df_mart[df_mart['등록월'] == month]
    if df.empty:
        return {'month': month, 'total': 0, 'done': 0, 'done_rate': 0,
                'avg_days': '-', 'issue_types': '-'}
    total     = len(df)
    done      = int((df['처리완료여부'] == 1).sum())
    done_rate = round(done / total * 100) if total else 0
    avg_raw   = df['처리소요기간'].dropna().astype(float)
    avg_days  = round(float(avg_raw.mean()), 1) if not avg_raw.empty else '-'
    type_top  = df['issue_type'].value_counts().head(3)
    issue_str = ', '.join(f"{k}({v}건)" for k, v in type_top.items()) or '-'
    return {'month': month, 'total': total, 'done': done,
            'done_rate': done_rate, 'avg_days': avg_days, 'issue_types': issue_str}


def _fallback_ai_result(cur: dict, prev: dict, delta_total: int, delta_pct_str: str, delta_done_rate: int) -> dict:
    """LLM JSON 재시도까지 실패해도 월별 AI 컬럼이 비지 않도록 기본 해설을 만든다."""
    return {
        'ai_현상요약': (
            f"{cur['month']} CS는 {cur['total']}건으로 전월 대비 "
            f"{delta_total:+d}건({delta_pct_str}) 변화했습니다."
        ),
        'ai_예상문제': (
            f"완료율 {cur['done_rate']}% 흐름이 지속되면 미완료 건 관리 부담이 커질 수 있습니다."
        ),
        'ai_제안액션': (
            f"주요 이슈 {cur['issue_types']} 중심으로 미완료 건을 우선 점검하세요."
        ),
    }


def _append_llm_log(log_path: Path, entry: dict) -> None:
    """llm_log.md에 월별 실행 결과 1건을 누적 append."""
    ts = _dt.datetime.now().strftime('%Y-%m-%d %H:%M KST')
    status = '실패' if entry.get('error') else '성공'

    lines = [
        f"\n## {ts} | {entry.get('등록월', '-')} | {status}\n",
        "**프롬프트:**",
        '```\n' + entry.get('prompt', '') + '\n```',
    ]
    if entry.get('error'):
        lines.append(f"**오류:** {entry['error']}")
    else:
        lines.append("**응답:**")
        lines.append('```json\n' + entry.get('response_raw', '') + '\n```')
        for col in AI_COLUMNS:
            lines.append(f"- {col}: {entry.get(col, '')}")
    lines.append("")

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"[ai_columns] llm_log.md 업데이트: {log_path}")


def add_ai_columns(df_mart: pd.DataFrame, log_path: Path = None) -> pd.DataFrame:
    """
    최근 2개월 각각 이전달과 비교한 월별 통계를 LLM에 전달하고,
    결과 3개 컬럼을 해당 월 전 행에 동일 값으로 broadcast한다.

    예) 5월 행 → 4↔5월 비교 / 6월 행 → 5↔6월 비교

    Parameters
    ----------
    df_mart   : build_cs_issue_mart() 반환 DataFrame
    log_path  : llm_log.md 경로 (None이면 로그 저장 생략)
    """
    from modules.transform.utility.qwen_client import query_qwen_json

    df = df_mart.copy()
    for col in AI_COLUMNS:
        df[col] = None

    valid_months = df['등록월'].dropna()
    if valid_months.empty:
        print("[ai_columns] 등록월 없음 → LLM 스킵")
        return df

    sorted_months  = sorted(valid_months.unique())
    target_months  = sorted_months  # 전체 월

    for seq, cur_month in enumerate(target_months, 1):
        cur_pos    = sorted_months.index(cur_month)
        prev_month = sorted_months[cur_pos - 1] if cur_pos > 0 else None

        cur  = _compute_month_stats(df, cur_month)
        prev = (_compute_month_stats(df, prev_month) if prev_month
                else {'month': '-', 'total': 0, 'done': 0, 'done_rate': 0,
                      'avg_days': '-', 'issue_types': '-'})

        delta_total     = cur['total'] - prev['total']
        delta_pct_str   = (f"{delta_total / prev['total'] * 100:+.0f}%"
                           if prev['total'] else '-')
        delta_done_rate = cur['done_rate'] - prev['done_rate']

        prompt = _AI_MONTH_PROMPT_TMPL.format(
            cur_month        = cur['month'],
            cur_total        = cur['total'],
            cur_done         = cur['done'],
            cur_done_rate    = cur['done_rate'],
            cur_avg_days     = cur['avg_days'],
            cur_issue_types  = cur['issue_types'],
            prev_month       = prev['month'],
            prev_total       = prev['total'],
            prev_done_rate   = prev['done_rate'],
            prev_avg_days    = prev['avg_days'],
            prev_issue_types = prev['issue_types'],
            delta_total      = delta_total,
            delta_total_pct  = delta_pct_str,
            delta_done_rate  = delta_done_rate,
        )
        retry_prompt = _AI_RETRY_PROMPT_TMPL.format(
            cur_month        = cur['month'],
            cur_total        = cur['total'],
            cur_done         = cur['done'],
            cur_done_rate    = cur['done_rate'],
            cur_avg_days     = cur['avg_days'],
            cur_issue_types  = cur['issue_types'],
            prev_month       = prev['month'],
            prev_total       = prev['total'],
            prev_done_rate   = prev['done_rate'],
            prev_avg_days    = prev['avg_days'],
            prev_issue_types = prev['issue_types'],
            delta_total      = delta_total,
            delta_total_pct  = delta_pct_str,
            delta_done_rate  = delta_done_rate,
        )
        print(f"[ai_columns] LLM 호출 {seq}/{len(target_months)}: {prev['month']} vs {cur_month}")

        result    = {}
        error_msg = None
        used_fallback = False
        _JSON_MODELS = ["qwen2.5:14b", "qwen2.5:7b", "qwen2.5"]
        try:
            result = query_qwen_json(prompt, preferred_models=_JSON_MODELS)
            if 'parse_error' in result:
                raise ValueError(f"JSON 파싱 실패: {result['parse_error']}")
        except Exception as e:
            error_msg = str(e)
            print(f"[ai_columns] ⚠️ {cur_month} LLM 1차 실패 → 짧은 프롬프트 재시도: {e}")
            try:
                result = query_qwen_json(retry_prompt, preferred_models=_JSON_MODELS)
                if 'parse_error' in result:
                    raise ValueError(f"JSON 파싱 실패: {result['parse_error']}")
                error_msg = None
                print(f"[ai_columns] ✅ {cur_month} LLM 재시도 성공")
            except Exception as retry_error:
                error_msg = f"1차={error_msg} | 재시도={retry_error}"
                result = _fallback_ai_result(cur, prev, delta_total, delta_pct_str, delta_done_rate)
                used_fallback = True
                print(f"[ai_columns] ⚠️ {cur_month} LLM 재시도 실패 → 기본 해설로 채움: {retry_error}")

        mask = df['등록월'] == cur_month
        for col in AI_COLUMNS:
            val = result.get(col)
            if val:
                df.loc[mask, col] = str(val)[:160]

        print(f"[ai_columns] broadcast {mask.sum()}행 ({cur_month})"
              + (" ← 기본 해설 사용" if used_fallback else ""))

        if log_path:
            entry = {
                '등록월':       cur_month,
                'prompt':      retry_prompt if used_fallback else prompt,
                'response_raw': str(result),
                **{col: result.get(col, '') for col in AI_COLUMNS},
            }
            if error_msg and used_fallback:
                entry['fallback'] = '기본 해설 사용'
                entry['error'] = error_msg
            elif error_msg:
                entry['error'] = error_msg
            try:
                _append_llm_log(log_path, entry)
            except Exception as e:
                print(f"[ai_columns] ⚠️ llm_log.md 저장 실패 (무시): {e}")

    return df


def save_mart(df: pd.DataFrame, mart_dir: Path) -> Path:
    """
    cs_issue_mart.csv 저장.

    Parameters
    ----------
    df       : build_cs_issue_mart() 결과
    mart_dir : MART_DB / "fdam_cs_report" 경로

    Returns
    -------
    Path  저장된 파일 경로
    """
    import os
    mart_dir.mkdir(parents=True, exist_ok=True)
    out_path = mart_dir / "cs_issue_mart.csv"

    # 직접 쓰기 시도 (open-truncate는 rename과 달리 파일 소유자 무관)
    try:
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
    except PermissionError:
        # 읽기 전용 파일인 경우 chmod 후 재시도
        if out_path.exists():
            os.chmod(out_path, 0o664)
        df.to_csv(out_path, index=False, encoding='utf-8-sig')

    size_kb = out_path.stat().st_size // 1024
    print(f"[cs_issue_mart] 저장 완료: {out_path} ({size_kb} KB, {len(df)}건)")
    return out_path
