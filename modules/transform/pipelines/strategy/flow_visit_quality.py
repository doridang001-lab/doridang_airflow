"""Flow 방문일지 산출물 품질 평가."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from modules.transform.utility.paths import FLOW_VISIT_ISSUE_PARQUET, FLOW_VISIT_VIZ_PARQUET

QUALITY_CASES_PATH = Path(__file__).with_name("flow_visit_quality_cases.jsonl")
DISPLAY_REQUIRED_COLS = ("owner_voice", "sv_action")


def _as_text(value: Any) -> str:
    return "" if value is None else str(value)


def load_quality_cases(path: Path = QUALITY_CASES_PATH) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                cases.append(json.loads(line))
    return cases


def _read_outputs(issue_path: Path = FLOW_VISIT_ISSUE_PARQUET) -> pd.DataFrame:
    if issue_path.exists():
        return pd.read_parquet(issue_path)
    if FLOW_VISIT_VIZ_PARQUET.exists():
        return pd.read_parquet(FLOW_VISIT_VIZ_PARQUET)
    raise RuntimeError(f"Flow 방문일지 issue/viz 산출물이 없습니다: {issue_path} / {FLOW_VISIT_VIZ_PARQUET}")


def evaluate_visit_quality(
    issue_df: pd.DataFrame | None = None,
    cases: list[dict[str, Any]] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    df = issue_df.copy() if issue_df is not None else _read_outputs()
    case_rows = cases or load_quality_cases()
    if df.empty:
        raise RuntimeError("Flow 방문일지 issue 산출물이 비어 있습니다.")
    if "post_id" not in df.columns or "issue_key" not in df.columns:
        raise RuntimeError("품질 평가에는 post_id, issue_key 컬럼이 필요합니다.")

    df["post_id"] = df["post_id"].astype(str)
    rows = []
    for case in case_rows:
        post_id = _as_text(case.get("post_id"))
        observed_df = df[df["post_id"] == post_id]
        observed = {key for key in observed_df["issue_key"].dropna().astype(str) if key}
        expected = set(case.get("expected_issue_keys") or [])
        must_not = set(case.get("must_not_keys") or [])
        matched = sorted(expected & observed)
        missing = sorted(expected - observed)
        forbidden = sorted(must_not & observed)
        rows.append({
            "post_id": post_id,
            "store_name": case.get("store_name"),
            "visit_date": case.get("visit_date"),
            "expected_cnt": len(expected),
            "observed_cnt": len(observed),
            "matched_cnt": len(matched),
            "recall": round(len(matched) / max(len(expected), 1), 4),
            "missing_keys": missing,
            "forbidden_keys": forbidden,
            "observed_keys": sorted(observed),
            "human_summary": case.get("human_summary"),
            "prompt_notes": case.get("prompt_notes"),
        })
    result = pd.DataFrame(rows)
    issue_models = {}
    if "llm_model" in df.columns:
        issue_models = df["llm_model"].fillna("unknown").astype(str).value_counts().to_dict()
    fallback_count = 0
    if "is_fallback" in df.columns:
        fallback_count = int(df["is_fallback"].fillna(False).astype(bool).sum())
    weak_evidence_count = 0
    weak_evidence_rows: list[dict[str, str]] = []
    if {"raw_text", "issue_key"}.issubset(df.columns):
        compact_raw = df["raw_text"].map(lambda value: re.sub(r"\s+", "", _as_text(value)))
        weak_mask = compact_raw.str.len().lt(8) & df["issue_key"].fillna("").astype(str).ne("기타")
        weak_evidence_count = int(weak_mask.sum())
        weak_evidence_rows = (
            df.loc[weak_mask, ["post_id", "issue_key", "raw_text"]]
            .head(10)
            .fillna("")
            .astype(str)
            .to_dict("records")
        )
    display_blank_counts = {}
    for col in DISPLAY_REQUIRED_COLS:
        if col in df.columns:
            display_blank_counts[col] = int(df[col].map(lambda value: not _as_text(value).strip()).sum())
    summary = {
        "case_cnt": len(result),
        "issue_cnt": int(len(df)),
        "avg_recall": round(float(result["recall"].mean()), 4) if not result.empty else 0.0,
        "perfect_case_cnt": int((result["missing_keys"].map(len) == 0).sum()) if not result.empty else 0,
        "missing_total": int(result["missing_keys"].map(len).sum()) if not result.empty else 0,
        "forbidden_total": int(result["forbidden_keys"].map(len).sum()) if not result.empty else 0,
        "fallback_count": fallback_count,
        "issue_models": issue_models,
        "weak_evidence_count": weak_evidence_count,
        "weak_evidence_rows": weak_evidence_rows,
        "display_blank_counts": display_blank_counts,
    }
    return result, summary


def quality_report_markdown(
    issue_df: pd.DataFrame | None = None,
    cases: list[dict[str, Any]] | None = None,
) -> str:
    result, summary = evaluate_visit_quality(issue_df, cases)
    lines = [
        "# Flow 방문일지 품질검사",
        "",
        f"- 기준 글 수: {summary['case_cnt']}",
        f"- 추출 이슈 수: {summary['issue_cnt']}",
        f"- 기대 이슈 평균 회수율: {summary['avg_recall']:.1%}",
        f"- 완전 회수 글 수: {summary['perfect_case_cnt']}/{summary['case_cnt']}",
        f"- 누락 이슈 수: {summary['missing_total']}",
        f"- 금지 오분류 수: {summary['forbidden_total']}",
        f"- 짧은 근거 오분류 의심 수: {summary['weak_evidence_count']}",
        f"- 표시 필드 빈값: {json.dumps(summary['display_blank_counts'], ensure_ascii=False)}",
        f"- fallback 이슈 수: {summary['fallback_count']}",
        f"- 모델 분포: {json.dumps(summary['issue_models'], ensure_ascii=False)}",
        "",
        "## 글별 점검",
    ]
    for row in result.to_dict("records"):
        missing = ", ".join(row["missing_keys"]) if row["missing_keys"] else "없음"
        forbidden = ", ".join(row["forbidden_keys"]) if row["forbidden_keys"] else "없음"
        lines.extend([
            "",
            f"### {row['store_name']} {row['visit_date']} post_id={row['post_id']}",
            f"- 회수율: {row['recall']:.1%} ({row['matched_cnt']}/{row['expected_cnt']})",
            f"- 누락: {missing}",
            f"- 금지 오분류: {forbidden}",
            f"- 사람 기준 요약: {row['human_summary']}",
            f"- 다음 프롬프트 힌트: {row['prompt_notes']}",
        ])
    return "\n".join(lines)


def build_agent_prompt_context(cases: list[dict[str, Any]] | None = None) -> str:
    case_rows = cases or load_quality_cases()
    lines = [
        "Flow 방문일지 개선 작업 시 아래 원칙을 우선 적용한다.",
        "1. 원문 세그먼트의 명시 표현과 taxonomy alias가 일치하면 규칙 분류를 우선한다.",
        "2. 댓글은 해당 post 안에서도 관련 alias가 있는 이슈에만 연결하고 전체 글 분류에는 전파하지 않는다.",
        "3. GPT-OSS는 애매한 세그먼트에만 JSON issue_key 선택기로 쓰고, 요약은 원문 문장 기반으로 조립한다.",
        "4. 변경 후 flow_visit_quality.evaluate_visit_quality()의 누락/금지 오분류를 확인한다.",
        "",
        "사람 기준 사례:",
    ]
    for case in case_rows:
        lines.append(
            f"- {case['store_name']} {case['visit_date']} post_id={case['post_id']}: "
            f"{case['human_summary']} / 기대={','.join(case['expected_issue_keys'])}"
        )
    return "\n".join(lines)
