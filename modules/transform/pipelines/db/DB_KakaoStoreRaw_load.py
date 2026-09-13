"""카카오톡 대화 TXT 신규 파일 탐색 -> 파싱 -> dedup -> kakao_store_raw.parquet 적재.

멱등성은 이중 방어로 보장한다:
1차 - 처리 완료 TXT는 raw 폴더로 이동시켜 다음 실행에서 아예 안 보이게 함
2차 - message_id + store 조합이 기존 parquet에 이미 있으면 적재하지 않음
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.DB_KakaoStoreRaw_config import (
    KakaoSourceReadError,
    KakaoUnmappedRoomError,
)
from modules.transform.pipelines.db.DB_KakaoStoreRaw_parser import (
    KakaoFilenameParseError,
    REQUIRED_COLUMNS,
    build_rows_for_file,
)
from modules.transform.utility.paths import (
    COLLECT_DB,
    KAKAO_STORE_RAW_DIR,
    KAKAO_STORE_RAW_PARQUET,
    KAKAO_STORE_RAW_STATE_JSON,
)

logger = logging.getLogger(__name__)

SOURCE_GLOB_PATTERN = "카카오톡_수집*"
_SORT_KEYS = ["message_datetime", "conversation_id", "store"]
_STRING_COLUMNS = [
    "message_id",
    "conversation_id",
    "store",
    "chat_room_name",
    "sender",
    "type",
    "message_type",
    "message",
    "source_file",
]


def _iter_source_files() -> list[Path]:
    if not COLLECT_DB.exists():
        return []
    files: list[Path] = []
    for entry in sorted(COLLECT_DB.glob(SOURCE_GLOB_PATTERN)):
        if entry.is_dir():
            files.extend(sorted(entry.rglob("*.txt")))
        elif entry.suffix.lower() == ".txt":
            files.append(entry)
    return files


def _load_state() -> dict:
    if not KAKAO_STORE_RAW_STATE_JSON.exists():
        return {}
    try:
        return json.loads(KAKAO_STORE_RAW_STATE_JSON.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("카카오 처리 이력 읽기 실패, 빈 이력으로 진행: %s", exc)
        return {}


def _write_state_atomic(state: dict) -> None:
    KAKAO_STORE_RAW_STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    tmp = KAKAO_STORE_RAW_STATE_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(KAKAO_STORE_RAW_STATE_JSON)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame({col: pd.Series(dtype="object") for col in REQUIRED_COLUMNS})


def _load_existing_parquet() -> pd.DataFrame:
    if not KAKAO_STORE_RAW_PARQUET.exists():
        return _empty_frame()
    try:
        return pd.read_parquet(KAKAO_STORE_RAW_PARQUET)
    except Exception as exc:
        logger.warning("기존 kakao_store_raw.parquet 읽기 실패, 빈 데이터로 진행: %s", exc)
        return _empty_frame()


def _apply_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in _STRING_COLUMNS:
        out[col] = out[col].astype("string")
    out["attachment_count"] = (
        pd.to_numeric(out["attachment_count"], errors="coerce").fillna(0).astype("int64")
    )
    out["message_date"] = pd.to_datetime(out["message_date"]).dt.date
    out["message_datetime"] = pd.to_datetime(out["message_datetime"])
    out["collected_at"] = pd.to_datetime(out["collected_at"])
    return out


def _save_parquet_atomic(df: pd.DataFrame) -> None:
    KAKAO_STORE_RAW_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    tmp = KAKAO_STORE_RAW_PARQUET.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(KAKAO_STORE_RAW_PARQUET)


def _archive_path(path: Path) -> Path:
    KAKAO_STORE_RAW_DIR.mkdir(parents=True, exist_ok=True)
    target = KAKAO_STORE_RAW_DIR / path.name
    if not target.exists():
        return target
    stem, suffix = path.stem, path.suffix
    for idx in range(1, 1000):
        candidate = KAKAO_STORE_RAW_DIR / f"{stem}.{idx}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"cannot create archive path for {path}")


def _norm(value) -> str | None:
    """NaN/None/pd.NA를 전부 None으로 통일해 sender 값 변경 여부를 비교한다."""
    return None if pd.isna(value) else str(value)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="cp949", errors="replace")


def run() -> str:
    all_files = _iter_source_files()
    state = _load_state()
    processed_names = {
        name
        for name, entry in state.items()
        if isinstance(entry, dict) and entry.get("status") == "ok"
    }
    pending = [path for path in all_files if path.name not in processed_names]

    parsed_rows: list[dict] = []
    failed_files: list[str] = []
    read_failed_files: list[str] = []
    unmapped_files: list[str] = []
    ok_files: list[Path] = []

    for path in pending:
        try:
            text = _read_text(path)
        except OSError as exc:
            if exc.errno == 5:  # EIO: OneDrive 클라우드 전용(placeholder) 파일 미다운로드 가능성
                logger.warning(
                    "TXT 읽기 실패(I/O 오류, OneDrive 미다운로드 의심 - 호스트에서 "
                    "'attrib +P' 로 항상 이 장치에 유지 설정 필요): %s / %s",
                    path.name,
                    exc,
                )
            else:
                logger.warning("TXT 읽기 실패: %s / %s", path.name, exc)
            read_failed_files.append(path.name)
            continue
        except Exception as exc:
            logger.warning("TXT 읽기 실패: %s / %s", path.name, exc)
            read_failed_files.append(path.name)
            continue

        try:
            rows = build_rows_for_file(path, text)
        except KakaoFilenameParseError as exc:
            logger.warning("파일명 파싱 실패: %s / %s", path.name, exc)
            failed_files.append(path.name)
            continue
        except KakaoUnmappedRoomError as exc:
            logger.error("미등록 카톡방, CONVERSATION_MAP 추가 필요: %s / %s", path.name, exc)
            unmapped_files.append(path.name)
            continue

        parsed_rows.extend(rows)
        ok_files.append(path)

    existing = _load_existing_parquet()
    existing_index = {
        key: idx
        for idx, key in enumerate(zip(existing["message_id"], existing["store"]))
    }

    seen_in_run: set[tuple[str, str]] = set()
    new_rows: list[dict] = []
    sender_updates = 0
    for row in parsed_rows:
        key = (row["message_id"], row["store"])
        if key in existing_index:
            idx = existing_index[key]
            if _norm(existing.at[idx, "sender"]) != _norm(row["sender"]):
                # 닉네임이 바뀌었으면 최신 sender/type으로 덮어쓴다.
                # message_id는 sender와 무관하게 고정이라 이 갱신이 멱등성을 해치지 않는다.
                existing.at[idx, "sender"] = row["sender"]
                existing.at[idx, "type"] = row["type"]
                sender_updates += 1
            continue
        if key in seen_in_run:
            continue
        seen_in_run.add(key)
        new_rows.append(row)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        combined = pd.concat([existing, new_df], ignore_index=True) if not existing.empty else new_df
    else:
        combined = existing
    combined = _apply_dtypes(combined)
    combined = combined.sort_values(_SORT_KEYS, kind="stable").reset_index(drop=True)
    _save_parquet_atomic(combined)

    now_iso = pd.Timestamp.now(tz="Asia/Seoul").isoformat()
    for path in ok_files:
        state[path.name] = {
            "source_file": path.name,
            "processed_at": now_iso,
            "parsed_row_count": sum(1 for row in parsed_rows if row["source_file"] == path.name),
            "new_row_count": sum(1 for row in new_rows if row["source_file"] == path.name),
            "status": "ok",
        }
    _write_state_atomic(state)

    moved = 0
    for path in ok_files:
        try:
            target = _archive_path(path)
            shutil.move(str(path), str(target))
            moved += 1
        except Exception as exc:
            logger.warning(
                "raw 폴더 이동 실패 (처리 이력엔 이미 완료로 기록됨): %s / %s", path.name, exc
            )

    summary = (
        f"[카카오 적재] 검색 파일: {len(all_files)} / 신규 파일: {len(pending)} / "
        f"파싱 메시지: {len(parsed_rows)} / 중복 메시지: {len(parsed_rows) - len(new_rows)} / "
        f"신규 추가: {len(new_rows)} / 닉네임 갱신: {sender_updates} / 최종 행 수: {len(combined)} / "
        f"파일명 파싱 실패: {len(failed_files)} / 읽기 실패: {len(read_failed_files)} / "
        f"미등록 카톡방: {len(unmapped_files)} / raw 이동: {moved}"
    )
    logger.info(summary)

    if unmapped_files:
        raise KakaoUnmappedRoomError(
            f"미등록 카톡방 파일 {len(unmapped_files)}건, CONVERSATION_MAP 갱신 필요: {unmapped_files}"
        )

    if read_failed_files:
        # 정상 파일은 이미 parquet 적재/raw 이동까지 끝났으므로 데이터 유실은 없다.
        # 다음 실행에서 자동 재시도되지만, 조용히 넘기면 수집 누락을 못 알아채므로
        # 태스크를 실패시켜 retry/텔레그램 알림이 동작하게 한다.
        raise KakaoSourceReadError(
            f"TXT 읽기 실패 {len(read_failed_files)}건(OneDrive 미다운로드 의심): {read_failed_files}"
        )

    return summary
