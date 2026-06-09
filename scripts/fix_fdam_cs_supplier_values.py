"""
Clean Fdam CS supplier values in the source Google Sheet.

Default mode is dry-run. Use --apply to update only the "매입처" column.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CS_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1DHbZX5jDkiZfe0SPr3eRinPXuSFomYm2AN-gMADoBkI/edit?gid=1488056813#gid=1488056813"
CS_SHEET_NAME = "시트1"
CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
FALLBACK_CREDENTIALS_PATH = "/opt/airflow/config/glowing-palace-465904-h6-7f82df929812.json"


def _credential_candidates() -> list[str]:
    paths = []
    env_cred = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_cred:
        paths.append(env_cred)
    paths.extend([CREDENTIALS_PATH, FALLBACK_CREDENTIALS_PATH])

    # Local Windows fallback for running this script outside the container.
    paths.extend([
        str(ROOT / "config" / "rare-ethos-483607-i5-45c9bec5b193.json"),
        str(ROOT / "config" / "glowing-palace-465904-h6-7f82df929812.json"),
    ])

    deduped = []
    for path in paths:
        if path and path not in deduped and os.path.exists(path):
            deduped.append(path)
    return deduped


def _has_text(value) -> bool:
    return pd.notna(value) and str(value).strip() != ""


def _extract_supplier_from_memo(memo_value) -> str:
    if not _has_text(memo_value):
        return ""
    text = str(memo_value).replace("\r\n", "\n").replace("\r", "\n")
    match = re.search(r"(?m)^\s*업체\s*[:：]\s*([^\n]*)", text)
    if not match:
        return ""
    return match.group(1).strip()


def _build_clean_values(df: pd.DataFrame) -> pd.Series:
    if "매입처" not in df.columns:
        raise RuntimeError("'매입처' column not found")
    if "비공개메모" not in df.columns:
        raise RuntimeError("'비공개메모' column not found")

    return df["비공개메모"].apply(_extract_supplier_from_memo)


def _connect_sheet():
    import gspread
    from google.oauth2.service_account import Credentials

    candidates = _credential_candidates()
    if not candidates:
        raise RuntimeError("No usable service account JSON found")

    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(candidates[0], scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(CS_GSHEET_URL)
    return sh.worksheet(CS_SHEET_NAME)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="update the Google Sheet")
    args = parser.parse_args()

    ws = _connect_sheet()
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        print("No rows found.")
        return 0

    cleaned = _build_clean_values(df)
    current = df["매입처"].fillna("").astype(str)
    changed = current.str.strip() != cleaned.str.strip()

    filled_after = cleaned.apply(_has_text)

    print(f"Rows loaded: {len(df)}")
    print(f"Rows with 업체 value in memo: {int(filled_after.sum())}")
    print(f"Supplier filled after cleanup: {int(filled_after.sum())}")
    print(f"Rows to update: {int(changed.sum())}")

    if changed.any():
        preview_cols = [c for c in ["접수번호", "매장명", "매입처"] if c in df.columns]
        preview = df.loc[changed, preview_cols].copy()
        preview["정리후_매입처"] = cleaned.loc[changed].values
        print(preview.head(30).to_string(index=False))

    if not args.apply:
        print("Dry-run only. Re-run with --apply to update the sheet.")
        return 0

    from gspread.utils import rowcol_to_a1

    supplier_col_idx = list(df.columns).index("매입처") + 1
    start_cell = rowcol_to_a1(2, supplier_col_idx)
    end_cell = rowcol_to_a1(len(df) + 1, supplier_col_idx)
    ws.update(values=[[value] for value in cleaned.tolist()], range_name=f"{start_cell}:{end_cell}")
    print(f"Updated range: {start_cell}:{end_cell}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
