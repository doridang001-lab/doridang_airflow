"""
구글시트 CS 데이터에서 '완료' 이후 중복 저장된 행을 삭제하는 정리 스크립트.

문제: 진행상태가 '완료'가 된 접수번호에 대해 이후 날짜의 수집일로 계속 row가 추가됨.
해결: 각 접수번호별로 '완료' 상태가 처음 나타난 수집일의 행만 유지하고,
      그 이후 날짜의 '완료' 행을 모두 삭제.

실행 전 DRY_RUN=True 로 먼저 확인 후 DRY_RUN=False 로 실제 삭제할 것.
"""

import sys
import json
import pandas as pd
from pathlib import Path

# ── 설정 ─────────────────────────────────────────────────────────
DRY_RUN = False  # True: 삭제 대상 확인만, False: 실제 삭제 수행

CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
FALLBACK_CREDENTIALS_PATH = "/opt/airflow/config/glowing-palace-465904-h6-7f82df929812.json"
CS_GSHEET_URL   = "https://docs.google.com/spreadsheets/d/1DHbZX5jDkiZfe0SPr3eRinPXuSFomYm2AN-gMADoBkI/edit?gid=1488056813#gid=1488056813"
CS_SHEET_NAME   = "시트1"
DONE_STATUS     = "완료"
# ─────────────────────────────────────────────────────────────────


def main():
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # 인증
    cred_path = None
    for path in [CREDENTIALS_PATH, FALLBACK_CREDENTIALS_PATH]:
        if Path(path).exists():
            cred_path = path
            break
    if not cred_path:
        print("❌ 서비스 계정 JSON 파일을 찾을 수 없습니다.")
        sys.exit(1)

    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc    = gspread.authorize(creds)

    sh = gc.open_by_url(CS_GSHEET_URL)
    ws = sh.worksheet(CS_SHEET_NAME)

    print("📥 구글시트 데이터 로드 중...")
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        print("데이터 없음")
        return

    header = all_values[0]
    rows   = all_values[1:]
    df     = pd.DataFrame(rows, columns=header)
    df.index = range(2, len(df) + 2)  # 구글시트 1-based row 번호 (헤더=1행)

    print(f"✅ 총 {len(df)}행 로드")

    # 필수 컬럼 확인
    for col in ['접수번호', '진행상태', '수집일']:
        if col not in df.columns:
            print(f"❌ '{col}' 컬럼이 없습니다. 컬럼 목록: {list(df.columns)}")
            sys.exit(1)

    # 전처리
    df['접수번호'] = df['접수번호'].astype(str).str.strip()
    df['진행상태'] = df['진행상태'].astype(str).str.strip()
    df['수집일']   = df['수집일'].astype(str).str.strip().str[:10]

    # 접수번호별 '완료' 최초 수집일 계산
    done_df = df[df['진행상태'] == DONE_STATUS].copy()
    if done_df.empty:
        print("완료 상태의 행이 없습니다.")
        return

    # merge() 후 인덱스 리셋 문제 방지: 시트 행 번호를 컬럼으로 보존
    done_df['_sheet_row'] = done_df.index

    first_done_date = (
        done_df.groupby('접수번호')['수집일']
        .min()
        .rename('first_done_date')
    )
    print(f"\n📊 완료 접수번호 {len(first_done_date)}건 발견:")
    for receipt, fdate in sorted(first_done_date.items()):
        total_done = len(done_df[done_df['접수번호'] == receipt])
        print(f"  접수번호={receipt:>6} | 최초완료수집일={fdate} | 완료행수={total_done}")

    # 삭제 대상: '완료'이면서 최초완료수집일 이후 날짜 행
    df_done_merged = done_df.merge(first_done_date, on='접수번호', how='left')
    delete_mask = df_done_merged['수집일'] > df_done_merged['first_done_date']
    # 시트 행 번호(_sheet_row) 사용 — merge 후 0-based 인덱스 대신 원본 시트 위치 유지
    rows_to_delete_idx = df_done_merged[delete_mask]['_sheet_row'].tolist()

    print(f"\n🗑️  삭제 대상: {len(rows_to_delete_idx)}행")

    if not rows_to_delete_idx:
        print("삭제 대상 행이 없습니다.")
        return

    # 미리보기 (최대 30건)
    preview = df.loc[rows_to_delete_idx[:30], ['접수번호', '수집일', '진행상태', '매장명']] \
                if '매장명' in df.columns \
                else df.loc[rows_to_delete_idx[:30], ['접수번호', '수집일', '진행상태']]
    print("\n삭제 대상 미리보기 (최대 30건):")
    print(preview.to_string())

    if DRY_RUN:
        print(f"\n⚠️  DRY_RUN=True → 실제 삭제는 수행하지 않았습니다.")
        print(f"    실제 삭제하려면 DRY_RUN = False 로 변경 후 재실행하세요.")
        return

    # 실제 삭제: 내림차순으로 삭제 (행 번호가 밀리지 않도록)
    rows_to_delete_sorted = sorted(rows_to_delete_idx, reverse=True)
    print(f"\n🗑️  {len(rows_to_delete_sorted)}행 삭제 시작...")

    BATCH_SIZE = 500
    deleted = 0
    for i in range(0, len(rows_to_delete_sorted), BATCH_SIZE):
        batch = rows_to_delete_sorted[i:i + BATCH_SIZE]
        requests = []
        for row_idx in batch:
            requests.append({
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": row_idx - 1,  # 0-based
                        "endIndex": row_idx          # 0-based exclusive
                    }
                }
            })
        # 배치 내에서도 내림차순 정렬 필수
        requests.sort(key=lambda r: r["deleteDimension"]["range"]["startIndex"], reverse=True)
        sh.batch_update({"requests": requests})
        deleted += len(batch)
        print(f"  진행: {deleted}/{len(rows_to_delete_sorted)}행 삭제 완료")

    print(f"\n✅ 삭제 완료: 총 {deleted}행")


if __name__ == "__main__":
    main()
