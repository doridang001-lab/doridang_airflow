"""
기존 unified_sales parquet 파일 일괄 수정 스크립트

변경 내용:
1. source == "toorder" 행 삭제 (toorder 수집 중단)
2. order_type 재분류 (platform 기반):
   - 홀       → 홀_테이블
   - 홀 포장  → 홀_포장
   - *포장*   → 플랫폼_포장  (배민 포장, 쿠팡 포장 등)
   - 포스/제휴사주문 → 홀_테이블 (테이블명 없으므로 기본값)
   - 그 외    → 배달
"""

from pathlib import Path
import pandas as pd

UNIFIED_ROOT = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "mart" / "unified_sales_grp"


def reclassify(platform: str) -> str:
    p = str(platform).strip()
    if p == "홀":
        return "홀_테이블"
    if p == "홀 포장":
        return "홀_포장"
    if p in ("포스", "포스전화(CID)", "제휴사주문"):
        return "홀_테이블"
    if "포장" in p:
        return "플랫폼_포장"
    return "배달"


def fix_file(path: Path) -> str:
    df = pd.read_parquet(path)
    before = len(df)

    # 1. toorder 삭제
    df = df[df["source"] != "toorder"].copy()
    dropped = before - len(df)

    # 2. order_type 재분류
    df["order_type"] = df["platform"].apply(reclassify)

    df.to_parquet(path, index=False)
    return f"{path.name}: {before}행 → {len(df)}행 (toorder {dropped}행 삭제, order_type 재분류)"


def main():
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet"))
    if not files:
        print(f"파일 없음: {UNIFIED_ROOT}")
        return

    print(f"대상 파일 {len(files)}개\n")
    for f in files:
        print(fix_file(f))
    print("\n완료")


if __name__ == "__main__":
    main()
