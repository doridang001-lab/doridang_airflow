"""
fin_product 폴더를 data/mart/fin_product에서 data/mart 루트로 되돌리는 롤백 스크립트.

실행:
  python scripts/rollback_fin_product_move.py
"""

from pathlib import Path


def rollback_fin_product_move():
    """fin_product 파일을 mart/fin_product/에서 mart/ 루트로 다시 이동."""
    src_dir = Path(r'C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\fin_product')
    dst_dir = Path(r'C:\Users\민준\OneDrive - 주식회사 도리당\data\mart')

    files = [
        'fin_product_grp.csv',
        'fin_product_mart.csv',
        'fin_product_posfeed_whitelist.csv',
        'fin_product_review.csv',
        'fin_product_alias.csv',
    ]

    print(f"[롤백] {src_dir} → {dst_dir}")

    moved_count = 0
    for name in files:
        src = src_dir / name
        dst = dst_dir / name

        if src.exists():
            if dst.exists():
                print(f"  SKIP {name}: 대상에 이미 존재")
            else:
                src.replace(dst)
                print(f"  MOVED {name}")
                moved_count += 1
        else:
            print(f"  NOTFOUND {name}")

    print(f"\n총 {moved_count}개 파일 이동 완료")

    print("\n[롤백 후 상태]")
    for name in ['fin_product_grp.csv', 'fin_product_mart.csv', 'fin_product_posfeed_whitelist.csv']:
        p = dst_dir / name
        print(f"  {name}: {p.exists()} (size={p.stat().st_size if p.exists() else 'n/a'})")


def rollback_paths_py():
    """paths.py를 fin_product 서브폴더 이전 상태(mart 루트)로 되돌림."""
    paths_py = Path(r'c:\airflow\modules\transform\utility\paths.py')

    old_content = """FIN_PRODUCT_CSV_PATH = MART_DB / "fin_product" / "fin_product_grp.csv"
FIN_PRODUCT_REVIEW_CSV_PATH = MART_DB / "fin_product" / "fin_product_review.csv"
FIN_PRODUCT_ALIAS_CSV_PATH = MART_DB / "fin_product" / "fin_product_alias.csv"
FIN_PRODUCT_MART_CSV_PATH = MART_DB / "fin_product" / "fin_product_mart.csv"
POSFEED_WHITELIST_CSV_PATH = MART_DB / "fin_product" / "fin_product_posfeed_whitelist.csv"
"""

    new_content = """FIN_PRODUCT_CSV_PATH = MART_DB / "fin_product_grp.csv"
FIN_PRODUCT_REVIEW_CSV_PATH = MART_DB / "fin_product_review.csv"
FIN_PRODUCT_ALIAS_CSV_PATH = MART_DB / "fin_product_alias.csv"
FIN_PRODUCT_MART_CSV_PATH = MART_DB / "fin_product_mart.csv"
POSFEED_WHITELIST_CSV_PATH = MART_DB / "fin_product_posfeed_whitelist.csv"
"""

    content = paths_py.read_text(encoding='utf-8')

    if old_content in content:
        content = content.replace(old_content, new_content)
        paths_py.write_text(content, encoding='utf-8')
        print("[롤백] paths.py: MART_DB 루트 경로로 되돌림 완료")
        return True
    elif new_content in content:
        print("[롤백] paths.py: 이미 원래 상태입니다.")
        return False
    else:
        print("[롤백] paths.py: 예상하지 못한 상태, 수동 확인 필요")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("fin_product 경로 이동 롤백 시작")
    print("=" * 60)

    print("\n[1단계] 파일 이동 롤백")
    print("-" * 60)
    rollback_fin_product_move()

    print("\n[2단계] paths.py 롤백")
    print("-" * 60)
    result = rollback_paths_py()

    print("\n" + "=" * 60)
    if result is True:
        print("✓ 롤백 완료: fin_product가 MART_DB 루트로 복구됨")
    elif result is False:
        print("- 롤백 불필요: 이미 원래 상태")
    else:
        print("⚠ 일부 항목은 수동 확인 필요")
    print("=" * 60)
