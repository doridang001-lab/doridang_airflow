"""
경로 설정 테스트 스크립트
"""
from pathlib import Path
import glob
import sys

sys.path.insert(0, r"C:\airflow")

from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB

print("=" * 70)
print("🔍 경로 설정 검증")
print("=" * 70)

# 1. LOCAL_DB 확인
print(f"\n📁 LOCAL_DB: {LOCAL_DB}")
print(f"   존재: {LOCAL_DB.exists()}")

# 2. 주문 알림 파일 확인
PATH_ORDERS_ALERTS = LOCAL_DB / "영업관리부_DB" / "sales_daily_orders_alerts.csv"
print(f"\n📁 PATH_ORDERS_ALERTS: {PATH_ORDERS_ALERTS}")
print(f"   존재: {PATH_ORDERS_ALERTS.exists()}")

# 3. OneDrive 경로 확인 (로컬)
COLLECT_DB = Path.home() / "OneDrive - 주식회사 도리당" / "Collect_Data"
print(f"\n📁 COLLECT_DB: {COLLECT_DB}")
print(f"   존재: {COLLECT_DB.exists()}")

# 4. 배민 파일 패턴 확인
PATH_NOW = str(COLLECT_DB / "영업관리부_수집" / "baemin_metrics_*.csv")
print(f"\n📁 baemin_metrics 패턴: {PATH_NOW}")
files_now = sorted(glob.glob(PATH_NOW))
print(f"   찾은 파일: {len(files_now)}개")
for f in files_now[:3]:
    print(f"      - {Path(f).name}")

# 5. 변경이력 파일 패턴 확인
PATH_HISTORY = str(COLLECT_DB / "영업관리부_수집" / "baemin_change_history_*.csv")
print(f"\n📁 baemin_change_history 패턴: {PATH_HISTORY}")
files_hist = sorted(glob.glob(PATH_HISTORY))
print(f"   찾은 파일: {len(files_hist)}개")
for f in files_hist[:3]:
    print(f"      - {Path(f).name}")

# 6. 토더 파일 확인 (로컬 downloads)
DOWNLOAD_DIR = Path.home() / "Downloads"
PATH_TOORDER = str(DOWNLOAD_DIR / "toorder_review_doridang1_*.xlsx")
print(f"\n📁 toorder 패턴: {PATH_TOORDER}")
files_toorder = sorted(glob.glob(PATH_TOORDER))
print(f"   찾은 파일: {len(files_toorder)}개")
for f in files_toorder[:3]:
    print(f"      - {Path(f).name}")

print("\n" + "=" * 70)
print("✅ 검증 완료")
print("=" * 70)
