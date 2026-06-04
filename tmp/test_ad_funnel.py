"""광고 funnel 수집 단독 테스트 — 나홀로 역삼점."""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from modules.transform.pipelines.db.DB_Beamin_05_ad_funnel import collect_ad_funnel_for_account

ACCOUNT_ID = "doriys"
PASSWORD   = "ever123@"
STORE_LIST = [{"store_id": "14822058", "brand": "나홀로", "store": "역삼점"}]

failed = collect_ad_funnel_for_account(ACCOUNT_ID, PASSWORD, STORE_LIST)
if failed:
    print(f"실패: {[s['store'] for s in failed]}")
else:
    print("완료: 나홀로 역삼점 광고 funnel 수집 성공")
