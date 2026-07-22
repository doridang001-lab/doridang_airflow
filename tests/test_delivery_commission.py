from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_DeliveryCommission as delivery


def _partition(root: Path, brand: str, store: str) -> Path:
    path = root / f"brand={brand}" / f"store={store}" / "ym=2026-07"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_baemin_order(root: Path, brand: str, store: str, total: int, deposit: int) -> None:
    pd.DataFrame(
        [{
            "주문번호": "공통주문번호",
            "주문시각": "2026. 07. 19. (일) 오후 12:00:00",
            "총결제금액": total,
            "입금예정금액": deposit,
        }]
    ).to_parquet(_partition(root, brand, store) / "orders_2026-07.parquet", index=False)


def _write_baemin_ad(root: Path, brand: str, store_name: str, spend: int) -> None:
    path = _partition(root, brand, "광고") / "metrics.csv"
    pd.DataFrame([{"날짜": "2026-07-19", "store_name": store_name, "광고지출": spend}]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )


def _write_coupang_order(root: Path, brand: str, store: str, total: int, settlement: int) -> None:
    pd.DataFrame(
        [{
            "order_id": "공통주문번호",
            "order_date": "2026.07.19 12:00:00",
            "매출액": total,
            "정산_예정_금액": settlement,
        }]
    ).to_parquet(_partition(root, brand, store) / "orders_2026-07.parquet", index=False)


def _configure_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    output = tmp_path / "mart" / "delivery_commission.parquet"
    monkeypatch.setattr(delivery, "BAEMIN_ORDERS_DB", tmp_path / "baemin_orders")
    monkeypatch.setattr(delivery, "BAEMIN_OUR_STORE_CLICKS_DB", tmp_path / "baemin_ads")
    monkeypatch.setattr(delivery, "COUPANG_ORDERS_DB", tmp_path / "coupang_orders")
    monkeypatch.setattr(delivery, "DELIVERY_COMMISSION_PATH", output)
    return output


def test_build_combines_both_brands_and_brand_aware_store_aliases(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _configure_paths(monkeypatch, tmp_path)

    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점", 5_000, 4_000)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "도리당", "도리당 부산대신점", 100)
    _write_baemin_ad(delivery.BAEMIN_OUR_STORE_CLICKS_DB, "나홀로", "나홀로 대신점", 200)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "도리당", "부산대신점", 7_000, 5_000)
    _write_coupang_order(delivery.COUPANG_ORDERS_DB, "나홀로", "대신점", 3_000, 2_000)

    delivery.build_delivery_commission()

    result = pd.read_parquet(output).sort_values("platform").reset_index(drop=True)
    assert list(result.columns) == delivery.OUTPUT_COLUMNS
    assert not result.duplicated(["sale_date", "store", "platform"]).any()
    assert set(result["store"]) == {"부산대신점"}

    baemin = result[result["platform"].eq("배달의민족")].iloc[0]
    assert baemin["total_amt"] == 15_000
    assert baemin["settlement_amount"] == 11_700
    assert baemin["diff_amt"] == 3_300

    coupang = result[result["platform"].eq("쿠팡이츠")].iloc[0]
    assert coupang["total_amt"] == 10_000
    assert coupang["settlement_amount"] == 7_000
    assert coupang["diff_amt"] == 3_000


def test_read_failure_preserves_existing_mart(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output = _configure_paths(monkeypatch, tmp_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"existing mart")

    _write_baemin_order(delivery.BAEMIN_ORDERS_DB, "도리당", "부산대신점", 10_000, 8_000)
    broken = _partition(delivery.BAEMIN_ORDERS_DB, "나홀로", "대신점") / "orders_2026-07.parquet"
    broken.write_bytes(b"not parquet")

    with pytest.raises(RuntimeError, match="원천 파일 1개 읽기 실패"):
        delivery.build_delivery_commission()

    assert output.read_bytes() == b"existing mart"
