import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db import DB_Beamin_combined as combined
from modules.transform.pipelines.db import beamin_staging as staging


class SimulatedTaskTimeout(BaseException):
    pass


def _accounts() -> list[dict]:
    return [
        {
            "account_id": str(idx),
            "password": "pw",
            "store_name": f"도리당 테스트{idx}점",
            "store_id": str(idx),
        }
        for idx in range(1, 4)
    ]


def _profile() -> dict:
    return {
        "name": "test",
        "max_session_recovery_per_account": 2,
        "driver_restart_every_stores": 999,
        "account_wait_range": (0, 0),
    }


def test_collect_resume_records_only_completed_accounts(tmp_path):
    progress_file = tmp_path / "_collect_progress.json"
    sessions: list[str] = []

    def build_session(account, _metrics, _profile):
        sessions.append(account["account_id"])
        driver = MagicMock()
        driver.current_url = "https://self.baemin.com/"
        return driver

    def order_result(_driver, store_info, *, target_date=None):
        return {
            "ok": True,
            "validation": [{"store_id": store_info["store_id"], "target_date": target_date}],
        }

    with patch.object(combined, "resolve_stability_profile", return_value=_profile()), \
         patch.object(combined, "_build_account_session", side_effect=build_session), \
         patch.object(combined, "is_on_main_dashboard", return_value=True), \
         patch.object(combined, "wait_for_page", return_value=True), \
         patch.object(
             combined,
             "collect_woori_for_driver",
             side_effect=[None, SimulatedTaskTimeout("중단")],
         ), \
         patch.object(combined, "collect_shop_operation_for_driver"), \
         patch.object(combined, "collect_orders_for_driver", side_effect=order_result), \
         patch.object(combined, "logout_baemin"), \
         patch.object(combined, "quit_driver_safely"), \
         patch.object(combined.random, "uniform", return_value=0), \
         patch.object(combined.time, "sleep"):
        with pytest.raises(SimulatedTaskTimeout):
            combined.collect_now_and_woori(
                _accounts(),
                target_date="2026-07-23",
                stability_profile="test",
                progress_file=progress_file,
                progress_run_id="scheduled__test",
                _allow_login_second_pass=False,
            )

    first_progress = staging.load_progress(
        progress_file,
        run_id="scheduled__test",
        target_date="2026-07-23",
    )
    assert first_progress is not None
    assert first_progress["done_accounts"] == ["1"]
    assert first_progress["success"] == 1
    assert [item["store_id"] for item in first_progress["carry"]["validation"]] == ["1"]

    sessions.clear()
    with patch.object(combined, "resolve_stability_profile", return_value=_profile()), \
         patch.object(combined, "_build_account_session", side_effect=build_session), \
         patch.object(combined, "is_on_main_dashboard", return_value=True), \
         patch.object(combined, "wait_for_page", return_value=True), \
         patch.object(combined, "collect_woori_for_driver", return_value=None), \
         patch.object(combined, "collect_shop_operation_for_driver"), \
         patch.object(combined, "collect_orders_for_driver", side_effect=order_result), \
         patch.object(combined, "logout_baemin"), \
         patch.object(combined, "quit_driver_safely"), \
         patch.object(combined.random, "uniform", return_value=0), \
         patch.object(combined.time, "sleep"):
        result = combined.collect_now_and_woori(
            _accounts(),
            target_date="2026-07-23",
            stability_profile="test",
            progress_file=progress_file,
            progress_run_id="scheduled__test",
            _allow_login_second_pass=False,
        )

    assert sessions == ["2", "3"]
    assert result["summary"] == (
        "계정 루프 완료 3/3 계정 "
        "(완전성공 3, 부분실패 0, 계정실패 0, orders 실패 0, ads 실패 0, stages 실패 0)"
    )
    assert [item["store_id"] for item in result["validation"]] == ["1", "2", "3"]
    assert [item["account_id"] for item in result["store_info_per_account"]] == ["1", "2", "3"]


def test_progress_mismatch_and_corrupt_json_return_none(tmp_path):
    path = tmp_path / "_collect_progress.json"
    staging.init_progress(
        path,
        run_id="run-1",
        target_date="2026-07-23",
        total_accounts=3,
    )

    assert staging.load_progress(path, run_id="run-2", target_date="2026-07-23") is None
    assert staging.load_progress(path, run_id="run-1", target_date="2026-07-24") is None

    path.write_text("{broken", encoding="utf-8")
    assert staging.load_progress(path, run_id="run-1", target_date="2026-07-23") is None


def test_progress_file_stays_outside_exported_baemin_folder(tmp_path):
    local_analytics = tmp_path / "analytics"
    local_baemin = local_analytics / "baemin_macro"
    local_baemin.mkdir(parents=True)
    (local_baemin / "result.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
    prog_path = staging.progress_path(local_analytics)
    staging.init_progress(
        prog_path,
        run_id="run-1",
        target_date="2026-07-23",
        total_accounts=1,
    )

    exported = staging.export_staging_to_inbox(local_baemin, "run-1", tmp_path / "inbox")

    assert prog_path.parent == local_analytics
    assert local_baemin.parent == local_analytics
    assert not list(exported.rglob(staging.PROGRESS_FILENAME))


def test_batch_progress_paths_are_separate():
    local_analytics = Path("analytics")

    batch_1 = staging.progress_path(local_analytics, "batch_1_3")
    batch_2 = staging.progress_path(local_analytics, "batch_2_3")

    assert batch_1.name == "_collect_progress_batch_1_3.json"
    assert batch_2.name == "_collect_progress_batch_2_3.json"
    assert batch_1 != batch_2


def test_progress_file_none_keeps_default_behavior():
    result = combined.collect_now_and_woori([], progress_file=None)

    assert result["summary"] == (
        "계정 루프 완료 0/0 계정 "
        "(완전성공 0, 부분실패 0, 계정실패 0, orders 실패 0, ads 실패 0, stages 실패 0)"
    )
