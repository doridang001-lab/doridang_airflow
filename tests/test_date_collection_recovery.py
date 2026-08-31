import pendulum
import pytest

from modules.transform.utility import date_collection_recovery as recovery


def test_default_schedule_date_range_uses_weekend_window_before_monday():
    now = pendulum.datetime(2026, 8, 10, 9, 0, tz="Asia/Seoul")

    assert recovery.default_schedule_date_range(now) == ("2026-08-08", "2026-08-09")


def test_build_recovery_plan_maps_schedule_dates_to_previous_sale_dates():
    items = recovery.build_recovery_plan(
        schedule_date_from="2026-08-08",
        schedule_date_to="2026-08-09",
        parent_dag_id="DB_DateCollection_Recovery_Dags",
        parent_run_id="manual__parent",
        target_dag_ids=["DB_OKPOS_Sales_Dags"],
    )

    assert len(items) == 1
    assert items[0].sale_date_from == "2026-08-07"
    assert items[0].sale_date_to == "2026-08-08"
    assert items[0].conf["sale_date_from"] == "2026-08-07"
    assert items[0].conf["sale_date_to"] == "2026-08-08"
    assert items[0].conf["schedule_date_from"] == "2026-08-08"
    assert items[0].run_id == "date_recovery__DB_OKPOS_Sales_Dags__20260807_20260808__manual__parent"


def test_single_date_targets_are_split_per_sale_date():
    items = recovery.build_recovery_plan(
        schedule_date_from="2026-08-08",
        schedule_date_to="2026-08-09",
        parent_dag_id="DB_DateCollection_Recovery_Dags",
        parent_run_id="manual__parent",
        target_dag_ids=["DB_UnifiedSales"],
    )

    assert [item.conf["sale_date"] for item in items] == ["2026-08-07", "2026-08-08"]
    assert [item.sale_date_from for item in items] == ["2026-08-07", "2026-08-08"]


def test_default_plan_only_includes_source_group():
    items = recovery.build_recovery_plan(
        schedule_date_from="2026-08-08",
        schedule_date_to="2026-08-09",
        parent_dag_id="DB_DateCollection_Recovery_Dags",
        parent_run_id="manual__parent",
    )

    assert items
    assert {item.target.group for item in items} == {"source"}
    assert "DB_UnifiedSales" not in {item.target.dag_id for item in items}


def test_mart_group_can_be_selected_explicitly():
    items = recovery.build_recovery_plan(
        schedule_date_from="2026-08-08",
        schedule_date_to="2026-08-09",
        parent_dag_id="DB_DateCollection_Recovery_Dags",
        parent_run_id="manual__parent",
        target_groups=["mart"],
    )

    assert {item.target.dag_id for item in items} == {"DB_UnifiedSales", "DB_OrderCrossAnalysis_Dags"}


def test_target_date_mode_sets_baemin_target_date_and_all_batches():
    items = recovery.build_recovery_plan(
        schedule_date_from="2026-08-09",
        schedule_date_to="2026-08-09",
        parent_dag_id="DB_DateCollection_Recovery_Dags",
        parent_run_id="manual__parent",
        target_dag_ids=["DB_Beamin_Macro_Dags"],
    )

    assert len(items) == 1
    assert items[0].conf["target_date"] == "2026-08-08"
    assert items[0].conf["run_all_batches"] is True


def test_unknown_target_dag_id_fails_with_known_targets():
    with pytest.raises(ValueError, match="미등록 복구 대상 DAG"):
        recovery.build_recovery_plan(
            schedule_date_from="2026-08-08",
            schedule_date_to="2026-08-09",
            parent_dag_id="DB_DateCollection_Recovery_Dags",
            target_dag_ids=["NO_SUCH_DAG"],
        )


def test_resolve_schedule_date_range_rejects_reversed_range():
    with pytest.raises(ValueError, match="date_from"):
        recovery.resolve_schedule_date_range({"date_from": "2026-08-10", "date_to": "2026-08-09"})


def test_parse_target_groups_rejects_unknown_group():
    with pytest.raises(ValueError, match="target_groups"):
        recovery.parse_target_groups(["source", "report"])
