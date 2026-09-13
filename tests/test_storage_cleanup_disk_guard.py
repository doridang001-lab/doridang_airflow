"""DB_StorageCleanup 디스크 여유 가드.

2026-09-11 장애: C: 여유가 10GB로 떨어지자 Docker 백엔드가 무너졌고
컨테이너 DNS가 깨져 스케줄러가 멈췄다. 정리 모듈이 남은 공간을 보고하지
않으면 같은 상황을 미리 알 수 없다.
"""

import shutil
from collections import namedtuple

from modules.transform.pipelines.db import DB_StorageCleanup as cleanup

_Usage = namedtuple("_Usage", "total used free")

GB = 1024**3


def test_check_disk_headroom_warns_below_threshold(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(465 * GB, 455 * GB, 10 * GB))
    monkeypatch.setattr(cleanup, "send_telegram", lambda text: sent.append(text))

    result = cleanup.check_disk_headroom(tmp_path, warn_gb=30.0)

    assert result["low"] is True
    assert result["free_gb"] == 10.0
    assert len(sent) == 1
    assert "10.0 GB" in sent[0]


def test_check_disk_headroom_quiet_when_healthy(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(465 * GB, 265 * GB, 200 * GB))
    monkeypatch.setattr(cleanup, "send_telegram", lambda text: sent.append(text))

    result = cleanup.check_disk_headroom(tmp_path, warn_gb=30.0)

    assert result["low"] is False
    assert sent == []


def test_check_disk_headroom_survives_unreadable_path(monkeypatch, tmp_path):
    """경로를 못 읽어도 정리 작업 자체를 실패시키지 않는다."""
    def _boom(_p):
        raise OSError("mount gone")

    monkeypatch.setattr(shutil, "disk_usage", _boom)

    result = cleanup.check_disk_headroom(tmp_path)

    assert "error" in result
    assert "low" not in result


def test_check_disk_headroom_alert_failure_does_not_raise(monkeypatch, tmp_path):
    def _boom(_text):
        raise RuntimeError("telegram down")

    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(465 * GB, 460 * GB, 5 * GB))
    monkeypatch.setattr(cleanup, "send_telegram", _boom)

    result = cleanup.check_disk_headroom(tmp_path, warn_gb=30.0)

    assert result["low"] is True
