import os
import time

from modules.transform.pipelines.db import DB_Beamin_Macro_upload as upload
from modules.transform.pipelines.db import DB_Beamin_pc2_distribute as dist


def test_sweep_stale_tmp_quarantines_only_old_tmp(tmp_path):
    sent = []
    old_tmp = tmp_path / "_tmp__manual__bottom__old"
    new_tmp = tmp_path / "_tmp__manual__bottom__new"
    keep = tmp_path / "manual__bottom__keep"
    old_tmp.mkdir()
    new_tmp.mkdir()
    keep.mkdir()
    old_mtime = time.time() - 48 * 3600
    os.utime(old_tmp, (old_mtime, old_mtime))
    from modules.transform.utility import notifier

    original = notifier.send_telegram
    notifier.send_telegram = sent.append
    try:
        moved = dist._sweep_stale_tmp_folders(tmp_path)
    finally:
        notifier.send_telegram = original

    assert moved == 1
    assert len(sent) == 1
    assert "잔해 회수" in sent[0]
    assert not old_tmp.exists()
    assert (tmp_path / dist.QUARANTINE_DIR_NAME / old_tmp.name).is_dir()
    assert new_tmp.is_dir()
    assert keep.is_dir()


def test_empty_ingest_stats_includes_stale_tmp():
    assert dist._empty_ingest_stats()["stale_tmp"] == 0


def test_ingest_no_matching_pattern_reports_swept_stale_tmp(tmp_path):
    sent = []
    old_tmp = tmp_path / "_tmp__manual__bottom__old"
    top = tmp_path / "manual__top__keep"
    old_tmp.mkdir()
    top.mkdir()
    old_mtime = time.time() - 48 * 3600
    os.utime(old_tmp, (old_mtime, old_mtime))
    from modules.transform.utility import notifier

    original = notifier.send_telegram
    notifier.send_telegram = sent.append
    try:
        result = dist.ingest_inbox(
            tmp_path,
            folder_pattern=dist.BOTTOM_FOLDER_PATTERN,
        )
    finally:
        notifier.send_telegram = original

    assert result["stats"]["folders"] == 0
    assert result["stats"]["stale_tmp"] == 1
    assert len(sent) == 1
    assert (tmp_path / dist.QUARANTINE_DIR_NAME / old_tmp.name).is_dir()
    assert top.is_dir()


def test_warn_if_upload_inbox_stale_sends_telegram(monkeypatch, tmp_path):
    stale = tmp_path / "manual__bottom__old"
    recent = tmp_path / "manual__bottom__new"
    ignored = tmp_path / dist.QUARANTINE_DIR_NAME / "manual__bottom__ignored"
    stale.mkdir()
    recent.mkdir()
    ignored.mkdir(parents=True)
    old_mtime = time.time() - 24 * 3600
    os.utime(stale, (old_mtime, old_mtime))
    os.utime(ignored, (old_mtime, old_mtime))
    sent = []
    monkeypatch.setattr(dist, "UPLOAD_INBOX_DIR", tmp_path)
    monkeypatch.setattr(upload, "STALE_ALERT_MARKER_DIR", tmp_path / "markers")
    monkeypatch.setattr(upload, "send_telegram", sent.append)

    upload._warn_if_upload_inbox_stale(dist.BOTTOM_FOLDER_PATTERN)

    assert len(sent) == 1
    assert "적체" in sent[0]
    assert stale.name in sent[0]
    assert recent.name not in sent[0]
    assert ignored.name not in sent[0]


def test_warn_if_upload_inbox_stale_skips_recent_folder(monkeypatch, tmp_path):
    recent = tmp_path / "manual__bottom__new"
    recent.mkdir()
    sent = []
    monkeypatch.setattr(dist, "UPLOAD_INBOX_DIR", tmp_path)
    monkeypatch.setattr(upload, "STALE_ALERT_MARKER_DIR", tmp_path / "markers")
    monkeypatch.setattr(upload, "send_telegram", sent.append)

    upload._warn_if_upload_inbox_stale(dist.BOTTOM_FOLDER_PATTERN)

    assert sent == []
