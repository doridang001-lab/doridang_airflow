"""실제 마트 fixture → 조회 → 한국어 PDF 생성·텍스트 대사."""
from dataclasses import replace
from pathlib import Path
import pytest
from test_samjeon_material_mart import _fixture
from test_samjeon_material_mart_v3 import build
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
from modules.transform.pipelines.db.samjeon_material_service.pdf_report import create_pdf


def test_pdf_same_snapshot_and_cache(tmp_path):
    pytest.importorskip('reportlab')
    pypdf = pytest.importorskip('pypdf')
    if not Path('C:/Windows/Fonts/malgun.ttf').is_file():
        pytest.skip('운영 Windows 한국어 글꼴이 필요합니다')
    plan, _ = _fixture(tmp_path)
    snapshot = build(plan)
    settings = Settings(state_root=str(tmp_path / 'service'), snapshot=str(snapshot))
    repo = Repository(settings)
    analysis = {'snapshot': str(snapshot), 'snapshot_id': plan['snapshot_id'],
                'date_from': '2026-10-01', 'date_to': '2026-10-11'}
    document = create_pdf(settings, repo, analysis, [])
    content = '\n'.join(p.extract_text() for p in pypdf.PdfReader(document).pages)
    assert '송파삼전점' in content and '현장 직원식대' in content
    assert '5,000원' in content and plan['snapshot_id'] in content
    old = document.stat().st_mtime_ns
    assert create_pdf(settings, repo, analysis, []) == document
    assert document.stat().st_mtime_ns == old
