"""같은 분석 판본의 상세 증빙 PDF. 생성은 명시적인 요청 시에만 수행한다."""
import hashlib
import json
import os
from pathlib import Path
from xml.sax.saxutils import escape

from .config import local_path
from .presentation import render_query, briefing

REPORT_VERSION = 'conversation-v1'


def create_pdf(settings, repository, analysis, notes):
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.pagesizes import A4
    context = repository.period_context(analysis['snapshot'], analysis['snapshot_id'], analysis['date_from'], analysis['date_to'])
    query = lambda kind: repository.query(analysis['snapshot'], kind, analysis['date_from'], analysis['date_to'],
                                         **({'product_key': analysis['focus']} if analysis.get('focus') and kind in ('compare', 'evidence') else {}))
    results = {k: query(k) for k in ('summary', 'compare', 'staff_meal', 'forecast')}
    purchase_evidence = [repository.query(analysis['snapshot'], 'evidence', analysis['date_from'], analysis['date_to'],
                         source=source, limit=50, **({'product_key': analysis['focus']} if analysis.get('focus') else {}))
                         for source in ('food_guide', 'logistics_dam')]
    if any(r['status'] == '범위 밖' for r in purchase_evidence):
        results['evidence'] = next(r for r in purchase_evidence if r['status'] == '범위 밖')
    else:
        results['evidence'] = {**purchase_evidence[0], 'result': {
            'total': sum(r['result']['total'] for r in purchase_evidence),
            'rows': [row for r in purchase_evidence for row in r['result']['rows']]}}
    identity = {'snapshot': analysis['snapshot_id'], 'from': analysis['date_from'], 'to': analysis['date_to'],
                'notes': notes, 'focus': analysis.get('focus'), 'version': REPORT_VERSION,
                'template': hashlib.sha256(Path(__file__).read_bytes() + Path(__file__).with_name('presentation.py').read_bytes()).hexdigest()}
    key = hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:24]
    folder = local_path(settings.state_root) / 'reports' / analysis['date_to']
    target = folder / f'{key}.pdf'
    if target.is_file():
        return target
    if not Path(settings.font_path).is_file():
        raise ValueError('PDF용 한국어 글꼴이 등록되지 않았습니다')
    font = 'SamjeonKorean'
    if font not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(TTFont(font, settings.font_path))
    styles = getSampleStyleSheet()
    for style in styles.byName.values():
        style.fontName = font
        style.wordWrap = 'CJK'
    styles.add(ParagraphStyle('BodyKR', fontName=font, fontSize=10, leading=16, wordWrap='CJK', spaceAfter=6))
    styles.add(ParagraphStyle('CellKR', fontName=font, fontSize=8, leading=12, wordWrap='CJK'))
    p = lambda text, name='BodyKR': Paragraph(escape(str(text)).replace('\n', '<br/>'), styles[name])
    story = [p('송파삼전점 확인 사항과 상세 근거', 'Title'),
             p(f"조회 기간 {analysis['date_from']} ~ {analysis['date_to']}"),
             p(f"분석 판본 {analysis['snapshot_id']} / 자료 상태 {context['status']}"),
             p('확인할 사항', 'Heading2'), p(briefing(context))]
    for kind, title in (('summary', '매출·매입과 목표'), ('compare', '구매 변화와 증가 품목'),
                        ('staff_meal', '현장 직원식대'), ('forecast', '조건부 월말 전망')):
        story.extend([p(title, 'Heading2'), p(render_query(results[kind]))])
    if notes:
        story.append(p('대표님이 알려주신 설명', 'Heading2'))
        for note in notes:
            story.append(p(f"{note['created']} / 사용자 설명: {note['text']}"))
        story.append(p('사용자 설명이며 원천 자료의 수정 또는 검수 승인으로 처리하지 않았습니다.'))
    evidence = results['evidence']
    story.append(p('원천 근거', 'Heading2'))
    if evidence['status'] != '범위 밖':
        value = evidence['result']
        story.append(p(f"구매 근거 전체 {value['total']}행 중 공급처별 앞 50행씩, 총 {len(value['rows'])}행입니다. 추가 내역은 텔레그램 근거 조회로 확인할 수 있습니다."))
        rows = [[p(x, 'CellKR') for x in ('날짜', '품목/원천', '금액(원)', '근거 ID')]]
        for row in value['rows']:
            rows.append([p(x, 'CellKR') for x in (row.get('purchase_date', row.get('sale_date')),
                         row.get('item_name', row['source']), f"{row['amount_won']:,}", row['evidence_id'])])
        table = Table(rows, colWidths=[65, 155, 75, 210], repeatRows=1, hAlign='LEFT')
        table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E8EEF4')),
                                  ('VALIGN', (0, 0), (-1, -1), 'TOP'), ('GRID', (0, 0), (-1, -1), .3, colors.lightgrey),
                                  ('TOPPADDING', (0, 0), (-1, -1), 5), ('BOTTOMPADDING', (0, 0), (-1, -1), 5)]))
        story.append(table)
    else:
        story.append(p(render_query(evidence)))
    folder.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.pending')
    def footer(canvas, doc):
        canvas.setFont(font, 8)
        canvas.drawRightString(A4[0] - 45, 24, f'{doc.page} / 삼전점 관리 자료')
    try:
        SimpleDocTemplate(str(temporary), pagesize=A4, rightMargin=45, leftMargin=45,
                          topMargin=42, bottomMargin=42, title='삼전점 상세 근거').build(story, onFirstPage=footer, onLaterPages=footer)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target
