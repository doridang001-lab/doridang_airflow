# -*- coding: utf-8 -*-
from pptx import Presentation
from pptx.util import Pt, Cm
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

OUT = r"C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\프담CS관리전환_2_애니메이션없는_ppt.pptx"

prs = Presentation()
prs.slide_width  = Cm(33.87)
prs.slide_height = Cm(19.05)

# 색상
BG    = RGBColor(0x0d,0x0d,0x0d)
MINT  = RGBColor(0x00,0xe5,0xa0)
WHITE = RGBColor(0xFF,0xFF,0xFF)
GRAY  = RGBColor(0xAA,0xAA,0xAA)
CARD  = RGBColor(0x1a,0x1a,0x1a)
DARK2 = RGBColor(0x0a,0x23,0x18)
RED   = RGBColor(0xFF,0x70,0x70)

W = prs.slide_width
H = prs.slide_height

def blank():
    sl = prs.slides.add_slide(prs.slide_layouts[6])
    bg = sl.background.fill
    bg.solid(); bg.fore_color.rgb = BG
    return sl

def tb(sl, text, l, t, w, h, size=14, bold=False,
        color=WHITE, align=PP_ALIGN.LEFT, wrap=True):
    tx = sl.shapes.add_textbox(l,t,w,h)
    tx.word_wrap = wrap
    tf = tx.text_frame; tf.word_wrap = wrap
    p  = tf.paragraphs[0]; p.alignment = align
    r  = p.add_run(); r.text = text
    r.font.size  = Pt(size)
    r.font.bold  = bold
    r.font.color.rgb = color
    r.font.name  = '맑은 고딕'
    return tx

def rect(sl, l, t, w, h, fill, line=None, lw=Pt(1)):
    sh = sl.shapes.add_shape(1,l,t,w,h)
    sh.fill.solid(); sh.fill.fore_color.rgb = fill
    if line: sh.line.color.rgb=line; sh.line.width=lw
    else:    sh.line.fill.background()
    return sh

def hline(sl, t, color=MINT):
    rect(sl, Cm(3), t, W-Cm(6), Pt(1), color)

def header(sl, tag, title, sub=None):
    hline(sl, Cm(4.8))
    tb(sl, tag,   Cm(3), Cm(2.8), Cm(25), Cm(1),   size=10, color=MINT)
    tb(sl, title, Cm(3), Cm(3.4), Cm(28), Cm(1.8), size=22, bold=True)
    if sub:
        tb(sl, sub, Cm(3), Cm(5.0), Cm(28), Cm(1), size=13, color=GRAY)

# ───────────────────────────────────────────
# S1: 표지
# ───────────────────────────────────────────
sl1 = blank()
rect(sl1, Cm(0),Cm(0), W//2, H, RGBColor(0x0a,0x23,0x18))
tb(sl1, '프담 CS 관리 혁신 보고  ·  2026.04',
   Cm(3), Cm(3), Cm(26), Cm(1.2), size=11, color=MINT)
tb(sl1, '기억이 아닌 시스템이\nCS를 관리한다',
   Cm(3), Cm(4.5), Cm(27), Cm(5), size=38, bold=True)
tb(sl1, '미처리 알림  ·  자동 보고  ·  실시간 대시보드 구축',
   Cm(3), Cm(9.8), Cm(27), Cm(1.3), size=14, color=GRAY)

tags = [('KPI 전환','처리율 → 응답 소요일'),
        ('트리거','4일 / 7일 / 14일 자동화'),
        ('현재','응답 소요일 평균 5.63일')]
tw = Cm(8.5); gap = Cm(0.6); sy = Cm(11.8)
for i,(k,v) in enumerate(tags):
    lx = Cm(3) + i*(tw+gap)
    rect(sl1, lx, sy, tw, Cm(2.8), RGBColor(0x12,0x2b,0x22), MINT)
    tb(sl1, k, lx+Cm(.4), sy+Cm(.3), tw-Cm(.8), Cm(.9), size=10, color=MINT)
    tb(sl1, v, lx+Cm(.4), sy+Cm(1.1), tw-Cm(.8), Cm(1.2), size=12, bold=True, wrap=True)

# ───────────────────────────────────────────
# S2: 선결론
# ───────────────────────────────────────────
sl2 = blank()
header(sl2, 'SELECT CONCLUSION', 'CS 관리 구조를 3가지로 전환했다')

cards = [
    ('기억 기반 →', '시스템 기반', '알림·보고·대시보드가 자동으로\nCS를 노출·추적'),
    ('처리율 →', '응답 소요일', '비공개 메모 최초 입력일수 기준\n"얼마나 처리했냐 → 얼마나 빠르냐"'),
    ('월말 보고 →', '실시간 모니터링', '일별 스냅샷 누적 대시보드로\n상시 현황 파악 가능'),
]
cw = Cm(8.8); gap = Cm(0.6); cy = Cm(5.8)
for i,(frm,to,desc) in enumerate(cards):
    lx = Cm(3) + i*(cw+gap)
    rect(sl2, lx, cy, cw, Cm(10.5), RGBColor(0x0c,0x25,0x1b), MINT)
    tb(sl2, frm, lx+Cm(.5), cy+Cm(.5), cw-Cm(1), Cm(1), size=11, color=GRAY)
    tb(sl2, to,  lx+Cm(.5), cy+Cm(1.5), cw-Cm(1), Cm(1.6), size=18, bold=True, color=MINT)
    hline(sl2, cy+Cm(3.3))
    tb(sl2, desc, lx+Cm(.5), cy+Cm(3.8), cw-Cm(1), Cm(5), size=12, color=GRAY, wrap=True)

# ───────────────────────────────────────────
# S3: 문제 및 배경
# ───────────────────────────────────────────
sl3 = blank()
header(sl3, 'PROBLEM', 'CS가 처리되어도 개선 여부를 알 수 없었다')

probs = [
    ('01  강제 관리 구조 없음',   'CS가 14일 이상 지연되어도 알림·보고 없이 방치 가능했음'),
    ('02  장기 방치 → 점주 대기', '처리중 상태로 장기 방치 시 완료예정일·지연사유 없이 점주가 기다려야 하는 구조'),
    ('03  사람 기억이 관리 수단', '장기 지연 건을 담당자 기억에 의존해 관리 → 누락 위험 상존'),
]
py = Cm(5.8)
for t,d in probs:
    rect(sl3, Cm(3), py, W-Cm(6), Cm(2.5), CARD, RGBColor(0x2a,0x2a,0x2a))
    tb(sl3, t, Cm(3.6), py+Cm(.35), Cm(20), Cm(.9), size=14, bold=True, color=MINT)
    tb(sl3, d, Cm(3.6), py+Cm(1.2), W-Cm(7.5), Cm(1), size=12, color=GRAY, wrap=True)
    py += Cm(2.9)

rect(sl3, Cm(3), py+Cm(.3), W-Cm(6), Cm(1.9), RGBColor(0x1e,0x10,0x10), RGBColor(0x80,0x30,0x30))
tb(sl3, '문의건 처리는 되고 있음 — 그러나 개선되고 있는지는 알 수 없는 구조',
   Cm(3.5), py+Cm(.6), W-Cm(7), Cm(1), size=14, color=RED, align=PP_ALIGN.CENTER)

# ───────────────────────────────────────────
# S4: 4단계 구조
# ───────────────────────────────────────────
sl4 = blank()
header(sl4, 'CORE DESIGN', '4단계 자동 트리거로 지연 CS를 숨길 수 없게 만들었다')

steps = [
    ('4일',  '첫 트리거', '자동 알림 발송\n비공개 메모 작성 필수', MINT),
    ('미작성','에스컬레이션','재알림 발송\n완료예정일 입력 강제', MINT),
    ('7일',  '지연 알림',  '지연건 자동 알림\n전체 노출',           MINT),
    ('14일', '최종 보고',  'Flow 자동 업로드\n전사 공개',           RED),
]
sw = Cm(7.2); sy = Cm(5.8); gap = Cm(.6)
for i,(day,label,acts,col) in enumerate(steps):
    lx = Cm(3) + i*(sw+gap)
    rect(sl4, lx, sy, sw, Cm(7.5), CARD, col)
    tb(sl4, day,   lx+Cm(.4), sy+Cm(.5), sw-Cm(.8), Cm(1.6), size=26, bold=True, color=col, align=PP_ALIGN.CENTER)
    tb(sl4, label, lx+Cm(.4), sy+Cm(2.1), sw-Cm(.8), Cm(.9), size=11, color=GRAY, align=PP_ALIGN.CENTER)
    hline(sl4, sy+Cm(3.2))
    tb(sl4, acts,  lx+Cm(.4), sy+Cm(3.6), sw-Cm(.8), Cm(3.2), size=12, color=WHITE, align=PP_ALIGN.CENTER, wrap=True)
    if i < 3:
        tb(sl4, '→', Cm(3)+i*(sw+gap)+sw+Cm(.1), sy+Cm(3.2), Cm(.5), Cm(1), size=16, color=MINT, bold=True)

rect(sl4, Cm(3), Cm(14.2), W-Cm(6), Cm(2), RGBColor(0x0c,0x25,0x1b), MINT)
tb(sl4, '지연 CS가 숨겨질 수 없는 구조 — 4일부터 단계적으로 노출·강제·보고가 자동 실행됨',
   Cm(3.5), Cm(14.6), W-Cm(7), Cm(1), size=14, color=WHITE, align=PP_ALIGN.CENTER)

# ───────────────────────────────────────────
# S5: 결과 — KPI & 대시보드
# ───────────────────────────────────────────
sl5 = blank()
header(sl5, 'RESULT', '응답 소요일이 줄고 있다 — 대시보드로 실시간 추적 중')

# 왼쪽 카드
rect(sl5, Cm(3), Cm(5.5), Cm(13.5), Cm(11), CARD, RGBColor(0x2a,0x2a,0x2a))
tb(sl5, '처리 현황 (최근 2개월)', Cm(3.5), Cm(5.9), Cm(13), Cm(.9), size=11, color=MINT)

kpis = [('21건','처리 완료'), ('34건','전체 의뢰'), ('38.2%','완료율')]
kx = Cm(3.5)
for v,l in kpis:
    tb(sl5, v, kx, Cm(7.0), Cm(4), Cm(1.5), size=22, bold=True, color=MINT, align=PP_ALIGN.CENTER)
    tb(sl5, l, kx, Cm(8.6), Cm(4), Cm(.8),  size=10, color=GRAY, align=PP_ALIGN.CENTER)
    kx += Cm(4.2)

checks = ['14일 이상 건  Flow 자동 업로드 운영 중',
          '비공개 메모 미입력 건 자동 알림 운영 중',
          '일별 스냅샷 누적 → 월별 추이 추적 가능']
cy2 = Cm(10.1)
for c in checks:
    rect(sl5, Cm(3.5), cy2, Cm(13), Cm(1.5), RGBColor(0x1e,0x1e,0x1e))
    rect(sl5, Cm(3.7), cy2+Cm(.55), Cm(.35), Cm(.35), MINT)
    tb(sl5, c, Cm(4.4), cy2+Cm(.25), Cm(11.5), Cm(1), size=11, color=WHITE)
    cy2 += Cm(1.8)

# 오른쪽 카드
rect(sl5, Cm(17.5), Cm(5.5), Cm(13.8), Cm(11), CARD, RGBColor(0x2a,0x2a,0x2a))
tb(sl5, '응답 소요일 평균 추이', Cm(18), Cm(5.9), Cm(13), Cm(.9), size=11, color=MINT)

trends = [('6.88일','3/2~8'), ('6.63일','3/9~15'), ('6.63일','3/16~22'),
          ('5.63일','3/23~29'), ('↓단축중','3/30~')]
bar_h = [4.5, 4.2, 4.2, 3.5, 2.8]
bar_colors = [RGBColor(0x22,0x44,0x36)]*3 + [RGBColor(0x00,0xb5,0x80)] + [MINT]
bx = Cm(18); bw = Cm(2.3); bgap = Cm(.3)
bottom = Cm(15.5)
for i,(val,dt) in enumerate(trends):
    lx = bx + i*(bw+bgap)
    bh = Cm(bar_h[i])
    rect(sl5, lx, bottom-bh, bw, bh, bar_colors[i])
    col = MINT if i>=3 else GRAY
    tb(sl5, val, lx, bottom-bh-Cm(1.2), bw, Cm(1), size=10, bold=(i>=3), color=col, align=PP_ALIGN.CENTER)
    tb(sl5, dt,  lx, bottom+Cm(.1),     bw, Cm(.8), size=9,  color=GRAY, align=PP_ALIGN.CENTER)

# ───────────────────────────────────────────
# S6: 성과 요약
# ───────────────────────────────────────────
sl6 = blank()
header(sl6, 'SUMMARY', 'CS 관리가 기억에서 모니터링·액션·검증 구조로 전환됐다')

struct_items = [
    '장기 미처리 CS 자동 노출 구조 완성',
    '14일 이상 건  Flow 자동 업로드 체계 구축',
    'KPI 기반 개선 추적 가능 — 응답 소요일 중심',
]
ops_items = [
    '응답 소요일 평균 5.63일 수준 확인 및 하락 추이',
    '4일 초과 알림 운영 → 단축 가능 구간 도출',
    '담당자 기억 의존 → 데이터 기반 관리로 전환',
]

for col_i, (title, items) in enumerate([('구조적 성과', struct_items), ('운영 성과', ops_items)]):
    lx = Cm(3) + col_i*Cm(14.4)
    tb(sl6, title, lx, Cm(5.5), Cm(13.5), Cm(1), size=12, color=MINT)
    hline(sl6, Cm(6.7))
    iy = Cm(7.2)
    for it in items:
        rect(sl6, lx, iy, Cm(13.5), Cm(2.1), CARD)
        rect(sl6, lx, iy, Cm(.35), Cm(2.1), MINT)
        tb(sl6, it, lx+Cm(.7), iy+Cm(.4), Cm(12.5), Cm(1.4), size=13, color=WHITE, wrap=True)
        iy += Cm(2.4)

rect(sl6, Cm(3), Cm(15.5), W-Cm(6), Cm(2), RGBColor(0x0c,0x25,0x1b), MINT)
tb(sl6, '다음 목표 — 응답 소요일 4일 이내로 단축  ·  CS 이슈유형별 재발 방지 체계 구축',
   Cm(3.5), Cm(15.9), W-Cm(7), Cm(1), size=14, color=WHITE, align=PP_ALIGN.CENTER)

prs.save(OUT)
print('저장 완료:', OUT)
