#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 다크 테마 + 실제 스크린샷 이미지 포함

from pptx import Presentation
from pptx.util import Cm, Pt
from pptx.enum.text import PP_ALIGN
from pptx.dml.color import RGBColor
from pathlib import Path

output_path = r"C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\프담_CS_DX전환_2_애니메이션없는_ppt.pptx"

IMG_DASH   = Path(r"C:\Users\민준\AppData\Local\Temp\wmux\screenshot-1778720674583.png")  # 대시보드
IMG_ALERTS = Path(r"C:\Users\민준\AppData\Local\Temp\wmux\screenshot-1778720681969.png")  # 알림 3개

BG     = RGBColor(0x0d, 0x0d, 0x0d)
DARK2  = RGBColor(0x11, 0x11, 0x11)
MINT   = RGBColor(0x00, 0xe5, 0xa0)
WHITE  = RGBColor(0xff, 0xff, 0xff)
GRAY   = RGBColor(0x99, 0x99, 0x99)
GRAY2  = RGBColor(0x55, 0x55, 0x55)
RED    = RGBColor(0xe0, 0x55, 0x55)
AMBER  = RGBColor(0xf0, 0xa5, 0x00)
PURPLE = RGBColor(0x8b, 0x5c, 0xf6)
MINT_BG   = RGBColor(0x0a, 0x18, 0x0e)
RED_BG    = RGBColor(0x18, 0x0a, 0x0a)
AMBER_BG  = RGBColor(0x16, 0x0e, 0x00)

prs = Presentation()
prs.slide_width  = Cm(33.87)
prs.slide_height = Cm(19.05)

def blank(bg=BG):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    s.background.fill.solid()
    s.background.fill.fore_color.rgb = bg
    return s

def tb(s, text, l, t, w, h, size, bold=False, color=WHITE, align=PP_ALIGN.LEFT):
    box = s.shapes.add_textbox(l, t, w, h)
    tf = box.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.alignment = align
    r = p.add_run()
    r.text = text
    r.font.size = Pt(size)
    r.font.bold = bold
    r.font.color.rgb = color
    r.font.name = '맑은 고딕'
    return box

def rect(s, l, t, w, h, fill, line=None, lw=1.5):
    shp = s.shapes.add_shape(1, l, t, w, h)
    shp.fill.solid()
    shp.fill.fore_color.rgb = fill
    if line:
        shp.line.color.rgb = line
        shp.line.width = Pt(lw)
    else:
        shp.line.fill.background()
    return shp

def img(s, path, l, t, w, h):
    if Path(path).exists():
        s.shapes.add_picture(str(path), l, t, w, h)

def note(s, text):
    s.notes_slide.notes_text_frame.text = text

def tag_title(s, tag, title):
    tb(s, tag, Cm(0), Cm(1.5), Cm(33.87), Cm(1), 11, color=MINT, align=PP_ALIGN.CENTER)
    tb(s, title, Cm(0), Cm(2.5), Cm(33.87), Cm(2), 28, bold=True, align=PP_ALIGN.CENTER)

# ── S1: 표지 ──────────────────────────────────────────────
s = blank()
rect(s, Cm(0), Cm(0), Cm(0.5), Cm(19.05), MINT)
tb(s, '전략기획부  ·  2026.05', Cm(1.5), Cm(4.5), Cm(20), Cm(1), 11, color=MINT)
tb(s, '프담 CS DX전환', Cm(1.5), Cm(5.8), Cm(28), Cm(4.5), 44, bold=True)
rect(s, Cm(1.5), Cm(10.5), Cm(6), Cm(0.25), MINT)
tb(s, '기억 기반  →  시스템 기반 관리 전환', Cm(1.5), Cm(11), Cm(28), Cm(1.5), 18, color=GRAY)
tb(s, '미처리 감지  ·  자동 알림  ·  비공개 메모  ·  Dashboard', Cm(1.5), Cm(12.8), Cm(28), Cm(1.2), 12, color=GRAY2)
note(s, "안녕하세요, 전략기획부 조민준 PM입니다.\n\n이번 프로젝트의 핵심은\n기억에 의존하던 CS 운영을\n시스템이 먼저 감지하고 관리하는 구조로 전환한 것입니다.\n\n단순히 대시보드나 알림 기능을 만든 것이 아니라,\n업무 흐름 자체를 다시 설계한 사례라고 보시면 됩니다.")

# ── S2: 기존 CS 운영 구조 ──────────────────────────────────
s = blank()
tag_title(s, '기존 상황', '기존 CS 운영 구조')

flow_labels = ['접수', '카톡 공유', '이사님 판단', '영업관리부\n소통', '모니터링']
bw, bh = Cm(5.2), Cm(2.8)
gap = Cm(0.9)
total_w = len(flow_labels) * bw + (len(flow_labels)-1) * gap
sx = (Cm(33.87) - total_w) / 2
by = Cm(5.2)
for i, label in enumerate(flow_labels):
    x = sx + i * (bw + gap)
    rect(s, x, by, bw, bh, DARK2, GRAY2, 1)
    tb(s, label, x, by+Cm(0.5), bw, Cm(1.8), 12, bold=True, align=PP_ALIGN.CENTER)
    if i < len(flow_labels)-1:
        tb(s, '→', x+bw, by+Cm(0.6), gap, Cm(1.5), 16, color=GRAY2, align=PP_ALIGN.CENTER)

ty = Cm(9)
rect(s, Cm(2.5), ty, Cm(29), Cm(0.8), MINT)
tb(s, '단계', Cm(3), ty+Cm(0.1), Cm(9), Cm(0.7), 11, bold=True, color=BG, align=PP_ALIGN.CENTER)
tb(s, '문제점', Cm(12), ty+Cm(0.1), Cm(19), Cm(0.7), 11, bold=True, color=BG, align=PP_ALIGN.CENTER)
rows = [
    ('카톡 공유', '사람이 직접 전달해야 인지 가능 — 담당자가 전달하지 않으면 아무도 모름'),
    ('이사님↔담당자 소통', '둘만 상황 인지, 기록 부재 — 제3자가 현황 파악 불가'),
    ('모니터링', '개별 건을 직접 찾아서 재정리해야 함 — 장기 지연 감지 어려움'),
]
for i, (stage, prob) in enumerate(rows):
    ry = ty + Cm(0.8 + i * 2.8)
    rc = DARK2 if i % 2 == 0 else RGBColor(0x14, 0x14, 0x14)
    rect(s, Cm(2.5), ry, Cm(29), Cm(2.8), rc, GRAY2, 0.5)
    tb(s, stage, Cm(3), ry+Cm(0.6), Cm(9), Cm(1.5), 11, bold=True, align=PP_ALIGN.CENTER)
    tb(s, prob,  Cm(12), ry+Cm(0.6), Cm(19), Cm(1.5), 11, color=GRAY)

note(s, "기존에는 접수 이후 영업이 직접 카카오톡으로 공유해야\n이사님이 상황을 인지할 수 있는 구조였습니다.\n\n즉, 사람이 직접 전달해야 다음 단계가 진행되는 방식이었습니다.\n\n이사님과 담당자가 소통은 하고 있었지만,\n그 내용이 별도로 기록되지 않아\n담당자 외에는 현재 상황 파악이 어려웠습니다.\n\n모니터링도 개별 건을 직접 확인해야 해서\n장기 지연이나 반복 이슈를 빠르게 판단하기 어려웠습니다.")

# ── S3: 핵심 문제 ──────────────────────────────────────────
s = blank()
tag_title(s, '근본 원인', '기존 운영의 핵심 문제')

problems = [
    ('①', '사람 전달 기반', '누락 가능\n특정 담당자 의존'),
    ('②', '기록 부재', '진행 상황 비가시화\n담당자 외 판단 어려움'),
    ('③', '실시간 모니터링\n어려움', '장기 지연 추적 어려움\n재정리 필요'),
]
bw2, bh2 = Cm(9), Cm(9.5)
gap2 = Cm(2.0)
total2 = len(problems) * bw2 + (len(problems)-1) * gap2
sx2 = (Cm(33.87) - total2) / 2
by2 = Cm(5.5)
for idx, (num, title, desc) in enumerate(problems):
    x = sx2 + idx * (bw2 + gap2)
    rect(s, x, by2, bw2, bh2, DARK2, RED, 2)
    tb(s, num,   x, by2+Cm(0.8), bw2, Cm(1.8), 30, bold=True, color=RED, align=PP_ALIGN.CENTER)
    tb(s, title, x+Cm(0.3), by2+Cm(2.6), bw2-Cm(0.6), Cm(1.8), 13, bold=True, align=PP_ALIGN.CENTER)
    tb(s, desc,  x+Cm(0.3), by2+Cm(4.5), bw2-Cm(0.6), Cm(4),   12, color=GRAY, align=PP_ALIGN.CENTER)

rect(s, Cm(4), Cm(16.2), Cm(26), Cm(2), MINT_BG, MINT, 1.5)
tb(s, '업무가 시스템이 아니라 사람 기억 중심으로 운영되고 있었습니다.', Cm(4), Cm(16.6), Cm(26), Cm(1.3), 13, bold=True, color=MINT, align=PP_ALIGN.CENTER)

note(s, "결국 가장 큰 문제는\n업무가 시스템이 아니라 사람 기억 중심으로 운영되고 있었다는 점입니다.\n\n누가 전달했는지, 누가 기억하고 있는지에 따라\n업무 추적 가능 여부가 달라지는 구조였습니다.")

# ── S4: 현재 운영 구조 ──────────────────────────────────────
s = blank()
tag_title(s, '개선 방향', '현재 운영 구조')

flow_after = ['접수', '자동 알림', '비공개\n메모 기록', '지연 자동\n감지', 'Dashboard\n실시간 모니터링']
bw3, bh3 = Cm(5.0), Cm(2.8)
gap3 = Cm(1.0)
total3 = len(flow_after) * bw3 + (len(flow_after)-1) * gap3
sx3 = (Cm(33.87) - total3) / 2
by3 = Cm(5.2)
for i, label in enumerate(flow_after):
    x = sx3 + i * (bw3 + gap3)
    rect(s, x, by3, bw3, bh3, MINT_BG, MINT, 1.5)
    tb(s, label, x, by3+Cm(0.4), bw3, Cm(2), 11, bold=True, color=MINT, align=PP_ALIGN.CENTER)
    if i < len(flow_after)-1:
        tb(s, '→', x+bw3, by3+Cm(0.5), gap3, Cm(1.5), 16, color=MINT, align=PP_ALIGN.CENTER)

ty2 = Cm(9)
rect(s, Cm(2.5), ty2, Cm(14), Cm(0.8), RED_BG, RED, 1)
rect(s, Cm(16.5), ty2, Cm(15), Cm(0.8), MINT_BG, MINT, 1)
tb(s, '기존', Cm(2.5), ty2+Cm(0.1), Cm(14), Cm(0.7), 11, bold=True, color=RED, align=PP_ALIGN.CENTER)
tb(s, '현재', Cm(16.5), ty2+Cm(0.1), Cm(15), Cm(0.7), 11, bold=True, color=MINT, align=PP_ALIGN.CENTER)
compare = [
    ('카톡 전달 필요', '자동 알림'),
    ('담당자만 상황 인지', '비공개 메모 기록'),
    ('기록 시점 불명확', '즉시 기록'),
    ('개별 확인 필요', 'Dashboard 실시간 확인'),
    ('사람이 찾아야 함', '시스템이 먼저 감지'),
]
for i, (bf, af) in enumerate(compare):
    ry2 = ty2 + Cm(0.8 + i * 1.7)
    rc = DARK2 if i % 2 == 0 else RGBColor(0x14, 0x14, 0x14)
    rect(s, Cm(2.5), ry2, Cm(14), Cm(1.7), rc, RED, 0.5)
    rect(s, Cm(16.5), ry2, Cm(15), Cm(1.7), rc, MINT, 0.5)
    tb(s, bf, Cm(2.5),  ry2+Cm(0.3), Cm(14), Cm(1.1), 11, color=RED, align=PP_ALIGN.CENTER)
    tb(s, af, Cm(16.5), ry2+Cm(0.3), Cm(15), Cm(1.1), 11, bold=True, color=MINT, align=PP_ALIGN.CENTER)

note(s, "이번에는 업무 흐름 자체를 다시 설계했습니다.\n\n자동 알림 구조를 추가하여\n사람이 직접 전달하지 않아도 시스템이 먼저 인지할 수 있도록 변경했습니다.\n\n비공개 메모를 통해 진행 상황과 이슈 내용을 기록하도록 구조를 변경했습니다.\n\n이를 통해 특정 담당자만 알고 있던 업무를\n조직 전체가 함께 추적 가능한 구조로 전환했습니다.")

# ── S5: KPI 재정의 ──────────────────────────────────────────
s = blank()
tag_title(s, '성과 지표', 'KPI 재정의')

rect(s, Cm(3), Cm(5), Cm(28), Cm(4), RED_BG, RED, 1.5)
tb(s, '"얼마나 처리했는가"', Cm(3), Cm(5.8), Cm(28), Cm(2.2), 22, color=RED, align=PP_ALIGN.CENTER)
tb(s, '↓', Cm(0), Cm(9.5), Cm(33.87), Cm(1.5), 32, bold=True, color=MINT, align=PP_ALIGN.CENTER)
rect(s, Cm(3), Cm(11), Cm(28), Cm(4), MINT_BG, MINT, 1.5)
tb(s, '"얼마나 빠르게 대응했는가"', Cm(3), Cm(11.8), Cm(28), Cm(2.2), 22, bold=True, color=MINT, align=PP_ALIGN.CENTER)
rect(s, Cm(5), Cm(15.8), Cm(24), Cm(2.3), DARK2, GRAY2, 1)
for i, item in enumerate(['비공개 메모 최초 입력일까지 측정', '응답 속도 중심 관리', '지연 원인 데이터화 가능']):
    tb(s, f'· {item}', Cm(7), Cm(16.0 + i * 0.65), Cm(20), Cm(0.7), 11, color=GRAY)

note(s, "KPI도 함께 재정의했습니다.\n\n기존에는 처리 건수가 기준이었다면,\n이번에는 얼마나 빠르게 대응했는가를 측정합니다.\n\n비공개 메모 최초 입력일까지의 시간을 응답 속도로 측정하고,\n이를 통해 지연 원인을 데이터로 남길 수 있게 됐습니다.")

# ── S6: 지연 CS 자동 감지 + 알림 스크린샷 ──────────────────
s = blank()
tag_title(s, '자동화', '지연 CS가 숨겨질 수 없는 구조')

# 타임라인 뱃지 + 레이블
timeline = [
    ('4',  '4일 후',    '기록 요청',  AMBER,  AMBER_BG),
    ('7',  '7일 후',    '지연 알림',  RED,    RED_BG),
    ('14', '14일 이상', 'Flow 자동 보고', PURPLE, RGBColor(0x10, 0x0a, 0x1e)),
]
bwt, bht = Cm(7.5), Cm(3.2)
gapt = Cm(2.3)
totalt = len(timeline) * bwt + (len(timeline)-1) * gapt
sxt = (Cm(33.87) - totalt) / 2
byt = Cm(4.5)

for idx, (num, label, action, color, bg) in enumerate(timeline):
    x = sxt + idx * (bwt + gapt)
    rect(s, x, byt, bwt, bht, bg, color, 2)
    tb(s, num,    x, byt+Cm(0.3), bwt, Cm(1.4), 22, bold=True, color=color, align=PP_ALIGN.CENTER)
    tb(s, label,  x, byt+Cm(1.6), bwt, Cm(0.8), 12, bold=True, align=PP_ALIGN.CENTER)
    tb(s, action, x, byt+Cm(2.4), bwt, Cm(0.7), 11, color=GRAY, align=PP_ALIGN.CENTER)
    if idx < len(timeline)-1:
        tb(s, '→', x+bwt, byt+Cm(0.8), gapt, Cm(1.5), 20, color=GRAY2, align=PP_ALIGN.CENTER)

# 알림 스크린샷 이미지
img(s, IMG_ALERTS, Cm(1.5), Cm(8.2), Cm(31), Cm(9.5))

rect(s, Cm(4), Cm(17.9), Cm(26), Cm(1), MINT_BG, MINT, 1)
tb(s, '사람이 직접 찾지 않아도  시스템이 먼저 감지하도록 설계', Cm(4), Cm(18.1), Cm(26), Cm(0.8), 11, bold=True, color=MINT, align=PP_ALIGN.CENTER)

note(s, "핵심은 사람이 직접 찾지 않아도\n시스템이 먼저 감지하도록 만든 점입니다.\n\n4일 이후에는 기록 요청,\n7일 이후에는 지연 알림,\n14일 이상 건은 자동 보고되도록 구성했습니다.")

# ── S7: Dashboard + 대시보드 스크린샷 ──────────────────────
s = blank()
tag_title(s, '가시화', '보이지 않던 업무를 보이는 구조로')

# 왼쪽: 대시보드 이미지
img(s, IMG_DASH, Cm(0.8), Cm(4.5), Cm(19.5), Cm(12.5))

# 오른쪽: 설명 박스들
rect(s, Cm(21.5), Cm(4.5), Cm(11.5), Cm(5.8), DARK2, GRAY2, 1)
tb(s, '기존', Cm(22), Cm(4.8), Cm(5), Cm(0.8), 11, color=GRAY2)
tb(s, '개별 건 직접 확인\n필요할 때 찾아보는 방식', Cm(22), Cm(5.7), Cm(11), Cm(3.5), 13, color=GRAY)

tb(s, '↓', Cm(21.5), Cm(10.7), Cm(11.5), Cm(1.2), 20, bold=True, color=MINT, align=PP_ALIGN.CENTER)

rect(s, Cm(21.5), Cm(12), Cm(11.5), Cm(5), MINT_BG, MINT, 2)
tb(s, '현재', Cm(22), Cm(12.3), Cm(5), Cm(0.8), 11, color=MINT)
tb(s, 'KPI + 처리 현황\n실시간 확인 가능\n즉시 판단 가능한 구조', Cm(22), Cm(13.2), Cm(11), Cm(3.5), 13, bold=True, color=MINT)

note(s, "기존에는 개별 건을 직접 확인해야 했다면,\n현재는 KPI와 처리 현황을 실시간으로 확인할 수 있는 구조로 변경했습니다.\n\n즉, 필요할 때 찾아보는 방식이 아니라\n즉시 판단 가능한 구조로 바뀌었습니다.")

# ── S8: 결과 ────────────────────────────────────────────────
s = blank()
tag_title(s, '성과', '운영 구조 전환 완료')

rect(s, Cm(3), Cm(5), Cm(28), Cm(4), DARK2, GRAY2, 1)
tb(s, '기존  기억 기반', Cm(3.5), Cm(5.4), Cm(27), Cm(0.8), 13, bold=True)
tb(s, '사람에 따라 인지 가능 여부가 달라짐  —  담당자가 기억하지 않으면 진행 불가', Cm(3.5), Cm(6.3), Cm(27), Cm(1.5), 12, color=GRAY)

tb(s, '↓', Cm(0), Cm(9.7), Cm(33.87), Cm(1.5), 28, bold=True, color=MINT, align=PP_ALIGN.CENTER)

rect(s, Cm(3), Cm(11.5), Cm(28), Cm(4), MINT_BG, MINT, 2)
tb(s, '현재  조직 추적 가능', Cm(3.5), Cm(11.9), Cm(27), Cm(0.8), 13, bold=True, color=MINT)
tb(s, '누구든 상황을 파악하고 의사결정 가능  —  기억 기반 운영에서 탈피', Cm(3.5), Cm(12.8), Cm(27), Cm(1.5), 12, color=WHITE)

note(s, "기억 기반 운영에서 조직 추적 가능 구조로 전환했습니다.\n\n담당자가 기억하지 않아도,\n시스템이 현황을 추적하고 알려주는 구조입니다.\n\n누구든 상황을 파악하고 의사결정할 수 있게 됐습니다.")

# ── S9: 확장 방향 ────────────────────────────────────────────
s = blank()
tag_title(s, '의미', '이번 사례의 핵심 의미')

rect(s, Cm(3), Cm(5), Cm(28), Cm(4.5), MINT_BG, MINT, 2)
tb(s, '사람이 기억해야 운영되는 구조', Cm(3), Cm(5.6), Cm(28), Cm(1.2), 18, color=GRAY, align=PP_ALIGN.CENTER)
tb(s, '↓', Cm(0), Cm(6.9), Cm(33.87), Cm(1.2), 22, bold=True, color=MINT, align=PP_ALIGN.CENTER)
tb(s, '시스템이 먼저 감지하는 구조로 전환', Cm(3), Cm(7.8), Cm(28), Cm(1.2), 18, bold=True, color=MINT, align=PP_ALIGN.CENTER)

rect(s, Cm(3), Cm(10.3), Cm(28), Cm(3), DARK2, GRAY2, 1)
tb(s, '단순히 CS 업무를 개선한 것이 아니라,', Cm(3.5), Cm(10.7), Cm(27), Cm(0.8), 12, color=GRAY)
tb(s, '반복되고 누락될 수 있는 업무를 데이터 기반 구조로 전환한 사례', Cm(3.5), Cm(11.5), Cm(27), Cm(0.8), 12, bold=True)

rect(s, Cm(3), Cm(14.1), Cm(28), Cm(3.5), DARK2, MINT, 1.5)
tb(s, '다른 부서의 반복 업무와 지연 업무도', Cm(3.5), Cm(14.5), Cm(27), Cm(0.8), 12, color=GRAY)
tb(s, '동일한 방식으로 충분히 개선 가능합니다', Cm(3.5), Cm(15.4), Cm(27), Cm(1), 14, bold=True, color=MINT)

note(s, "이번 사례는 단순히 CS 업무를 개선한 것이 아니라,\n반복되고 누락될 수 있는 업무를\n데이터 기반 구조로 전환한 사례라고 생각합니다.\n\n동일한 방식으로\n다른 부서의 반복 업무나 지연 업무도\n충분히 개선 가능하다고 보고 있습니다.")

# ── 저장 ────────────────────────────────────────────────────
prs.save(output_path)
print("saved:", output_path)
print("slides: 9")
