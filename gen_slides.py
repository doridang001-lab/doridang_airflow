"""
Generate slide13.xml (마무리) and slide14.xml (DX/AX 현황)
and wire them into presentation.xml, presentation.xml.rels, [Content_Types].xml
"""
import pathlib

BASE = pathlib.Path(r"C:\airflow\pptx_unpacked2")
SLIDES_DIR = BASE / "ppt" / "slides"
RELS_DIR = BASE / "ppt" / "slides" / "_rels"

FONT_KO = (
    '<a:latin typeface="210 네버랜드"/>'
    '<a:ea typeface="210 네버랜드"/>'
    '<a:cs typeface="210 네버랜드"/>'
    '<a:sym typeface="210 네버랜드"/>'
)


def fill(color):
    return f'<a:solidFill><a:srgbClr val="{color}"/></a:solidFill>'


def rpr(lang="ko-KR", sz=2400, bold=False, color="000000", italic=False):
    alt = "en-US" if lang == "ko-KR" else "ko-KR"
    b = ' b="1"' if bold else ""
    i = ' i="1"' if italic else ""
    return (
        f'<a:rPr lang="{lang}" altLang="{alt}" sz="{sz}" dirty="0"{b}{i}>'
        f"{fill(color)}{FONT_KO}"
        f"</a:rPr>"
    )


def run(text, lang="ko-KR", sz=2400, bold=False, color="000000", italic=False):
    return f"<a:r>{rpr(lang,sz,bold,color,italic)}<a:t>{text}</a:t></a:r>"


def para(runs_xml, align="l", lnSpc=None, spBef=0, spAft=0, marL=None, indent=None, bullet=False):
    ppr_parts = [f'algn="{align}"']
    if marL is not None:
        ppr_parts.append(f'marL="{marL}"')
    if indent is not None:
        ppr_parts.append(f'indent="{indent}"')
    ppr_attrs = " ".join(ppr_parts)

    inner = ""
    if lnSpc is not None:
        inner += f"<a:lnSpc><a:spcPts val=\"{lnSpc}\"/></a:lnSpc>"
    if spBef:
        inner += f"<a:spcBef><a:spcPts val=\"{spBef}\"/></a:spcBef>"
    if spAft:
        inner += f"<a:spcAft><a:spcPts val=\"{spAft}\"/></a:spcAft>"
    if not bullet:
        inner += "<a:buNone/>"
    else:
        inner += '<a:buChar char="•"/>'

    return f"<a:p><a:pPr {ppr_attrs}>{inner}</a:pPr>{runs_xml}</a:p>"


def empty_para():
    return "<a:p><a:endParaRPr lang=\"ko-KR\" altLang=\"en-US\" dirty=\"0\"/></a:p>"


def textbox(id_, name, x, y, cx, cy, body_xml, anchor="t", noFill=True):
    fill_xml = '<a:noFill/>' if noFill else ''
    return f"""<p:sp>
  <p:nvSpPr>
    <p:cNvPr id="{id_}" name="{name}"/>
    <p:cNvSpPr txBox="1"><a:spLocks noGrp="1"/></p:cNvSpPr>
    <p:nvPr/>
  </p:nvSpPr>
  <p:spPr>
    <a:xfrm><a:off x="{x}" y="{y}"/><a:ext cx="{cx}" cy="{cy}"/></a:xfrm>
    <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
    {fill_xml}
    <a:ln><a:noFill/></a:ln>
  </p:spPr>
  <p:txBody>
    <a:bodyPr wrap="square" anchor="{anchor}"><a:normAutofit/></a:bodyPr>
    <a:lstStyle/>
    {body_xml}
  </p:txBody>
</p:sp>"""


def filled_box(id_, name, x, y, cx, cy, fill_color, body_xml="", anchor="ctr", radius=457200):
    rad = f'<a:round val="{radius}"/>' if radius else ""
    return f"""<p:sp>
  <p:nvSpPr>
    <p:cNvPr id="{id_}" name="{name}"/>
    <p:cNvSpPr><a:spLocks noGrp="1"/></p:cNvSpPr>
    <p:nvPr/>
  </p:nvSpPr>
  <p:spPr>
    <a:xfrm><a:off x="{x}" y="{y}"/><a:ext cx="{cx}" cy="{cy}"/></a:xfrm>
    <a:prstGeom prst="roundRect"><a:avLst><a:gd name="adj" fmla="val 50000"/></a:avLst></a:prstGeom>
    <a:solidFill><a:srgbClr val="{fill_color}"/></a:solidFill>
    <a:ln><a:noFill/></a:ln>
  </p:spPr>
  <p:txBody>
    <a:bodyPr wrap="square" anchor="{anchor}" lIns="180000" rIns="180000" tIns="180000" bIns="180000"><a:normAutofit/></a:bodyPr>
    <a:lstStyle/>
    {body_xml}
  </p:txBody>
</p:sp>"""


def rect_box(id_, name, x, y, cx, cy, fill_color, body_xml="", anchor="t"):
    return f"""<p:sp>
  <p:nvSpPr>
    <p:cNvPr id="{id_}" name="{name}"/>
    <p:cNvSpPr><a:spLocks noGrp="1"/></p:cNvSpPr>
    <p:nvPr/>
  </p:nvSpPr>
  <p:spPr>
    <a:xfrm><a:off x="{x}" y="{y}"/><a:ext cx="{cx}" cy="{cy}"/></a:xfrm>
    <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
    <a:solidFill><a:srgbClr val="{fill_color}"/></a:solidFill>
    <a:ln><a:noFill/></a:ln>
  </p:spPr>
  <p:txBody>
    <a:bodyPr wrap="square" anchor="{anchor}" lIns="220000" rIns="220000" tIns="220000" bIns="220000"><a:normAutofit/></a:bodyPr>
    <a:lstStyle/>
    {body_xml}
  </p:txBody>
</p:sp>"""


def header_group(title_text, base_id=10):
    """Green badge + title textbox matching existing slide pattern."""
    badge_xml = (
        para(run("마무리", sz=2200, bold=True, color="FFFFFF"), align="ctr")
        if title_text == "마무리"
        else para(run(title_text, sz=1800, bold=True, color="FFFFFF"), align="ctr")
    )
    badge_w = 2200000 if title_text == "마무리" else 4200000
    return f"""<p:grpSp>
  <p:nvGrpSpPr>
    <p:cNvPr id="{base_id}" name="Group {base_id}"/>
    <p:cNvGrpSpPr/>
    <p:nvPr/>
  </p:nvGrpSpPr>
  <p:grpSpPr>
    <a:xfrm>
      <a:off x="275797" y="303268"/>
      <a:ext cx="17656784" cy="800000"/>
      <a:chOff x="275797" y="303268"/>
      <a:chExt cx="17656784" cy="800000"/>
    </a:xfrm>
  </p:grpSpPr>
  {filled_box(base_id+1, f"Badge{base_id+1}", 275797, 303268, badge_w, 680000, "86B499", badge_xml, anchor="ctr", radius=457200)}
  {textbox(base_id+2, f"Title{base_id+2}", 624116, 292485, 17308465, 900000,
           para(run(title_text, sz=4066, bold=True, color="2E7D32"), align="l"), anchor="ctr")}
</p:grpSp>"""


def wrap_slide(content):
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
       xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
       xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">
  <p:cSld><p:spTree>
    <p:nvGrpSpPr>
      <p:cNvPr id="1" name="Canvas 1"/>
      <p:cNvGrpSpPr/>
      <p:nvPr/>
    </p:nvGrpSpPr>
    <p:grpSpPr>
      <a:xfrm><a:off x="0" y="0"/><a:ext cx="18288000" cy="10287000"/>
              <a:chOff x="0" y="0"/><a:chExt cx="18288000" cy="10287000"/></a:xfrm>
    </p:grpSpPr>
    {content}
  </p:spTree></p:cSld>
  <p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr>
</p:sld>"""


def build_slide13():
    """마무리 슬라이드"""
    # Header
    hdr = header_group("마무리", base_id=10)

    # Tagline
    tagline = textbox(
        20, "Tagline", 500000, 1600000, 17288000, 900000,
        para(run("AI는 사람을 대체하지 않습니다", sz=5600, bold=True, color="004AAD"), align="ctr"),
        anchor="ctr"
    )

    # Sub description
    sub = textbox(
        21, "Sub", 500000, 2620000, 17288000, 700000,
        para(run("반복 정리·요약·기록은 AI가 보조하고, 현장 판단·소통·의사결정은 사람이 담당합니다.", sz=2400, color="333333"), align="ctr"),
        anchor="ctr"
    )

    # Left dark box: AI가 보조하는 것
    left_items = [
        ("문서 정리 / 회의록 자동 요약", "CCCCCC"),
        ("데이터 분류 및 반복 집계", "CCCCCC"),
        ("표준 문구 생성 / 이메일 초안", "CCCCCC"),
        ("정보 검색 및 빠른 요약", "CCCCCC"),
    ]
    left_body = para(run("AI가 보조하는 것", sz=2600, bold=True, color="86B499"), align="l", spBef=0)
    left_body += empty_para()
    for item_text, col in left_items:
        left_body += para(
            run("▸  ", sz=2200, color="86B499") + run(item_text, sz=2200, color="EEEEEE"),
            align="l", lnSpc=3600, spBef=100
        )

    left_box = rect_box(30, "LeftBox", 400000, 3500000, 8100000, 5800000, "1E1E1E", left_body, anchor="t")

    # Right blue box: 사람이 담당하는 것
    right_items = [
        ("현장 상황 판단 및 의사결정", "FFFFFF"),
        ("팀원·고객과의 소통 및 신뢰 형성", "FFFFFF"),
        ("창의적 아이디어 및 문제 해결", "FFFFFF"),
        ("책임 있는 실행과 결과 관리", "FFFFFF"),
    ]
    right_body = para(run("사람이 담당하는 것", sz=2600, bold=True, color="FFFFFF"), align="l", spBef=0)
    right_body += empty_para()
    for item_text, col in right_items:
        right_body += para(
            run("▸  ", sz=2200, color="FFFFFF") + run(item_text, sz=2200, color="FFFFFF"),
            align="l", lnSpc=3600, spBef=100
        )

    right_box = rect_box(31, "RightBox", 9788000, 3500000, 8100000, 5800000, "004AAD", right_body, anchor="t")

    # Divider line (thin vertical)
    divider = f"""<p:sp>
  <p:nvSpPr><p:cNvPr id="32" name="Divider"/><p:cNvSpPr><a:spLocks noGrp="1"/></p:cNvSpPr><p:nvPr/></p:nvSpPr>
  <p:spPr>
    <a:xfrm><a:off x="9144000" y="3500000"/><a:ext cx="50000" cy="5800000"/></a:xfrm>
    <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
    <a:solidFill><a:srgbClr val="DDDDDD"/></a:solidFill>
    <a:ln><a:noFill/></a:ln>
  </p:spPr>
  <p:txBody><a:bodyPr/><a:lstStyle/></p:txBody>
</p:sp>"""

    # Horse quote
    quote = textbox(
        33, "Quote", 500000, 9450000, 17288000, 700000,
        para(
            run(
                '"사람은 말과 달리기 시합을 하지 않습니다. 말이 더 빠르기 때문이 아니라, 함께 더 멀리 가기 위해서입니다."',
                sz=2000, color="777777", italic=True
            ),
            align="ctr"
        ),
        anchor="ctr"
    )

    content = "\n".join([hdr, tagline, sub, left_box, divider, right_box, quote])
    return wrap_slide(content)


def build_slide14():
    """회사 DX/AX 전환 현황 슬라이드"""
    hdr = header_group("회사 DX / AX 전환 현황", base_id=10)

    # Vertical divider
    vdivider = f"""<p:sp>
  <p:nvSpPr><p:cNvPr id="20" name="VDivider"/><p:cNvSpPr><a:spLocks noGrp="1"/></p:cNvSpPr><p:nvPr/></p:nvSpPr>
  <p:spPr>
    <a:xfrm><a:off x="9119000" y="1400000"/><a:ext cx="50000" cy="8600000"/></a:xfrm>
    <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
    <a:solidFill><a:srgbClr val="DDDDDD"/></a:solidFill>
    <a:ln><a:noFill/></a:ln>
  </p:spPr>
  <p:txBody><a:bodyPr/><a:lstStyle/></p:txBody>
</p:sp>"""

    # DX column header
    dx_header = textbox(
        21, "DXHeader", 400000, 1350000, 8500000, 700000,
        para(run("DX  Data Transformation", sz=3200, bold=True, color="2E7D32"), align="l"),
        anchor="ctr"
    )

    # DX current
    dx_current_body = (
        para(run("현재 진행 중", sz=2400, bold=True, color="2E7D32"), align="l", spBef=0) +
        para(run("▸  매출·주문 데이터 자동 수집 (배민·쿠팡·포스 연동)", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  일 마감 자동 집계 파이프라인 (Airflow DAG 운영)", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  PowerBI 대시보드 → 매출 현황 실시간 조회", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100)
    )
    dx_current = rect_box(22, "DXCurrent", 400000, 2150000, 8500000, 3000000, "F1F8F1", dx_current_body, anchor="t")

    # DX future
    dx_future_body = (
        para(run("앞으로 방향", sz=2400, bold=True, color="2E7D32"), align="l", spBef=0) +
        para(run("▸  전 지점 표준 KPI 자동화 (이상 감지 알림 포함)", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  메뉴·원가·재고 데이터 통합 분석", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  의사결정용 경영 지표 자동 리포트", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100)
    )
    dx_future = rect_box(23, "DXFuture", 400000, 5350000, 8500000, 2900000, "E8F5E8", dx_future_body, anchor="t")

    # AX column header
    ax_header = textbox(
        24, "AXHeader", 9400000, 1350000, 8500000, 700000,
        para(run("AX  AI Transformation", sz=3200, bold=True, color="004AAD"), align="l"),
        anchor="ctr"
    )

    # AX current
    ax_current_body = (
        para(run("현재 진행 중", sz=2400, bold=True, color="004AAD"), align="l", spBef=0) +
        para(run("▸  ChatGPT·Claude 업무 활용 (문서 작성·요약·번역)", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  AI 기반 품목 자동 분류 (포스피드 화이트리스트)", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  본사 AI 도구 사용 교육 (오늘 DXAX 교육 포함)", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100)
    )
    ax_current = rect_box(25, "AXCurrent", 9400000, 2150000, 8500000, 3000000, "F0F4FF", ax_current_body, anchor="t")

    # AX future
    ax_future_body = (
        para(run("앞으로 방향", sz=2400, bold=True, color="004AAD"), align="l", spBef=0) +
        para(run("▸  AI 어시스턴트 → 반복 보고·알림 자동화", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  매장 CS 대응·VOC 분석 자동 요약", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100) +
        para(run("▸  조리·운영 매뉴얼 AI 기반 업데이트 시스템", sz=2100, color="222222"), align="l", lnSpc=3600, spBef=100)
    )
    ax_future = rect_box(26, "AXFuture", 9400000, 5350000, 8500000, 2900000, "E8EDFF", ax_future_body, anchor="t")

    # Goal box
    goal_body = (
        para(run("최종 목표", sz=2600, bold=True, color="FFFFFF"), align="ctr") +
        para(run("데이터 기반 의사결정 + AI 보조 실행 = 더 빠르고 정확한 도리당", sz=2400, color="FFFFFF"), align="ctr", spBef=100)
    )
    goal = rect_box(27, "GoalBox", 400000, 8450000, 17488000, 1600000, "2E7D32", goal_body, anchor="ctr")

    content = "\n".join([hdr, vdivider, dx_header, dx_current, dx_future, ax_header, ax_current, ax_future, goal])
    return wrap_slide(content)


def slide_rels(layout_num=7):
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout"
                Target="../slideLayouts/slideLayout{layout_num}.xml"/>
</Relationships>"""


# Write slide XML files
slide13_path = SLIDES_DIR / "slide13.xml"
slide14_path = SLIDES_DIR / "slide14.xml"
slide13_rels = RELS_DIR / "slide13.xml.rels"
slide14_rels = RELS_DIR / "slide14.xml.rels"

slide13_path.write_text(build_slide13(), encoding="utf-8")
print(f"Written: {slide13_path}")

slide14_path.write_text(build_slide14(), encoding="utf-8")
print(f"Written: {slide14_path}")

slide13_rels.write_text(slide_rels(7), encoding="utf-8")
print(f"Written: {slide13_rels}")

slide14_rels.write_text(slide_rels(7), encoding="utf-8")
print(f"Written: {slide14_rels}")

# --- Update presentation.xml ---
pres_path = BASE / "ppt" / "presentation.xml"
pres_xml = pres_path.read_text(encoding="utf-8")

OLD_TAG = "</p:sldIdLst>"
NEW_ENTRIES = (
    '<p:sldId id="268" r:id="rId19"/>'
    '<p:sldId id="269" r:id="rId20"/>'
    "</p:sldIdLst>"
)

if "rId19" not in pres_xml:
    pres_xml = pres_xml.replace(OLD_TAG, NEW_ENTRIES)
    pres_path.write_text(pres_xml, encoding="utf-8")
    print("Updated presentation.xml")
else:
    print("presentation.xml already has rId19 — skipped")

# --- Update presentation.xml.rels ---
pres_rels_path = BASE / "ppt" / "_rels" / "presentation.xml.rels"
pres_rels = pres_rels_path.read_text(encoding="utf-8")

REL_ANCHOR = "</Relationships>"
NEW_RELS = (
    '<Relationship Id="rId19" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" '
    'Target="slides/slide13.xml"/>'
    '<Relationship Id="rId20" '
    'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" '
    'Target="slides/slide14.xml"/>'
    "</Relationships>"
)

if "slide13.xml" not in pres_rels:
    pres_rels = pres_rels.replace(REL_ANCHOR, NEW_RELS)
    pres_rels_path.write_text(pres_rels, encoding="utf-8")
    print("Updated presentation.xml.rels")
else:
    print("presentation.xml.rels already has slide13 — skipped")

# --- Update [Content_Types].xml ---
ct_path = BASE / "[Content_Types].xml"
ct_xml = ct_path.read_text(encoding="utf-8")

CT_ANCHOR = "</Types>"
NEW_CT = (
    '<Override PartName="/ppt/slides/slide13.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>'
    '<Override PartName="/ppt/slides/slide14.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>'
    "</Types>"
)

if "slide13.xml" not in ct_xml:
    ct_xml = ct_xml.replace(CT_ANCHOR, NEW_CT)
    ct_path.write_text(ct_xml, encoding="utf-8")
    print("Updated [Content_Types].xml")
else:
    print("[Content_Types].xml already has slide13 — skipped")

print("Done.")
