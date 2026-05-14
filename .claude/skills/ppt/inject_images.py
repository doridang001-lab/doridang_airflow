#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64, sys, re
sys.stdout.reconfigure(encoding='utf-8')

with open(r'C:\Users\민준\AppData\Local\Temp\wmux\screenshot-1778720674583.png', 'rb') as f:
    b64_dash = base64.b64encode(f.read()).decode()
with open(r'C:\Users\민준\AppData\Local\Temp\wmux\screenshot-1778720681969.png', 'rb') as f:
    b64_alerts = base64.b64encode(f.read()).decode()

html_path = r'C:\Users\민준\OneDrive - 주식회사 도리당\data\report\PowerPoint\프담_CS_DX전환_1.html'
with open(html_path, encoding='utf-8') as f:
    content = f.read()

# S6: 타임라인 아래에 알림 이미지 삽입 (data-step="3" → data-step="4"로 밀기)
content = content.replace(
    '<div class="message-box" data-step="3">\n<p>사람이 직접 찾지 않아도',
    f'<div data-step="3" style="width:97%;margin:0.5vw auto 1vw;">'
    f'<img src="data:image/png;base64,{b64_alerts}" '
    f'style="width:100%;border-radius:.6vw;border:1px solid #333;" alt="알림 화면">'
    f'</div>\n'
    f'<div class="message-box" data-step="4">\n<p>사람이 직접 찾지 않아도'
)

# S7: content 내부를 이미지+설명 2단 구조로 교체
old_s7 = (
    '<div class="content">\n'
    '<div class="conclusion-box" data-step="2">\n'
    '<h3>기존</h3>\n'
    '<p>개별 건을 직접 확인해야 함</p>\n'
    '</div>\n'
    '<div style="color:#666;font-size:2vw;margin:1vw 0;">↓</div>\n'
    '<div class="conclusion-box" data-step="3">\n'
    '<h3>현재</h3>\n'
    '<p>KPI와 처리 현황을<br>실시간으로 확인 가능</p>\n'
    '</div>\n'
    '<div class="message-box" data-step="4">\n'
    '<p>필요할 때 찾아보는 방식이 아니라<br><strong>즉시 판단 가능한 구조</strong>로 전환</p>\n'
    '</div>\n'
    '</div>'
)

new_s7 = (
    '<div class="content" style="flex-direction:row;gap:2vw;align-items:flex-start;padding-top:0.5vw;">\n'
    f'<div data-step="2" style="flex:1.6;">'
    f'<img src="data:image/png;base64,{b64_dash}" '
    f'style="width:100%;border-radius:.6vw;border:1px solid #1a3a28;" alt="CS 현황판">'
    f'</div>\n'
    '<div style="flex:1;display:flex;flex-direction:column;gap:1.5vw;justify-content:center;">\n'
    '<div class="conclusion-box" data-step="3">\n'
    '<h3>기존</h3>\n'
    '<p>개별 건 직접 확인<br>필요할 때 찾아보는 방식</p>\n'
    '</div>\n'
    '<div style="color:#666;font-size:1.5vw;text-align:center;">↓</div>\n'
    '<div class="conclusion-box" data-step="4">\n'
    '<h3>현재</h3>\n'
    '<p>KPI + 처리 현황<br>실시간 확인 · 즉시 판단 가능</p>\n'
    '</div>\n'
    '</div>\n'
    '</div>'
)

if old_s7 in content:
    content = content.replace(old_s7, new_s7)
    print("S7 replaced OK")
else:
    print("S7 pattern not found - skipping")

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(content)
print("HTML updated:", html_path)
