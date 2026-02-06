#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
원본 컨텐츠와 생성된 요약을 비교 분석
LLM 요약이 제대로 추출되는지, 아니면 템플릿 생성인지 확인
"""

import pandas as pd
import sys
from collections import Counter

# CSV 로드
csv_path = r"C:\Local_DB\temp\visit_log_final_with_llm_v2.csv"
try:
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"✅ 파일 로드 성공: {len(df)} 행, {len(df.columns)} 열\n")
except Exception as e:
    print(f"❌ 파일 로드 실패: {e}")
    sys.exit(1)

# 필요한 컬럼 확인
required_cols = ['visit_date', 'project', 'llm_summary', 'llm_advice']
missing = [col for col in required_cols if col not in df.columns]
if missing:
    print(f"❌ 누락된 컬럼: {missing}")
    print(f"사용 가능한 컬럼: {df.columns.tolist()}")
    sys.exit(1)

# content_text 또는 visit_content 찾기
content_cols = [col for col in df.columns if 'content' in col.lower() or 'text' in col.lower()]
if not content_cols:
    print(f"❌ 원본 컨텐츠 컬럼을 찾을 수 없습니다")
    print(f"가능한 컬럼: {df.columns.tolist()[:20]}")
    sys.exit(1)

content_col = content_cols[0]
print(f"📝 원본 컨텐츠 컬럼: '{content_col}'\n")

# ============================================================================
# 1. 요약 다양성 분석
# ============================================================================
print("=" * 80)
print("1️⃣  요약(llm_summary) 다양성 분석")
print("=" * 80)

summaries = df['llm_summary'].dropna().astype(str)
summary_counts = Counter(summaries)

print(f"📊 총 요약 개수: {len(summaries)}")
print(f"📊 고유한 요약 개수: {len(summary_counts)}")
print(f"📊 다양성 비율: {len(summary_counts)/len(summaries)*100:.1f}%")
print(f"📊 가장 흔한 요약 top 10:\n")

for rank, (summary, count) in enumerate(summary_counts.most_common(10), 1):
    pct = count / len(summaries) * 100
    print(f"   {rank}. [{count:3d}건 {pct:5.1f}%] {summary[:60]}")

print()

# ============================================================================
# 2. 조치(llm_advice) 다양성 분석
# ============================================================================
print("=" * 80)
print("2️⃣  조치(llm_advice) 다양성 분석")
print("=" * 80)

advice = df['llm_advice'].dropna().astype(str)
advice_counts = Counter(advice)

print(f"📊 총 조치 개수: {len(advice)}")
print(f"📊 고유한 조치 개수: {len(advice_counts)}")
print(f"📊 다양성 비율: {len(advice_counts)/len(advice)*100:.1f}%")
print(f"📊 가장 흔한 조치 top 10:\n")

for rank, (adv, count) in enumerate(advice_counts.most_common(10), 1):
    pct = count / len(advice) * 100
    print(f"   {rank}. [{count:3d}건 {pct:5.1f}%] {adv[:60]}")

print()

# ============================================================================
# 3. 원본 ↔ 요약 매칭 샘플 (다양한 패턴)
# ============================================================================
print("=" * 80)
print("3️⃣  원본 컨텐츠 ↔ 생성된 요약 샘플 매칭")
print("=" * 80)

# 모든 (요약, 조치) 조합 분류
df['summary_advice_combo'] = df['llm_summary'].astype(str) + " | " + df['llm_advice'].astype(str)
combo_groups = df.groupby('summary_advice_combo').size().sort_values(ascending=False)

print(f"\n총 {len(combo_groups)} 개의 (요약, 조치) 조합 발견\n")
print(f"상위 5개 조합별로 샘플 1건씩 보여줍니다:\n")

for combo_idx, (combo, count) in enumerate(combo_groups.head(5).items(), 1):
    summary_text, advice_text = combo.rsplit(' | ', 1)
    sample_row = df[df['summary_advice_combo'] == combo].iloc[0]
    
    print(f"\n{'─' * 78}")
    print(f"조합 #{combo_idx}: {count}건에서 발견")
    print(f"{'─' * 78}")
    print(f"📅 날짜: {sample_row['visit_date']}")
    print(f"🏪 프로젝트: {sample_row['project']}")
    print(f"\n📄 원본 컨텐츠 (처음 200자):")
    orig_text = str(sample_row[content_col])[:200]
    print(f"   {orig_text}")
    if len(str(sample_row[content_col])) > 200:
        print(f"   ... (총 {len(str(sample_row[content_col]))}자)")
    print(f"\n✅ 생성된 요약: {summary_text}")
    print(f"⚡ 생성된 조치: {advice_text}")

print("\n\n" + "=" * 80)
print("📌 분석 결론")
print("=" * 80)

print(f"""
✅ 요약 다양성: {len(summary_counts)} 가지
✅ 조치 다양성: {len(advice_counts)} 가지
✅ (요약, 조치) 조합: {len(combo_groups)} 가지

🔍 해석:
  • 다양성 비율이 {len(summary_counts)/len(summaries)*100:.1f}%면, 
    {'✓ 충분한 다양성 있음' if len(summary_counts)/len(summaries) > 0.5 else '⚠️  낮은 다양성 (템플릿 가능성)'}
  • 상위 1개 조합이 전체의 {combo_counts.iloc[0]/len(df)*100:.1f}%를 차지
""")
