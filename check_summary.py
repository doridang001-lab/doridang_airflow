import pandas as pd

df = pd.read_csv(r'C:\Local_DB\temp\visit_log_final_with_llm_v2.csv', encoding='utf-8-sig')

print('='*100)
print('🔍 요약/조치 다양성 분석')
print('='*100)

print(f'\n📊 요약 통계:')
print(f'   전체: {len(df):,}건')
print(f'   고유: {df["llm_summary"].nunique()}종류')
print(f'   다양성: {df["llm_summary"].nunique()/len(df)*100:.1f}%')

print(f'\n✅ 상위 15개 반복 요약:')
for i, (s, c) in enumerate(df['llm_summary'].value_counts().head(15).items(), 1):
    print(f'   {i:2d}. [{c:3d}건] {s}')

print(f'\n📊 조치 통계:')
print(f'   전체: {len(df):,}건')
print(f'   고유: {df["llm_advice"].nunique()}종류')
print(f'   다양성: {df["llm_advice"].nunique()/len(df)*100:.1f}%')

print(f'\n✅ 상위 10개 조치:')
for i, (a, c) in enumerate(df['llm_advice'].value_counts().head(10).items(), 1):
    pct = c / len(df) * 100
    print(f'   {i:2d}. [{c:3d}건 {pct:5.1f}%] {a}')

# 원본 샘플 확인
print(f'\n✅ 원본 콘텐츠 샘플 (처음 5개):')
print('='*100)
for i in range(min(5, len(df))):
    print(f'\n[{i+1}] {df.iloc[i]["project"]}')
    content = df.iloc[i]["content_text"] if "content_text" in df.columns else "N/A"
    print(f'   원본: {str(content)[:100]}...')
    print(f'   요약: {df.iloc[i]["llm_summary"]}')
    print(f'   조치: {df.iloc[i]["llm_advice"]}')

print('\n' + '='*100)
