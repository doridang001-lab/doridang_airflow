# Doridang Report Format

## Save Path
- Save the markdown file to:
  - `/mnt/c/Users/민준/OneDrive - 주식회사 도리당/data/report/`
- Use the filename format:
  - `YYMMDD_제목.md`

## Report Sections
- Title line with date and weekday
- `# 0. 선결론` when an executive conclusion exists
- `# 1. 문제 및 배경 / 분석 목적`
- `# 2. 목표`
- `# 3. 분석 기준`
- `# 4. 가설`
- `# 5. 분석 한계점` when needed
- `# 6. 결론`

## Formatting Rules
- Express data comparisons as markdown tables.
- Express rate changes as `+X%` or `-X%`.
- Put key insights and recommendations in block quotes.
- Use channel markers where relevant:
  - `🛵 배민`
  - `🚀 쿠팡`
  - `🥕 당근`

## Behavior Rules
- Infer missing sections from the source when the intent is still clear.
- Split multiple hypotheses into `4.1`, `4.2`, and so on.
- Use `[데이터 필요]` only when the missing data prevents a supported claim.
- Show the report body before saving when the user asks for review first.
