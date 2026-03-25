---
name: shop-trend
description: 배민 마케팅 OneDrive 파티션 데이터로 매장 1페이지 PDF 분석 리포트 생성
---

## instructions

1. `$ARGUMENTS`를 공백으로 분리해 브랜드명과 매장명 키워드 파싱
   - 형식: `/shop-trend <브랜드> <매장명키워드>`
   - 예: `/shop-trend 도리당 상도점`
   - 브랜드 또는 매장명이 누락되면 사용법 안내 후 종료

2. 다음 명령어 실행:
   ```
   python scripts/analyze/shop_trend_report.py --brand <브랜드> --store <매장명키워드>
   ```

3. 실행 후 stdout에 출력된 JSON 파일 경로를 파싱해 JSON 파일 읽기

4. JSON에서 아래 정보를 추출해 한국어로 보고:
   - `summary.store`: 실제 매장 전체명
   - `summary.yms`: 분석된 월 목록
   - `summary.warn_count`: 경고 항목 수
   - `summary.result`: WARN / PASS
   - `meta.pdf_path`: PDF 저장 경로
   - `stats.warnings`: 경고 항목별 label, value, tip

## behavior

- 브랜드/매장명 누락 시: "사용법: /shop-trend <브랜드> <매장명> (예: /shop-trend 도리당 상도점)" 출력 후 종료
- 스크립트 exit code 1 (실패) 시: JSON의 `issues[0].traceback` 내용을 요약해서 표시
- 성공 시 출력 형식:
  ```
  ✅ 리포트 생성 완료

  매장: {store}
  분석기간: {yms}
  경고 항목: {warn_count}개 ({result})

  ⚠️ 경고 내역:
    - {label} {value} → {tip}
    ...

  📄 PDF 저장: {pdf_path}
  ```
- 경고 없으면 "✓ 모든 지표 정상 범위" 표시
- PDF 경로는 절대경로로 명시
