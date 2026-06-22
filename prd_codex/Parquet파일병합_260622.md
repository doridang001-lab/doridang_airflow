# Parquet 파일 병합

## Task
배민 주문 수집 과정에서 hostname suffix가 붙은 파일(`orders_2026-06-DESKTOP-HG136JL.parquet`)이 생성되었다.
이 파일을 메인 파일(`orders_2026-06.parquet`)에 병합하고 중복을 제거한 뒤 소스 파일을 삭제한다.

## Target Path
```
C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\baemin_macro\orders\brand=도리당\store=송파삼전점\ym=2026-06\
├── orders_2026-06.parquet            ← 최종 유지 (덮어쓰기)
└── orders_2026-06-DESKTOP-HG136JL.parquet  ← 병합 후 삭제
```

## Implementation Steps

Python 인라인 스크립트로 실행한다. 별도 파일 생성 없음.

```python
import pandas as pd
from pathlib import Path

base = Path(r"C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\baemin_macro\orders\brand=도리당\store=송파삼전점\ym=2026-06")

src = base / "orders_2026-06-DESKTOP-HG136JL.parquet"
dst = base / "orders_2026-06.parquet"

df_src = pd.read_parquet(src)
print(f"[src] rows: {len(df_src)}")

if dst.exists():
    df_dst = pd.read_parquet(dst)
    print(f"[dst] rows: {len(df_dst)}")
    df_merged = pd.concat([df_dst, df_src], ignore_index=True)
else:
    df_merged = df_src.copy()

df_merged.drop_duplicates(inplace=True)
print(f"[merged] rows after dedup: {len(df_merged)}")

df_merged.to_parquet(dst, index=False, engine="pyarrow")
print(f"[saved] {dst}")

src.unlink()
print(f"[deleted] {src}")
```

## Test Cases

1. **src 파일 존재 확인**
   ```python
   assert Path(r"...\orders_2026-06-DESKTOP-HG136JL.parquet").exists()
   ```
   → 기대: True

2. **병합 후 dst row 수 >= 이전 dst row 수**
   → 기대: len(df_merged) >= len(df_dst)

3. **src 파일 삭제 확인**
   ```python
   assert not Path(r"...\orders_2026-06-DESKTOP-HG136JL.parquet").exists()
   ```
   → 기대: True

4. **dst 파일 정상 읽기**
   ```python
   df = pd.read_parquet(r"...\orders_2026-06.parquet")
   print(df.shape)
   ```
   → 기대: (N, M) 형태 출력, 오류 없음

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS
```

## Constraints
- dst 파일이 없을 경우 src 파일만으로 저장 (concat 없이)
- drop_duplicates는 모든 컬럼 기준 (subset 지정 없음)
- engine은 pyarrow 고정
- src 파일은 병합 성공 후에만 삭제

## Do Not Ask — Decide Yourself
- dst 파일이 이미 존재하면: 읽어서 concat
- dst 파일이 없으면: src만 저장
- 중복 기준: 모든 컬럼 (subset=None)
- 저장 포맷: parquet, index=False
