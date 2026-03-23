#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
쿠팡이츠 CMG 파티션 파이프라인 검증 스크립트

사용법:
  python3 scripts/test_coupang_eats_cmg.py
"""

import sys
import json
from pathlib import Path
from glob import glob

# 기본 경로 설정
source_dir = Path("E:/down/업로드_temp")
file_pattern = "coupangeats_cmg_*.csv"

print("\n" + "="*70)
print("쿠팡이츠 CMG 파티션 파이프라인 검증")
print("="*70)

# 1. 파일 수집 검증
print("\n[1단계] 파일 수집 검증")
print(f"  소스 경로: {source_dir}")
print(f"  파일 패턴: {file_pattern}")

files = sorted(glob(str(source_dir / file_pattern)))
print(f"  발견 파일 수: {len(files)}")

if files:
    print(f"  샘플 파일 (처음 3개):")
    for f in files[:3]:
        p = Path(f)
        size_mb = p.stat().st_size / (1024**2)
        print(f"    - {p.name} ({size_mb:.2f} MB)")
    if len(files) > 3:
        print(f"    ... 외 {len(files) - 3}개 파일")
else:
    print("  X CSV 파일을 찾을 수 없습니다!")
    sys.exit(1)

# 2. 디렉토리 검증
print("\n[2단계] 출력 디렉토리 검증")

analytics_base = Path.home() / "OneDrive - 주식회사 도리당" / "data" / "analytics"
coupang_marketing_dir = analytics_base / "coupang_marketing"

if analytics_base.exists():
    print(f"  OK Analytics 기본 경로 존재")
    print(f"     {analytics_base}")
else:
    print(f"  INFO Analytics 기본 경로 없음 (초기 실행 시 생성됨)")
    print(f"     {analytics_base}")

print(f"\n  쿠팡 마케팅 디렉토리:")
print(f"    {coupang_marketing_dir}")
if coupang_marketing_dir.exists():
    print(f"  OK 디렉토리 존재")
    # 기존 파티션 확인
    partitions = list(coupang_marketing_dir.glob("brand=*/store=*/ym=*"))
    print(f"  기존 파티션 수: {len(partitions)}")
    if partitions:
        print(f"  샘플 파티션 (처음 3개):")
        for p in partitions[:3]:
            csv_file = p / "data.csv"
            if csv_file.exists():
                with open(csv_file, 'r', encoding='utf-8-sig') as f:
                    lines = len(f.readlines()) - 1  # 헤더 제외
                    print(f"    - {p.relative_to(coupang_marketing_dir)} ({lines} 행)")
else:
    print(f"  INFO 디렉토리 없음 (초기 실행 시 생성됨)")

# 3. 설정 검증
print("\n[3단계] 설정 검증")

config = {
    "source_dir": str(source_dir),
    "file_pattern": file_pattern,
    "dataset_name": "coupang_marketing",
    "brand": "도리당",
    "store_col": "매장명",
    "date_col": "조회일자",
    "timestamp_col": "collected_at",
    "dedup_cols": ["매장명", "조회일자"],
    "encoding": "utf-8-sig",
    "output_base": str(coupang_marketing_dir),
}

for key, value in config.items():
    if isinstance(value, list):
        print(f"  {key}: {value}")
    else:
        print(f"  {key}: {value}")

# 4. 의존성 검증
print("\n[4단계] 모듈 의존성 검증")

try:
    sys.path.insert(0, str(Path(__file__).parent.parent))

    # 경로 모듈
    try:
        from modules.transform.utility.paths import ANALYTICS_DB
        print(f"  OK ANALYTICS_DB: {ANALYTICS_DB}")
    except ImportError as e:
        print(f"  WARN ANALYTICS_DB import 실패: {e}")

    # OneDrive 모듈
    try:
        from modules.transform.utility.onedrive import backup_to_onedrive
        print(f"  OK backup_to_onedrive 함수 사용 가능")
    except ImportError as e:
        print(f"  WARN backup_to_onedrive import 실패: {e}")

    # 파이프라인 모듈
    try:
        from modules.transform.pipelines.strategy.SMP_CoupangEats_CMG_Partition import load_coupangeats_cmg_partition
        print(f"  OK load_coupangeats_cmg_partition 함수 사용 가능")
    except ImportError as e:
        print(f"  WARN 파이프라인 모듈 import 실패: {e}")

except Exception as e:
    print(f"  ERROR 의존성 검증 실패: {e}")

# 5. 요약
print("\n" + "="*70)
print("검증 완료")
print("="*70)
print("\nOK 준비 완료!")
print("\n다음 단계:")
print("  1. Airflow 웹 UI에서 DAG 활성화")
print("     DAG ID: Strategy_CoupangEats_CMG_Partition_Dags")
print("  2. 또는 수동 실행:")
print("     airflow dags test Strategy_CoupangEats_CMG_Partition_Dags 2026-03-23")
print("\n스케줄:")
print("  매일 22:00 (UTC+9 기준)")
print("\n문서:")
print("  PIPELINE_COUPANG_EATS_CMG.md 참조")
print()
