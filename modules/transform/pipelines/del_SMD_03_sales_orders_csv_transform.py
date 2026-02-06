# modules/transform/pipelines/SMD_03_sales_orders_csv_transform.py
"""
판매 주문서 데이터 변환 파이프라인 (Task 분리형)
각 단계를 독립적인 Task로 실행
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR
from modules.transform.utility.io3 import load_files
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ============================================================
# 📥 Step 1: 데이터 로드
# ============================================================

def load_sales_orders_daily_csv(**context):
    """메인 주문 데이터 로드"""
    return load_files(
        patterns=['sales_orders_daily.csv'],
        search_paths=[LOCAL_DB / "영업관리부_DB"],
        xcom_key='sales_orders_daily_path',
        file_type='csv',
        **context
    )


def load_baemin_store_now(**context):
    """배민 우리가게 현황 로드"""
    return load_files(
        patterns=['baemin_metrics_*.csv'],
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'),
            COLLECT_DB / "영업관리부_수집",
        ],
        xcom_key='baemin_store_now_path',
        file_type='csv',
        **context
    )


# modules/transform/pipelines/SMD_03_sales_orders_csv_transform.py

def load_toorder_review(**context):
    """토더 리뷰 로드 및 전처리 (Document 5 로직 그대로)"""
    import pandas as pd
    import glob
    from pathlib import Path
    
    ti = context['task_instance']
    
    upload_temp_path = Path('/opt/airflow/download/업로드_temp')
    onedrive_path = COLLECT_DB / "영업관리부_수집"
    download_path = Path('/opt/airflow/download')
    
    # ============================================================
    # 1. CSV 파일 찾기
    # ============================================================
    csv_files = []
    
    if upload_temp_path.exists():
        temp_csvs = list(upload_temp_path.glob('toorder_review_*.csv'))
        if temp_csvs:
            csv_files.extend(temp_csvs)
    
    if onedrive_path.exists():
        onedrive_csvs = list(onedrive_path.glob('toorder_review_*.csv'))
        if onedrive_csvs:
            csv_files.extend(onedrive_csvs)
    
    if csv_files:
        print(f"[✅ CSV 재사용] 총 {len(csv_files)}개 파일 발견")
        
        # 중복 파일 제거
        unique_files = {}
        for f in csv_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        
        # CSV 읽기
        dfs = []
        for fpath in file_paths:
            print(f"   읽는 중: {fpath}")
            df = pd.read_csv(fpath)
            dfs.append(df)
            print(f"   ✓ {len(df)}행 로드")
        
        result_df = pd.concat(dfs, ignore_index=True)
        print(f"병합 완료: {len(result_df):,}행")
        
        # ⭐ 중복 제거
        before = len(result_df)
        result_df.drop_duplicates(subset=['date', 'stores_name'], keep='last', inplace=True)
        after = len(result_df)
        if before - after > 0:
            print(f"[중복 제거] {before - after:,}건 제거됨 → {after:,}행")
        
    else:
        # ============================================================
        # 2. Excel 파일 찾기
        # ============================================================
        excel_files = []
        
        if upload_temp_path.exists():
            temp_excels = list(upload_temp_path.glob('toorder_review_doridang1_*.xlsx'))
            if temp_excels:
                excel_files.extend(temp_excels)
        
        if download_path.exists():
            download_excels = list(download_path.glob('toorder_review_doridang1_*.xlsx'))
            if download_excels:
                excel_files.extend(download_excels)
        
        if not excel_files:
            print(f"[❌ 에러] 토더 리뷰 파일 없음")
            ti.xcom_push(key='toorder_review_path', value=None)
            return "0건 (파일 없음)"
        
        print(f"[✅ 엑셀 로드] 총 {len(excel_files)}개 파일 발견")
        
        # 중복 파일 제거
        unique_files = {}
        for f in excel_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        
        # Excel 읽기
        dfs = []
        for fpath in file_paths:
            print(f"   읽는 중: {fpath}")
            df = pd.read_excel(fpath, header=3)
            
            # 파일명에서 날짜 추출
            file_name = fpath.name
            try:
                date_str = file_name.split('_')[-1].split('.')[0]
                df['date'] = date_str
            except Exception as e:
                print(f"[경고] 날짜 추출 실패: {file_name}, {e}")
                df['date'] = None
            
            dfs.append(df)
            print(f"   ✓ {len(df)}행 로드")
        
        result_df = pd.concat(dfs, ignore_index=True)
        print(f"병합 완료: {len(result_df):,}행")
        
        # ============================================================
        # 3. 전처리 (Document 5 로직 그대로)
        # ============================================================
        print(f"\n[전처리] 시작...")
        
        # 컬럼 선택
        col = ['date', '매장명.1', '채널', '주문 수', '리뷰 수', '답변완료 수', '평균 별점']
        result_df = result_df[col]
        
        # 결측치 제거
        result_df = result_df[~result_df["채널"].isnull()]
        
        # 0점 제외 평균 함수
        def mean_excluding_zero(x):
            non_zero = x[x > 0]
            return non_zero.mean() if len(non_zero) > 0 else 0
        
        # 그룹화
        result_df = result_df.groupby(["date", "매장명.1"]).agg(
            전체_주문수=("주문 수", "sum"),
            전체_리뷰수=("리뷰 수", "sum"),
            전체_답변완료수=("답변완료 수", "sum"),
            전체_평균별점=("평균 별점", mean_excluding_zero)
        ).reset_index()
        
        # 매장명 정리
        result_df.rename(columns={"매장명.1": "stores_name"}, inplace=True)
        result_df["stores_name"] = "도리당 " + result_df["stores_name"]
        result_df["stores_name"] = result_df["stores_name"].replace({
            "도리당 일산백석점": "도리당 백석점",
            "도리당 서울대점": "도리당 서울대점",
            "도리당 구로디지털단지점": "도리당 구로디지털점",
            "도리당 충주봉방점": "도리당 충주역점"
        })
        
        print(f"[전처리] 완료: {len(result_df):,}행")
    
    # ============================================================
    # 4. Parquet 저장
    # ============================================================
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    output_path = temp_dir / f"toorder_review_{context['ds_nodash']}.parquet"
    result_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='toorder_review_path', value=str(output_path))
    
    return f"✅ 토더: {len(result_df):,}건"


def load_baemin_history(**context):
    """배민 변경이력 로드"""
    return load_files(
        patterns=['baemin_change_history_*.csv'],
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'),
            COLLECT_DB / "영업관리부_수집",
        ],
        xcom_key='baemin_history_path',
        file_type='csv',
        **context
    )


# ============================================================
# 🔄 Step 2: 플랫폼별 집계
# ============================================================

def aggregate_by_platform(**context):
    """플랫폼별 집계 + Pivot"""
    ti = context['task_instance']
    
    # 입력 데이터 로드
    sales_path = ti.xcom_pull(task_ids='load_sales_orders_daily_csv', key='sales_orders_daily_path')
    if not sales_path:
        raise FileNotFoundError("sales_daily_orders.csv를 찾을 수 없습니다")
    
    df = pd.read_parquet(sales_path)
    print(f"\n{'='*60}")
    print(f"[입력] {len(df):,}행 × {len(df.columns)}컬럼")
    
    # ⭐ 날짜 변환
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df = df[df['order_date'].notna()].copy()
    df['order_daily'] = df['order_date'].dt.strftime('%Y-%m-%d')
    df['order_daily'] = pd.to_datetime(df['order_daily'])
    
    # ⭐ 매장명 정리
    if '매장명' not in df.columns:
        df['매장명'] = df.get('stores', df.get('store_names', 'Unknown'))
    
    # ============================================================
    # 1. 기본 집계 (플랫폼 합침)
    # ============================================================
    print("\n[집계] 기본 집계 중...")
    
    base_agg = df.groupby(
        ['order_daily', '매장명', '담당자', 'email'],
        dropna=False
    ).agg(
        total_order_count=('order_id', 'nunique'),
        total_amount=('total_amount', 'sum'),
        fee_ad=('fee_ad', 'sum'),
        settlement_amount=('settlement_amount', 'sum'),
        실오픈일=('실오픈일', 'first'),
        platform=('platform', lambda x: ','.join(sorted(x.dropna().astype(str).unique()))),
    ).reset_index()
    
    # ============================================================
    # 2. 플랫폼별 집계 + Pivot
    # ============================================================
    print("\n[집계] 플랫폼별 Pivot 중...")
    
    platform_agg = df.groupby(
        ['order_daily', '매장명', '담당자', 'email', 'platform'],
        dropna=False
    ).agg(
        total_order_count=('order_id', 'nunique'),
        total_amount=('total_amount', 'sum'),
        fee_ad=('fee_ad', 'sum'),
        settlement_amount=('settlement_amount', 'sum'),
    ).reset_index()
    
    # Pivot: 플랫폼을 컬럼으로 펼치기
    metrics = ['total_order_count', 'total_amount', 'fee_ad', 'settlement_amount']
    
    pivoted_dfs = []
    for metric in metrics:
        pivot = platform_agg.pivot_table(
            index=['order_daily', '매장명', '담당자', 'email'],
            columns='platform',
            values=metric,
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        pivot.columns = [f"{metric}_{col}" if col not in ['order_daily', '매장명', '담당자', 'email'] 
                        else col for col in pivot.columns]
        
        pivoted_dfs.append(pivot)
    
    # 모든 pivot 합치기
    platform_cols = pivoted_dfs[0].copy()
    for df_pivot in pivoted_dfs[1:]:
        platform_cols = platform_cols.merge(
            df_pivot,
            on=['order_daily', '매장명', '담당자', 'email'],
            how='outer'
        )
    
    # ============================================================
    # 3. 기본 집계 + 플랫폼별 컬럼 합치기
    # ============================================================
    print("\n[집계] 최종 병합 중...")
    
    result = base_agg.merge(
        platform_cols,
        on=['order_daily', '매장명', '담당자', 'email'],
        how='left'
    )
    
    # NaN을 0으로 채우기
    platform_metric_cols = [col for col in result.columns 
                           if any(col.startswith(f"{m}_") for m in metrics)]
    result[platform_metric_cols] = result[platform_metric_cols].fillna(0)
    
    # ARPU 계산
    result['ARPU'] = (result['total_amount'] / result['total_order_count'].replace(0, np.nan)).round(0)
    
    print(f"\n[출력] {len(result):,}행 × {len(result.columns)}컬럼")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"aggregated_{context['ds_nodash']}.parquet"
    result.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='aggregated_path', value=str(output_path))
    return f"✅ 집계 완료: {len(result):,}건"


# ============================================================
# 📊 Step 3: 스코어 계산
# ============================================================

def calculate_scores(**context):
    """스코어 계산 (trend, total, 7d, 4week)"""
    ti = context['task_instance']
    
    # 입력 데이터 로드
    agg_path = ti.xcom_pull(task_ids='aggregate_by_platform', key='aggregated_path')
    if not agg_path:
        raise FileNotFoundError("집계 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(agg_path)
    print(f"\n{'='*60}")
    print(f"[입력] {len(df):,}행")
    
    # ⭐ 날짜 정렬
    df = df.sort_values(['매장명', 'order_daily'])
    
    # ============================================================
    # 이동평균 계산
    # ============================================================
    print("\n[스코어] 이동평균 계산 중...")
    
    df['ma_14'] = df.groupby('매장명')['total_amount'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(2)
    
    df['ma_28'] = df.groupby('매장명')['total_amount'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(2)
    
    df['current_avg_2week'] = df.groupby('매장명')['total_amount'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(2)
    
    df['current_avg_4week'] = df.groupby('매장명')['total_amount'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(2)
    
    # ============================================================
    # 주간 합계
    # ============================================================
    df['sum_7d_recent'] = df.groupby('매장명')['total_amount'].transform(
        lambda x: x.rolling(window=7, min_periods=1).sum()
    ).astype(int)
    
    df['sum_7d_prev'] = df.groupby('매장명')['total_amount'].transform(
        lambda x: x.shift(7).rolling(window=7, min_periods=1).sum()
    ).fillna(0).astype(int)
    
    # ============================================================
    # 요일별 비교
    # ============================================================
    df['weekday'] = df['order_daily'].dt.day_name()
    df['prev_week_same_day'] = df.groupby(['매장명', 'weekday'])['total_amount'].shift(1).fillna(0).astype(int)
    df['prev_2week_same_day'] = df.groupby(['매장명', 'weekday'])['total_amount'].shift(2).fillna(0).astype(int)
    df['prev_3week_same_day'] = df.groupby(['매장명', 'weekday'])['total_amount'].shift(3).fillna(0).astype(int)
    
    # ============================================================
    # 점수 계산 함수
    # ============================================================
    def calc_score(current, baseline):
        if pd.isna(current) or pd.isna(baseline):
            return 0
        
        if baseline == 0:
            return 0 if current > 0 else 0
        
        change_rate = (current - baseline) / baseline
        
        if change_rate >= 0:
            return 0
        elif change_rate > -0.1:
            return 1
        else:
            return 2
    
    # ============================================================
    # 4가지 점수 계산
    # ============================================================
    print("\n[스코어] 점수 계산 중...")
    
    df['score_trend'] = df.apply(lambda row: calc_score(row['ma_14'], row['ma_28']), axis=1)
    df['score_total'] = df.apply(lambda row: calc_score(row['current_avg_2week'], row['ma_14']), axis=1)
    df['score_7d_total'] = df.apply(lambda row: calc_score(row['sum_7d_recent'], row['sum_7d_prev']), axis=1)
    df['score_4week_total'] = df.apply(lambda row: calc_score(row['current_avg_2week'], row['current_avg_4week']), axis=1)
    
    # 종합 점수
    df['score'] = (
        df['score_trend'] + 
        df['score_total'] + 
        df['score_7d_total'] + 
        df['score_4week_total']
    )
    
    # 상태 판정
    df['status'] = np.where(
        df['score'] >= 6, '위험',
        np.where(df['score'] >= 4, '주의', '정상')
    )
    
    df['pre_status'] = df.groupby('매장명')['status'].shift(1).fillna('정상')
    df['uploaded_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"[스코어] 분포: {df['status'].value_counts().to_dict()}")
    print(f"\n[출력] {len(df):,}행 × {len(df.columns)}컬럼")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"scored_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='scored_path', value=str(output_path))
    return f"✅ 스코어 완료: {len(df):,}건"


# ============================================================
# 📅 Step 4: 전기 비교
# ============================================================

def add_period_comparisons(**context):
    """전일/전주/전월 비교 데이터 추가"""
    ti = context['task_instance']
    
    # 입력 데이터 로드
    scored_path = ti.xcom_pull(task_ids='calculate_scores', key='scored_path')
    if not scored_path:
        raise FileNotFoundError("스코어 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(scored_path)
    print(f"\n{'='*60}")
    print(f"[입력] {len(df):,}행")
    
    # ============================================================
    # 날짜 관련 컬럼 생성
    # ============================================================
    print("\n[전기비교] 날짜 컬럼 생성 중...")
    
    df['요일_한글'] = df['order_daily'].dt.dayofweek.map({
        0:'월', 1:'화', 2:'수', 3:'목', 4:'금', 5:'토', 6:'일'
    })
    df['요일'] = df['weekday']
    df['order_week'] = df['order_daily'].dt.to_period('W')
    df['order_month'] = df['order_daily'].dt.to_period('M')
    
    df['join_pre_date'] = df['order_daily'] - timedelta(days=1)
    df['join_pre_week_sameday'] = df['order_daily'] - timedelta(days=7)
    df['join_pre_week'] = df['order_week'] - 1
    df['join_pre_month'] = df['order_month'] - 1
    
    # ============================================================
    # 집계 컬럼 정의
    # ============================================================
    agg_columns = {
        'total_amount': 'sum',
        'total_order_count': 'sum',
        'settlement_amount': 'sum',
        'total_amount_배민': 'sum',
        'total_amount_쿠팡': 'sum',
        'settlement_amount_배민': 'sum',
        'settlement_amount_쿠팡': 'sum',
        'total_order_count_배민': 'sum',
        'total_order_count_쿠팡': 'sum',
    }
    
    # ============================================================
    # 전일 (전체)
    # ============================================================
    print("\n[전기비교] 전일 데이터 생성 중...")
    
    daily_total = df.groupby('order_daily').agg(agg_columns).reset_index()
    
    pre_date_total = daily_total.rename(columns={
        'order_daily': 'join_pre_date',
        'total_amount': '전일_전체매출',
        'total_order_count': '전일_전체주문건수',
        'settlement_amount': '전일_전체정산금액',
        'total_amount_배민': '전일_전체매출_배민',
        'total_amount_쿠팡': '전일_전체매출_쿠팡',
        'settlement_amount_배민': '전일_전체정산금액_배민',
        'settlement_amount_쿠팡': '전일_전체정산금액_쿠팡',
        'total_order_count_배민': '전일_전체주문건수_배민',
        'total_order_count_쿠팡': '전일_전체주문건수_쿠팡',
    })
    
    df = df.merge(pre_date_total, on='join_pre_date', how='left')
    
    # 전일 (매장)
    pre_date_store = df[['order_daily', '매장명', 'total_amount', 'total_order_count', 'settlement_amount',
                         'total_amount_배민', 'total_amount_쿠팡', 'settlement_amount_배민', 'settlement_amount_쿠팡',
                         'total_order_count_배민', 'total_order_count_쿠팡']].rename(columns={
        'order_daily': 'join_pre_date',
        'total_amount': '전일_매장매출',
        'total_order_count': '전일_매장주문건수',
        'settlement_amount': '전일_매장정산금액',
        'total_amount_배민': '전일_매장매출_배민',
        'total_amount_쿠팡': '전일_매장매출_쿠팡',
        'settlement_amount_배민': '전일_매장정산금액_배민',
        'settlement_amount_쿠팡': '전일_매장정산금액_쿠팡',
        'total_order_count_배민': '전일_매장주문건수_배민',
        'total_order_count_쿠팡': '전일_매장주문건수_쿠팡',
    })
    
    df = df.merge(pre_date_store, on=['join_pre_date', '매장명'], how='left')
    
    # ============================================================
    # 전주동요일 (전체)
    # ============================================================
    print("\n[전기비교] 전주동요일 데이터 생성 중...")
    
    pre_week_sameday_total = daily_total.rename(columns={
        'order_daily': 'join_pre_week_sameday',
        'total_amount': '전주동요일_전체매출',
        'total_order_count': '전주동요일_전체주문건수',
        'settlement_amount': '전주동요일_전체정산금액',
        'total_amount_배민': '전주동요일_전체매출_배민',
        'total_amount_쿠팡': '전주동요일_전체매출_쿠팡',
        'settlement_amount_배민': '전주동요일_전체정산금액_배민',
        'settlement_amount_쿠팡': '전주동요일_전체정산금액_쿠팡',
        'total_order_count_배민': '전주동요일_전체주문건수_배민',
        'total_order_count_쿠팡': '전주동요일_전체주문건수_쿠팡',
    })
    
    df = df.merge(pre_week_sameday_total, on='join_pre_week_sameday', how='left')
    
    # 전주동요일 (매장)
    pre_week_sameday_store = df[['order_daily', '매장명', 'total_amount', 'total_order_count', 'settlement_amount',
                                  'total_amount_배민', 'total_amount_쿠팡', 'settlement_amount_배민', 'settlement_amount_쿠팡',
                                  'total_order_count_배민', 'total_order_count_쿠팡']].rename(columns={
        'order_daily': 'join_pre_week_sameday',
        'total_amount': '전주동요일_매장매출',
        'total_order_count': '전주동요일_매장주문건수',
        'settlement_amount': '전주동요일_매장정산금액',
        'total_amount_배민': '전주동요일_매장매출_배민',
        'total_amount_쿠팡': '전주동요일_매장매출_쿠팡',
        'settlement_amount_배민': '전주동요일_매장정산금액_배민',
        'settlement_amount_쿠팡': '전주동요일_매장정산금액_쿠팡',
        'total_order_count_배민': '전주동요일_매장주문건수_배민',
        'total_order_count_쿠팡': '전주동요일_매장주문건수_쿠팡',
    })
    
    df = df.merge(pre_week_sameday_store, on=['join_pre_week_sameday', '매장명'], how='left')
    
    # ============================================================
    # 전주 (전체)
    # ============================================================
    print("\n[전기비교] 전주 데이터 생성 중...")
    
    weekly_total = df.groupby('order_week').agg(agg_columns).reset_index()
    
    pre_week_total = weekly_total.rename(columns={
        'order_week': 'join_pre_week',
        'total_amount': '전주_전체매출',
        'total_order_count': '전주_전체주문건수',
        'settlement_amount': '전주_전체정산금액',
        'total_amount_배민': '전주_전체매출_배민',
        'total_amount_쿠팡': '전주_전체매출_쿠팡',
        'settlement_amount_배민': '전주_전체정산금액_배민',
        'settlement_amount_쿠팡': '전주_전체정산금액_쿠팡',
        'total_order_count_배민': '전주_전체주문건수_배민',
        'total_order_count_쿠팡': '전주_전체주문건수_쿠팡',
    })
    
    df = df.merge(pre_week_total, on='join_pre_week', how='left')
    
    # 전주 (매장)
    weekly_store = df.groupby(['order_week', '매장명']).agg(agg_columns).reset_index()
    
    pre_week_store = weekly_store.rename(columns={
        'order_week': 'join_pre_week',
        'total_amount': '전주_매장매출',
        'total_order_count': '전주_매장주문건수',
        'settlement_amount': '전주_매장정산금액',
        'total_amount_배민': '전주_매장매출_배민',
        'total_amount_쿠팡': '전주_매장매출_쿠팡',
        'settlement_amount_배민': '전주_매장정산금액_배민',
        'settlement_amount_쿠팡': '전주_매장정산금액_쿠팡',
        'total_order_count_배민': '전주_매장주문건수_배민',
        'total_order_count_쿠팡': '전주_매장주문건수_쿠팡',
    })
    
    df = df.merge(pre_week_store, on=['join_pre_week', '매장명'], how='left')
    
    # ============================================================
    # 전월 (전체)
    # ============================================================
    print("\n[전기비교] 전월 데이터 생성 중...")
    
    monthly_total = df.groupby('order_month').agg(agg_columns).reset_index()
    
    pre_month_total = monthly_total.rename(columns={
        'order_month': 'join_pre_month',
        'total_amount': '전월_전체매출',
        'total_order_count': '전월_전체주문건수',
        'settlement_amount': '전월_전체정산금액',
        'total_amount_배민': '전월_전체매출_배민',
        'total_amount_쿠팡': '전월_전체매출_쿠팡',
        'settlement_amount_배민': '전월_전체정산금액_배민',
        'settlement_amount_쿠팡': '전월_전체정산금액_쿠팡',
        'total_order_count_배민': '전월_전체주문건수_배민',
        'total_order_count_쿠팡': '전월_전체주문건수_쿠팡',
    })
    
    df = df.merge(pre_month_total, on='join_pre_month', how='left')
    
    # 전월 (매장)
    monthly_store = df.groupby(['order_month', '매장명']).agg(agg_columns).reset_index()
    
    pre_month_store = monthly_store.rename(columns={
        'order_month': 'join_pre_month',
        'total_amount': '전월_매장매출',
        'total_order_count': '전월_매장주문건수',
        'settlement_amount': '전월_매장정산금액',
        'total_amount_배민': '전월_매장매출_배민',
        'total_amount_쿠팡': '전월_매장매출_쿠팡',
        'settlement_amount_배민': '전월_매장정산금액_배민',
        'settlement_amount_쿠팡': '전월_매장정산금액_쿠팡',
        'total_order_count_배민': '전월_매장주문건수_배민',
        'total_order_count_쿠팡': '전월_매장주문건수_쿠팡',
    })
    
    df = df.merge(pre_month_store, on=['join_pre_month', '매장명'], how='left')
    
    # ============================================================
    # 기간구분
    # ============================================================
    today = df['order_daily'].max()
    df['기간구분'] = df['order_daily'].apply(
        lambda x: '금일' if x == today else ('전일' if x == (today - timedelta(days=1)) else '')
    )
    df['금일여부'] = df['order_daily'].apply(lambda x: '금일' if x == today else '')
    df['전일여부'] = df['order_daily'].apply(lambda x: '전일' if x == (today - timedelta(days=1)) else '')
    
    # 편의 컬럼
    df['전일_매출'] = df['전일_매장매출']
    df['전일_주문건수'] = df['전일_매장주문건수']
    df['전주_매출'] = df['전주_매장매출']
    df['전주_주문건수'] = df['전주_매장주문건수']
    df['전월_매출'] = df['전월_매장매출']
    df['전월_주문건수'] = df['전월_매장주문건수']
    
    print(f"\n[출력] {len(df):,}행 × {len(df.columns)}컬럼")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"compared_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='compared_path', value=str(output_path))
    return f"✅ 전기비교 완료: {len(df):,}건"


# ============================================================
# 💰 Step 5: 수수료율 + 증감
# ============================================================

def calculate_fees_and_changes(**context):
    """수수료율 계산 + 증감률 계산"""
    ti = context['task_instance']
    
    # 입력 데이터 로드
    compared_path = ti.xcom_pull(task_ids='add_period_comparisons', key='compared_path')
    if not compared_path:
        raise FileNotFoundError("전기비교 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(compared_path)
    print(f"\n{'='*60}")
    print(f"[입력] {len(df):,}행")
    
    # ============================================================
    # 수수료율 계산
    # ============================================================
    print("\n[수수료] 수수료율 계산 중...")
    
    df['수수료율'] = ((df['total_amount'] - df['settlement_amount']) / df['total_amount'] * 100).round(2)
    df['수수료율_배민'] = ((df['total_amount_배민'] - df['settlement_amount_배민']) / df['total_amount_배민'] * 100).round(2)
    df['수수료율_쿠팡'] = ((df['total_amount_쿠팡'] - df['settlement_amount_쿠팡']) / df['total_amount_쿠팡'] * 100).round(2)
    
    df['전일_수수료율'] = ((df['전일_매장매출'] - df['전일_매장정산금액']) / df['전일_매장매출'] * 100).round(2)
    df['전일_수수료율_배민'] = ((df['전일_매장매출_배민'] - df['전일_매장정산금액_배민']) / df['전일_매장매출_배민'] * 100).round(2)
    df['전일_수수료율_쿠팡'] = ((df['전일_매장매출_쿠팡'] - df['전일_매장정산금액_쿠팡']) / df['전일_매장매출_쿠팡'] * 100).round(2)
    
    df['전주동요일_수수료율'] = ((df['전주동요일_매장매출'] - df['전주동요일_매장정산금액']) / df['전주동요일_매장매출'] * 100).round(2)
    df['전주동요일_수수료율_배민'] = ((df['전주동요일_매장매출_배민'] - df['전주동요일_매장정산금액_배민']) / df['전주동요일_매장매출_배민'] * 100).round(2)
    df['전주동요일_수수료율_쿠팡'] = ((df['전주동요일_매장매출_쿠팡'] - df['전주동요일_매장정산금액_쿠팡']) / df['전주동요일_매장매출_쿠팡'] * 100).round(2)
    
    # ============================================================
    # 증감 계산
    # ============================================================
    print("\n[증감] 증감률 계산 중...")
    
    df['전일대비_매출증감액'] = df['total_amount'] - df['전일_매장매출']
    df['전일대비_매출증감률'] = ((df['total_amount'] - df['전일_매장매출']) / df['전일_매장매출'] * 100).round(2)
    df['전일대비_수수료율증감'] = (df['수수료율'] - df['전일_수수료율']).round(2)
    df['전일대비_매출증감률_배민'] = ((df['total_amount_배민'] - df['전일_매장매출_배민']) / df['전일_매장매출_배민'] * 100).round(2)
    df['전일대비_수수료율증감_배민'] = (df['수수료율_배민'] - df['전일_수수료율_배민']).round(2)
    df['전일대비_매출증감률_쿠팡'] = ((df['total_amount_쿠팡'] - df['전일_매장매출_쿠팡']) / df['전일_매장매출_쿠팡'] * 100).round(2)
    df['전일대비_수수료율증감_쿠팡'] = (df['수수료율_쿠팡'] - df['전일_수수료율_쿠팡']).round(2)
    
    df['전주동요일대비_매출증감액'] = df['total_amount'] - df['전주동요일_매장매출']
    df['전주동요일대비_매출증감률'] = ((df['total_amount'] - df['전주동요일_매장매출']) / df['전주동요일_매장매출'] * 100).round(2)
    df['전주동요일대비_수수료율증감'] = (df['수수료율'] - df['전주동요일_수수료율']).round(2)
    df['전주동요일대비_매출증감률_배민'] = ((df['total_amount_배민'] - df['전주동요일_매장매출_배민']) / df['전주동요일_매장매출_배민'] * 100).round(2)
    df['전주동요일대비_수수료율증감_배민'] = (df['수수료율_배민'] - df['전주동요일_수수료율_배민']).round(2)
    df['전주동요일대비_매출증감률_쿠팡'] = ((df['total_amount_쿠팡'] - df['전주동요일_매장매출_쿠팡']) / df['전주동요일_매장매출_쿠팡'] * 100).round(2)
    df['전주동요일대비_수수료율증감_쿠팡'] = (df['수수료율_쿠팡'] - df['전주동요일_수수료율_쿠팡']).round(2)
    
    # 비교용 평균
    df['일별_담당자별_평균'] = df.groupby(['order_daily', '담당자'])['total_amount'].transform('mean').round(0)
    df['일별_전체_평균'] = df.groupby('order_daily')['total_amount'].transform('mean').round(0)
    df['vs일별전체_비율'] = (df['total_amount'] / df['일별_전체_평균'] * 100).round(1).replace([np.inf, -np.inf], 100)
    
    print(f"\n[출력] {len(df):,}행 × {len(df.columns)}컬럼")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"fees_calculated_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='fees_calculated_path', value=str(output_path))
    return f"✅ 수수료 완료: {len(df):,}건"


# ============================================================
# 🔗 Step 6-8: 외부 데이터 조인
# ============================================================

def join_baemin_metrics(**context):
    """배민 우리가게 현황 조인"""
    ti = context['task_instance']
    
    # 메인 데이터 로드
    fees_path = ti.xcom_pull(task_ids='calculate_fees_and_changes', key='fees_calculated_path')
    if not fees_path:
        raise FileNotFoundError("수수료 계산 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(fees_path)
    print(f"\n{'='*60}")
    print(f"[메인] {len(df):,}행")
    
    # 배민 데이터 로드
    baemin_path = ti.xcom_pull(task_ids='load_baemin_store_now', key='baemin_store_now_path')
    
    if not baemin_path:
        print("  ⚠️ 배민 데이터 없음 - 스킵")
        output_path = TEMP_DIR / f"joined_baemin_{context['ds_nodash']}.parquet"
        df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key='joined_baemin_path', value=str(output_path))
        return f"⚠️ 배민 스킵: {len(df):,}건"
    
    print("\n[배민] 조인 시작...")
    
    baemin_df = pd.read_parquet(baemin_path)
    
    # 전처리
    baemin_df['collected_date'] = baemin_df['collected_at'].str[:10]
    baemin_df['stores_name'] = baemin_df['store_name'].str.split(' ').str[-2:].str.join(' ')
    
    cols = ['collected_date', 'stores_name', '조리소요시간', '조리소요시간_순위비율',
            '주문접수시간', '주문접수시간_순위비율', '조리시간준수율', '조리시간준수율_순위비율',
            '주문접수율', '주문접수율_순위비율', '최근별점']
    
    baemin_df = baemin_df[[c for c in cols if c in baemin_df.columns]]
    
    # 조인
    df['order_daily_str'] = df['order_daily'].dt.strftime('%Y-%m-%d')
    df = df.merge(
        baemin_df,
        left_on=['order_daily_str', '매장명'],
        right_on=['collected_date', 'stores_name'],
        how='left'
    )
    df.drop(columns=['order_daily_str', 'collected_date', 'stores_name'], errors='ignore', inplace=True)
    
    print(f"[배민] 조인 완료: {len(df):,}행")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"joined_baemin_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='joined_baemin_path', value=str(output_path))
    return f"✅ 배민 조인: {len(df):,}건"


def join_toorder_reviews(**context):
    """토더 리뷰 조인"""
    ti = context['task_instance']
    
    # 메인 데이터 로드
    baemin_joined_path = ti.xcom_pull(task_ids='join_baemin_metrics', key='joined_baemin_path')
    if not baemin_joined_path:
        raise FileNotFoundError("배민 조인 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(baemin_joined_path)
    print(f"\n{'='*60}")
    print(f"[메인] {len(df):,}행")
    
    # 토더 데이터 로드
    toorder_path = ti.xcom_pull(task_ids='load_toorder_review', key='toorder_review_path')
    
    if not toorder_path:
        print("  ⚠️ 토더 데이터 없음 - 스킵")
        output_path = TEMP_DIR / f"joined_toorder_{context['ds_nodash']}.parquet"
        df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key='joined_toorder_path', value=str(output_path))
        return f"⚠️ 토더 스킵: {len(df):,}건"
    
    print("\n[토더] 조인 시작...")
    
    toorder_df = pd.read_parquet(toorder_path)
    
    cols = ['date', 'stores_name', '전체_주문수', '전체_리뷰수', '전체_답변완료수', '전체_평균별점']
    toorder_df = toorder_df[[c for c in cols if c in toorder_df.columns]]
    
    df['order_daily_str'] = df['order_daily'].dt.strftime('%Y-%m-%d')
    df = df.merge(
        toorder_df,
        left_on=['order_daily_str', '매장명'],
        right_on=['date', 'stores_name'],
        how='left'
    )
    df.drop(columns=['order_daily_str', 'date', 'stores_name'], errors='ignore', inplace=True)
    
    print(f"[토더] 조인 완료: {len(df):,}행")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"joined_toorder_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='joined_toorder_path', value=str(output_path))
    return f"✅ 토더 조인: {len(df):,}건"


def join_baemin_history(**context):
    """배민 변경이력 조인"""
    ti = context['task_instance']
    
    # 메인 데이터 로드
    toorder_joined_path = ti.xcom_pull(task_ids='join_toorder_reviews', key='joined_toorder_path')
    if not toorder_joined_path:
        raise FileNotFoundError("토더 조인 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(toorder_joined_path)
    print(f"\n{'='*60}")
    print(f"[메인] {len(df):,}행")
    
    # 변경이력 데이터 로드
    history_path = ti.xcom_pull(task_ids='load_baemin_history', key='baemin_history_path')
    
    if not history_path:
        print("  ⚠️ 변경이력 데이터 없음 - 스킵")
        output_path = TEMP_DIR / f"joined_history_{context['ds_nodash']}.parquet"
        df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key='joined_history_path', value=str(output_path))
        return f"⚠️ 변경이력 스킵: {len(df):,}건"
    
    print("\n[변경이력] 조인 시작...")
    
    history_df = pd.read_parquet(history_path)
    
    history_df['change_date'] = history_df['변경시간'].str[:10]
    history_df['stores_name'] = history_df['매장명'].str.split(' ').str[-2:].str.join(' ')
    
    # Pivot
    history_pivot = history_df.groupby(['change_date', 'stores_name', '대분류']).size().reset_index(name='cnt')
    history_pivot = history_pivot.pivot_table(
        index=['change_date', 'stores_name'],
        columns='대분류',
        values='cnt',
        fill_value=0,
        aggfunc='sum'
    ).reset_index()
    
    df['order_daily_str'] = df['order_daily'].dt.strftime('%Y-%m-%d')
    df = df.merge(
        history_pivot,
        left_on=['order_daily_str', '매장명'],
        right_on=['change_date', 'stores_name'],
        how='left'
    )
    df.drop(columns=['order_daily_str', 'change_date', 'stores_name'], errors='ignore', inplace=True)
    
    print(f"[변경이력] 조인 완료: {len(df):,}행")
    print(f"{'='*60}\n")
    
    # Parquet 저장
    output_path = TEMP_DIR / f"joined_history_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='joined_history_path', value=str(output_path))
    return f"✅ 변경이력 조인: {len(df):,}건"


# ============================================================
# 💾 Step 9: CSV 저장
# ============================================================

def save_to_csv(**context):
    """최종 CSV 저장"""
    ti = context['task_instance']
    
    # 최종 데이터 로드
    history_joined_path = ti.xcom_pull(task_ids='join_baemin_history', key='joined_history_path')
    if not history_joined_path:
        raise FileNotFoundError("변경이력 조인 데이터를 찾을 수 없습니다")
    
    df = pd.read_parquet(history_joined_path)
    print(f"\n{'='*60}")
    print(f"[최종] {len(df):,}행 × {len(df.columns)}컬럼")
    
    # CSV 저장
    output_csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_upload.csv'
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # datetime 컬럼 변환
    datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    if datetime_cols:
        for col in datetime_cols:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    
    # Period 타입 처리
    for col in df.columns:
        if df[col].dtype.name.startswith('period'):
            df[col] = df[col].astype(str)
    
    # CSV 저장
    df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
    
    csv_size = output_csv_path.stat().st_size / (1024 * 1024)
    
    print(f"\n[저장] ✅ CSV 저장 완료: {output_csv_path}")
    print(f"       {len(df):,}행 × {len(df.columns)}컬럼")
    print(f"       파일 크기: {csv_size:.2f} MB")
    print(f"{'='*60}\n")
    
    return f"✅ 저장 완료: {len(df):,}건"