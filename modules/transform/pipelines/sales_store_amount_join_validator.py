"""
최종 병합 데이터 검증 함수
"""
import pandas as pd


def validate_final_join(**context):
    """최종 병합 데이터 검증"""
    ti = context['task_instance']
    
    final_path = ti.xcom_pull(
        task_ids='fin_left_join_orders_now_toorder_history',
        key='joined_orders_now_toorder_history_path'
    )
    
    if not final_path:
        print("[❌ 오류] 최종 데이터 없음")
        return "검증 실패"
    
    final_df = pd.read_parquet(final_path)
    
    # 1. 기본 통계
    print("\n" + "="*70)
    print("🔍 최종 데이터 검증")
    print("="*70)
    print(f"✅ 행 수: {len(final_df):,}행")
    print(f"✅ 열 수: {len(final_df.columns):,}컬럼")
    
    # 2. 주문 기준 확인 (left join이므로 주문 행 수 유지)
    orders_path = ti.xcom_pull(
        task_ids='load_sales_daily_orders_alerts',
        key='sales_daily_orders_alerts_path'
    )
    if orders_path:
        orders_df = pd.read_parquet(orders_path)
        print(f"\n📊 주문 데이터 (LEFT 기준)")
        print(f"   - 원본 행 수: {len(orders_df):,}행")
        print(f"   - 최종 행 수: {len(final_df):,}행")
        print(f"   - 유지율: {(len(final_df)/len(orders_df)*100):.1f}%")
        
        if len(final_df) != len(orders_df):
            print(f"   ⚠️  경고: left join이므로 행 수가 같아야 합니다")
    
    # 3. 일별 그룹화 확인
    if 'order_daily' in final_df.columns:
        daily_groups = final_df['order_daily'].nunique()
        print(f"\n📅 일별 그룹화")
        print(f"   - 고유 날짜: {daily_groups}개")
        
        stores_per_day = final_df.groupby('order_daily')['매장명'].nunique()
        print(f"   - 날짜별 평균 매장: {stores_per_day.mean():.1f}개")
        print(f"   - 날짜별 최대 매장: {stores_per_day.max():.0f}개")
    
    # 4. 중복 확인 (left join이므로 일부 중복은 정상)
    if 'order_daily' in final_df.columns and '매장명' in final_df.columns:
        daily_store_dup = final_df.duplicated(subset=['order_daily', '매장명']).sum()
        print(f"\n🔢 중복 확인 (order_daily + 매장명)")
        print(f"   - 중복 행: {daily_store_dup}개")
        if daily_store_dup == 0:
            print(f"   ✅ 정상: 일별/매장별로 고유함 (left join 기준)")
        else:
            print(f"   ⚠️  주의: left join이므로 일부 중복은 정상입니다")
    
    # 5. null 값 확인
    print(f"\n❓ Null 값 현황 (상위 10개 컬럼)")
    null_stats = final_df.isnull().sum().sort_values(ascending=False).head(10)
    for col, cnt in null_stats.items():
        pct = (cnt / len(final_df) * 100)
        if pct > 50:
            symbol = "⚠️ "
        elif pct > 10:
            symbol = "📌 "
        else:
            symbol = "✅ "
        print(f"   {symbol} {col}: {cnt:,}개 ({pct:.1f}%)")
    
    # 6. 컬럼 목록
    print(f"\n📋 최종 컬럼 목록 ({len(final_df.columns)}개)")
    cols = final_df.columns.tolist()
    for i, col in enumerate(cols, 1):
        print(f"   {i:2d}. {col}")
    
    print("\n" + "="*70)
    print("✅ 검증 완료\n")
    
    return f"검증 완료: {len(final_df):,}행 × {len(final_df.columns):,}컬럼"
