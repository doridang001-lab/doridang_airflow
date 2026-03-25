"""
📧 매출 이상 알림 전용 - 핵심 함수들

SMD_07 DAG용 필터링 및 이메일 발송 함수
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ============================================================
# 📧 알림 대상 필터링 함수
# ============================================================
def filter_alerts(
    output_xcom_key='alert_targets',
    **context
):
    """
    매출 이상 알림 대상 필터링
    
    등급 체계: 정상 / 관심 / 주의 / 위험 (4단계)
    조건:
    - 위험(5~6점): 즉시 알림 (5개 지표 전체 적용)
    - 주의(3~4점): 알림 없음
    - 관심(2점): 알림 없음
    - 정상(0~1점): 알림 없음
    
    ✅ CSV 파일 기반 (SMD_06이 생성한 파일 읽기)
    """
    from modules.transform.utility.paths import LOCAL_DB
    from modules.transform.utility.store_name_mapping import normalize_store_names
    
    ti = context['task_instance']
    
    print(f"\n{'='*60}")
    print(f"[필터링] 알림 대상 필터링 시작")
    
    # ============================================================
    # 1. CSV 파일에서 데이터 로드 (SMD_06이 생성한 파일)
    # ============================================================
    csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_upload.csv'
    
    if not csv_path.exists():
        print(f"[경고] CSV 파일 없음: {csv_path}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (CSV 파일 없음)"
    
    try:
        # ⭐ CSV 파일 읽기 (dtype=str로 모든 컬럼을 문자열로)
        df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype=str, low_memory=False)
        print(f"[필터링] CSV 파일 읽기 성공: {len(df):,}건")
    except Exception as e:
        print(f"[에러] CSV 파일 읽기 실패: {e}")
        import traceback
        print(f"[상세 오류]\n{traceback.format_exc()}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"0건 (파일 읽기 실패: {e})"
    
    # 필수 컬럼 확인
    required_cols = ['order_daily', '매장명', '담당자', 'status', 'pre_status', 'email']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"[경고] 필수 컬럼 없음: {missing_cols}")
        print(f"[참고] 사용 가능한 컬럼: {df.columns.tolist()[:20]}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"0건 (필수 컬럼 없음: {missing_cols})"
    
    # ============================================================
    # 2. ⭐ 날짜 형식 안전하게 변환
    # ============================================================
    print(f"\n[필터링] 날짜 컬럼 변환 중...")
    
    try:
        # order_daily를 datetime으로 변환 (에러 무시)
        df['order_daily'] = pd.to_datetime(df['order_daily'], errors='coerce')
        
        # 변환 실패한 행 확인
        invalid_dates = df['order_daily'].isna()
        invalid_count = invalid_dates.sum()
        
        if invalid_count > 0:
            print(f"[경고] 날짜 변환 실패: {invalid_count}건")
            # 변환 실패한 행 제거
            df = df[~invalid_dates].copy()
            print(f"[필터링] 유효한 데이터: {len(df):,}건")
        
    except Exception as e:
        print(f"[에러] 날짜 변환 실패: {e}")
        import traceback
        print(f"[상세 오류]\n{traceback.format_exc()}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"0건 (날짜 변환 실패: {e})"
    
    # ============================================================
    # 3. 대상 일자 결정
    # ============================================================
    max_date = df['order_daily'].max()
    ds_str = context.get('ds')
    ds_dt = pd.to_datetime(ds_str) if ds_str else None

    # 날짜 역행 방지: logical date(ds)와 무관하게 실제 데이터 최신일자를 사용한다.
    # ds가 과거여도(수동 실행/재실행) 최신 수집일 기준으로 grp를 생성해야 한다.
    target_date = max_date
    target_reason = "데이터 최신일자(고정)"
    
    print(f"\n[필터링] 데이터 최신일자: {max_date.date() if pd.notnull(max_date) else 'N/A'}")
    print(f"[필터링] ds: {ds_dt.date() if ds_dt is not None else 'None'}")
    print(f"[필터링] 사용 일자: {target_date.date() if pd.notnull(target_date) else 'N/A'} ({target_reason})")
    if ds_dt is not None and pd.notnull(max_date) and ds_dt.date() != max_date.date():
        print(f"[주의] ds({ds_dt.date()})와 최신 데이터일({max_date.date()})이 다릅니다. 최신 데이터일로 처리합니다.")
    
    # ============================================================
    # 4. 전체 집계 데이터 정렬 (중간 CSV 저장 없음)
    # ============================================================
    combined_df = df.sort_values(['매장명', '담당자', 'order_daily']).reset_index(drop=True)
    print(f"\n[CSV] 전체 집계 데이터: {len(combined_df):,}건")
    
    # ============================================================
    # ⭐ sales_employee.csv와 left join으로 최신 담당자 정보 업데이트
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[담당자 업데이트] sales_employee.csv로 최신 담당자 정보 반영 시작...")
    
    employee_path = LOCAL_DB / '영업관리부_DB' / 'sales_employee.csv'
    
    if employee_path.exists():
        try:
            # 여러 인코딩 시도
            for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
                try:
                    employee_df = pd.read_csv(employee_path, encoding=encoding)
                    print(f"[담당자 업데이트] sales_employee.csv 로드: {len(employee_df):,}건 ({encoding})")
                    break
                except UnicodeDecodeError:
                    continue
            
            if '매장명' in employee_df.columns:
                # 매장명 정규화
                employee_df['매장명_key'] = employee_df['매장명'].astype(str).str.strip()
                
                # ⭐ 지점명 변경 매핑 적용 (수집 시 이전 이름 → 현재 이름)
                if '매장명' in combined_df.columns:
                    combined_df['매장명'] = normalize_store_names(combined_df['매장명'])
                    print(f"[담당자 업데이트] 매장명 지점명 매핑 적용 완료")
                
                # ⭐ 필요한 컬럼만 선택하고 컬럼명 미리 변경 (충돌 방지)
                employee_cols = ['매장명_key', '담당자', 'email']
                available_cols = [col for col in employee_cols if col in employee_df.columns]
                employee_df_clean = employee_df[available_cols].drop_duplicates(subset=['매장명_key'], keep='first')
                
                # ⭐ 컬럼명 변경으로 merge 시 충돌 방지
                employee_df_clean = employee_df_clean.rename(columns={
                    '담당자': '담당자_신규',
                    'email': 'email_신규'
                })
                
                print(f"[담당자 업데이트] 중복 제거 후: {len(employee_df_clean):,}건")
                
                # ⭐ 기존 컬럼 개수 확인 (검증용)
                cols_before = len(combined_df.columns)
                has_담당자_before = '담당자' in combined_df.columns
                has_email_before = 'email' in combined_df.columns
                
                # ⭐ left join 수행 (컬럼명 충돌 없음)
                combined_df = combined_df.merge(
                    employee_df_clean,
                    left_on='매장명',
                    right_on='매장명_key',
                    how='left'
                )
                
                # ⭐ 담당자 업데이트 (신규 정보 우선, 없으면 기존 정보 유지)
                if '담당자_신규' in combined_df.columns:
                    if has_담당자_before:
                        # 기존 담당자 컬럼이 있었다면 업데이트
                        updated_count = (combined_df['담당자_신규'].notna() & 
                                       (combined_df['담당자_신규'] != combined_df['담당자'])).sum()
                        combined_df['담당자'] = combined_df['담당자_신규'].fillna(combined_df['담당자'])
                        print(f"[담당자 업데이트] 변경된 건수: {updated_count:,}건")
                    else:
                        # 기존 컬럼이 없었다면 새로 생성
                        combined_df['담당자'] = combined_df['담당자_신규']
                
                if 'email_신규' in combined_df.columns:
                    if has_email_before:
                        combined_df['email'] = combined_df['email_신규'].fillna(combined_df['email'])
                    else:
                        combined_df['email'] = combined_df['email_신규']
                
                # ⭐ 최종 정리: NaN → '미배정' / 공백
                combined_df['담당자'] = combined_df['담당자'].fillna('미배정')
                combined_df['email'] = combined_df['email'].fillna('')
                
                # ⭐ 임시 컬럼만 제거 (기존 컬럼은 그대로 유지)
                drop_cols = ['매장명_key', '담당자_신규', 'email_신규']
                for col in drop_cols:
                    if col in combined_df.columns:
                        combined_df.drop(columns=[col], inplace=True)
                
                # ⭐ 검증: 컬럼 개수가 크게 변하지 않았는지 확인
                cols_after = len(combined_df.columns)
                if abs(cols_after - cols_before) > 5:
                    print(f"[경고] 컬럼 개수 변화 큼: {cols_before} → {cols_after}")
                else:
                    print(f"[검증 완료] 컬럼 개수: {cols_before} → {cols_after} (기존 컬럼 유지됨)")
                
                # 업데이트 후 담당자 분포
                manager_counts_after = combined_df['담당자'].value_counts()
                print(f"[담당자 업데이트 후] 담당자 분포: 총 {len(manager_counts_after)}명")
                print(f"[담당자 업데이트 후] 미배정 건수: {manager_counts_after.get('미배정', 0):,}건")
                print(f"[담당자 업데이트 후] 담당자 TOP 5: {dict(manager_counts_after.head())}")
                
            else:
                print(f"[경고] sales_employee.csv에 '매장명' 컬럼이 없습니다")
        
        except Exception as e:
            print(f"[경고] 담당자 업데이트 실패 (기존 데이터 유지): {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"[경고] sales_employee.csv 파일이 없습니다: {employee_path}")
    
    print(f"{'='*60}\n")
    
    print(f"[INFO] 중간 파일 sales_daily_orders_alerts.csv 저장 생략 (alerts_grp만 생성)")
    
    # ============================================================
    # 5. 대상 일자 필터링
    # ============================================================
    # 담당자/이메일 최신화가 반영된 combined_df 기준으로 대상일 필터링
    df_target = combined_df[combined_df['order_daily'] == target_date].copy()
    print(f"\n[필터링] 대상일({target_date.date() if pd.notnull(target_date) else 'N/A'}) 데이터: {len(df_target):,}건")
    
    if len(df_target) == 0:
        print(f"[경고] 대상일 데이터가 없습니다 → 알림 대상 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"알림 대상 없음 (전체 집계 {len(combined_df):,}건 저장됨)"
    
    # ============================================================
    # 6. ⭐ 숫자 컬럼 안전하게 변환
    # ============================================================
    print(f"\n[필터링] 숫자 컬럼 변환 중...")
    
    numeric_cols = ['total_amount', 'score', 'score_trend', 'score_total', 
                    'score_7d_total', 'score_4week_total', 'ma_14', 'ma_28',
                    'current_avg_2week', 'amt_curr_mtd', 'amt_prev_mtd',
                    'sum_7d_recent', 'sum_7d_prev',
                    'settlement_amount_배민', 'settlement_amount_쿠팡', 
                    '쿠팡_광고비', '배민_광고비']
    
    for col in numeric_cols:
        if col in df_target.columns:
            try:
                df_target[col] = pd.to_numeric(df_target[col], errors='coerce')
            except Exception as e:
                print(f"[경고] {col} 숫자 변환 실패: {e}")

    # ============================================================
    # 6.5 ⭐ 영업일 기준 7일 재계산 (옵션)
    # ============================================================
    # 기본값은 비활성화하여 불필요한 대량 계산을 피한다.
    recalc_business_7d = bool(context.get('recalc_business_7d', False))
    if recalc_business_7d:
        print(f"\n[7d재계산] 영업일 기준 7일 재계산 시작...")

        RECALC_AGGS = [
            # (결과_recent,                    결과_prev,                      원본컬럼,     집계)
            ('sum_7d_recent',                 'sum_7d_prev',                 'total_amount',  'sum'),
            ('avg_7d_recent',                 'avg_7d_prev',                 'fee_ratio',     'mean'),
            ('쿠팡_광고효과_avg_7d_recent',    '쿠팡_광고효과_avg_7d_prev',    '쿠팡_광고효과',  'mean'),
            ('배민_광고효과_avg_7d_recent',    '배민_광고효과_avg_7d_prev',    '배민_광고효과',  'mean'),
            ('조리시간_7d_recent',             '조리시간_7d_prev',             '조리소요시간',   'mean'),
            ('접수시간_7d_recent',             '접수시간_7d_prev',             '주문접수시간',   'mean'),
            ('재주문율_7d_recent',             '재주문율_7d_prev',             '최근재주문율',   'mean'),
            ('별점_7d_recent',                '별점_7d_prev',                '최근별점',       'mean'),
        ]

        combined_df['_dt'] = pd.to_datetime(combined_df['order_daily'], errors='coerce')
        target_dt = pd.to_datetime(target_date)

        for idx in df_target.index:
            store = str(df_target.at[idx, '매장명']).strip()
            store_data = combined_df[combined_df['매장명'] == store].copy()

            # 최근 7영업일 (target_date 포함)
            recent_data = store_data[store_data['_dt'] <= target_dt].nlargest(7, '_dt')
            # 직전 7영업일 (recent 창 바로 이전)
            if not recent_data.empty:
                prev_cutoff = recent_data['_dt'].min()
                prev_data = store_data[store_data['_dt'] < prev_cutoff].nlargest(7, '_dt')
            else:
                prev_data = pd.DataFrame()

            for col_rec, col_prev, src_col, func in RECALC_AGGS:
                if src_col in store_data.columns:
                    r_vals = pd.to_numeric(recent_data[src_col], errors='coerce').dropna()
                    p_vals = pd.to_numeric(prev_data[src_col],  errors='coerce').dropna() if not prev_data.empty else pd.Series([], dtype=float)
                    if func == 'sum':
                        df_target.at[idx, col_rec]  = float(r_vals.sum()) if len(r_vals) > 0 else None
                        df_target.at[idx, col_prev] = float(p_vals.sum()) if len(p_vals) > 0 else None
                    else:
                        df_target.at[idx, col_rec]  = round(float(r_vals.mean()), 4) if len(r_vals) > 0 else None
                        df_target.at[idx, col_prev] = round(float(p_vals.mean()), 4) if len(p_vals) > 0 else None

        print(f"[7d재계산] 완료")
    else:
        print(f"\n[7d재계산] 생략 (recalc_business_7d=False)")

    # ============================================================
    # 7. 알람 조건 적용 (5개 지표 모두 확인)
    # ============================================================
    print(f"\n[필터링] 알람 조건 적용 중...")
    
    # ⭐ 5개 지표별 (status, pre_status) 매핑
    STATUS_PAIRS = [
        ('status', 'pre_status'),                          # 매출
        ('fee_status', 'pre_fee_status'),                  # 수수료율
        ('쿠팡_광고효과_status', 'pre_쿠팡_광고효과_status'),  # 쿠팡 광고
        ('배민_광고효과_status', 'pre_배민_광고효과_status'),  # 배민 광고
        ('service_status', 'pre_service_status'),          # 성실영업(서비스)
    ]
    
    # 각 지표별 알림 조건: 전주·금주 모두 "위험" (연속 위험)인 경우만 알림
    alert_mask = pd.Series(False, index=df_target.index)

    for st_col, pre_st_col in STATUS_PAIRS:
        if st_col in df_target.columns:
            curr_danger = df_target[st_col] == '위험'
            if pre_st_col in df_target.columns:
                # 전주(pre)도 위험인 경우만 포함
                pair_mask = curr_danger & (df_target[pre_st_col] == '위험')
                matched = pair_mask.sum()
                if matched > 0:
                    print(f"  [{st_col}] 연속위험 알림 대상: {matched}건")
            else:
                # pre 컬럼 없으면 금주 위험만 체크 (fallback)
                pair_mask = curr_danger
                matched = pair_mask.sum()
                if matched > 0:
                    print(f"  [{st_col}] 알림 대상(위험, pre 컬럼 없음): {matched}건")
            alert_mask = alert_mask | pair_mask
        else:
            if st_col in ['status']:  # 매출 status는 필수
                print(f"[경고] {st_col} 컬럼 없음")
    
    alert_targets = df_target[alert_mask].copy()
    
    print(f"[필터링] 알람 대상: {len(alert_targets):,}건")
    
    if len(alert_targets) > 0:
        # 매출 위험/주의 분포
        if 'status' in alert_targets.columns:
            danger_count = len(alert_targets[alert_targets['status'] == '위험'])
            warning_count = len(alert_targets[alert_targets['status'] == '주의'])
            print(f"[매출] 🚨 위험: {danger_count}건, ⚠️ 주의: {warning_count}건")
        
        # 추가 지표별 분포
        for label, st_col in [('수수료', 'fee_status'), 
                               ('쿠팡광고', '쿠팡_광고효과_status'),
                               ('배민광고', '배민_광고효과_status'),
                               ('서비스', 'service_status')]:
            if st_col in alert_targets.columns:
                bad = alert_targets[st_col].isin(['위험', '주의']).sum()
                if bad > 0:
                    print(f"[{label}] ⚠️ 이상: {bad}건")
    
    # ============================================================
    # 8. 전처리 로직 - 5개 지표별 상세 필터링은 이미 완료됨
    # ============================================================
    # 위에서 이미 alert_targets에 필터링된 데이터가 있음
    # 추가 전처리가 필요하다면 여기서 수행
    
    alert_targets_processed = alert_targets.copy()
    
    # ============================================================
    # 9. 알림 대상 CSV 저장: sales_daily_orders_alerts_grp.csv
    # ============================================================
    alerts_csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts_grp.csv'
    alerts_csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    if len(alert_targets_processed) > 0:
        alert_targets_sorted = alert_targets_processed.sort_values(
            ['매장명', '담당자', 'order_daily']
        ).reset_index(drop=True)
        
        try:
            alert_save = alert_targets_sorted.copy()
            
            # 불필요한 컬럼 제거
            cols_to_drop = ['_row_hash', 'collected_at', 'order_date_with_time']
            for col in cols_to_drop:
                if col in alert_save.columns:
                    alert_save.drop(columns=[col], inplace=True)
            
            # order_daily 문자열 변환
            if 'order_daily' in alert_save.columns:
                if pd.api.types.is_datetime64_any_dtype(alert_save['order_daily']):
                    alert_save['order_daily'] = alert_save['order_daily'].dt.strftime('%Y-%m-%d')
            
            alert_save.to_csv(alerts_csv_path, index=False, encoding='utf-8-sig')
            print(f"[✅ CSV] 알림 대상 저장: {len(alert_save):,}건 → {alerts_csv_path.name}")
            
        except Exception as e:
            print(f"[❌ 에러] 알림 대상 CSV 저장 실패: {e}")
            import traceback
            print(f"[상세 오류]\n{traceback.format_exc()}")
    else:
        print(f"[INFO] 알림 대상 없음 → {alerts_csv_path.name} 파일 미생성")
    
    # ============================================================
    # 10. Parquet 저장 (XCom 전달용)
    # ============================================================
    if len(alert_targets) > 0:
        temp_dir = LOCAL_DB / 'temp'
        temp_dir.mkdir(exist_ok=True, parents=True)
        
        parquet_output = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        
        try:
            alert_targets.to_parquet(parquet_output, index=False, engine='pyarrow')
            print(f"[Parquet] 저장: {parquet_output.name}")
            ti.xcom_push(key=output_xcom_key, value=str(parquet_output))
        except Exception as e:
            print(f"[경고] Parquet 저장 실패: {e}")
            ti.xcom_push(key=output_xcom_key, value=None)
    else:
        ti.xcom_push(key=output_xcom_key, value=None)
    
    print(f"{'='*60}\n")
    
    return f"✅ 저장 완료: 전체 {len(combined_df):,}건, 알림 {len(alert_targets):,}건"

# ===========================================================
# llm 함수선언
# ==========================================================
"""
=============================================================================
📊 도리당 매장 진단 & 제안 리포트 v4
=============================================================================
status가 "주의" 또는 "위험"인 매장만 대상.
상황 분석은 최소화하고, "문제 → 제안" 구조로 핵심만 출력.

  generate_expert_report(df_all)
  generate_expert_report(df_all, target_date='2026-02-08')
  prompts = generate_store_prompts(df_all)
=============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime


def _prepare_df(df_all, target_date=None, manager=None):
    df = df_all.copy()
    if 'order_daily' in df.columns:
        if df['order_daily'].dtype.name.startswith('period'):
            df['order_daily'] = df['order_daily'].dt.to_timestamp()
        else:
            df['order_daily'] = pd.to_datetime(df['order_daily'], errors='coerce')
    if target_date:
        df_day = df[df['order_daily'] == pd.to_datetime(target_date)].copy()
    else:
        df_day = df[df['order_daily'] == df['order_daily'].max()].copy()
    if manager:
        df_day = df_day[df_day['담당자'] == manager].copy()
    
    # ⭐ 5개 지표 중 하나라도 주의/위험이면 포함
    status_cols = ['status', 'fee_status', '쿠팡_광고효과_status', '배민_광고효과_status', 'service_status']
    available_status_cols = [col for col in status_cols if col in df_day.columns]
    
    if available_status_cols:
        mask = pd.Series(False, index=df_day.index)
        for col in available_status_cols:
            mask = mask | df_day[col].isin(['주의', '위험'])
        df_day = df_day[mask].copy()
    
    return df_day


def _v(row, col, default=0):
    v = row.get(col)
    return v if pd.notna(v) else default


def _vs(row, col, default='N/A'):
    v = row.get(col)
    return str(v).strip() if pd.notna(v) and str(v).strip() else default


# =============================================================================
# 핵심 진단 → 제안 엔진
# =============================================================================
def diagnose_and_suggest(row):
    """
    4개 영역(매출/수수료/광고/성실영업)을 진단하고
    영역 간 교차 판단하여 제안을 생성.
    
    Returns: list of (category_icon, problem, suggestion)
    """
    suggestions = []

    # ── 기본 수치 ──
    amount = _v(row, 'total_amount')
    orders = _v(row, 'total_order_count')
    arpu = _v(row, 'ARPU')
    dc = _v(row, '전일대비_증감률')
    wc = _v(row, '전주대비_증감률')
    ma14 = _v(row, 'ma_14')
    ma28 = _v(row, 'ma_28')
    s7 = _v(row, 'sum_7d_recent')
    s7p = _v(row, 'sum_7d_prev')
    prev_orders = _v(row, '전주_주문건수')
    prev_amount = _v(row, '전주_매출')
    avg_2w = _v(row, 'current_avg_2week')
    avg_4w = _v(row, 'amt_curr_mtd')
    avg_4w_prev = _v(row, 'amt_prev_mtd')
    fr = _v(row, '수수료율')
    fr_bm = _v(row, '수수료율_배민')
    fr_cp = _v(row, '수수료율_쿠팡')
    fr_prev = np.nan
    fr_prevweek = np.nan
    fee_status = _vs(row, 'fee_status')
    fee_ad = _v(row, 'fee_ad')
    fee_ad_bm = _v(row, 'fee_ad_배민')
    fee_ad_cp = _v(row, 'fee_ad_쿠팡')

    # 광고 관련
    cp_ad_status = _vs(row, '쿠팡_광고효과_status')
    bm_ad_status = _vs(row, '배민_광고효과_status')
    cp_ad_val = _v(row, '쿠팡_광고효과')
    bm_ad_val = _v(row, '배민_광고효과')
    campaign = _vs(row, '캠페인정보')

    # 쿠팡 광고 세부
    cp_ad_spend = _v(row, '광고비용')
    cp_ad_orders = _v(row, '광고주문수')
    cp_ad_revenue = _v(row, '광고매출')
    cp_ad_clicks = _v(row, '광고클릭수')
    cp_ad_impressions = _v(row, '광고노출수')
    cp_ad_ratio = _v(row, '광고비율')

    # 배민 광고 세부
    bm_ad_spend = _v(row, '광고지출')
    bm_ad_impressions = _v(row, '노출수')
    bm_ad_clicks = _v(row, '클릭수')
    bm_ad_orders_cnt = _v(row, '주문수')
    bm_ad_revenue_val = _v(row, '주문금액')
    bm_ad_effect = _v(row, '광고효과')

    # 성실영업
    ct = _v(row, '조리소요시간')
    at = _v(row, '주문접수시간')
    ar = _v(row, '주문접수율')
    cc = _v(row, '조리시간준수율')
    reorder = _v(row, '최근재주문율')
    rating = _v(row, '최근별점')
    svc_status = _vs(row, 'service_status')

    # 플랫폼별
    bm_amt = _v(row, 'total_amount_배민')
    cp_amt = _v(row, 'total_amount_쿠팡')
    bm_ord = _v(row, 'total_order_count_배민')
    cp_ord = _v(row, 'total_order_count_쿠팡')

    # 상태값 종합
    fee_is_bad = fee_status in ['주의', '위험']
    cp_ad_is_bad = cp_ad_status in ['주의', '위험']
    bm_ad_is_bad = bm_ad_status in ['주의', '위험']
    svc_is_bad = svc_status in ['주의', '위험']
    ad_ok = not cp_ad_is_bad and not bm_ad_is_bad
    fee_ok = not fee_is_bad

    # ================================================================
    # 1. 매출 진단 → 제안
    # ================================================================
    if dc < -15 and wc < -15:
        # 동반 하락 → 원인 분해
        if prev_orders > 0 and orders > 0:
            order_chg = (orders / prev_orders - 1) * 100
            arpu_prev = prev_amount / prev_orders if prev_orders > 0 else 0
            arpu_chg = (arpu / arpu_prev - 1) * 100 if arpu_prev > 0 else 0

            if order_chg < -15:
                # 주문건수 급감이 주원인
                if fee_ok and ad_ok:
                    suggestions.append(('📊', 
                        f"매출 전일 {dc:+.1f}%·전주 {wc:+.1f}% 동반 하락. 주문건수가 전주 대비 {order_chg:+.0f}% 감소가 주원인",
                        f"광고·수수료 상태 정상이므로 마케팅 비용 증액(쿠팡 캠페인 할인율 상향 또는 배민 울트라콜 추가) 검토"))
                elif fee_is_bad:
                    suggestions.append(('📊',
                        f"매출 동반 하락, 주문건수 {order_chg:+.0f}% 감소. 수수료 상태도 '{fee_status}'",
                        f"광고비 증액보다 점주 통화로 영업중지·인력 문제 확인 우선. 수수료 정상화 후 마케팅 재투자"))
                else:
                    suggestions.append(('📊',
                        f"매출 동반 하락, 주문건수 {order_chg:+.0f}% 감소",
                        f"광고 효율이 낮은 상태이므로 캠페인 조건(할인율·키워드) 재설정 후 재투자"))
            elif abs(arpu_chg) > 10:
                suggestions.append(('📊',
                    f"매출 동반 하락. 객단가가 전주 대비 {arpu_chg:+.1f}% 변동이 주원인(ARPU {arpu:,.0f}원)",
                    f"메뉴 구성 점검(세트메뉴 비중·사이드 메뉴 추가) 또는 할인 캠페인 조건 조정"))
            else:
                suggestions.append(('📊',
                    f"매출 전일 {dc:+.1f}%·전주 {wc:+.1f}% 동반 하락",
                    f"점주 통화로 영업중지·인력·주변 환경 변화 확인"))
        else:
            suggestions.append(('📊',
                f"매출 전일 {dc:+.1f}%·전주 {wc:+.1f}% 동반 하락",
                f"점주 통화로 영업중지·인력·주변 환경 변화 확인"))

    elif dc < -15 and wc >= 0:
        suggestions.append(('📊',
            f"전일 {dc:+.1f}% 급락이나 전주 {wc:+.1f}% 정상",
            f"전일 영업중지·시스템 장애 여부 확인. 2~3일 추이 지켜본 후 판단"))

    # 중기 하락 추세
    if ma14 > 0 and ma28 > 0:
        gap = (ma14 / ma28 - 1) * 100
        if gap < -10:
            suggestions.append(('📊',
                f"14일 이평이 28일 이평 대비 {gap:.1f}% → 중기 하락 추세",
                f"2주 내 매출 미회복 시 프로모션·메뉴 리뉴얼 등 구조적 대응 전환"))

    # ================================================================
    # 2. 수수료 진단 → 제안
    # ================================================================
    if fee_is_bad:
        # 단기 급등 vs 구조적
        daily_spike = fr > 0 and fr_prev > 0 and (fr - fr_prev) > 5
        
        if daily_spike:
            suggestions.append(('💰',
                f"수수료율 '{fee_status}'. 전일 대비 {fr - fr_prev:+.1f}%p 급등({fr_prev:.1f}% → {fr:.1f}%)",
                f"당일 캠페인 변경·광고비 증가 내역 확인 후 원인 제거"))
        elif fr_bm > 0 and fr_cp > 0 and abs(fr_bm - fr_cp) > 10:
            higher = "쿠팡" if fr_cp > fr_bm else "배민"
            suggestions.append(('💰',
                f"수수료율 '{fee_status}'. {higher} 수수료율이 {max(fr_bm,fr_cp):.1f}%로 높음",
                f"{higher} 광고비·할인 캠페인 축소. 저수수료 플랫폼 주문 비중 확대"))
        else:
            suggestions.append(('💰',
                f"수수료율 '{fee_status}' ({fr:.1f}%)",
                f"플랫폼별 광고비·캠페인 항목 분해 후 비용 과다 항목 축소"))

    elif fr > 33 and not fee_is_bad:
        # 상태는 정상이지만 수수료율 자체가 높음
        suggestions.append(('💰',
            f"수수료율 {fr:.1f}%로 매출의 1/3 이상이 플랫폼 비용",
            f"캠페인 조건(할인율·최소주문금액) 재협상 검토"))

    # ================================================================
    # 3. 광고 진단 → 제안
    # ================================================================
    if cp_ad_is_bad:
        desc_parts = [f"쿠팡 광고 '{cp_ad_status}'"]
        if cp_ad_val > 0:
            desc_parts.append(f"ROAS {cp_ad_val:.1f}배")
        if campaign != 'N/A':
            desc_parts.append(f"캠페인: {campaign}")
        
        # 구체적 제안: 클릭 대비 주문 전환율 확인
        if cp_ad_clicks > 0 and cp_ad_orders > 0:
            cvr = cp_ad_orders / cp_ad_clicks * 100
            if cvr < 5:
                suggestions.append(('📣',
                    f"{' / '.join(desc_parts)}. 클릭→주문 전환율 {cvr:.1f}%로 낮음",
                    f"쿠팡 캠페인 할인율 상향 또는 메뉴 썸네일·가격 경쟁력 점검"))
            else:
                suggestions.append(('📣',
                    f"{' / '.join(desc_parts)}",
                    f"쿠팡 광고 키워드·노출 시간대 재설정. 효율 개선 안 되면 일시 축소"))
        else:
            suggestions.append(('📣',
                f"{' / '.join(desc_parts)}",
                f"쿠팡 광고 캠페인 설정·할인율 재검토"))

    if bm_ad_is_bad:
        desc_parts = [f"배민 광고 '{bm_ad_status}'"]
        if bm_ad_val > 0:
            desc_parts.append(f"ROAS {bm_ad_val:.1f}배")
        
        if bm_ad_clicks > 0 and bm_ad_orders_cnt > 0:
            cvr = bm_ad_orders_cnt / bm_ad_clicks * 100
            suggestions.append(('📣',
                f"{' / '.join(desc_parts)}. 전환율 {cvr:.1f}%",
                f"배민 울트라콜 위치·오픈리스트 효율 점검. 전환율 낮으면 메뉴 가격·이미지 개선 우선"))
        else:
            suggestions.append(('📣',
                f"{' / '.join(desc_parts)}",
                f"배민 광고 효율 점검(울트라콜 위치·오픈리스트)"))

    # 광고 정상 + 수수료 정상인데 매출 부진 → 마케팅 증액 제안
    if ad_ok and fee_ok and (dc < -10 or wc < -10):
        if fee_ad > 0 and amount > 0:
            ad_pct = fee_ad / amount * 100
            if ad_pct < 7:
                suggestions.append(('📣',
                    f"광고·수수료 모두 정상이나 매출 부진. 현재 광고비 비중 {ad_pct:.1f}%",
                    f"마케팅 비용 증액 여력 있음. 쿠팡 캠페인 할인율 상향 또는 배민 광고 슬롯 추가"))

    # ================================================================
    # 4. 성실영업지표 → 제안
    # ================================================================
    if ct > 25:
        suggestions.append(('⭐',
            f"조리시간 {ct:.0f}분으로 고객 이탈 임계점 크게 초과",
            f"조리 프로세스 개선(사전 준비·인력 배치). 조리시간이 매출 하락과 직결되는 구간"))
    elif ct > 20:
        suggestions.append(('⭐',
            f"조리시간 {ct:.0f}분으로 임계점(20분) 초과",
            f"피크 시간대 인력 배치 점검"))

    if ar > 0 and ar < 0.95:
        suggestions.append(('⭐',
            f"주문접수율 {ar*100:.0f}% → 95% 미만 페널티 구간",
            f"태블릿 꺼짐·영업중지 여부 확인. 접수율이 플랫폼 노출에 직접 영향"))

    if cc > 0 and cc < 0.9:
        suggestions.append(('⭐',
            f"조리시간준수율 {cc*100:.0f}% → 90% 미만",
            f"조리시간 설정값 재조정(현실적 시간으로 상향) 또는 인력 보강"))

    if reorder > 0 and reorder < 0.12:
        suggestions.append(('⭐',
            f"재주문율 {reorder*100:.1f}%로 낮음",
            f"재주문 유도 쿠폰 또는 리뷰 이벤트 검토"))

    if rating > 0 and rating < 4.3:
        suggestions.append(('⭐',
            f"별점 {rating}점",
            f"최근 저평점 리뷰 내용 확인 후 메뉴·서비스 개선 포인트 도출"))

    if svc_is_bad:
        suggestions.append(('⭐',
            f"서비스 종합 상태 '{svc_status}'",
            f"조리·접수·재주문·별점 중 어떤 항목이 악화됐는지 세부 점수 확인"))

    return suggestions


# =============================================================================
# 제안 우선순위 정리 (1순위 + 보조 + 관찰)
# =============================================================================
def prioritize_suggestions(suggestions):
    """
    1순위: 첫 번째 제안
    보조: 두 번째 제안
    관찰: 나머지
    """
    if not suggestions:
        return None, None, []
    primary = suggestions[0] if len(suggestions) > 0 else None
    secondary = suggestions[1] if len(suggestions) > 1 else None
    obs = suggestions[2:] if len(suggestions) > 2 else []
    return primary, secondary, obs


# =============================================================================
# LLM 프롬프트 생성
# =============================================================================
def _build_prompt(row):
    store = row['매장명']
    lines = []
    lines.append(f"매장명: {store}")
    lines.append(f"기준일: {row.get('order_daily', 'N/A')}")
    lines.append(f"담당자: {row.get('담당자', 'N/A')}")
    lines.append("")

    # 현재 상태
    lines.append("【현재 상태】")
    lines.append(f"- 매출 status: {_vs(row,'status')} (pre: {_vs(row,'pre_status')})")
    lines.append(f"- 수수료 status: {_vs(row,'fee_status')} (pre: {_vs(row,'pre_fee_status')})")
    lines.append(f"- 쿠팡광고 status: {_vs(row,'쿠팡_광고효과_status')} (pre: {_vs(row,'pre_쿠팡_광고효과_status')})")
    lines.append(f"- 배민광고 status: {_vs(row,'배민_광고효과_status')} (pre: {_vs(row,'pre_배민_광고효과_status')})")
    lines.append(f"- 운영 status: {_vs(row,'service_status')} (pre: {_vs(row,'pre_service_status')})")
    lines.append("")

    # 1. 매출
    lines.append("【1. 매출】")
    lines.append(f"- 매출: {_v(row,'total_amount'):,.0f}원 / 주문: {_v(row,'total_order_count'):.0f}건 / ARPU: {_v(row,'ARPU'):,.0f}원")
    lines.append(f"- 전일: {_v(row,'전일대비_증감률'):+.1f}% ({_v(row,'전일_매출'):,.0f}원) / 전주: {_v(row,'전주대비_증감률'):+.1f}% ({_v(row,'전주_매출'):,.0f}원)")
    lines.append(f"- 이평: 14일 {_v(row,'ma_14'):,.0f} / 28일 {_v(row,'ma_28'):,.0f} / 2주avg {_v(row,'current_avg_2week'):,.0f} / 당월MTD {_v(row,'amt_curr_mtd'):,.0f} / 전월MTD {_v(row,'amt_prev_mtd'):,.0f}")
    lines.append(f"- 7일누적: {_v(row,'sum_7d_recent'):,.0f} / 직전7일: {_v(row,'sum_7d_prev'):,.0f}")
    lines.append(f"- score: {_v(row,'score')}점 (추세:{_v(row,'score_trend')} 주간:{_v(row,'score_7d_total')} 월간:{_v(row,'score_4week_total')})")
    lines.append(f"- 배민 {_v(row,'total_amount_배민'):,.0f}원({_v(row,'total_order_count_배민'):.0f}건) / 쿠팡 {_v(row,'total_amount_쿠팡'):,.0f}원({_v(row,'total_order_count_쿠팡'):.0f}건)")
    lines.append("")

    # 2. 수수료
    lines.append("【2. 수수료】")
    lines.append(f"- 수수료율: {_v(row,'수수료율'):.1f}% (배민 {_v(row,'수수료율_배민'):.1f}% / 쿠팡 {_v(row,'수수료율_쿠팡'):.1f}%)")
    lines.append(f"- 플랫폼별: 배민 {_v(row,'수수료율_배민'):.1f}% / 쿠팡 {_v(row,'수수료율_쿠팡'):.1f}%")
    lines.append(f"- 광고비: {_v(row,'fee_ad'):,.0f}원 (배민 {_v(row,'fee_ad_배민'):,.0f} / 쿠팡 {_v(row,'fee_ad_쿠팡'):,.0f})")
    lines.append(f"- 정산: {_v(row,'settlement_amount'):,.0f}원")
    lines.append(f"- fee_status: {_vs(row,'fee_status')} (pre: {_vs(row,'pre_fee_status')})")
    lines.append("")

    # 3. 광고
    lines.append("【3. 광고】")
    lines.append(f"[쿠팡] 광고효과: {_v(row,'쿠팡_광고효과'):.1f}배 / status: {_vs(row,'쿠팡_광고효과_status')}")
    if _vs(row,'캠페인정보') != 'N/A':
        lines.append(f"  캠페인: {_vs(row,'캠페인정보')}")
    lines.append(f"  광고비: {_v(row,'광고비용'):,.0f} / 노출: {_v(row,'광고노출수'):,.0f} / 클릭: {_v(row,'광고클릭수'):,.0f} / 주문: {_v(row,'광고주문수'):.0f}건 / 매출: {_v(row,'광고매출'):,.0f}")
    lines.append(f"[배민] 광고효과: {_v(row,'배민_광고효과'):.1f}배 / status: {_vs(row,'배민_광고효과_status')}")
    lines.append(f"  광고비: {_v(row,'광고지출'):,.0f} / 노출: {_v(row,'노출수'):,.0f} / 클릭: {_v(row,'클릭수'):,.0f} / 주문: {_v(row,'주문수'):.0f}건 / 매출: {_v(row,'주문금액'):,.0f}")
    lines.append("")

    # 4. 성실영업
    lines.append("【4. 성실영업지표】")
    if _v(row,'조리소요시간') > 0:
        lines.append(f"- 조리시간: {_v(row,'조리소요시간'):.0f}분 (상위 {_v(row,'조리소요시간_순위비율')*100:.0f}%)")
    if _v(row,'주문접수시간') > 0:
        lines.append(f"- 접수시간: {_v(row,'주문접수시간'):.0f}분 (상위 {_v(row,'주문접수시간_순위비율')*100:.0f}%)")
    if _v(row,'조리시간준수율') > 0:
        lines.append(f"- 준수율: {_v(row,'조리시간준수율')*100:.0f}%")
    if _v(row,'주문접수율') > 0:
        lines.append(f"- 접수율: {_v(row,'주문접수율')*100:.0f}%")
    if _v(row,'최근재주문율') > 0:
        lines.append(f"- 재주문율: {_v(row,'최근재주문율')*100:.1f}%")
    if _v(row,'최근별점') > 0:
        lines.append(f"- 별점: {_v(row,'최근별점')}점 (전체평균 {_v(row,'전체_평균별점'):.2f})")
    lines.append(f"- service_status: {_vs(row,'service_status')} (pre: {_vs(row,'pre_service_status')})")

    return '\n'.join(lines)


def generate_store_prompts(df_all, target_date=None, manager=None):
    """주의/위험 매장별 LLM 프롬프트를 dict로 반환"""
    df_day = _prepare_df(df_all, target_date, manager)
    if len(df_day) == 0:
        print("⚠️ 주의/위험 매장이 없습니다.")
        return {}
    return {row['매장명']: _build_prompt(row) for _, row in df_day.iterrows()}


# =============================================================================
# 📊 메인: 리포트 생성
# =============================================================================
def generate_expert_report(df_all, target_date=None, manager=None):
    df_day = _prepare_df(df_all, target_date, manager)

    if len(df_day) == 0:
        print("✅ 주의/위험 매장 없음. 전 매장 정상.")
        return

    report_date = df_day['order_daily'].iloc[0]
    if hasattr(report_date, 'strftime'):
        report_date = report_date.strftime('%Y-%m-%d')

    S = {'관심':'🔵','주의':'🟡','위험':'🔴'}

    print("=" * 70)
    print(f"  📊 도리당 매장 진단 & 제안 리포트")
    print(f"  기준일: {report_date} | 대상: {len(df_day)}개 매장")
    print("=" * 70)

    store_focus = {}  # 담당자별 집중 매장

    for _, row in df_day.sort_values('score', ascending=False).iterrows():
        store = row['매장명']
        status = _vs(row, 'status')
        score = _v(row, 'score')
        emoji = S.get(status, '⚪')
        mgr = row['담당자']

        print(f"\n{'━'*70}")
        print(f"  {emoji} {store} | {status}({score}점) | 담당: {mgr}")
        print(f"{'━'*70}")

        # 한 줄 현황
        amount = _v(row, 'total_amount')
        orders = _v(row, 'total_order_count')
        fr = _v(row, '수수료율')
        dc = _v(row, '전일대비_증감률')
        wc = _v(row, '전주대비_증감률')
        print(f"  {amount:,.0f}원 / {orders:.0f}건 / 수수료 {fr:.1f}% / 전일 {dc:+.1f}% / 전주 {wc:+.1f}%")

        # 상태 한 줄 요약
        states = []
        fee_st = _vs(row, 'fee_status')
        cp_st = _vs(row, '쿠팡_광고효과_status')
        bm_st = _vs(row, '배민_광고효과_status')
        svc_st = _vs(row, 'service_status')
        if fee_st not in ['정상','N/A']: states.append(f"수수료:{fee_st}")
        if cp_st not in ['정상','N/A']: states.append(f"쿠팡광고:{cp_st}")
        if bm_st not in ['정상','N/A']: states.append(f"배민광고:{bm_st}")
        if svc_st not in ['정상','N/A']: states.append(f"서비스:{svc_st}")
        if states:
            print(f"  ⚠️ {' / '.join(states)}")

        # 진단 → 제안
        suggestions = diagnose_and_suggest(row)

        if suggestions:
            primary, secondary, obs = prioritize_suggestions(suggestions)

            if primary:
                icon, problem, suggest = primary
                print(f"\n  {icon} 문제: {problem}")
                print(f"     제안: {suggest}")

            if secondary:
                icon, problem, suggest = secondary
                print(f"\n  {icon} 문제: {problem}")
                print(f"     제안: {suggest}")

            if obs:
                print(f"\n  📋 관찰")
                for icon, problem, suggest in obs:
                    print(f"    {icon} {problem} → {suggest}")
        else:
            print(f"\n  ✅ 특이 사항 없음. 이상점수 기반 모니터링 유지")

        # 담당자 집중 매장 후보
        if mgr not in store_focus:
            store_focus[mgr] = (store, score, primary)
        elif score > store_focus[mgr][1]:
            store_focus[mgr] = (store, score, primary)

    # ═══════════ 담당자별 집중 매장 ═══════════
    print(f"\n{'━'*70}")
    print(f"  👤 담당자별 이번 주 집중 매장")
    print(f"{'━'*70}")
"""
=============================================================================
📊 도리당 매장 진단 & 제안 리포트 v5
=============================================================================
모든 매장 출력, "문제 > 제안" 형식으로 가독성 개선

  generate_expert_report(df_all)
  generate_expert_report(df_all, target_date='2026-02-08')
  prompts = generate_store_prompts(df_all)
=============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime


def _prepare_df(df_all, target_date=None, manager=None):
    df = df_all.copy()
    if 'order_daily' in df.columns:
        if df['order_daily'].dtype.name.startswith('period'):
            df['order_daily'] = df['order_daily'].dt.to_timestamp()
        else:
            df['order_daily'] = pd.to_datetime(df['order_daily'], errors='coerce')
    if target_date:
        df_day = df[df['order_daily'] == pd.to_datetime(target_date)].copy()
    else:
        df_day = df[df['order_daily'] == df['order_daily'].max()].copy()
    if manager:
        df_day = df_day[df_day['담당자'] == manager].copy()
    # 모든 매장 출력
    return df_day


def _v(row, col, default=0):
    v = row.get(col)
    return v if pd.notna(v) else default


def _vs(row, col, default='N/A'):
    v = row.get(col)
    return str(v).strip() if pd.notna(v) and str(v).strip() else default


# =============================================================================
# 핵심 진단 → 제안 엔진
# =============================================================================
def diagnose_and_suggest(row):
    """
    프랜차이즈 본사 영업관리부 관점의 진단 및 제안 (개선 v2)
    
    개선사항:
    1. 비정상적 급등/급락 패턴 감지
    2. 신규 오픈 매장 별도 처리
    3. 광고 미실행 상태 감지
    4. 실행 가능한 액션만 제안
    """
    suggestions = []

    # ── 기본 수치 ──
    amount = _v(row, 'total_amount')
    orders = _v(row, 'total_order_count')
    arpu = _v(row, 'ARPU')
    dc = _v(row, '전일대비_증감률')
    wc = _v(row, '전주대비_증감률')
    mc = _v(row, '전월대비_증감률')
    ma14 = _v(row, 'ma_14')
    ma28 = _v(row, 'ma_28')
    s7 = _v(row, 'sum_7d_recent')
    s7p = _v(row, 'sum_7d_prev')
    prev_orders = _v(row, '전주_주문건수')
    prev_amount = _v(row, '전주_매출')
    prev_day_amount = _v(row, '전일_매출')
    avg_2w = _v(row, 'current_avg_2week')
    avg_4w = _v(row, 'amt_curr_mtd')
    avg_4w_prev = _v(row, 'amt_prev_mtd')
    
    # 오픈일 확인
    open_date_str = _vs(row, '실오픈일', '')
    is_new_store = False
    if open_date_str:
        try:
            from datetime import datetime
            open_date = pd.to_datetime(open_date_str)
            days_since_open = (pd.to_datetime('2026-02-08') - open_date).days
            is_new_store = days_since_open < 90  # 3개월 미만
        except:
            pass

    # 수수료 관련
    fr = _v(row, '수수료율')
    fr_bm = _v(row, '수수료율_배민')
    fr_cp = _v(row, '수수료율_쿠팡')
    fr_prev = np.nan
    fr_prevweek = np.nan
    fee_status = _vs(row, 'fee_status')
    fee_ad = _v(row, 'fee_ad')
    fee_ad_bm = _v(row, 'fee_ad_배민')
    fee_ad_cp = _v(row, 'fee_ad_쿠팡')

    # 광고 관련
    cp_ad_status = _vs(row, '쿠팡_광고효과_status')
    bm_ad_status = _vs(row, '배민_광고효과_status')
    cp_ad_val = _v(row, '쿠팡_광고효과')
    bm_ad_val = _v(row, '배민_광고효과')
    campaign = _vs(row, '캠페인정보')

    # 쿠팡 광고 세부
    cp_ad_spend = _v(row, '광고비용')
    cp_ad_orders = _v(row, '광고주문수')
    cp_ad_revenue = _v(row, '광고매출')
    cp_ad_clicks = _v(row, '광고클릭수')
    
    # 배민 광고 세부
    bm_ad_spend = _v(row, '광고지출')
    bm_ad_clicks = _v(row, '클릭수')
    bm_ad_orders_cnt = _v(row, '주문수')
    bm_ad_revenue_val = _v(row, '주문금액')

    # 성실영업
    ct = _v(row, '조리소요시간')
    at = _v(row, '주문접수시간')
    ar = _v(row, '주문접수율')
    cc = _v(row, '조리시간준수율')
    reorder = _v(row, '최근재주문율')
    rating = _v(row, '최근별점')
    svc_status = _vs(row, 'service_status')

    # 상태값 종합
    fee_is_bad = fee_status in ['주의', '위험']
    cp_ad_is_bad = cp_ad_status in ['주의', '위험']
    bm_ad_is_bad = bm_ad_status in ['주의', '위험']
    svc_is_bad = svc_status in ['주의', '위험']
    ad_ok = not cp_ad_is_bad and not bm_ad_is_bad
    fee_ok = not fee_is_bad
    
    # 광고 실행 여부
    cp_ad_running = cp_ad_spend > 0 or cp_ad_orders > 0
    bm_ad_running = bm_ad_spend > 0 or bm_ad_orders_cnt > 0

    # ================================================================
    # 0. 신규 오픈 매장 또는 비정상 패턴 감지
    # ================================================================
    # 비정상적 급등 (전주 대비 +200% 이상)
    if wc > 200:
        if is_new_store:
            suggestions.append(('매출',
                f"신규 오픈 매장 - 전주 대비 {wc:+.1f}% 급등 (정상 성장 패턴)",
                f"🎯 모니터링: 향후 2주간 매출 안정화 추이 관찰\n"
                f"📋 확인 사항: 초기 프로모션 효과·지역 인지도 상승 여부\n"
                f"💡 신규 오픈 3개월간은 변동성 높음 (정상)"
            ))
        else:
            suggestions.append(('매출',
                f"⚠️ 비정상 급등 감지: 전주 대비 {wc:+.1f}% ({prev_amount:,.0f}원 → {amount:,.0f}원)",
                f"🎯 즉시 확인: 데이터 오류 또는 특수 상황 점검\n"
                f"📋 체크리스트:\n"
                f"  • POS/플랫폼 데이터 동기화 오류\n"
                f"  • 단체 주문·케이터링 등 일회성 대량 주문\n"
                f"  • 지역 행사·축제 등 특수 상황\n"
                f"💡 급등 원인 파악 후 지속 가능성 판단 필요"
            ))
        return suggestions  # 비정상 패턴이면 다른 진단 스킵

    # ================================================================
    # 1. 매출 진단 → 제안
    # ================================================================
    if dc < -15 and wc < -15:
        # 동반 하락 → 원인 분해
        if prev_orders > 0 and orders > 0:
            order_chg = (orders / prev_orders - 1) * 100
            arpu_prev = prev_amount / prev_orders if prev_orders > 0 else 0
            arpu_chg = (arpu / arpu_prev - 1) * 100 if arpu_prev > 0 else 0

            if order_chg < -15:
                # 주문건수 급감이 주원인
                if fee_ok and ad_ok:
                    est_cost = int(amount * 0.05)
                    suggestions.append(('매출', 
                        f"매출 전일 {dc:+.1f}%·전주 {wc:+.1f}% 동반 하락. 주문건수 {order_chg:+.0f}% 급감",
                        f"🎯 당일 조치: 점주 통화 필수 - 영업중지·인력 문제 확인\n"
                        f"📊 금주 내 실행: 쿠팡 할인율 +5%p 상향 또는 배민 울트라콜 1슬롯 추가\n"
                        f"💰 예상 추가비용: 주 {est_cost:,}원 (매출의 5%, 회복 시까지 2주 예상)\n"
                        f"🤝 본사 지원: 담당 영업팀장 연락 (마케팅 전략 상담)"
                    ))
                elif fee_is_bad:
                    suggestions.append(('매출',
                        f"매출 동반 하락 + 주문건수 {order_chg:+.0f}% 급감. 수수료도 '{fee_status}'",
                        f"🎯 최우선: 점주 긴급 통화 - 영업중지·인력·수수료 급등 원인 파악\n"
                        f"📊 순서: ① 수수료 정상화 → ② 마케팅 재투자\n"
                        f"⚠️ 주의: 수수료 문제 미해결 시 광고비 증액은 손실 확대"
                    ))
                else:
                    suggestions.append(('매출',
                        f"매출 동반 하락. 주문건수 {order_chg:+.0f}% 급감",
                        f"🎯 3일 내 조치: 현재 광고 캠페인 전면 재검토\n"
                        f"📋 액션: 쿠팡/배민 할인율·키워드 재설정\n"
                        f"💡 담당 영업팀장과 마케팅 전략 수립 후 재투자"
                    ))
            elif abs(arpu_chg) > 10:
                menu_guide = "세트메뉴 비중 확대 (단가 상승)" if arpu_chg < 0 else "단품 프로모션 (단가 방어)"
                suggestions.append(('매출',
                    f"매출 동반 하락. 객단가 전주 대비 {arpu_chg:+.1f}% 변동 (현재 {arpu:,.0f}원)",
                    f"🎯 1주일 내: 메뉴 구성 최적화\n"
                    f"📋 제안: {menu_guide}\n"
                    f"📊 목표: 객단가 {arpu_prev:,.0f}원 수준 회복\n"
                    f"🤝 본사 지원: 메뉴 기획팀 컨설팅 (무료)"
                ))
            else:
                suggestions.append(('매출',
                    f"매출 전일 {dc:+.1f}%·전주 {wc:+.1f}% 동반 하락",
                    f"🎯 당일: 점주 긴급 통화 (영업중지·인력·주변 환경·시스템 확인)\n"
                    f"📊 모니터링: 금일+내일(2일) 매출 추이 관찰 후 대응 결정"
                ))

    elif dc < -15 and wc >= 0:
        suggestions.append(('매출',
            f"전일 {dc:+.1f}% 급락 (전주 {wc:+.1f}% 정상)",
            f"🎯 당일: 전일 영업중지·시스템 장애·태블릿 꺼짐 확인\n"
            f"📊 모니터링: 금일(2/9) 매출 회복 여부 확인\n"
            f"💡 금일 회복 시: 일시적 이슈로 판단 / 미회복 시: 긴급 대응"
        ))

    # 중기 하락 추세
    if ma14 > 0 and ma28 > 0:
        gap = (ma14 / ma28 - 1) * 100
        if gap < -10:
            suggestions.append(('매출',
                f"14일 이평 {gap:.1f}% 하락 → 중기 하락 추세",
                f"🎯 2주 내 회복 목표: 14일 이평 {ma28:,.0f}원 도달\n"
                f"📋 구조적 대응:\n"
                f"  • 신규 프로모션 기획 (본사 승인 지원)\n"
                f"  • 시즌 메뉴 추가 (본사 레시피 제공)\n"
                f"  • 지역 SNS 마케팅 (본사 템플릿 제공)\n"
                f"🤝 담당 영업팀장과 회복 전략 회의 필수"
            ))

    # ================================================================
    # 2. 수수료 진단 → 제안
    # ================================================================
    if fee_is_bad:
        daily_spike = fr > 0 and fr_prev > 0 and (fr - fr_prev) > 5
        
        if daily_spike:
            suggestions.append(('수수료',
                f"수수료율 '{fee_status}'. 전일 {fr_prev:.1f}% → {fr:.1f}% (+{fr - fr_prev:.1f}%p 급등)",
                f"🎯 당일: 전일(2/7) 캠페인·광고비 변경 내역 확인\n"
                f"📋 체크리스트:\n"
                f"  • 배민/쿠팡 할인율 상향 여부\n"
                f"  • 광고 슬롯 추가 여부 (울트라콜·오픈리스트)\n"
                f"  • 신규 프로모션 등록 (쿠폰·할인)\n"
                f"📊 목표: 수수료율 {fr_prev:.1f}% 수준 복원\n"
                f"🤝 원인 불명 시: 담당 영업팀장 연락"
            ))
        elif fr_bm > 0 and fr_cp > 0 and abs(fr_bm - fr_cp) > 10:
            higher_platform = "쿠팡" if fr_cp > fr_bm else "배민"
            lower_platform = "배민" if fr_cp > fr_bm else "쿠팡"
            higher_rate = max(fr_bm, fr_cp)
            suggestions.append(('수수료',
                f"수수료율 '{fee_status}'. {higher_platform} {higher_rate:.1f}%로 과다",
                f"🎯 3일 내: {higher_platform} 광고·할인 단계적 축소\n"
                f"📋 실행:\n"
                f"  1단계: {higher_platform} 광고 슬롯 50% 축소\n"
                f"  2단계: 할인율 -2%p 조정\n"
                f"  3단계: {lower_platform} 주문 비중 확대\n"
                f"📊 목표: 수수료율 30% 이하\n"
                f"⚠️ 급격한 축소는 매출 하락 위험 (단계 진행)"
            ))
        else:
            suggestions.append(('수수료',
                f"수수료율 '{fee_status}' ({fr:.1f}%)",
                f"🎯 1주일 내: 플랫폼별 비용 분석\n"
                f"📋 분석: 배민 {fr_bm:.1f}% / 쿠팡 {fr_cp:.1f}%\n"
                f"💡 액션: 비효율 항목 단계적 축소\n"
                f"🤝 담당 영업팀장과 수수료 최적화 상담"
            ))

    elif fr > 33:
        suggestions.append(('수수료',
            f"수수료율 {fr:.1f}% (매출의 1/3 이상)",
            f"🎯 1개월 목표: 수수료율 30% 이하\n"
            f"📋 협상:\n"
            f"  • 캠페인 할인율 재협상 (-2~3%p)\n"
            f"  • 최소주문금액 상향\n"
            f"  • 광고 효율 재평가\n"
            f"💰 예상 절감: 월 {int(amount * 30 * 0.03):,}원"
        ))

    # ================================================================
    # 3. 광고 진단 → 제안
    # ================================================================
    # 쿠팡 광고
    if cp_ad_is_bad:
        if not cp_ad_running:
            # 광고 미실행 상태
            suggestions.append(('광고',
                f"⚠️ 쿠팡 광고 '{cp_ad_status}' 상태이나 광고비 0원 (미실행)",
                f"🎯 즉시: 쿠팡 광고 데이터 누락 확인\n"
                f"📋 점검:\n"
                f"  • 플랫폼 연동 오류\n"
                f"  • 광고 계정 정지 여부\n"
                f"  • 데이터 수집 오류\n"
                f"💡 정상 실행 중이라면 본사 데이터팀 문의"
            ))
        elif cp_ad_clicks > 0 and cp_ad_orders > 0:
            cvr = cp_ad_orders / cp_ad_clicks * 100
            if cvr < 5:
                suggestions.append(('광고',
                    f"쿠팡 광고 '{cp_ad_status}' (ROAS {cp_ad_val:.1f}배). 전환율 {cvr:.1f}% (목표 5% 미달)",
                    f"🎯 3일 내: 쿠팡 캠페인 긴급 개선\n"
                    f"📋 우선순위:\n"
                    f"  1. 할인율 +3~5%p 상향\n"
                    f"  2. 메뉴 썸네일 교체 (본사 템플릿 사용)\n"
                    f"  3. 주변 매장 대비 가격 경쟁력 확인\n"
                    f"📊 목표: 전환율 5% 이상\n"
                    f"🤝 본사 지원: 썸네일 디자인 무료 제공"
                ))
            else:
                suggestions.append(('광고',
                    f"쿠팡 광고 '{cp_ad_status}' (ROAS {cp_ad_val:.1f}배)",
                    f"🎯 1주일 내: 광고 최적화\n"
                    f"📋 실행:\n"
                    f"  • 키워드 재선정 (지역명·메뉴)\n"
                    f"  • 노출 시간 조정 (11~13시, 18~20시)\n"
                    f"  • 광고비 30% 축소 테스트\n"
                    f"📊 1주 후 ROAS {cp_ad_val + 2:.1f}배 미달 시 중단"
                ))

    # 배민 광고
    if bm_ad_is_bad:
        if not bm_ad_running:
            # 광고 미실행 상태
            suggestions.append(('광고',
                f"⚠️ 배민 광고 '{bm_ad_status}' 상태이나 광고비 0원 (미실행)",
                f"🎯 즉시: 배민 광고 시작 또는 데이터 확인\n"
                f"📋 옵션:\n"
                f"  1. 광고 미실행이면 → 울트라콜·오픈리스트 시작 검토\n"
                f"  2. 실행 중이면 → 플랫폼 연동 오류 확인\n"
                f"💡 본사 지원: 배민 광고 세팅 가이드 제공"
            ))
        elif bm_ad_clicks > 0 and bm_ad_orders_cnt > 0:
            cvr = bm_ad_orders_cnt / bm_ad_clicks * 100
            suggestions.append(('광고',
                f"배민 광고 '{bm_ad_status}' (ROAS {bm_ad_val:.1f}배). 전환율 {cvr:.1f}%",
                f"🎯 3일 내: 배민 광고 효율 개선\n"
                f"📋 점검:\n"
                f"  • 울트라콜 위치 (상단 3개 목표)\n"
                f"  • 오픈리스트 노출 시간\n"
                f"  • 광고 문구·이미지 품질\n"
                f"💡 전환율 5% 미만: 메뉴 가격·이미지 개선 우선\n"
                f"🤝 본사 지원: 배민 광고 최적화 가이드"
            ))

    # ================================================================
    # 4. 성실영업지표 → 제안
    # ================================================================
    if ct > 25:
        time_reduction = int(ct - 20)
        suggestions.append(('운영',
            f"조리시간 {ct:.0f}분 → 임계점(20분) {time_reduction}분 초과",
            f"🎯 3일 내: 조리시간 20분 이하 단축\n"
            f"📋 즉시 실행:\n"
            f"  • 피크타임(11~13시, 18~20시) 인력 +1명\n"
            f"  • 사전 준비 강화 (소스·반죽)\n"
            f"  • 주방 동선 개선 (본사 매뉴얼)\n"
            f"⚠️ 조리 5분 단축 = 주문 +10% (업계)\n"
            f"🤝 본사 지원: 주방 프로세스 방문 교육"
        ))
    elif ct > 20:
        suggestions.append(('운영',
            f"조리시간 {ct:.0f}분 → 임계점 초과",
            f"🎯 1주일 내: 조리시간 20분 이하\n"
            f"📋 개선: 피크타임 인력·주방 효율\n"
            f"💡 본사 매뉴얼 참고"
        ))

    if ar > 0 and ar < 0.95:
        missing_pct = (0.95 - ar) * 100
        suggestions.append(('운영',
            f"주문접수율 {ar*100:.0f}% → 95% 미만 페널티 ({missing_pct:.1f}%p 부족)",
            f"🎯 즉시: 접수율 95% 이상 회복\n"
            f"📋 긴급 점검:\n"
            f"  • 태블릿 전원·인터넷\n"
            f"  • 영업중지 설정\n"
            f"  • 알림음·진동\n"
            f"⚠️ 95% 미만 = 플랫폼 노출 하락\n"
            f"💡 문제 지속 시 담당 영업팀장 즉시 연락"
        ))

    if cc > 0 and cc < 0.9:
        suggestions.append(('운영',
            f"조리시간준수율 {cc*100:.0f}% → 90% 미만",
            f"🎯 1주일 내: 준수율 90% 이상\n"
            f"📋 두 가지 접근:\n"
            f"  1. 조리시간 설정 상향 (현실 반영)\n"
            f"  2. 실제 조리 단축 (인력·프로세스)\n"
            f"💡 권장: 1번 먼저, 2번 점진\n"
            f"🤝 본사: 적정 시간 설정 컨설팅"
        ))

    if reorder > 0 and reorder < 0.12:
        suggestions.append(('운영',
            f"재주문율 {reorder*100:.1f}% (업계 15% 미달)",
            f"🎯 1개월 목표: 재주문율 15%\n"
            f"📋 실행:\n"
            f"  • 재주문 쿠폰 (10%, 2주)\n"
            f"  • 리뷰 이벤트 (500원 쿠폰)\n"
            f"  • 단골 문자 (신메뉴 안내)\n"
            f"💰 비용: 월 10~15만원\n"
            f"📊 효과: 재주문율 +3~5%p"
        ))

    if rating > 0 and rating < 4.3:
        suggestions.append(('운영',
            f"별점 {rating}점 (목표 4.5점 미달)",
            f"🎯 즉시: 최근 1주 저평점 리뷰 분석\n"
            f"📋 프로세스:\n"
            f"  1. 주요 불만 TOP 3 파악\n"
            f"  2. 개선 계획 수립\n"
            f"  3. 리뷰 답변에 개선 명시\n"
            f"💡 일반 불만: 조리시간·품질·포장\n"
            f"🤝 본사: 리뷰 대응 템플릿 제공"
        ))

    return suggestions

# =============================================================================
# LLM 프롬프트 생성
# =============================================================================
def _build_prompt(row):
    store = row['매장명']
    lines = []
    lines.append(f"매장명: {store}")
    lines.append(f"기준일: {row.get('order_daily', 'N/A')}")
    lines.append(f"담당자: {row.get('담당자', 'N/A')}")
    lines.append("")

    # 현재 상태
    lines.append("【현재 상태】")
    lines.append(f"- 매출 status: {_vs(row,'status')} (pre: {_vs(row,'pre_status')})")
    lines.append(f"- 수수료 status: {_vs(row,'fee_status')} (pre: {_vs(row,'pre_fee_status')})")
    lines.append(f"- 쿠팡광고 status: {_vs(row,'쿠팡_광고효과_status')} (pre: {_vs(row,'pre_쿠팡_광고효과_status')})")
    lines.append(f"- 배민광고 status: {_vs(row,'배민_광고효과_status')} (pre: {_vs(row,'pre_배민_광고효과_status')})")
    lines.append(f"- 운영 status: {_vs(row,'service_status')} (pre: {_vs(row,'pre_service_status')})")
    lines.append("")

    # 1. 매출
    lines.append("【1. 매출】")
    lines.append(f"- 매출: {_v(row,'total_amount'):,.0f}원 / 주문: {_v(row,'total_order_count'):.0f}건 / ARPU: {_v(row,'ARPU'):,.0f}원")
    lines.append(f"- 전일: {_v(row,'전일대비_증감률'):+.1f}% ({_v(row,'전일_매출'):,.0f}원) / 전주: {_v(row,'전주대비_증감률'):+.1f}% ({_v(row,'전주_매출'):,.0f}원)")
    lines.append(f"- 이평: 14일 {_v(row,'ma_14'):,.0f} / 28일 {_v(row,'ma_28'):,.0f} / 2주avg {_v(row,'current_avg_2week'):,.0f} / 당월MTD {_v(row,'amt_curr_mtd'):,.0f} / 전월MTD {_v(row,'amt_prev_mtd'):,.0f}")
    lines.append(f"- 7일누적: {_v(row,'sum_7d_recent'):,.0f} / 직전7일: {_v(row,'sum_7d_prev'):,.0f}")
    lines.append(f"- score: {_v(row,'score')}점 (추세:{_v(row,'score_trend')} 주간:{_v(row,'score_7d_total')} 월간:{_v(row,'score_4week_total')})")
    lines.append(f"- 배민 {_v(row,'total_amount_배민'):,.0f}원({_v(row,'total_order_count_배민'):.0f}건) / 쿠팡 {_v(row,'total_amount_쿠팡'):,.0f}원({_v(row,'total_order_count_쿠팡'):.0f}건)")
    lines.append("")

    # 2. 수수료
    lines.append("【2. 수수료】")
    lines.append(f"- 수수료율: {_v(row,'수수료율'):.1f}% (배민 {_v(row,'수수료율_배민'):.1f}% / 쿠팡 {_v(row,'수수료율_쿠팡'):.1f}%)")
    lines.append(f"- 플랫폼별: 배민 {_v(row,'수수료율_배민'):.1f}% / 쿠팡 {_v(row,'수수료율_쿠팡'):.1f}%")
    lines.append(f"- 광고비: {_v(row,'fee_ad'):,.0f}원 (배민 {_v(row,'fee_ad_배민'):,.0f} / 쿠팡 {_v(row,'fee_ad_쿠팡'):,.0f})")
    lines.append(f"- 정산: {_v(row,'settlement_amount'):,.0f}원")
    lines.append(f"- fee_status: {_vs(row,'fee_status')} (pre: {_vs(row,'pre_fee_status')})")
    lines.append("")

    # 3. 광고
    lines.append("【3. 광고】")
    lines.append(f"[쿠팡] 광고효과: {_v(row,'쿠팡_광고효과'):.1f}배 / status: {_vs(row,'쿠팡_광고효과_status')}")
    if _vs(row,'캠페인정보') != 'N/A':
        lines.append(f"  캠페인: {_vs(row,'캠페인정보')}")
    lines.append(f"  광고비: {_v(row,'광고비용'):,.0f} / 노출: {_v(row,'광고노출수'):,.0f} / 클릭: {_v(row,'광고클릭수'):,.0f} / 주문: {_v(row,'광고주문수'):.0f}건 / 매출: {_v(row,'광고매출'):,.0f}")
    lines.append(f"[배민] 광고효과: {_v(row,'배민_광고효과'):.1f}배 / status: {_vs(row,'배민_광고효과_status')}")
    lines.append(f"  광고비: {_v(row,'광고지출'):,.0f} / 노출: {_v(row,'노출수'):,.0f} / 클릭: {_v(row,'클릭수'):,.0f} / 주문: {_v(row,'주문수'):.0f}건 / 매출: {_v(row,'주문금액'):,.0f}")
    lines.append("")

    # 4. 성실영업
    lines.append("【4. 성실영업지표】")
    if _v(row,'조리소요시간') > 0:
        lines.append(f"- 조리시간: {_v(row,'조리소요시간'):.0f}분 (상위 {_v(row,'조리소요시간_순위비율')*100:.0f}%)")
    if _v(row,'주문접수시간') > 0:
        lines.append(f"- 접수시간: {_v(row,'주문접수시간'):.0f}분 (상위 {_v(row,'주문접수시간_순위비율')*100:.0f}%)")
    if _v(row,'조리시간준수율') > 0:
        lines.append(f"- 준수율: {_v(row,'조리시간준수율')*100:.0f}%")
    if _v(row,'주문접수율') > 0:
        lines.append(f"- 접수율: {_v(row,'주문접수율')*100:.0f}%")
    if _v(row,'최근재주문율') > 0:
        lines.append(f"- 재주문율: {_v(row,'최근재주문율')*100:.1f}%")
    if _v(row,'최근별점') > 0:
        lines.append(f"- 별점: {_v(row,'최근별점')}점 (전체평균 {_v(row,'전체_평균별점'):.2f})")
    lines.append(f"- service_status: {_vs(row,'service_status')} (pre: {_vs(row,'pre_service_status')})")

    return '\n'.join(lines)


def generate_store_prompts(df_all, target_date=None, manager=None):
    """모든 매장별 LLM 프롬프트를 dict로 반환"""
    df_day = _prepare_df(df_all, target_date, manager)
    if len(df_day) == 0:
        print("⚠️ 매장 데이터가 없습니다.")
        return {}
    return {row['매장명']: _build_prompt(row) for _, row in df_day.iterrows()}


# =============================================================================
# 📊 메인: 리포트 생성
# =============================================================================
def generate_expert_report(df_all, target_date=None, manager=None):
    df_day = _prepare_df(df_all, target_date, manager)

    if len(df_day) == 0:
        print("⚠️ 매장 데이터가 없습니다.")
        return

    report_date = df_day['order_daily'].iloc[0]
    if hasattr(report_date, 'strftime'):
        report_date = report_date.strftime('%Y-%m-%d')

    S = {'관심':'[관심]','주의':'[주의]','위험':'[위험]','정상':'[정상]'}

    print("=" * 80)
    print(f"  📊 도리당 매장 진단 & 제안 리포트")
    print(f"  기준일: {report_date} | 대상: {len(df_day)}개 매장")
    print("=" * 80)

    # 주의/위험 매장만 따로 모음
    focus_stores = df_day[df_day['status'].isin(['주의', '위험'])].copy()
    store_focus = {}  # 담당자별 집중 매장

    for _, row in df_day.sort_values('score', ascending=False).iterrows():
        store = row['매장명']
        status = _vs(row, 'status')
        score = _v(row, 'score')
        status_mark = S.get(status, status)
        mgr = row['담당자']

        print(f"\n{'━'*80}")
        print(f"  {store} | {status_mark}({score}점) | 담당: {mgr}")
        print(f"{'━'*80}")

        # 한 줄 현황
        amount = _v(row, 'total_amount')
        orders = _v(row, 'total_order_count')
        fr = _v(row, '수수료율')
        dc = _v(row, '전일대비_증감률')
        wc = _v(row, '전주대비_증감률')
        print(f"  매출: {amount:,.0f}원 / 주문: {orders:.0f}건 / 수수료: {fr:.1f}% / 전일: {dc:+.1f}% / 전주: {wc:+.1f}%")

        # 상태 한 줄 요약
        states = []
        fee_st = _vs(row, 'fee_status')
        cp_st = _vs(row, '쿠팡_광고효과_status')
        bm_st = _vs(row, '배민_광고효과_status')
        svc_st = _vs(row, 'service_status')
        if fee_st not in ['정상','N/A']: states.append(f"수수료:{fee_st}")
        if cp_st not in ['정상','N/A']: states.append(f"쿠팡광고:{cp_st}")
        if bm_st not in ['정상','N/A']: states.append(f"배민광고:{bm_st}")
        if svc_st not in ['정상','N/A']: states.append(f"서비스:{svc_st}")
        if states:
            print(f"  ⚠️ 세부: {' / '.join(states)}")

        # 진단 → 제안
        suggestions = diagnose_and_suggest(row)

        if suggestions:
            # 영역별 그룹화
            grouped = {}
            for category, problem, suggest in suggestions:
                if category not in grouped:
                    grouped[category] = []
                grouped[category].append((problem, suggest))

            # 출력 순서: 매출 > 수수료 > 광고 > 운영
            order = ['매출', '수수료', '광고', '운영']
            for cat in order:
                if cat in grouped:
                    print(f"\n  [{cat} 문제]")
                    for i, (problem, suggest) in enumerate(grouped[cat], 1):
                        print(f"    {i}. {problem}")
                        print(f"       > {suggest}")
        else:
            if status in ['주의', '위험']:
                print(f"\n  ⚠️ 특이 사항 없음. 이상점수 기반 모니터링 유지")
            else:
                print(f"\n  ✅ 정상 운영 중")

        # 담당자 집중 매장 후보 (주의/위험만)
        if status in ['주의', '위험']:
            if mgr not in store_focus:
                store_focus[mgr] = (store, score, suggestions[0] if suggestions else None)
            elif score > store_focus[mgr][1]:
                store_focus[mgr] = (store, score, suggestions[0] if suggestions else None)

    # ═══════════ 담당자별 집중 매장 ═══════════
    if store_focus:
        print(f"\n{'━'*80}")
        print(f"  👤 담당자별 이번 주 집중 매장")
        print(f"{'━'*80}")

        for mgr in sorted(store_focus.keys()):
            focus_store, focus_score, focus_sugg = store_focus[mgr]
            print(f"\n  {mgr} → {focus_store} ({focus_score}점)")
            if focus_sugg:
                _, _, suggest = focus_sugg
                print(f"    1순위 제안: {suggest}")

    print(f"\n{'═'*80}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')} 생성")
    print(f"{'═'*80}")
    for mgr in df_day['담당자'].unique():
        if mgr in store_focus:
            focus_store, focus_score, focus_primary = store_focus[mgr]
            print(f"\n  {mgr} → {focus_store} ({focus_score}점)")
            if focus_primary:
                _, _, suggest = focus_primary
                print(f"    1순위: {suggest}")

    print(f"\n{'═'*70}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')} 생성")
    print(f"{'═'*70}")

# =============================================================================
# Airflow 태스크용 래퍼 함수
# =============================================================================

def llm_remport(df_all):
    """LLM 진단 결과를 생성하고 JSON으로 저장"""
    from modules.transform.utility.paths import LOCAL_DB
    import json
    
    df_day = _prepare_df(df_all)
    
    if len(df_day) == 0:
        print("✅ 주의/위험 매장 없음. 전 매장 정상.")
        return
    
    # 매장별 진단 결과 저장
    store_diagnoses = {}
    
    for _, row in df_day.iterrows():
        store = row['매장명']
        suggestions = diagnose_and_suggest(row)
        
        if suggestions:
            primary, secondary, obs = prioritize_suggestions(suggestions)
            
            diagnosis = {
                'primary': {
                    'icon': primary[0],
                    'problem': primary[1],
                    'suggestion': primary[2]
                } if primary else None,
                'secondary': {
                    'icon': secondary[0],
                    'problem': secondary[1],
                    'suggestion': secondary[2]
                } if secondary else None,
                'observations': [
                    {'icon': o[0], 'problem': o[1], 'suggestion': o[2]}
                    for o in obs
                ] if obs else []
            }
        else:
            diagnosis = {'message': '특이 사항 없음'}
        
        store_diagnoses[store] = diagnosis
    
    # JSON으로 저장
    output_path = LOCAL_DB / 'temp' / 'llm_diagnoses.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(store_diagnoses, f, ensure_ascii=False, indent=2)
    
    print(f"[LLM] 진단 결과 저장: {output_path}")
    print(f"[LLM] 진단 완료: {len(store_diagnoses)}개 매장")
    
    # 콘솔 출력도 유지
    generate_expert_report(df_all)
    
    return store_diagnoses
    


# ============================================================
# 📧 매출 이상 알림 이메일 발송 (수정 버전)
# ============================================================
def send_alert_email(**context):
    """
    담당자별 매출 이상 알람 이메일 발송
    
    ⭐ 수정사항:
    1. 수수료율/광고/성실지표도 매출과 동일한 표 구조
    2. 모든 담당자에게 이메일 발송 (누락 방지)
    3. LLM 진단 결과 포함
    """
    from modules.transform.utility.paths import LOCAL_DB
    from modules.transform.utility.io import (
        SMD_07_EMAIL_TEST_MODE,
        SMD_07_EMAIL_TEST_RECIPIENTS,
        SMD_07_EMAIL_DEV_CC_IN_PROD,
    )
    from modules.transform.utility.io import send_email
    
    print(f"\n{'='*60}")
    print(f"[알림] 이메일 발송 시작")
    
    # 이메일 발송 모드 설정 (io3.py에서 중앙 제어)
    TEST_MODE = bool(SMD_07_EMAIL_TEST_MODE)
    DEV_CC_IN_PROD = bool(SMD_07_EMAIL_DEV_CC_IN_PROD)
    TEST_RECIPIENTS = [str(x).strip() for x in SMD_07_EMAIL_TEST_RECIPIENTS if str(x).strip()]
    
    def get_recipients(manager_email=None):
        """이메일 수신자 결정"""
        if TEST_MODE:
            return TEST_RECIPIENTS if TEST_RECIPIENTS else ["a17019@kakao.com"]
        else:
            recipients = []
            if manager_email and pd.notna(manager_email) and manager_email.strip():
                recipients.append(manager_email)
            if DEV_CC_IN_PROD:
                recipients.insert(0, "a17019@kakao.com")
            return recipients
    
    ti = context['task_instance']
    
    def _vs(row, col, default='N/A'):
        v = row.get(col)
        return str(v).strip() if pd.notna(v) and str(v).strip() else default
    
    def _v(row, col, default=0):
        v = row.get(col)
        return v if pd.notna(v) else default
    
    # 날짜 정의
    dag_run_date = datetime.now().strftime('%Y-%m-%d')
    yesterday = (pd.to_datetime(dag_run_date) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"[알림] 수집일자 (DAG 실행일): {dag_run_date}")
    print(f"[알림] 알림기준일 (매출 분석일): {yesterday}")
    
    # CSV 파일 로드
    alerts_csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts_grp.csv'
    
    if not alerts_csv_path.exists():
        print(f"[알람] CSV 파일 없음: {alerts_csv_path}")
        return "알람 대상 없음 (CSV 파일 없음)"
    
    try:
        alert_df = pd.read_csv(alerts_csv_path, encoding='utf-8-sig')
        print(f"[알림] CSV 파일 읽기 성공: {len(alert_df):,}건")
    except Exception as e:
        print(f"[에러] CSV 파일 읽기 실패: {e}")
        return f"알람 대상 로드 실패: {e}"
    
    if len(alert_df) == 0:
        print("[알람] 알람 대상 0건 - 이메일 발송 생략")
        return "알람 대상 0건"
    
    target_date = alert_df['order_daily'].max()
    target_date_str = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    print(f"[알람] 알람 대상: {len(alert_df):,}건 (기준일: {target_date_str})")
    
    # LLM 진단 결과 로드
    llm_diagnoses = {}
    llm_path = LOCAL_DB / 'temp' / 'llm_diagnoses.json'
    
    if llm_path.exists():
        try:
            import json
            with open(llm_path, 'r', encoding='utf-8') as f:
                llm_diagnoses = json.load(f)
            print(f"[알림] LLM 진단 결과 로드: {len(llm_diagnoses)}개 매장")
        except Exception as e:
            print(f"[경고] LLM 진단 결과 로드 실패: {e}")
    
    # email 컬럼 확인
    if 'email' not in alert_df.columns:
        print(f"[경고] email 컬럼 없음 - 이메일 발송 불가")
        return "email 컬럼 없음"
    
    # 담당자별 그룹핑 (email이 없거나 공백이어도 포함)
    alert_df['email_for_group'] = alert_df['email'].fillna('미배정').replace('', '미배정')
    managers = alert_df.groupby(['담당자', 'email_for_group'])
    
    email_count = 0
    skipped_count = 0
    
    for (manager, email_group), group in managers:
        # 실제 발송용 email (미배정이면 None)
        actual_email = None if email_group == '미배정' else email_group
        
        # 매장별 상세 정보
        all_cols = ['매장명', 'status', 'fee_status', '쿠팡_광고효과_status', 
                    '배민_광고효과_status', 'service_status',
                    'score', 'total_amount', 'total_order_count',
                    'score_trend', 'score_7d_total', 'score_4week_total',
                    'ma_14', 'ma_28', 'current_avg_2week', 'amt_curr_mtd', 'amt_prev_mtd',
                    'sum_7d_recent', 'sum_7d_prev', 
                    'fee_ratio', 'fee_ratio_MA14', 'fee_ratio_MA28',
                    'fee_ratio_mtd_curr_month', 'fee_ratio_mtd_prev_month_same_day',
                    'avg_7d_recent', 'avg_7d_prev',
                    'fee_score_total',
                    '쿠팡_광고효과', '쿠팡_광고효과_MA14', '쿠팡_광고효과_MA28',
                    '쿠팡_광고효과_mtd_curr_month', '쿠팡_광고효과_mtd_prev_month_same_day',
                    '쿠팡_광고효과_avg_7d_recent', '쿠팡_광고효과_avg_7d_prev',
                    '쿠팡_광고효과_score_total',
                    '배민_광고효과', '배민_광고효과_MA14', '배민_광고효과_MA28',
                    '배민_광고효과_mtd_curr_month', '배민_광고효과_mtd_prev_month_same_day',
                    '배민_광고효과_avg_7d_recent', '배민_광고효과_avg_7d_prev',
                    '배민_광고효과_score_total',
                    '조리시간_total', '접수시간_total', '재주문율_total', '별점_total', 'service_score_total']
        
        available_cols = [c for c in all_cols if c in group.columns]
        store_details = group[available_cols].drop_duplicates(subset=['매장명'])
        
        if len(store_details) == 0:
            continue
        
        # 이메일 제목
        first_store = sorted(store_details['매장명'].tolist())[0]
        if len(store_details) > 1:
            subject = f"🚨 [운영이상] {first_store} 외 {len(store_details) - 1}건"
        else:
            subject = f"🚨 [운영이상] {first_store}"
        
        # 통계 (4가지 카테고리)
        amt_count = len(store_details[store_details['status'].isin(['위험', '주의'])]) if 'status' in store_details.columns else 0
        fee_count = len(store_details[store_details['fee_status'].isin(['위험', '주의'])]) if 'fee_status' in store_details.columns else 0
        ad_count = len(store_details[
            (store_details['쿠팡_광고효과_status'].isin(['위험', '주의'])) |
            (store_details['배민_광고효과_status'].isin(['위험', '주의']))
        ]) if '쿠팡_광고효과_status' in store_details.columns else 0
        svc_count = len(store_details[store_details['service_status'].isin(['위험', '주의'])]) if 'service_status' in store_details.columns else 0
        
        # HTML 생성 시작
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', Arial, sans-serif; background-color: #f5f7fa;">
    
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f5f7fa;">
        <tr>
            <td align="center" style="padding: 20px;">
                
                <table width="700" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                    
                    <!-- 헤더 -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); padding: 30px; text-align: center;">
                            <h1 style="margin: 0 0 8px 0; font-size: 22px; color: #ffffff; font-weight: 600;">🚨 매장 운영 이상 알림</h1>
                            <p style="margin: 0; font-size: 14px; color: rgba(255,255,255,0.9);">
                                <strong>수집일자:</strong> {dag_run_date} (자동화 실행일)
                            </p>
                            <p style="margin: 4px 0 0 0; font-size: 14px; color: rgba(255,255,255,0.9);">
                                <strong>알림기준일:</strong> {target_date_str} (분석 기준일)
                            </p>
                            <p style="margin: 15px 0 0 0; font-size: 13px; color: rgba(255,255,255,0.85);">
                                ({target_date_str}) 운영 데이터 기준 알림
                            </p>
                            
                            <table cellpadding="0" cellspacing="0" border="0" align="center" style="margin-top: 15px;">
                                <tr>
                                    <td style="background: rgba(255,255,255,0.2); padding: 6px 12px; border-radius: 15px; font-size: 12px; color: #ffffff; margin: 0 5px;">
                                        💰 매출 {amt_count}
                                    </td>
                                    <td width="8"></td>
                                    <td style="background: rgba(255,255,255,0.2); padding: 6px 12px; border-radius: 15px; font-size: 12px; color: #ffffff; margin: 0 5px;">
                                        💳 수수료 {fee_count}
                                    </td>
                                    <td width="8"></td>
                                    <td style="background: rgba(255,255,255,0.2); padding: 6px 12px; border-radius: 15px; font-size: 12px; color: #ffffff; margin: 0 5px;">
                                        📢 광고 {ad_count}
                                    </td>
                                    <td width="8"></td>
                                    <td style="background: rgba(255,255,255,0.2); padding: 6px 12px; border-radius: 15px; font-size: 12px; color: #ffffff; margin: 0 5px;">
                                        🏪 영업 {svc_count}
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- 인사말 -->
                    <tr>
                        <td style="padding: 25px 30px 15px 30px;">
                            <p style="margin: 0; font-size: 15px; color: #2c3e50; line-height: 1.6;">
                                <strong style="color: #e74c3c;">{manager}</strong>님 안녕하세요,<br>
                                담당 매장 중 <strong style="color: #e74c3c;">{len(store_details)}개 매장</strong>에 이상이 감지되었습니다.
                            </p>
                        </td>
                    </tr>
"""
        
        # 헬퍼 함수들
        def format_number(val):
            try:
                if pd.notna(val):
                    return f"{int(float(val)):,}"
            except:
                pass
            return "-"
        
        def calc_rate(val1, val2):
            try:
                if pd.notna(val1) and pd.notna(val2) and float(val2) > 0:
                    return ((float(val1) - float(val2)) / float(val2)) * 100
            except:
                pass
            return None
        
        def format_rate(rate, score_val):
            if rate is None:
                return "-"
            if score_val == 2:
                color = '#e74c3c'
            elif score_val == 1:
                color = '#f39c12'
            else:
                color = '#27ae60'
            return f"<span style='color: {color}; font-weight: 600;'>{rate:+.1f}%</span>"
        
        def score_badge(score_val):
            if score_val == 0:
                return "<span style='background: #27ae60; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;'>0점</span>"
            elif score_val == 1:
                return "<span style='background: #f39c12; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;'>1점</span>"
            else:
                return "<span style='background: #e74c3c; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;'>2점</span>"
        
        def status_badge(status):
            status_colors = {
                '위험': '#e74c3c', '주의': '#f39c12', '관심': '#3498db', '정상': '#27ae60', 'N/A': '#95a5a6'
            }
            color = status_colors.get(status, '#95a5a6')
            return f"<span style='background: {color}; color: white; padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 600;'>{status}</span>"

        def score_if_increase_bad(rate):
            if rate is None or pd.isna(rate):
                return 0
            if rate > 10:
                return 2
            if rate > 0:
                return 1
            return 0

        def score_if_decrease_bad(rate):
            if rate is None or pd.isna(rate):
                return 0
            if rate < -10:
                return 2
            if rate < 0:
                return 1
            return 0
        
        # 매장별 카드 생성
        store_details = store_details.sort_values(['status', 'score'], ascending=[True, False])
        
        for idx, (_, row) in enumerate(store_details.iterrows()):
            is_danger = row['status'] == '위험'
            card_border = '#e74c3c' if is_danger else '#f39c12'
            status_emoji = '🚨' if is_danger else '⚠️'
            
            # ⭐ 총점 계산: 이상 있는 카테고리의 점수만 합계
            # (주의/위험 상태인 카테고리만 포함)
            score = 0
            score_breakdown = []
            
            # 1️⃣ 매출 점수 (추세+주간+월간, 일일 제외)
            if pd.notna(row.get('status')) and row.get('status') in ['위험', '주의']:
                amt_score = (int(row.get('score_trend', 0)) +
                             int(row.get('score_7d_total', 0)) +
                             int(row.get('score_4week_total', 0)))
                score += amt_score
                score_breakdown.append(f"매출:{amt_score}")
            
            # 2️⃣ 수수료 점수
            if pd.notna(row.get('fee_status')) and row.get('fee_status') in ['위험', '주의']:
                fee_score = int(row.get('fee_score_total', 0))
                score += fee_score
                score_breakdown.append(f"수수료:{fee_score}")
            
            # 3️⃣ 쿠팡 광고 점수
            if pd.notna(row.get('쿠팡_광고효과_status')) and row.get('쿠팡_광고효과_status') in ['위험', '주의']:
                cp_ad_score = int(row.get('쿠팡_광고효과_score_total', 0))
                score += cp_ad_score
                score_breakdown.append(f"쿠팡광고:{cp_ad_score}")
            
            # 4️⃣ 배민 광고 점수
            if pd.notna(row.get('배민_광고효과_status')) and row.get('배민_광고효과_status') in ['위험', '주의']:
                bm_ad_score = int(row.get('배민_광고효과_score_total', 0))
                score += bm_ad_score
                score_breakdown.append(f"배민광고:{bm_ad_score}")
            
            # 5️⃣ 영업 점수
            if pd.notna(row.get('service_status')) and row.get('service_status') in ['위험', '주의']:
                svc_score = int(row.get('service_score_total', 0))
                score += svc_score
                score_breakdown.append(f"영업:{svc_score}")
            
            if score_breakdown:
                print(f"[점수 합계] {row['매장명']}: {score}점 ({', '.join(score_breakdown)})")
            else:
                print(f"[점수 합계] {row['매장명']}: {score}점 (이상 카테고리 없음)")
            
            total_amount = int(row['total_amount']) if pd.notna(row['total_amount']) else 0
            
            # 4가지 카테고리 상태
            amt_status = _vs(row, 'status', 'N/A')
            fee_status = _vs(row, 'fee_status', 'N/A')
            coupang_ad_status = _vs(row, '쿠팡_광고효과_status', 'N/A')
            baemin_ad_status = _vs(row, '배민_광고효과_status', 'N/A')
            service_status = _vs(row, 'service_status', 'N/A')
            
            html += f"""
                    <!-- 매장 카드: {row['매장명']} -->
                    <tr>
                        <td style="padding: 0 30px 20px 30px;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 2px solid {card_border}; border-radius: 10px; overflow: hidden;">
                                
                                <!-- 매장 헤더 -->
                                <tr>
                                    <td style="background: {card_border}; padding: 12px 15px;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td>
                                                    <span style="font-size: 16px; font-weight: 600; color: #ffffff;">{row['매장명']}</span>
                                                </td>
                                                <td align="right">
                                                    <span style="background: rgba(255,255,255,0.9); color: {card_border}; padding: 4px 12px; border-radius: 15px; font-size: 13px; font-weight: 700;">
                                                        {status_emoji} 총 {score}점
                                                    </span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                
                                <!-- 어제 매출 -->
                                <tr>
                                    <td style="padding: 15px; background: #f8f9fa; border-bottom: 1px solid #eee;">
                                        <p style="margin: 0; font-size: 13px; color: #666;">어제({target_date_str}) 매출</p>
                                        <p style="margin: 5px 0 0 0; font-size: 24px; font-weight: 700; color: #2c3e50;">{total_amount:,}원</p>
                                    </td>
                                </tr>
"""
            
            # 1. 💰 매출 점수 상세
            if amt_status in ['위험', '주의']:
                score_trend = int(_v(row, 'score_trend', 0))
                score_7d = int(_v(row, 'score_7d_total', 0))
                score_4week = int(_v(row, 'score_4week_total', 0))
                
                ma_14 = _v(row, 'ma_14')
                ma_28 = _v(row, 'ma_28')
                current_avg_2week = _v(row, 'current_avg_2week')
                amt_curr_mtd = _v(row, 'amt_curr_mtd')
                amt_prev_mtd = _v(row, 'amt_prev_mtd')
                sum_7d_recent = _v(row, 'sum_7d_recent')
                sum_7d_prev = _v(row, 'sum_7d_prev')
                
                rate_trend = calc_rate(ma_14, ma_28)
                rate_7d = calc_rate(sum_7d_recent, sum_7d_prev)
                rate_4week = calc_rate(amt_curr_mtd, amt_prev_mtd)
                
                html += f"""
                                <!-- 💰 매출 점수 상세 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">💰 매출 점수 상세</p>
                                        
                                        <table width="100%" cellpadding="8" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr style="background: #f0f0f0;">
                                                <th style="text-align: left; padding: 8px; border-radius: 5px 0 0 0;">지표</th>
                                                <th style="text-align: center; padding: 8px;">비교</th>
                                                <th style="text-align: center; padding: 8px;">변화율</th>
                                                <th style="text-align: center; padding: 8px; border-radius: 0 5px 0 0;">점수</th>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>추세</strong><br><span style="color: #888; font-size: 11px;">14일MA vs 28일MA</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{format_number(ma_14)} vs {format_number(ma_28)}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_trend, score_trend)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_trend)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>주간</strong><br><span style="color: #888; font-size: 11px;">최근7일 vs 직전7일</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{format_number(sum_7d_recent)} vs {format_number(sum_7d_prev)}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_7d, score_7d)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_7d)}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px;"><strong>월간</strong><br><span style="color: #888; font-size: 11px;">당월 누적합 vs 전월 동일일자 누적합</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{format_number(amt_curr_mtd)} vs {format_number(amt_prev_mtd)}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_4week, score_4week)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_4week)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
"""
            
            # 2. 💳 수수료 점수 상세
            if fee_status in ['위험', '주의']:
                fee_ratio_MA14 = _v(row, 'fee_ratio_MA14')
                fee_ratio_MA28 = _v(row, 'fee_ratio_MA28')
                fee_ratio_curr_mtd = _v(row, 'fee_ratio_mtd_curr_month')
                fee_ratio_prev_mtd = _v(row, 'fee_ratio_mtd_prev_month_same_day')
                avg_7d_recent = _v(row, 'avg_7d_recent')
                avg_7d_prev = _v(row, 'avg_7d_prev')
                
                rate_fee_trend = calc_rate(fee_ratio_MA14, fee_ratio_MA28)
                rate_fee_7d = calc_rate(avg_7d_recent, avg_7d_prev)
                rate_fee_month = calc_rate(fee_ratio_curr_mtd, fee_ratio_prev_mtd)
                fee_trand_score = score_if_increase_bad(rate_fee_trend)
                fee_avg_7d_score = score_if_increase_bad(rate_fee_7d)
                fee_month_score = score_if_increase_bad(rate_fee_month)
                
                html += f"""
                                <!-- 💳 수수료 점수 상세 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">💳 수수료 점수 상세</p>
                                        
                                        <table width="100%" cellpadding="8" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr style="background: #f0f0f0;">
                                                <th style="text-align: left; padding: 8px; border-radius: 5px 0 0 0;">지표</th>
                                                <th style="text-align: center; padding: 8px;">비교</th>
                                                <th style="text-align: center; padding: 8px;">변화율</th>
                                                <th style="text-align: center; padding: 8px; border-radius: 0 5px 0 0;">점수</th>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>추세</strong><br><span style="color: #888; font-size: 11px;">14일MA vs 28일MA</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{fee_ratio_MA14:.2f}% vs {fee_ratio_MA28:.2f}%</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_fee_trend, fee_trand_score)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(fee_trand_score)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>주간</strong><br><span style="color: #888; font-size: 11px;">최근7일 vs 직전7일</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{avg_7d_recent:.2f}% vs {avg_7d_prev:.2f}%</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_fee_7d, fee_avg_7d_score)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(fee_avg_7d_score)}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px;"><strong>월간</strong><br><span style="color: #888; font-size: 11px;">당월 누적 평균 vs 전월 동일일자 누적 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{fee_ratio_curr_mtd:.4f} vs {fee_ratio_prev_mtd:.4f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_fee_month, fee_month_score)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(fee_month_score)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
"""
            
            # 3. 📢 광고 점수 상세 (쿠팡)
            if coupang_ad_status in ['위험', '주의']:
                cp_ma14 = _v(row, '쿠팡_광고효과_MA14')
                cp_ma28 = _v(row, '쿠팡_광고효과_MA28')
                cp_4w = _v(row, '쿠팡_광고효과_mtd_curr_month')
                cp_4w_prev = _v(row, '쿠팡_광고효과_mtd_prev_month_same_day')
                cp_7d_recent = _v(row, '쿠팡_광고효과_avg_7d_recent')
                cp_7d_prev = _v(row, '쿠팡_광고효과_avg_7d_prev')
                
                rate_cp_trend = calc_rate(cp_ma14, cp_ma28)
                rate_cp_7d = calc_rate(cp_7d_recent, cp_7d_prev)
                rate_cp_month = calc_rate(cp_4w, cp_4w_prev)
                cp_trand = score_if_decrease_bad(rate_cp_trend)
                cp_7d = score_if_decrease_bad(rate_cp_7d)
                cp_month = score_if_decrease_bad(rate_cp_month)
                
                html += f"""
                                <!-- 📢 쿠팡 광고 점수 상세 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">📢 쿠팡 광고 점수 상세</p>
                                        
                                        <table width="100%" cellpadding="8" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr style="background: #f0f0f0;">
                                                <th style="text-align: left; padding: 8px; border-radius: 5px 0 0 0;">지표</th>
                                                <th style="text-align: center; padding: 8px;">비교</th>
                                                <th style="text-align: center; padding: 8px;">변화율</th>
                                                <th style="text-align: center; padding: 8px; border-radius: 0 5px 0 0;">점수</th>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>추세</strong><br><span style="color: #888; font-size: 11px;">14일 평균 vs 28일 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{cp_ma14:.2f} vs {cp_ma28:.2f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_cp_trend, cp_trand)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(cp_trand)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>주간</strong><br><span style="color: #888; font-size: 11px;">최근7일 평균 vs 직전7일 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{cp_7d_recent:.2f} vs {cp_7d_prev:.2f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_cp_7d, cp_7d)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(cp_7d)}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px;"><strong>월간</strong><br><span style="color: #888; font-size: 11px;">당월 누적 평균 vs 전월 동일일자 누적 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{cp_4w:.4f} vs {cp_4w_prev:.4f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_cp_month, cp_month)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(cp_month)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
"""
            
            # 4. 📢 배민 광고 점수 상세
            if baemin_ad_status in ['위험', '주의']:
                bm_ma14 = _v(row, '배민_광고효과_MA14')
                bm_ma28 = _v(row, '배민_광고효과_MA28')
                bm_4w = _v(row, '배민_광고효과_mtd_curr_month')
                bm_4w_prev = _v(row, '배민_광고효과_mtd_prev_month_same_day')
                bm_7d_recent = _v(row, '배민_광고효과_avg_7d_recent')
                bm_7d_prev = _v(row, '배민_광고효과_avg_7d_prev')
                
                rate_bm_trend = calc_rate(bm_ma14, bm_ma28)
                rate_bm_7d = calc_rate(bm_7d_recent, bm_7d_prev)
                rate_bm_month = calc_rate(bm_4w, bm_4w_prev)
                bm_trand = score_if_decrease_bad(rate_bm_trend)
                bm_7d = score_if_decrease_bad(rate_bm_7d)
                bm_month = score_if_decrease_bad(rate_bm_month)
                
                html += f"""
                                <!-- 📢 배민 광고 점수 상세 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">📢 배민 광고 점수 상세</p>
                                        
                                        <table width="100%" cellpadding="8" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr style="background: #f0f0f0;">
                                                <th style="text-align: left; padding: 8px; border-radius: 5px 0 0 0;">지표</th>
                                                <th style="text-align: center; padding: 8px;">비교</th>
                                                <th style="text-align: center; padding: 8px;">변화율</th>
                                                <th style="text-align: center; padding: 8px; border-radius: 0 5px 0 0;">점수</th>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>추세</strong><br><span style="color: #888; font-size: 11px;">14일 평균 vs 28일 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{bm_ma14:.2f} vs {bm_ma28:.2f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_bm_trend, bm_trand)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(bm_trand)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>주간</strong><br><span style="color: #888; font-size: 11px;">최근7일 평균 vs 직전7일 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{bm_7d_recent:.2f} vs {bm_7d_prev:.2f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_bm_7d, bm_7d)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(bm_7d)}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px;"><strong>월간</strong><br><span style="color: #888; font-size: 11px;">당월 누적 평균 vs 전월 동일일자 누적 평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{bm_4w:.4f} vs {bm_4w_prev:.4f}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_bm_month, bm_month)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(bm_month)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
"""
            
            # 5. 🏪 성실영업 점수 상세
            if service_status in ['위험', '주의']:
                cooking_total = int(_v(row, '조리시간_total', 0))
                accept_total = int(_v(row, '접수시간_total', 0))
                reorder_total = int(_v(row, '재주문율_total', 0))
                rating_total = int(_v(row, '별점_total', 0))
                
                html += f"""
                                <!-- 🏪 성실영업 점수 상세 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">🏪 성실영업 점수 상세 (TOTAL만 표시)</p>
                                        
                                        <table width="100%" cellpadding="8" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr style="background: #f0f0f0;">
                                                <th style="text-align: left; padding: 8px; border-radius: 5px 0 0 0;">지표</th>
                                                <th style="text-align: center; padding: 8px; border-radius: 0 5px 0 0;">점수</th>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>조리시간</strong></td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(cooking_total)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>접수시간</strong></td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(accept_total)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>재주문율</strong></td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(reorder_total)}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px;"><strong>별점</strong></td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(rating_total)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
"""
            
            # LLM 진단 결과 추가
            store_name = row['매장명']
            llm_diagnosis = llm_diagnoses.get(store_name, {})
            
            if llm_diagnosis:
                if 'message' in llm_diagnosis:
                    # 특이사항 없음
                    html += f"""
                                <!-- LLM 진단 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee; background: #f0f8ff;">
                                        <p style="margin: 0 0 8px 0; font-size: 13px; font-weight: 600; color: #2c3e50;">🤖 AI 진단</p>
                                        <p style="margin: 0; font-size: 12px; color: #666;">✅ {llm_diagnosis['message']}</p>
                                    </td>
                                </tr>
"""
                else:
                    # 진단 결과 있음
                    html += """
                                <!-- LLM 진단 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee; background: #f0f8ff;">
                                        <p style="margin: 0 0 12px 0; font-size: 13px; font-weight: 600; color: #2c3e50;">🤖 AI 진단 및 제안</p>
"""
                    
                    # 주요 문제 (primary)
                    if llm_diagnosis.get('primary'):
                        primary = llm_diagnosis['primary']
                        category = primary.get('icon', '📊')
                        problem = primary.get('problem', '')
                        suggestion = primary.get('suggestion', '')
                        
                        html += f"""
                                        <div style="margin-bottom: 10px; padding: 12px; background: white; border-left: 4px solid #e74c3c; border-radius: 4px;">
                                            <p style="margin: 0 0 6px 0; font-size: 12px; font-weight: 600; color: #e74c3c;">{category} 주요 문제</p>
                                            <p style="margin: 0 0 8px 0; font-size: 11px; color: #555; line-height: 1.5;">{problem}</p>
                                            <p style="margin: 0; font-size: 11px; color: #27ae60; line-height: 1.5;"><strong>💡 제안:</strong> {suggestion}</p>
                                        </div>
"""
                    
                    # 부가 문제 (secondary)
                    if llm_diagnosis.get('secondary'):
                        secondary = llm_diagnosis['secondary']
                        category = secondary.get('icon', '📊')
                        problem = secondary.get('problem', '')
                        suggestion = secondary.get('suggestion', '')
                        
                        html += f"""
                                        <div style="margin-bottom: 10px; padding: 12px; background: white; border-left: 4px solid #f39c12; border-radius: 4px;">
                                            <p style="margin: 0 0 6px 0; font-size: 12px; font-weight: 600; color: #f39c12;">{category} 부가 문제</p>
                                            <p style="margin: 0 0 8px 0; font-size: 11px; color: #555; line-height: 1.5;">{problem}</p>
                                            <p style="margin: 0; font-size: 11px; color: #27ae60; line-height: 1.5;"><strong>💡 제안:</strong> {suggestion}</p>
                                        </div>
"""
                    
                    # 관찰 사항 (observations)
                    if llm_diagnosis.get('observations') and len(llm_diagnosis['observations']) > 0:
                        html += """
                                        <div style="padding: 12px; background: white; border-left: 4px solid #95a5a6; border-radius: 4px;">
                                            <p style="margin: 0 0 8px 0; font-size: 12px; font-weight: 600; color: #95a5a6;">📋 추가 관찰 사항</p>
"""
                        for obs in llm_diagnosis['observations']:
                            obs_icon = obs.get('icon', '📊')
                            obs_problem = obs.get('problem', '')
                            obs_suggestion = obs.get('suggestion', '')
                            
                            html += f"""
                                            <div style="margin-bottom: 6px; padding-left: 8px; border-left: 2px solid #ddd;">
                                                <p style="margin: 0 0 4px 0; font-size: 10px; color: #666;">{obs_icon} {obs_problem}</p>
                                                <p style="margin: 0; font-size: 10px; color: #27ae60;">→ {obs_suggestion}</p>
                                            </div>
"""
                        
                        html += """
                                        </div>
"""
                    
                    html += """
                                    </td>
                                </tr>
"""
            
            # 상태 요약
            html += f"""
                                <!-- 4가지 카테고리 상태 -->
                                <tr>
                                    <td style="padding: 15px; border-top: 1px solid #eee; background: #fafafa;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">🎯 운영 상태 체크</p>
                                        <table width="100%" cellpadding="6" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr>
                                                <td style="padding: 6px; width: 30%; color: #666;">💰 매출</td>
                                                <td style="padding: 6px;">{status_badge(amt_status)}</td>
                                            </tr>
                                            <tr style="border-top: 1px solid #eee;">
                                                <td style="padding: 6px; width: 30%; color: #666;">💳 수수료</td>
                                                <td style="padding: 6px;">{status_badge(fee_status)}</td>
                                            </tr>
                                            <tr style="border-top: 1px solid #eee;">
                                                <td style="padding: 6px; width: 30%; color: #666;">📢 쿠팡광고</td>
                                                <td style="padding: 6px;">{status_badge(coupang_ad_status)}</td>
                                            </tr>
                                            <tr style="border-top: 1px solid #eee;">
                                                <td style="padding: 6px; width: 30%; color: #666;">📢 배민광고</td>
                                                <td style="padding: 6px;">{status_badge(baemin_ad_status)}</td>
                                            </tr>
                                            <tr style="border-top: 1px solid #eee;">
                                                <td style="padding: 6px; width: 30%; color: #666;">🏪 성실영업</td>
                                                <td style="padding: 6px;">{status_badge(service_status)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                
                            </table>
                        </td>
                    </tr>
"""
        
        # 푸터
        html += """
                    <!-- 푸터 -->
                    <tr>
                        <td style="padding: 20px 30px; text-align: center; border-top: 1px solid #eee;">
                            <p style="margin: 0; font-size: 12px; color: #7f8c8d;">이 이메일은 자동 발송되었습니다 | 문의: a17019@kakao.com</p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
    
</body>
</html>"""
        
        # 이메일 발송
        recipient_emails = get_recipients(actual_email)
        
        mode_text = "🧪 테스트" if TEST_MODE else "🚀 실전"
        email_info = f"email={actual_email}" if actual_email else "email=미배정"
        print(f"[알람] {mode_text} | {manager} ({email_info}): {len(store_details)}개 매장 -> {recipient_emails}")
        
        try:
            result = send_email(
                subject=subject,
                html_content=html,
                to_emails=recipient_emails,
                conn_id='doridang_conn_smtp_gmail',
                **context
            )
            email_count += 1
            print(f"[알람] ✅ {manager} 이메일 발송 성공")
        except Exception as e:
            skipped_count += 1
            print(f"[알람] ❌ {manager} 이메일 발송 실패: {e}")
            import traceback
            print(f"[상세 오류]\n{traceback.format_exc()}")
    
    print(f"\n[알람] 이메일 발송 완료: 성공 {email_count}명, 실패 {skipped_count}명")
    print(f"{'='*60}\n")
    
    return f"알람 이메일 발송 완료: 성공 {email_count}명, 실패 {skipped_count}명, 기준일: {target_date_str}"