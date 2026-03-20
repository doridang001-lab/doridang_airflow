"""
io.py - 범용 ETL 함수 모음 (통합본)
모든 파이프라인에서 사용하는 공통 로직
"""

import pandas as pd
from pathlib import Path
import hashlib

from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB

# 스케줄 상수 (schedule.py에서 re-export → 기존 import 호환)
from modules.transform.utility.schedule import (
    SMD_ORDERS_TIME,
    SMD_VISIT_LOG,
    SMP_TOORDER_VOC_TIME,
    SMP_FDAM_CS_TIME,
    SMD_07_EMAIL_TEST_MODE,
    SMD_07_EMAIL_TEST_RECIPIENTS,
    SMD_07_EMAIL_DEV_CC_IN_PROD,
)


# ============================================================
# 1. 수집 (Load) - io3 계열
# ============================================================

def load_files(
    patterns: list[str],
    search_paths: list[Path],
    xcom_key: str,
    file_type: str = 'auto',
    use_glob: bool = False,
    header: int = 0,
    dedup_key: list[str] | None = None,
    add_source_filename: bool = True,
    **context
) -> str:
    """범용 파일 로더"""
    ti = context['task_instance']

    all_files = []
    print(f"[검색 시작] 패턴: {patterns}")
    for search_path in search_paths:
        print(f"  📁 검색 경로: {search_path}")
        print(f"     경로 존재: {search_path.exists()}")
        if not search_path.exists():
            print(f"     ⚠️  경로가 없습니다.")
            continue
        for pattern in patterns:
            if use_glob:
                for ext in ['.parquet', '.pq', '.csv', '.xlsx', '.xls']:
                    found = list(search_path.glob(f"{pattern}{ext}"))
                    if found:
                        print(f"     ✓ {pattern}{ext}: {len(found)}개")
                    all_files.extend(found)
            else:
                found = list(search_path.glob(pattern))
                if found:
                    print(f"     ✓ {pattern}: {len(found)}개")
                all_files.extend(found)

    if not all_files:
        print(f"[❌] 파일 없음 (패턴: {patterns})")
        ti.xcom_push(key=xcom_key, value=None)
        return "0건 (파일 없음)"

    unique_files = _get_unique_files(all_files)
    print(f"[✅] {len(unique_files)}개 파일 발견")

    if file_type == 'auto':
        suffix = unique_files[0].suffix.lower()
        if suffix == '.csv':
            file_type = 'csv'
        elif suffix in ['.xlsx', '.xls']:
            file_type = 'excel'
        elif suffix in ['.parquet', '.pq']:
            file_type = 'parquet'
        else:
            raise ValueError(f"지원하지 않는 파일 타입: {suffix}")

    if file_type == 'csv':
        df = _load_csv_files(unique_files, add_source_filename=add_source_filename)
    elif file_type == 'excel':
        df = _load_excel_files(unique_files, header=header, add_source_filename=add_source_filename)
    elif file_type == 'parquet':
        df = _load_parquet_files(unique_files, add_source_filename=add_source_filename)
    else:
        raise ValueError(f"지원하지 않는 파일 타입: {file_type}")

    if dedup_key:
        valid_cols = [c for c in dedup_key if c in df.columns]
        if valid_cols:
            before = len(df)
            df = df.drop_duplicates(subset=valid_cols, keep='first')
            after = len(df)
            if before - after > 0:
                print(f"\n[중복 제거] {valid_cols} 기준: {before - after:,}건 제거")

    output_path = _save_parquet(df, context, prefix=xcom_key.replace('_path', '_raw'))
    ti.xcom_push(key=xcom_key, value=str(output_path))
    return f"✅ {len(df):,}건 로드"


# ============================================================
# 2. 전처리 (Preprocess) - io3 계열
# ============================================================

def preprocess_df(
    input_xcom_key: str,
    output_xcom_key: str,
    transform_func=None,
    natural_keys: list[str] | None = None,
    **context
) -> str:
    """범용 전처리 함수"""
    ti = context['task_instance']
    parquet_path = ti.xcom_pull(key=input_xcom_key)

    if not parquet_path:
        print(f"[경고] 입력 데이터 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"

    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")

    if transform_func:
        df = transform_func(df)
        print(f"커스텀 전처리 완료: {len(df):,}행")

    if natural_keys:
        df = _add_surrogate_key(df, natural_keys)
        df.rename(columns={'key': 'id'}, inplace=True)

    output_path = _save_parquet(df, context, prefix=output_xcom_key.replace('_path', ''))
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"전처리: {len(df):,}행"


# ============================================================
# 3. 조인 (Join)
# ============================================================

def join_dataframes(
    left_xcom_key,
    right_xcom_key,
    output_xcom_key,
    join_func,
    **context
):
    """두 DataFrame을 join하는 헬퍼 함수"""
    ti = context['ti']

    left_path = ti.xcom_pull(key=left_xcom_key)
    right_path = ti.xcom_pull(key=right_xcom_key)

    left_df = pd.read_parquet(left_path)
    right_df = pd.read_parquet(right_path)
    print(f"Left: {len(left_df)} rows, Right: {len(right_df)} rows")

    result_df = join_func(left_df, right_df)
    print(f"Result: {len(result_df)} rows")

    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    temp_path = TEMP_DIR / f'{output_xcom_key}_{context["ts_nodash"]}.parquet'
    result_df.to_parquet(temp_path, index=False, engine='pyarrow')
    ti.xcom_push(key=output_xcom_key, value=str(temp_path))
    return str(temp_path)


# ============================================================
# 4. 저장 (Save)
# ============================================================

def save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename=None,
    output_subdir=None,
    dedup_key=None,
    **context
):
    """Parquet 데이터를 로컬 DB에 CSV로 저장"""
    import os
    import shutil
    import tempfile

    ti = context['task_instance']
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)

    if not parquet_path:
        print(f"[경고] 저장할 데이터 없음")
        return "⚠️ 저장 스킵: 데이터 없음"

    if not os.path.exists(parquet_path):
        print(f"[경고] 파일 경로 없음: {parquet_path}")
        return "⚠️ 저장 스킵: 파일 없음"

    df = pd.read_parquet(parquet_path)
    print(f"\n{'='*60}")
    print(f"[입력] 데이터: {len(df):,}행 × {len(df.columns)}컬럼")

    if dedup_key:
        dedup_cols = [dedup_key] if isinstance(dedup_key, str) else dedup_key
        valid_cols = [c for c in dedup_cols if c in df.columns]
        if valid_cols:
            before = len(df)
            df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
            if before - len(df) > 0:
                print(f"\n[중복 제거] {valid_cols} 기준: {before - len(df):,}건 제거")

    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        assert output_subdir is not None, "output_subdir 또는 output_csv_path가 필요합니다"
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename

    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n[경로] 저장 위치: {local_csv_path}")

    datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    for col in datetime_cols:
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, dir=str(local_csv_path.parent),
            prefix='tmp_', suffix='.csv', encoding='utf-8-sig'
        ) as tmp_file:
            tmp_path = tmp_file.name

        df.to_csv(tmp_path, index=False, encoding='utf-8-sig')

        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)
            shutil.move(tmp_path, str(local_csv_path))
            backup_path.unlink()
        else:
            shutil.move(tmp_path, str(local_csv_path))

        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        print(f"[저장] ✅ CSV 저장 완료: {len(df):,}건 ({csv_size:.2f} MB)")

    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"저장 실패: {e}"

    print(f"{'='*60}\n")
    return f"✅ 저장 완료: {len(df):,}건"


def append_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename=None,
    output_subdir=None,
    dedup_key=None,
    **context
):
    """Parquet 데이터를 기존 CSV에 추가 저장 (중복 제거)"""
    import os
    import shutil
    import tempfile

    ti = context['task_instance']
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)

    if not parquet_path or not os.path.exists(parquet_path):
        print(f"[경고] 저장할 데이터 없음")
        return "⚠️ 저장 스킵: 데이터 없음"

    new_df = pd.read_parquet(parquet_path)
    print(f"\n{'='*60}")
    print(f"[입력] 새 데이터: {len(new_df):,}행 × {len(new_df.columns)}컬럼")

    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        assert output_subdir is not None, "output_subdir 또는 output_csv_path가 필요합니다"
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename

    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[경로] 저장 위치: {local_csv_path}")

    if local_csv_path.exists():
        try:
            existing_df = pd.read_csv(local_csv_path, encoding='utf-8-sig')
            print(f"[기존] 데이터: {len(existing_df):,}행 로드")
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            print(f"[경고] 기존 파일 로드 실패: {e}")
            combined_df = new_df
    else:
        print(f"[신규] 기존 파일 없음 → 새 파일 생성")
        combined_df = new_df

    if dedup_key:
        dedup_cols = [dedup_key] if isinstance(dedup_key, str) else dedup_key
        valid_cols = [c for c in dedup_cols if c in combined_df.columns]
        if valid_cols:
            before = len(combined_df)
            combined_df.drop_duplicates(subset=valid_cols, keep='last', inplace=True)
            if before - len(combined_df) > 0:
                print(f"\n[중복 제거] {valid_cols} 기준: {before - len(combined_df):,}건 제거")

    datetime_cols = combined_df.select_dtypes(include=['datetime64']).columns.tolist()
    for col in datetime_cols:
        combined_df[col] = combined_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, dir=str(local_csv_path.parent),
            prefix='tmp_', suffix='.csv', encoding='utf-8-sig'
        ) as tmp_file:
            tmp_path = tmp_file.name

        combined_df.to_csv(tmp_path, index=False, encoding='utf-8-sig')

        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)
            shutil.move(tmp_path, str(local_csv_path))
            backup_path.unlink()
        else:
            shutil.move(tmp_path, str(local_csv_path))

        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        print(f"\n[저장] ✅ CSV 저장 완료: {len(combined_df):,}건 ({csv_size:.2f} MB)")

    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"❌ 저장 실패: {e}"

    print(f"{'='*60}\n")
    return f"✅ 저장 완료: 전체 {len(combined_df):,}건 (신규: {len(new_df):,}건)"


# ============================================================
# 5. 정리 (Cleanup)
# ============================================================

def sales_cleanup_collected_csvs(patterns, dest_dir, source_dir=None, **context):
    """OneDrive 수집 폴더의 CSV 파일들을 지정된 경로로 이동"""
    import glob as glob_module
    import shutil
    from modules.transform.utility.paths import COLLECT_DB

    dest_path = Path(dest_dir)
    source_dirs = []
    if source_dir:
        source_dirs.append(Path(source_dir))
    else:
        source_dirs.append(COLLECT_DB / "영업관리부_수집")
    if COLLECT_DB not in source_dirs:
        source_dirs.append(COLLECT_DB)

    dest_path.mkdir(parents=True, exist_ok=True)
    moved_count = 0

    for pattern in patterns:
        print(f"\n[이동] 패턴: {pattern}")
        for collect_dir in source_dirs:
            files = glob_module.glob(str(collect_dir / pattern))
            if files:
                print(f"   경로: {collect_dir} → 찾은 파일: {len(files)}개")
            for file_path in files:
                try:
                    source = Path(file_path)
                    destination = dest_path / source.name
                    shutil.move(str(source), str(destination))
                    print(f"   ✅ 이동: {source.name} → {destination}")
                    moved_count += 1
                except Exception as e:
                    print(f"   ⚠️  이동 실패: {Path(file_path).name} - {e}")

    print(f"\n✅ 총 {moved_count}개 파일 이동 완료")
    return f"이동 완료: {moved_count}개"


def cleanup_temp_parquets(**context):
    """temp 디렉토리의 모든 parquet 파일 삭제"""
    import shutil

    temp_dirs = [TEMP_DIR]
    summary = []

    for temp_dir in temp_dirs:
        if not temp_dir.exists():
            continue

        parquet_files = list(temp_dir.glob('*.parquet'))
        if not parquet_files:
            continue

        total_size = sum(f.stat().st_size for f in parquet_files) / (1024 * 1024)
        print(f"[정리] 대상: {temp_dir} -> {len(parquet_files)}개 ({total_size:.2f} MB)")

        deleted_count = 0
        for parquet_file in parquet_files:
            try:
                parquet_file.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"[경고] 삭제 실패: {parquet_file.name} - {e}")

        summary.append(f"{temp_dir}: 삭제 {deleted_count}개 ({total_size:.2f} MB)")

    return " | ".join(summary) if summary else "삭제할 파일 없음"


# ============================================================
# 6. 검증 및 알림
# ============================================================

def validate_and_alert_settlement(
    df,
    platform,
    order_col,
    payment_col,
    settlement_col_candidates,
    store_col,
    manager_col=None,
    source_file_col='_source_file',
    recipients=None,
    smtp_conn_id='doridang_conn_smtp_gmail',
    **context
):
    """정산금액 검증: 결제금액은 있는데 정산금액이 없는 경우 이메일 알림"""
    import numpy as np

    print(f"\n{'='*60}")
    print(f"[{platform}] 정산금액 검증 시작")

    settlement_col = None
    for col in settlement_col_candidates:
        if col in df.columns:
            settlement_col = col
            print(f"[정산컬럼] '{col}' 발견")
            break

    if not settlement_col:
        print(f"[경고] 정산금액 컬럼 없음: {settlement_col_candidates}")
        return

    if source_file_col not in df.columns:
        df[source_file_col] = 'UNKNOWN'

    order_date_candidates = ['order_date', '주문일시', 'order_datetime', 'orderDate', '주문시간', 'orderTime']
    order_date_col = next((c for c in order_date_candidates if c in df.columns), None)

    problem_df = df[
        (df[payment_col].notna()) &
        (df[payment_col] > 0) &
        (df[settlement_col].isna() | (df[settlement_col] == 0))
    ].copy()

    if len(problem_df) == 0:
        print(f"[✅] 정산금액 검증 통과: 문제 없음")
        return

    def extract_store_from_filename(file_name: str) -> str:
        if not isinstance(file_name, str):
            return ''
        if '도리당_' in file_name:
            parts = file_name.split('도리당_')
            if len(parts) > 1:
                return '도리당 ' + parts[1].split('_')[0]
        return file_name.split('_')[0] if '_' in file_name else file_name

    group_keys = [source_file_col]
    if store_col in problem_df.columns:
        group_keys.append(store_col)

    manager_tmp_col = None
    if manager_col and (manager_col in problem_df.columns):
        group_keys.append(manager_col)
    else:
        manager_tmp_col = '__manager_fallback__'
        problem_df[manager_tmp_col] = '-'
        group_keys.append(manager_tmp_col)

    file_summary = (
        problem_df
        .groupby(group_keys)
        .agg(주문건수=(order_col, 'count'), 총결제금액=(payment_col, 'sum'))
        .reset_index()
    )
    file_summary['파일명'] = file_summary[source_file_col]

    if store_col in file_summary.columns:
        file_summary['매장명'] = file_summary[store_col].fillna('').map(str)
    else:
        file_summary['매장명'] = file_summary['파일명'].map(extract_store_from_filename)

    if manager_col and (manager_col in file_summary.columns):
        file_summary['담당자'] = file_summary[manager_col].fillna('-').map(str)
    else:
        file_summary['담당자'] = file_summary.get(manager_tmp_col, '-').fillna('-')

    file_summary = file_summary[['매장명', '담당자', '파일명', '주문건수', '총결제금액']].copy()
    print(f"[❌] 정산금액 누락 발견: {len(problem_df):,}건")

    if recipients and len(recipients) > 0:
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(smtp_conn_id)
            assert conn.login and conn.host and conn.port and conn.password
            ds_val = context.get('ds', 'N/A')
            total_payment = problem_df[payment_col].sum()

            html_body = f"""
<html><body>
<h2>[{platform}] 정산금액 누락 알림</h2>
<p>수집일시: {ds_val} | 문제건수: {len(problem_df):,}건 | 총결제금액: {total_payment:,.0f}원</p>
<table border="1" cellpadding="5">
<tr><th>매장명</th><th>담당자</th><th>파일명</th><th>주문건수</th><th>총결제금액</th></tr>
{''.join(f"<tr><td>{r['매장명']}</td><td>{r['담당자']}</td><td>{r['파일명']}</td><td>{r['주문건수']:,}</td><td>{r['총결제금액']:,.0f}원</td></tr>" for _, r in file_summary.iterrows())}
</table>
</body></html>"""

            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[도리당] {platform} 정산금액 누락 알림 ({len(problem_df):,}건)"
            msg['From'] = conn.login
            msg['To'] = ', '.join(recipients)
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

            server = smtplib.SMTP(conn.host, conn.port, timeout=30)
            server.starttls()
            server.login(conn.login, conn.password)
            server.send_message(msg)
            server.quit()
            print(f"\n[📧] 알림 이메일 발송 완료: {recipients}")

        except Exception as e:
            print(f"\n[⚠️] 이메일 발송 실패: {e}")
    else:
        print(f"\n[⚠️] 수신자 없음 - 이메일 발송 스킵")

    print(f"{'='*60}\n")


# ============================================================
# 7. 내부 헬퍼 함수
# ============================================================

def _get_unique_files(file_list):
    """중복 파일 제거 (최신 것만 유지)"""
    unique = {}
    for f in file_list:
        fname = f.name
        if fname not in unique or f.stat().st_mtime > unique[fname].stat().st_mtime:
            unique[fname] = f
    return list(unique.values())


def _load_csv_files(file_paths: list[Path], add_source_filename: bool = True) -> pd.DataFrame:
    """여러 CSV 파일을 로드하여 병합"""
    dataframes = []
    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            if add_source_filename:
                df['_source_file'] = file_path.name
            print(f"  📄 {file_path.name}: {len(df):,}행")
            dataframes.append(df)
        except Exception as e:
            print(f"  ❌ {file_path.name} 로드 실패: {e}")

    if not dataframes:
        raise ValueError("로드된 파일이 없습니다")

    return pd.concat(dataframes, ignore_index=True)


def _load_excel_files(files, header=0, add_source_filename=True):
    """Excel 파일들을 로드하여 병합"""
    dfs = []
    for file in files:
        try:
            df = pd.read_excel(file, header=header)
            if add_source_filename:
                df['_source_file'] = Path(file).name
            print(f"  ✅ {Path(file).name}: {len(df):,}행")
            dfs.append(df)
        except Exception as e:
            print(f"  ⚠️ {Path(file).name} 로드 실패: {e}")

    if not dfs:
        raise ValueError("로드된 Excel 파일이 없습니다")

    return pd.concat(dfs, ignore_index=True)


def _load_parquet_files(file_paths: list[Path], add_source_filename: bool = True) -> pd.DataFrame:
    """여러 Parquet 파일을 로드하여 병합"""
    dataframes = []
    for file_path in file_paths:
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            if add_source_filename:
                df['_source_file'] = file_path.name
            print(f"  📄 {file_path.name}: {len(df):,}행")
            dataframes.append(df)
        except Exception as e:
            print(f"  ❌ {file_path.name} 로드 실패: {e}")

    if not dataframes:
        raise ValueError("로드된 파일이 없습니다")

    return pd.concat(dataframes, ignore_index=True)


def _save_parquet(df, context, prefix):
    """Parquet 저장 (데이터 정리 포함)"""
    df_clean = df.copy()

    unnamed_cols = [col for col in df_clean.columns if 'Unnamed' in str(col)]
    if unnamed_cols:
        df_clean = df_clean.drop(columns=unnamed_cols)

    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].fillna('').astype(str)

    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{prefix}_{context['ds_nodash']}.parquet"

    try:
        df_clean.to_parquet(output_path, index=False, engine='pyarrow')
        print(f"[💾] Parquet 저장 완료: {output_path} ({len(df_clean):,}행)")
    except Exception as e:
        print(f"[⚠️] Parquet 저장 실패: {e}")
        csv_path = output_path.with_suffix('.csv')
        df_clean.to_csv(csv_path, index=False, encoding='utf-8-sig')
        output_path = csv_path

    return output_path


def _add_surrogate_key(df, natural_key_cols):
    """Surrogate Key 생성"""
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)
    df['key'] = uk_series.apply(lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16])
    cols = ['key'] + [c for c in df.columns if c != 'key']
    return df[cols]


# ============================================================
# 8. 호환 함수 (구 io.py에서 병합)
# ============================================================

def read_csv_glob(
    files,
    add_source_col: bool = False,
    source_col_name: str = "source_file",
    ignore_index: bool = True,
    on_error: str = "raise",
    **read_csv_kwargs,
) -> pd.DataFrame:
    """Glob 패턴 또는 파일 경로 리스트로 여러 CSV를 읽어 DataFrame으로 반환"""
    import glob

    if isinstance(files, str):
        file_list = sorted(glob.glob(files))
    elif isinstance(files, list):
        file_list = files
    else:
        raise TypeError("files는 glob 패턴(str) 또는 파일 경로 리스트(list[str])여야 합니다.")

    if not file_list:
        raise FileNotFoundError("매칭되는 CSV 파일이 없습니다.")

    dfs = []
    for fpath in file_list:
        try:
            df = pd.read_csv(fpath, **read_csv_kwargs)
            if add_source_col:
                import os
                df[source_col_name] = os.path.basename(fpath)
            dfs.append(df)
        except Exception as e:
            if on_error == "skip":
                print(f"[경고] 파일 '{fpath}'을(를) 건너뜁니다. 이유: {e}")
                continue
            raise

    return pd.concat(dfs, ignore_index=ignore_index) if dfs else pd.DataFrame()


def load_data(
    file_path,
    xcom_key='parquet_path',
    use_glob=True,
    dedup_key=None,
    add_row_hash=False,
    **context
):
    """CSV 데이터 범용 로드 함수 (XCom 저장)"""
    import glob as glob_module
    import hashlib

    ti = context['task_instance']
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    parquet_path = TEMP_DIR / f"{xcom_key}_{context['ds_nodash']}.parquet"

    try:
        if use_glob:
            file_list = sorted(glob_module.glob(str(file_path)))
        else:
            file_list = [str(file_path)] if Path(file_path).exists() else []

        if not file_list:
            print(f"[경고] 파일 없음: {file_path}")
            ti.xcom_push(key=xcom_key, value=None)
            return "0건 (파일 없음)"

        dfs = []
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']

        for fpath in file_list:
            df = None
            for encoding in encodings:
                try:
                    df = pd.read_csv(fpath, encoding=encoding)
                    if add_row_hash:
                        df['_row_hash'] = df.apply(
                            lambda row: hashlib.md5(str(tuple(row.values)).encode('utf-8')).hexdigest()[:12],
                            axis=1
                        )
                    dfs.append(df)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue

        if not dfs:
            ti.xcom_push(key=xcom_key, value=None)
            return "0건 (파일 읽기 실패)"

        result_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

        if dedup_key:
            dedup_cols = [dedup_key] if isinstance(dedup_key, str) else list(dedup_key)
            valid_cols = [col for col in dedup_cols if col in result_df.columns]
            if valid_cols:
                before = len(result_df)
                result_df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
                if before - len(result_df) > 0:
                    print(f"[중복제거] {before - len(result_df):,}건 제거됨")

        result_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        ti.xcom_push(key=xcom_key, value=str(parquet_path))
        return f"{len(result_df):,}건"

    except Exception as e:
        import traceback
        print(f"[에러] 데이터 로드 실패: {str(e)}")
        print(traceback.format_exc())
        ti.xcom_push(key=xcom_key, value=None)
        return "0건 (에러)"


# 이메일 함수 (mailer.py에서 re-export → 기존 import 호환)
from modules.transform.utility.mailer import text_to_html, send_email


def create_sub_order_id_simple(
    df: pd.DataFrame,
    order_col: str = 'order_id',
    output_col: str = 'sub_order_id'
) -> pd.DataFrame:
    """주문번호 + 순번 방식으로 sub_order_id 생성"""
    df = df.copy()
    df[output_col] = (
        df[order_col].astype(str) + '_' +
        (df.groupby(order_col).cumcount() + 1).astype(str)
    )
    print(f"[sub_order_id] 단순 순번 방식 완료: {df[output_col].nunique():,}개 고유 ID")
    return df


def create_sub_order_id_hash(
    df: pd.DataFrame,
    natural_key_cols: list,
    output_col: str = 'sub_order_id'
) -> pd.DataFrame:
    """해시 기반으로 sub_order_id 생성"""
    df = df.copy()
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)
    df[output_col] = uk_series.apply(
        lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]
    )
    print(f"[sub_order_id] 해시 방식 완료: {df[output_col].nunique():,}개 고유 ID")
    return df


def save_to_csv_with_mapping(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='sales_daily_orders.csv',
    output_subdir='영업관리부_DB',
    dedup_key='sub_order_id',
    mode='append',
    **context
):
    """
    Parquet 데이터를 로컬 DB에 CSV로 저장 (컬럼 매핑 + OneDrive 백업 포함)
    구 save_to_csv (io.py) 호환 버전
    """
    import os
    import shutil
    import tempfile
    import numpy as np

    ti = context['task_instance']

    STANDARD_COLUMNS = [
        'platform', 'order_date', 'store_id', 'store_names',
        'ad_id', 'ad_product', 'order_id', 'sub_order_id',
        'order_summary', 'menu_option_name',
        'delivery_type', 'total_amount', 'menu_amount', 'menu_option_price',
        'instant_discount_coupon', 'instant_discount_coupon_YN',
        'collected_at', 'option_qty', 'fee_ad', 'settlement_amount',
        '담당자', 'email', '상세주소', '광역', '시군구', '읍면동',
        '_row_hash', '실오픈일'
    ]

    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    if not parquet_path:
        return "저장 실패: 데이터 없음"

    new_df = pd.read_parquet(parquet_path)
    print(f"\n[입력] 새 데이터: {len(new_df):,}행")

    # 컬럼 매핑
    if 'collected_at_x' in new_df.columns and 'collected_at' not in new_df.columns:
        new_df.rename(columns={'collected_at_x': 'collected_at'}, inplace=True)
    if 'collected_at_y' in new_df.columns:
        new_df = new_df.drop(columns=['collected_at_y'])
    if 'instant_discount_amount_YN' in new_df.columns and 'instant_discount_coupon_YN' not in new_df.columns:
        new_df.rename(columns={'instant_discount_amount_YN': 'instant_discount_coupon_YN'}, inplace=True)

    temp_cols = ['store_names_key', 'store_name_normalized', 'store_name_original',
                 '매장명_normalized', '매장명_full', 'store_names_match', 'stores']
    for col in temp_cols:
        if col in new_df.columns:
            new_df = new_df.drop(columns=[col])

    # 날짜 형식 통일
    for col in ['order_date', 'collected_at', '실오픈일']:
        if col in new_df.columns and pd.api.types.is_datetime64_any_dtype(new_df[col]):
            fmt = '%Y-%m-%d' if col == '실오픈일' else '%Y-%m-%d %H:%M:%S'
            new_df[col] = new_df[col].dt.strftime(fmt)

    # 컬럼 순서 정렬
    available_cols = [col for col in STANDARD_COLUMNS if col in new_df.columns]
    extra_cols = set(new_df.columns) - set(STANDARD_COLUMNS)
    available_cols.extend(list(extra_cols))
    new_df = new_df[available_cols].copy()

    # 중복 제거 키
    dedup_cols = [dedup_key] if isinstance(dedup_key, str) else list(dedup_key)

    # 출력 경로
    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename

    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[로컬DB] 경로: {local_csv_path}")

    # 기존 CSV 읽기
    existing_df = None
    if mode == 'append' and local_csv_path.exists():
        try:
            for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
                try:
                    existing_df = pd.read_csv(local_csv_path, encoding=encoding, low_memory=False)
                    if 'instant_discount_amount_YN' in existing_df.columns:
                        existing_df.rename(columns={'instant_discount_amount_YN': 'instant_discount_coupon_YN'}, inplace=True)
                    print(f"[기존] {len(existing_df):,}행 로드 ({encoding})")
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
        except Exception as e:
            print(f"[경고] 기존 CSV 읽기 실패: {e}")

    # 데이터 병합
    if existing_df is None or existing_df.empty:
        combined_df = new_df.copy()
    else:
        col_order = [c for c in new_df.columns if c in existing_df.columns]
        existing_df = existing_df[col_order]
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"[병합] 기존 {len(existing_df):,}행 + 신규 {len(new_df):,}행")

    # 중복 제거
    valid_cols = [c for c in dedup_cols if c in combined_df.columns]
    if valid_cols:
        before = len(combined_df)
        combined_df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
        combined_df.drop_duplicates(inplace=True)
        print(f"[중복 제거] 총 제거: {before - len(combined_df):,}건 → {len(combined_df):,}행")

    # CSV 저장
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, dir=str(local_csv_path.parent),
            prefix='tmp_', suffix='.csv', encoding='utf-8-sig'
        ) as tmp_file:
            tmp_path = tmp_file.name

        combined_df.to_csv(tmp_path, index=False, encoding='utf-8-sig')

        backup_path = None
        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)

        shutil.move(tmp_path, str(local_csv_path))

        if backup_path and backup_path.exists():
            backup_path.unlink()

        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        print(f"[저장] ✅ CSV 저장 완료: {len(combined_df):,}건 ({csv_size:.2f} MB)")

        # OneDrive 백업 (실패해도 계속)
        try:
            from modules.transform.utility.paths import ONEDRIVE_DB
            from modules.load.backup_to_onedrive import backup_to_onedrive
            onedrive_csv_path = ONEDRIVE_DB / output_subdir / output_filename
            result = backup_to_onedrive(local_csv_path, onedrive_csv_path)
            if result.get('success'):
                print(f"[백업] ✅ OneDrive 백업 완료")
            else:
                print(f"[백업] ⚠️ OneDrive 백업 실패: {result.get('message')}")
        except Exception as e:
            print(f"[백업] ⚠️ OneDrive 백업 중 예외 발생: {e}")

    except Exception as e:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"저장 실패: {e}"

    return f"✅ 저장 완료: {len(combined_df):,}건"


# ============================================================
# 9. io2 호환 함수 (구 io2.py에서 병합)
# ============================================================

def load_and_concat_csv(file_paths):
    """CSV 파일들을 읽어서 병합"""
    dfs = []
    for fpath in file_paths:
        print(f"   읽는 중: {fpath}")
        df = pd.read_csv(fpath)
        dfs.append(df)
        print(f"   ✓ {len(df)}행 로드")
    result_df = pd.concat(dfs, ignore_index=True)
    print(f"병합 완료: {len(result_df):,}행")
    return result_df


def save_to_parquet(df, context, filename_prefix):
    """DataFrame을 Parquet으로 저장 (공개 버전)"""
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{filename_prefix}_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    return output_path
