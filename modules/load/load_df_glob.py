def load_data(
    file_path,
    xcom_key='parquet_path',
    use_glob=True,
    dedup_key=None,
    add_row_hash=False,
    add_source_info=False,
    source_info_extractor=None,
    excel_header=0,  # ⭐ 새 파라미터: Excel 헤더 행 위치
    **context
):
    """
    CSV/Excel 데이터 범용 로드 함수
    
    Args:
        file_path: 파일 경로 또는 glob 패턴
        xcom_key: XCom에 저장할 키 이름
        use_glob: True면 glob 패턴으로 처리
        dedup_key: 중복 제거 기준 컬럼
        add_row_hash: True면 각 행에 고유 해시 추가
        add_source_info: True면 파일명에서 정보 추출하여 컬럼 추가
        source_info_extractor: 파일명 → dict 변환 함수
        excel_header: Excel 파일의 헤더 행 위치 (0-based, 기본값 0)
    """
    import glob as glob_module
    import hashlib
    from pathlib import Path
    import pandas as pd
    from modules.transform.utility.paths import resolve_temp_dir
    
    ti = context['task_instance']
    
    temp_dir = resolve_temp_dir()
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    parquet_path = temp_dir / f"{xcom_key}_{context['ds_nodash']}.parquet"
    print(f"[DEBUG] temp_dir: {temp_dir}")
    print(f"[DEBUG] parquet_path: {parquet_path}")
    print(f"[DEBUG] temp_dir 존재: {temp_dir.exists()}")
    
    try:
        # 1. 파일 목록 가져오기
        if use_glob:
            file_list = sorted(glob_module.glob(str(file_path)))
            print(f"[DEBUG] glob 패턴: {file_path}")
            print(f"[DEBUG] 찾은 파일: {len(file_list)}개")
        else:
            # ⭐ file_path가 리스트인 경우 처리
            if isinstance(file_path, (list, tuple)):
                # 파일 리스트가 직접 전달된 경우
                file_list = [str(f) for f in file_path if Path(f).exists()]
                print(f"[DEBUG] 직접 전달된 파일 목록: {len(file_path)}개 → 존재하는 파일 {len(file_list)}개")
            else:
                # 단일 파일 경로 전달
                file_list = [str(file_path)] if Path(file_path).exists() else []
        
        if not file_list:
            print(f"[경고] 파일 없음: {file_path}")
            ti.xcom_push(key=xcom_key, value=None)
            return f"0건 (파일 없음)"
        
        print(f"[INFO] 로드할 파일: {len(file_list)}개")
        
        # 2. 각 파일 읽기
        dfs = []
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
        
        for file_idx, fpath in enumerate(file_list):
            file_name = Path(fpath).name
            print(f"   처리중: {file_name}")
            
            df = None
            for encoding in encodings:
                try:
                    # Excel 또는 CSV 읽기
                    if fpath.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(fpath, header=excel_header)  # ⭐ 동적 header
                        print(f"   ✓ Excel 읽음: {len(df)}행 (header={excel_header})")
                    else:
                        df = pd.read_csv(fpath, encoding=encoding)
                        print(f"   ✓ {encoding}으로 읽음: {len(df)}행")
                    
                    # ⭐ 병합 전에 파일명 기반 컬럼 추가
                    if add_source_info:
                        if source_info_extractor:
                            # 커스텀 추출 함수 사용
                            info_dict = source_info_extractor(file_name)
                            for col_name, col_value in info_dict.items():
                                df[col_name] = col_value
                                print(f"   [추가] {col_name} = {col_value}")
                        else:
                            # 기본: 파일명 그대로 저장
                            df['_source_file'] = file_name
                    
                    # ⭐ 행 해시 추가 (옵션)
                    if add_row_hash:
                        df['_row_hash'] = df.apply(
                            lambda row: hashlib.md5(
                                str(tuple(row.values)).encode('utf-8')
                            ).hexdigest()[:12],
                            axis=1
                        )
                        print(f"   [해시] {df['_row_hash'].nunique()}개 고유값")
                    
                    dfs.append(df)
                    break
                    
                except (UnicodeDecodeError, LookupError):
                    continue
                except Exception as e:
                    print(f"   ✗ 읽기 실패: {str(e)}")
                    continue
            
            if df is None:
                print(f"   ✗ 읽기 실패: 모든 인코딩 실패")
        
        if not dfs:
            print(f"[경고] 읽을 수 있는 파일 없음")
            ti.xcom_push(key=xcom_key, value=None)
            return f"0건 (파일 읽기 실패)"
        
        # 3. 병합 (이제 파일 출처 정보 포함됨)
        result_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        print(f"\n병합 완료: {len(result_df):,}행")
        print(f"컬럼: {list(result_df.columns)}")
        
        # 4. 중복 제거
        if dedup_key:
            before = len(result_df)
            
            if isinstance(dedup_key, str):
                dedup_cols = [dedup_key]
            else:
                dedup_cols = list(dedup_key)
            
            valid_cols = [col for col in dedup_cols if col in result_df.columns]
            
            if valid_cols:
                print(f"[중복제거] 키: {valid_cols}")
                result_df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
                after = len(result_df)
                removed = before - after
                if removed > 0:
                    print(f"[중복제거] {removed:,}건 제거됨")
        
        # 5. Parquet 저장
        for col in result_df.columns:
            if result_df[col].dtype == 'object':
                if col in ['캠페인ID', 'ad_id', 'store_id']:
                    result_df[col] = result_df[col].astype(str)
        
        result_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        print(f"\n✅ 데이터 로드 완료: {len(result_df):,}건")
        print(f"   저장 경로: {parquet_path}")
        
        ti.xcom_push(key=xcom_key, value=str(parquet_path))
        return f"{len(result_df):,}건"
        
    except Exception as e:
        print(f"[에러] 데이터 로드 실패: {str(e)}")
        import traceback
        print(traceback.format_exc())
        ti.xcom_push(key=xcom_key, value=None)
        return f"0건 (에러)"


# ============================================================
# 수집된 CSV 파일 정리 및 업로드 함수
# ============================================================

def cleanup_collected_csvs(patterns, dest_dir, **context):
    """
    OneDrive 수집 폴더의 CSV 파일들을 지정된 경로로 이동
    
    Args:
        patterns: 이동할 파일 패턴 리스트 (예: ['baemin_*.csv'])
        dest_dir: 목적지 디렉토리 (컨테이너 경로)
    """
    import glob as glob_module
    import shutil
    from pathlib import Path
    from modules.transform.utility.paths import COLLECT_DB
    
    collect_dir = COLLECT_DB / "영업관리부_수집"
    dest_path = Path(dest_dir)
    
    # 목적지 디렉토리 생성
    dest_path.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    for pattern in patterns:
        file_pattern = str(collect_dir / pattern)
        files = glob_module.glob(file_pattern)
        
        print(f"\n[이동] 패턴: {pattern}")
        print(f"   찾은 파일: {len(files)}개")
        
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


def move_download_files(patterns, dest_dir, base_dir="/opt/airflow/download", **context):
    """일반 다운로드 폴더에서 파일 이동 (toorder 엑셀 등)

    Args:
        patterns: 이동할 파일 패턴 리스트
        dest_dir: 목적지 디렉토리 (컨테이너 경로)
        base_dir: 검색할 기본 디렉토리 (기본: /opt/airflow/download)
    """
    import glob as glob_module
    import shutil
    from pathlib import Path

    base_path = Path(base_dir)
    dest_path = Path(dest_dir)

    dest_path.mkdir(parents=True, exist_ok=True)

    moved_count = 0
    for pattern in patterns:
        file_pattern = str(base_path / pattern)
        files = glob_module.glob(file_pattern)

        print(f"\n[이동] 패턴: {pattern} (base: {base_path})")
        print(f"   찾은 파일: {len(files)}개")

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


def upload_final_csv(source_filename, source_subdir, dest_dir, **context):
    """
    로컬 DB의 CSV를 지정된 경로로 복사
    
    Args:
        source_filename: 원본 파일명
        source_subdir: 원본 하위 디렉토리
        dest_dir: 목적지 디렉토리
    """
    import shutil
    from pathlib import Path
    from modules.transform.utility.paths import LOCAL_DB
    
    source_path = LOCAL_DB / source_subdir / source_filename
    dest_path = Path(dest_dir) / source_filename
    
    print(f"\n[업로드] 파일 복사")
    print(f"   원본: {source_path}")
    print(f"   목적지: {dest_path}")
    
    if not source_path.exists():
        print(f"   ❌ 원본 파일 없음")
        return "업로드 실패: 원본 파일 없음"
    
    # 목적지 디렉토리 생성
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        shutil.copy2(source_path, dest_path)
        print(f"   ✅ 복사 완료")
        return f"업로드 완료: {dest_path}"
    except Exception as e:
        print(f"   ❌ 복사 실패: {e}")
        return f"업로드 실패: {e}"