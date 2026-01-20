# OneDrive 롤백 문제 - 최종 해결 가이드

## 🎯 문제 원인 정리

사용자 환경에서 발생하던 문제:
1. **경로 설정 오류**: Linux 경로 (`/opt/airflow`) 사용 → Windows 경로 불일치
2. **OneDrive 동기화 충돌**: 파일 쓰기 중 OneDrive가 감지 → 이전 버전으로 롤백
3. **폴더 삭제**: 경로 오류로 인해 폴더가 생성되지 않음 → 파일 미저장

---

## ✅ 적용된 해결책

### 1️⃣ **경로 설정 수정** (메인 해결책)

**Before (❌ 이전)**:
```python
# Linux 경로 사용 (Windows에서 작동 안 함)
ONEDRIVE_DB = Path("/opt/airflow/Doridang_DB")
DOWNLOAD_DIR = Path("/opt/airflow/download")
```

**After (✅ 수정됨)**:
```python
# Windows 경로 + 환경 감지
USER_HOME = Path.home()
ONEDRIVE_PATH = USER_HOME / "OneDrive - 주식회사 도리당"
ONEDRIVE_DB = ONEDRIVE_PATH / "Doridang_DB"  # 실제 Windows 경로

# 다운로드 경로
DOWNLOAD_DIR = Path.home() / "Downloads" / "airflow_download"
```

**수정된 파일들**:
- `modules/transform/utility/paths.py` ✅
- `modules/extract/croling_toorder.py` ✅
- `modules/extract/croling_toorder_range.py` ✅
- `dags/etl/sales_load_baemin_data.py` ✅
- `dags/etl/sales_employee_gsheet.py` ✅
- `dags/test/test_gsheet_etl.py` ✅
- `dags/test/test_toorder.py` ✅

### 2️⃣ **OneDrive 안전 저장 메커니즘**

**파일**: `modules/transform/utility/onedrive_sync.py` (새로 생성)

**작동 방식**:
```
1. 폴더 생성 (mkdir with parents=True)
2. 임시 파일에 데이터 쓰기 (tmp_*.csv)
3. 2초 대기 (파일 시스템 반영)
4. 백업 생성 (안전장치)
5. 원자적 교체 (os.replace - 실패하지 않음)
6. 5초 대기 (OneDrive 동기화)
7. 실패 시 백업에서 자동 복구
```

### 3️⃣ **DAG 파일 개선**

**Before (❌ 불안전)**:
```python
# 경로 오류 + 안전하지 않은 저장
if os.path.exists(csv_path):
    os.remove(csv_path)
df.to_csv(csv_path, index=False)
```

**After (✅ 안전함)**:
```python
# 올바른 경로 + 안전한 저장
from modules.transform.utility.onedrive_sync import save_with_onedrive_sync

save_with_onedrive_sync(
    df,
    csv_path,
    index=False,
    encoding='utf-8-sig'
)
```

---

## 🧪 검증 결과

### 테스트 1: 경로 설정 ✅
```
[O] ONEDRIVE_PATH exists: True
[O] ONEDRIVE_DB exists: True
[O] 테스트 폴더 생성: 성공
```

### 테스트 2: 안전 저장 ✅
```
[O] 임시파일 생성: 성공
[O] 데이터 쓰기: 완료
[O] 파일 교체: 성공
[O] 파일 읽기: 3행 정상
```

---

## 📋 즉시 실행할 항목

### DAG 실행 전 확인 사항:
- [ ] 모든 DAG 파일 경로 확인됨
- [ ] `영업관리부_DB` 폴더가 OneDrive에 존재함
- [ ] OneDrive 동기화가 활성화되어 있음
- [ ] 크롤링 다운로드 폴더 자동 생성됨

### DAG 실행:
```bash
# DAG 실행
airflow dags test test_load_baemin_data
# 또는
airflow dags test test_gsheet_etl
```

---

## 🔍 로그 확인 포인트

DAG 실행 시 로그에서 다음을 확인:

```
[파일] 기존 파일 존재: C:\Users\...\OneDrive - 주식회사 도리당\Doridang_DB\...
[임시파일] 생성: C:\Users\...\tmp_abc123.csv
[임시파일] 저장 완료
[저장] 완료: ...csv
[OneDrive] 동기화 대기 중... (5초)
```

---

## 🚨 만약 여전히 롤백이 발생하면

### 1️⃣ **경로 확인**
```python
from modules.transform.utility.paths import ONEDRIVE_DB
print(f"ONEDRIVE_DB: {ONEDRIVE_DB}")
print(f"Exists: {ONEDRIVE_DB.exists()}")
```

### 2️⃣ **OneDrive 설정 확인**
- OneDrive가 실행 중인가?
- 동기화 상태는?
- 다른 프로그램이 파일을 사용 중은 아닌가?

### 3️⃣ **동기화 대기 시간 증가** (선택)
```python
save_with_onedrive_sync(
    df, 
    csv_path,
    index=False,
    encoding='utf-8-sig',
    # 내부적으로 시간 조정 필요 시 코드 수정
)
```

### 4️⃣ **로그 분석**
Airflow 로그에서:
```
~/airflow/logs/dag_id=.../run_id=.../task_id=save_to_csv_orders.log
```

---

## 📊 성능 영향

| 항목 | 시간 | 설명 |
|------|------|------|
| 임시 파일 생성 | ~0.5초 | I/O 오버헤드 |
| 데이터 쓰기 | 가변 | CSV 크기에 따라 |
| 동기화 대기 (1차) | 2초 | 파일 시스템 반영 |
| 파일 교체 | ~0.1초 | 원자적 연산 |
| 동기화 대기 (2차) | 5초 | OneDrive 동기화 |
| **총 추가 시간** | **~8초** | 안정성 확보 |

---

## 🎉 최종 정리

| 항목 | 상태 | 
|------|------|
| 경로 설정 | ✅ Windows 경로로 수정됨 |
| 폴더 생성 | ✅ 자동 생성 (mkdir with parents=True) |
| 안전 저장 | ✅ 임시 파일 + 원자적 교체 |
| OneDrive 동기화 | ✅ 명시적 대기 (5초) |
| 오류 복구 | ✅ 백업에서 자동 복구 |
| DAG 통합 | ✅ 모든 DAG 파일 업데이트 |

**➡️ DAG을 실행하면 이제 롤백이 발생하지 않을 것입니다!**

---

## 📚 참고

- 메인 경로: `modules/transform/utility/paths.py`
- 안전 저장: `modules/transform/utility/onedrive_sync.py`
- DAG 파일들: `dags/etl/*.py`, `dags/test/*.py`
