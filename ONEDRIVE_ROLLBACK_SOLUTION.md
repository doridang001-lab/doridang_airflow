# OneDrive 롤백 문제 해결 가이드

## 🔍 문제 분석

### Airflow ↔ OneDrive 충돌 메커니즘

```
[Airflow DAG 실행]
       ↓
[CSV 파일 쓰기 시작]
       ↓
[OneDrive가 쓰기 중인 파일 감지] ← ❌ 문제 발생!
       ↓
[이전 버전으로 업로드/덮어쓰기]
       ↓
[새로운 데이터가 롤백됨]
```

### 핵심 원인
1. **OneDrive 실시간 동기화**: 파일 시스템의 모든 변경을 즉시 감지
2. **버전 충돌**: 아직 저장 중인 파일을 "불완전한 버전"으로 판단
3. **비동기 처리**: Airflow 저장 ≠ OneDrive 동기화 완료 시간

---

## ✅ 해결 방법 (3가지 레벨)

### **Level 1: 현재 적용된 방식 (기본)**
- ✅ **임시 파일 → 원자적 교체** (`shutil.move` / `os.replace`)
- ✅ **기존 코드에서 이미 구현됨**

**문제점**: 임시 파일도 OneDrive가 감지할 수 있음

---

### **Level 2: 개선된 방식 (권장) ⭐**
**파일: `modules/transform/utility/io.py` 수정됨**

추가된 기능:
```python
✅ OneDrive 파일 접근 가능 대기 (재시도 로직)
✅ 임시 파일 생성 → 데이터 쓰기
✅ 동기화 대기 (2초)
✅ 백업 생성 (안전장치)
✅ 원자적 교체 (Windows os.replace() 사용)
✅ 추가 동기화 대기 (5초)
✅ 실패 시 백업에서 자동 복구
```

**사용 방법**:
```python
# 현재 DAG에서 자동으로 적용됨
# modules/transform/utility/io.py의 save_to_csv() 함수가 개선됨
```

---

### **Level 3: 고급 방식 (선택사항)**
**파일: `modules/transform/utility/onedrive_sync.py` 새로 생성됨**

`OneDriveSafeWriter` 클래스 사용:

```python
from modules.transform.utility.onedrive_sync import OneDriveSafeWriter
from pathlib import Path

csv_path = Path('/opt/airflow/Doridang_DB/영업팀_DB/sales.csv')

with OneDriveSafeWriter(csv_path) as writer:
    writer.safe_write(
        df.to_csv,
        index=False,
        encoding='utf-8-sig'
    )
```

**추가 기능**:
- OneDrive 임시 파일 감지 (`.~`, `~$`, `.lock` 등)
- Context manager 지원
- 커스텀 재시도 정책
- 자세한 로깅

---

## 📋 다른 DAG에 적용하기

### 1️⃣ **`save_to_csv()` 함수 사용 중인 DAG**
자동으로 Level 2가 적용됨 ✅

```python
# 수정 없이 그대로 사용 가능
save_to_csv_task = PythonOperator(
    task_id='save_to_csv_orders',
    python_callable=save_to_csv,
    op_kwargs={
        'input_task_id': 'previous_task',
        'input_xcom_key': 'data_key',
        'output_csv_path': '/path/to/file.csv',
        'mode': 'append'
    },
    dag=dag,
)
```

### 2️⃣ **직접 `to_csv()` 사용 중인 경우**
Level 3 적용:

```python
# Before (❌ OneDrive 롤백 위험)
df.to_csv(csv_path, index=False)

# After (✅ 안전함)
from modules.transform.utility.onedrive_sync import save_with_onedrive_sync

save_with_onedrive_sync(
    df,
    csv_path,
    index=False,
    encoding='utf-8-sig'
)
```

### 3️⃣ **더 세밀한 제어가 필요한 경우**

```python
from modules.transform.utility.onedrive_sync import OneDriveSafeWriter

csv_path = Path('/opt/airflow/Doridang_DB/sales.csv')

with OneDriveSafeWriter(csv_path, max_retries=15, retry_delay=3) as writer:
    # 재시도 설정 커스터마이징
    writer.safe_write(df.to_csv, index=False, encoding='utf-8-sig')
```

---

## 🔧 추가 권장사항

### 1. **Airflow 설정 최적화**
`airflow.cfg`에서:
```ini
# 태스크 재시도 정책 조정
max_tries = 2
retry_delay = 300  # 5분

# 병렬 DAG 실행 제한 (OneDrive 부하 감소)
parallelism = 2
dag_concurrency = 2
```

### 2. **OneDrive 설정 조정 (로컬 PC)**
1. OneDrive 설정 → 네트워크
2. **"대역폭 제한 설정"** 활성화
3. **"업로드 속도"** 제한 (필요시)

### 3. **폴더 구조 최적화**
```
Doridang_DB/
├── 영업팀_DB/           # ✅ OneDrive 동기화 중
│   └── sales_daily_orders.csv
├── 임시_작업/           # ⚠️ OneDrive 동기화 제외 (선택사항)
│   └── temp_files/
└── 로그/
    └── airflow_logs.txt
```

OneDrive 동기화 제외 폴더 설정:
- OneDrive 설정 → 계정 → "이 PC 관리" 
- 제외할 폴더 선택

### 4. **모니터링**
DAG 로그에서 다음을 확인:
```
[OneDrive] 파일 접근 대기 1/10 (2초)...
[임시파일] 저장 완료
[OneDrive] 동기화 대기 중... (5초)
[저장] 완료: /path/to/file.csv
```

---

## 🚨 여전히 문제가 있는 경우

### 증상: 파일이 여전히 롤백됨

**원인별 해결책**:

| 원인 | 해결책 |
|------|------|
| OneDrive 임시 폴더 위치가 같음 | Airflow 임시 폴더를 OneDrive 제외 경로로 이동 |
| 재시도 횟수 부족 | `max_retries` 값 증가 (기본값: 10) |
| 동기화 대기 시간 부족 | 대기 시간 연장 (현재: 5초 → 10초) |
| OneDrive 대역폭 제한 | PC의 OneDrive 설정에서 업로드 속도 확인 |
| CSV 파일이 다른 프로세스에서 사용 중 | Excel/Python 등 다른 곳에서 열려있지 않은지 확인 |

---

## 📊 성능 영향

| 단계 | 대기시간 | 목적 |
|------|--------|------|
| 임시 파일 생성 | ~0.5초 | 쓰기 시작 전 대기 |
| CSV 저장 | 데이터 크기 의존 | 실제 쓰기 |
| 동기화 대기 (1차) | 2초 | 파일 시스템 반영 |
| 백업 생성 | ~0.5초 | 안전장치 |
| 파일 교체 | ~0.1초 | 원자적 연산 |
| 동기화 대기 (2차) | 5초 | OneDrive 동기화 시간 |

**총 추가 시간: ~8초** (데이터 크기에 따라 변동)

---

## 📝 체크리스트

- [ ] 현재 DAG이 `save_to_csv()` 함수 사용 여부 확인
- [ ] 직접 `to_csv()` 사용 중이면 `save_with_onedrive_sync()` 적용
- [ ] DAG 테스트 실행
- [ ] 로그에서 "OneDrive 동기화" 메시지 확인
- [ ] 여러 번 반복 실행해서 안정성 검증
- [ ] 다른 DAG도 같은 패턴 적용

---

## 📚 참고

- `modules/transform/utility/io.py`: 메인 `save_to_csv()` 개선 코드
- `modules/transform/utility/onedrive_sync.py`: 고급 유틸리티 클래스
