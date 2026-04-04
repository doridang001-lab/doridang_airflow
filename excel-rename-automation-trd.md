# Excel Rename Automation — TRD

## 1. 시스템 아키텍처

단일 Python 스크립트(`rename.py`) 구조로 외부 의존성 없이 표준 라이브러리만 사용한다.

```
rename.py
├── CLI 진입점 (main)
│   ├── argparse: --folder, --date-source, --dry-run
│   └── config.json 로드 (선택)
├── Scanner: 폴더 스캔 → FileInfo 목록 반환
├── Planner: FileInfo → Renameplan 목록 생성 (날짜 접두어 계산, 충돌 감지)
├── Previewer: Renameplan 테이블 출력 + 사용자 확인
└── Executor: os.rename() 일괄 실행 + 결과 로그
```

실행 흐름: `입력 → 스캔 → 계획 → 미리보기 확인 → 실행 → 결과 출력`

---

## 2. 기술 스택

- **언어**: Python 3.8+
- **표준 라이브러리만 사용**:
  - `pathlib.Path` — 경로 처리 (Windows/macOS 호환)
  - `os` — 파일 이름 변경 (`os.rename`)
  - `datetime` — 오늘 날짜, 파일 수정일(mtime) 변환
  - `argparse` — CLI 인자 파싱
  - `json` — config.json 로드
  - `re` — 날짜 접두어 패턴 감지 (`^\d{8}_`)
- **외부 라이브러리**: 없음 (pip install 불필요)

---

## 3. API 설계 (모듈 인터페이스)

### `scan_files(folder: Path) -> list[FileInfo]`
```python
@dataclass
class FileInfo:
    path: Path          # 절대 경로
    name: str           # 파일명 (확장자 포함)
    stem: str           # 확장자 제외 이름
    mtime: datetime     # 파일 수정 시각
    already_dated: bool # 날짜 접두어 이미 존재 여부
```

### `build_plan(files: list[FileInfo], date_source: str) -> list[RenamePlan]`
```python
@dataclass
class RenamePlan:
    original: Path
    new_name: str
    new_path: Path
    conflict: bool   # 동일 이름 파일이 이미 존재하는지
    skip: bool       # already_dated=True인 경우 skip=True
```
- `date_source="today"` → `datetime.now().strftime("%Y%m%d")`
- `date_source="mtime"` → `file.mtime.strftime("%Y%m%d")`

### `preview(plans: list[RenamePlan]) -> bool`
터미널에 변경 전/후 테이블 출력 후 `y/n` 입력 받아 `bool` 반환.

### `execute(plans: list[RenamePlan]) -> dict`
`conflict=True` 또는 `skip=True` 항목 제외 후 `os.rename()` 실행. 결과 딕셔너리 반환.

---

## 4. 데이터 모델

### config.json 스키마
```json
{
  "target_folder": "C:/reports",
  "date_source": "today",
  "skip_dated": true
}
```

| 필드 | 타입 | 기본값 | 설명 |
|------|------|--------|------|
| target_folder | string | cwd | 스캔 대상 폴더 경로 |
| date_source | "today"\|"mtime" | "today" | 날짜 접두어 기준 |
| skip_dated | bool | true | 이미 날짜 있는 파일 건너뜀 |

### 실행 결과 구조
```python
{
  "success": [str],   # 성공한 파일 목록
  "skipped": [str],   # 건너뛴 파일 목록
  "failed":  [str],   # 실패한 파일 목록
}
```

---

## 5. 보안 고려사항

- **원본 덮어쓰기 방식** — 복사본 생성 없이 `os.rename()` 사용. 실행 전 Preview 확인 필수.
- **권한 오류 처리** — `PermissionError` 예외 catch 후 해당 파일 건너뛰기, 오류 목록 출력.
- **경로 인젝션 방지** — `pathlib.Path.resolve()`로 상대경로/심볼릭링크 정규화.
- **읽기 전용 파일 감지** — `os.access(path, os.W_OK)` 사전 체크.

---

## 6. 성능

- 파일 100개 기준 실행 시간 목표: **3초 이내**
- 스캔은 `Path.iterdir()` 단일 패스 (O(n))
- `os.rename()`은 동일 드라이브 내에서 메타데이터 변경만 발생 (파일 복사 없음, 매우 빠름)
- 메모리: `FileInfo` 리스트는 파일명 문자열만 보관 (파일 내용 미로드)

---

## 7. 구현 단계

### Phase 1 — 핵심 기능 (MVP)
1. `scan_files()` 구현 — `.xlsx` 필터링, `already_dated` 감지
2. `build_plan()` 구현 — `today` 모드, 충돌 감지
3. `preview()` 구현 — 테이블 출력, y/n 입력
4. `execute()` 구현 — `os.rename()`, 결과 출력
5. `main()` + argparse (`--folder`) 연결

### Phase 2 — 옵션 확장
6. `--date-source mtime` 옵션 지원
7. `config.json` 로드 지원
8. `--dry-run` 플래그 (실행 없이 Preview만)

### Phase 3 — 품질
9. 단위 테스트 작성 (pytest, tmp_path 활용)
10. 결과 로그 파일 저장 옵션 (`--log`)

---

## 8. 기능별 기술 노트

### R-SCANA1 파일 스캔
```python
def scan_files(folder: Path) -> list[FileInfo]:
    DATE_PREFIX = re.compile(r'^\d{8}_')
    return [
        FileInfo(
            path=f,
            name=f.name,
            stem=f.stem,
            mtime=datetime.fromtimestamp(f.stat().st_mtime),
            already_dated=bool(DATE_PREFIX.match(f.name))
        )
        for f in folder.iterdir()
        if f.suffix.lower() == '.xlsx' and f.is_file()
    ]
```

### R-PREVC2 Preview 출력
- `str.ljust()`로 컬럼 정렬, 터미널 너비 80자 기준
- `conflict=True` 항목은 `[충돌 경고]` 표시, 빨간색 ANSI 코드 적용 (지원 시)
- `skip=True` 항목은 `[건너뜀]` 표시 후 제외

### R-RENAME4 일괄 변경
- `os.rename(src, dst)` — 동일 드라이브라면 atomic rename
- 실패 시 `failed` 목록에 추가하고 계속 진행 (전체 중단 없음)

### R-DUPCHK5 중복 감지
- `build_plan()` 단계에서 `new_path.exists()` 체크
- 동일 `new_name`이 계획 내에서 2개 이상 생성될 경우(같은 stem 파일) 양쪽 모두 `conflict=True`
