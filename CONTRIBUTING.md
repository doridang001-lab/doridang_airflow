# 기여 가이드

## 1. 저장소 받기

```bash
git clone https://github.com/doridang001-lab/doridang_airflow.git
cd doridang_airflow
```

## 2. 브랜치 만들기

`main`에 직접 push하지 않습니다. 작업 내용에 맞는 prefix로 브랜치를 만드세요.

| prefix | 용도 |
|--------|------|
| `feat/` | 새 DAG, 새 파이프라인 추가 |
| `fix/`  | 버그 수정 |
| `chore/` | 설정 변경, 리팩터링, 문서 |

```bash
git checkout -b feat/주문집계-수정-0610
```

## 3. 코드 작성 규칙

- **DAG 파일** → `dags/` 폴더 (오케스트레이션만, 비즈니스 로직 금지)
- **파이프라인 로직** → `modules/transform/pipelines/`
- **경로 상수** → `modules/transform/utility/paths.py` 사용 (하드코딩 금지)
- **스케줄 상수** → `modules/transform/utility/schedule.py` 사용
- **로깅** → `logger = logging.getLogger(__name__)` (`print` 금지)

## 4. PR 제출

```bash
git add <변경파일>
git commit -m "feat: 주문집계 수정"
git push origin feat/주문집계-수정-0610
```

GitHub에서 **New Pull Request** → base: `main` ← compare: 내 브랜치 선택 후 제출.

PR 템플릿의 체크리스트를 확인하고 제출하세요.

## 5. 로컬 실행 (WSL)

```bash
# WSL 진입 후
source /mnt/c/airflow/.venv_wsl/bin/activate
docker compose up -d
# DAG 수동 트리거
docker exec airflow-airflow-scheduler-1 airflow dags trigger {DAG_ID}
```
