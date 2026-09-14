# 팀원 Airflow clone 세팅 가이드

이 가이드는 팀원이 Git으로 Airflow repo를 받은 뒤 개인 PC 경로만 설정해서 실행하는 절차입니다.  
개인 경로 때문에 `docker-compose.yaml`을 수정하지 않습니다. 수정하는 파일은 `.env` 하나입니다.

## 1. 준비물
- Docker Desktop 실행 가능 상태
- Git 설치
- 회사 OneDrive 동기화 완료
- Google Service Account JSON 등 민감 설정 파일은 담당자에게 별도 전달받기

## 2. repo 받기
모델 submodule이 필요 없으면 아래처럼 받습니다.

```powershell
git clone <repo-url> --no-recurse-submodules
cd airflow
```

이미 clone했다면 최신 상태만 받습니다.

```powershell
git pull
```

## 3. 개인 설정 파일 만들기
`.env.example`을 `.env`로 복사합니다.

```powershell
copy .env.example .env
notepad .env
```

`.env`에서 최소 `ONEDRIVE_ROOT`만 본인 PC 경로로 바꿉니다.

```env
ONEDRIVE_ROOT=c:/Users/팀원계정/OneDrive - 주식회사 도리당
LOCAL_DB_ROOT=c:/Local_DB
DORIDANG_ROOT=c:/Doridang
DOWN_DIR=e:/down
D_DOWN_DIR=e:/d_down
```

`ONEDRIVE_ROOT` 예시는 아래와 같습니다.

```env
ONEDRIVE_ROOT=c:/Users/hong/OneDrive - 주식회사 도리당
```

배민 매크로를 두 PC에서 나눠 수집하면 각 PC의 `.env`에 역할을 하나만 지정합니다.

```env
# 중앙 PC(상위 절반 수집 및 Upload DAG 실행)
BAEMIN_MACRO_ROLE=top

# 두 번째 PC(하위 절반 수집, 위 설정 대신 사용)
BAEMIN_MACRO_ROLE=bottom
```

역할값은 `top` 또는 `bottom`을 권장합니다. 역할을 바꾼 뒤에는 Airflow 컨테이너를
재생성하고 scheduler와 worker에 값이 전달됐는지 확인합니다.

```powershell
docker compose up -d
docker compose exec airflow-scheduler printenv BAEMIN_MACRO_ROLE
docker compose exec airflow-worker printenv BAEMIN_MACRO_ROLE
```

두 PC 모두 `DB_Beamin_Macro_Dags`를 실행하지만,
`DB_Beamin_Macro_Upload_Dags`는 중앙 PC에서만 활성화합니다.

## 4. 로컬 폴더 만들기
기본값을 그대로 쓸 경우 아래 폴더가 있어야 합니다.

```powershell
mkdir c:\Local_DB
mkdir c:\Doridang
mkdir e:\down
mkdir e:\d_down
```

E 드라이브가 없으면 `.env`에서 `DOWN_DIR`, `D_DOWN_DIR`을 실제 폴더로 바꿉니다.

```env
DOWN_DIR=c:/down
D_DOWN_DIR=c:/d_down
```

## 5. 민감 파일 배치
담당자에게 전달받은 JSON/key 파일을 정해진 위치에 둡니다.

```text
config/
keys/
```

이 파일들은 Git에 올리지 않습니다.

## 6. 실행
WSL 세팅이 필요한 경우 먼저 실행합니다.

```bash
bash /mnt/c/airflow/setup_wsl.sh
```

Airflow를 실행합니다.

```powershell
docker compose up -d
docker compose ps
```

웹 UI:

```text
http://localhost:8080
id: airflow
pw: airflow
```

## 7. 확인 방법
개인 경로가 제대로 들어갔는지 확인합니다.

```powershell
docker compose config | findstr /I "OneDrive Repository Collect_Data"
```

`docker compose ps`에서 주요 서비스가 `healthy`로 보이면 기본 실행 준비는 완료입니다.

## 주의
- `docker-compose.yaml`은 수정하지 않습니다.
- `.env`는 개인 PC 전용 파일이며 Git에 올리지 않습니다.
- pull 후 충돌이 나면 `.env`가 아니라 Git 추적 파일을 수정했는지 먼저 확인합니다.
