# Playwright 브라우저 설치/재발 방지

## 증상

Airflow 로그에 아래와 같은 메시지가 나오며 실패하는 경우:

- `BrowserType.launch: Executable doesn't exist at /ms-playwright/...`
- `Please run the following command to download new browsers: playwright install`

이는 **Python 패키지로 설치된 `playwright` 버전과 브라우저 바이너리(/ms-playwright)가 불일치**하거나, 브라우저가 아직 설치되지 않은 상태에서 발생합니다.

## 재발 방지(기본값)

- `docker-compose.yaml` 에서 `PLAYWRIGHT_AUTO_INSTALL=1` 로 설정되어 있어, 실행 중 브라우저가 없으면 자동으로 `python -m playwright install chromium` 를 1회 시도합니다.
- `Dockerfile` 에서 `/ms-playwright` 디렉터리를 `airflow` 유저가 쓸 수 있도록 권한을 설정합니다.

## 수동 복구(권장: 이미지 재빌드)

1) 이미지 재빌드/재기동:

```bash
docker compose build --no-cache
docker compose up -d
```

2) (긴급) 실행 중 컨테이너에 직접 설치:

```bash
docker compose exec airflow-worker bash -lc "python -m playwright install chromium"
```

## 설정 참고

- `PLAYWRIGHT_BROWSERS_PATH=/ms-playwright` (브라우저 설치 경로)
- `PLAYWRIGHT_AUTO_INSTALL=1` (브라우저 누락 시 자동 설치)

