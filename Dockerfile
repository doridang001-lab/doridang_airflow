# Airflow 베이스 이미지
FROM apache/airflow:2.10.4

# root로 전환하여 시스템 패키지 설치
USER root

# Chrome 및 필요한 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    xvfb \
    xauth \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && CHROME_VERSION=$(google-chrome --version | awk '{print $3}') \
    && CHROME_MAJOR=${CHROME_VERSION%%.*} \
    && DRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_MAJOR}") \
    && wget -q -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/${DRIVER_VERSION}/linux64/chromedriver-linux64.zip" \
    && unzip /tmp/chromedriver.zip -d /tmp/chromedriver \
    && mv /tmp/chromedriver/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/chromedriver /tmp/chromedriver.zip \
    && rm -rf /var/lib/apt/lists/*

# Chrome 경로 환경변수 설정
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Playwright 브라우저 캐시 경로를 모든 런타임 사용자(root/airflow)에서 공용으로 사용
RUN mkdir -p /ms-playwright \
    && chown -R airflow:0 /ms-playwright \
    && chmod -R 775 /ms-playwright

# airflow 유저로 복귀
USER airflow

# Python 패키지 설치 (requirements.txt 사용)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
