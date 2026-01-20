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
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Chrome 경로 환경변수 설정
ENV CHROME_BIN=/usr/bin/google-chrome

# airflow 유저로 복귀
USER airflow

# Python 패키지 설치 (requirements.txt 사용)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
