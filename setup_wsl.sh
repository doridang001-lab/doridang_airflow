#!/bin/bash
# WSL 전용 가상환경 초기 설정 스크립트
# 실행: bash /mnt/c/airflow/setup_wsl.sh

set -e  # 오류 발생 시 즉시 중단

PROJECT_DIR="/mnt/c/airflow"
VENV_DIR="$PROJECT_DIR/.venv_wsl"

echo "======================================"
echo " WSL 가상환경 초기 설정 시작"
echo "======================================"

# 1. 시스템 패키지 설치 (python3-venv, pip, libpq)
echo ""
echo "[1/5] 시스템 패키지 확인 및 설치..."
sudo apt-get update -q
sudo apt-get install -y -q \
    python3-venv \
    python3-pip \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    gcc

# 2. 가상환경 생성
echo ""
echo "[2/5] 가상환경 생성 → $VENV_DIR"
if [ -d "$VENV_DIR" ]; then
    echo "  ⚠ 이미 존재함. 재생성하려면 rm -rf $VENV_DIR 후 재실행"
else
    python3 -m venv "$VENV_DIR"
    echo "  ✓ 생성 완료"
fi

# 3. pip 업그레이드
echo ""
echo "[3/5] pip 업그레이드..."
"$VENV_DIR/bin/pip" install --upgrade pip --quiet

# 4. requirements.txt 설치
echo ""
echo "[4/5] 패키지 설치 (requirements.txt)..."
"$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt"

# 5. Playwright 브라우저 설치 (playwright가 포함된 경우)
echo ""
echo "[5/5] Playwright 브라우저 설치..."
if "$VENV_DIR/bin/python" -c "import playwright" 2>/dev/null; then
    "$VENV_DIR/bin/playwright" install chromium
    echo "  ✓ Chromium 설치 완료"
else
    echo "  - playwright 미설치 / 스킵"
fi

# 6. ~/.bashrc에 cc alias 등록
echo ""
echo "[6/6] 단축 명령어 'cc' 등록 (~/.bashrc)..."
ALIAS_MARK="# [airflow] cc alias"
if grep -q "$ALIAS_MARK" ~/.bashrc 2>/dev/null; then
    echo "  ⚠ 이미 등록됨. 스킵"
else
    cat >> ~/.bashrc << 'EOF'

# [airflow] cc alias
# cc : airflow 프로젝트로 이동 + venv 활성화 + claude 실행
cc() {
    cd /mnt/c/airflow && source .venv_wsl/bin/activate && claude
}
EOF
    echo "  ✓ 등록 완료"
fi

# ~/.zshrc도 존재하면 동일하게 등록
if [ -f ~/.zshrc ]; then
    if grep -q "$ALIAS_MARK" ~/.zshrc 2>/dev/null; then
        echo "  ⚠ zshrc: 이미 등록됨. 스킵"
    else
        cat >> ~/.zshrc << 'EOF'

# [airflow] cc alias
cc() {
    cd /mnt/c/airflow && source .venv_wsl/bin/activate && claude
}
EOF
        echo "  ✓ zshrc에도 등록 완료"
    fi
fi

echo ""
echo "======================================"
echo " 설정 완료!"
echo "======================================"
echo ""
echo "▶ 지금 바로 적용 (현재 터미널):"
echo "   source ~/.bashrc"
echo ""
echo "▶ 이후로는 WSL 터미널에서 'cc' 만 입력하면:"
echo "   → /mnt/c/airflow 이동"
echo "   → .venv_wsl 활성화"
echo "   → claude 실행"
