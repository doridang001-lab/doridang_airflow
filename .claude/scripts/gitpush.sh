#!/bin/bash
# Git push automation script

echo "📋 Git Status:"
git status

echo -e "\n✅ Staging all changes..."
git add .

echo -e "\n📝 Creating commit..."
CHANGED_FILES=$(git diff --cached --name-only)
SUMMARY=$(echo "$CHANGED_FILES" | head -3 | paste -sd, -)

# 커밋 메시지 생성
if [ -z "$CHANGED_FILES" ]; then
    echo "No changes to commit"
    exit 0
fi

# 변경 타입 판단
if echo "$CHANGED_FILES" | grep -q "^requirements"; then
    COMMIT_MSG="chore: update dependencies"
elif echo "$CHANGED_FILES" | grep -q "^dags/"; then
    COMMIT_MSG="feat: update DAG configurations"
elif echo "$CHANGED_FILES" | grep -q "^modules/"; then
    COMMIT_MSG="feat: update modules"
else
    COMMIT_MSG="chore: update files ($SUMMARY)"
fi

echo "💬 Commit message: $COMMIT_MSG"
git commit -m "$COMMIT_MSG"

echo -e "\n🚀 Pushing to remote..."
git push

echo -e "\n✨ Done!"
