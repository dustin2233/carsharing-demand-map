#!/bin/bash
# ngrok 터널 시작 (고정 도메인) + server.py 실행
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
NGROK_DOMAIN="emil-unemancipated-joya.ngrok-free.dev"

# 기존 프로세스 종료
pkill -f "ngrok http" 2>/dev/null || true
sleep 1

# ngrok 고정 도메인으로 실행
ngrok http 8080 --url "$NGROK_DOMAIN" --log=stdout > /dev/null &
NGROK_PID=$!
echo "ngrok 시작 (PID: $NGROK_PID)"
echo "URL: https://$NGROK_DOMAIN"
echo ""
echo "ngrok 종료: kill $NGROK_PID"
