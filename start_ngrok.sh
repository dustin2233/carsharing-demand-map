#!/bin/bash
# ngrok 터널 시작 (도메인 2개)
# - dustin.ngrok.app          → 실적 대시보드 (/dashboard.html)
# - emil-unemancipated-joya.ngrok-free.dev → 수요 지도 (/gyeonggi/)

DIR="$(cd "$(dirname "$0")" && pwd)"
DOMAIN_DASH="dustin.ngrok.app"
DOMAIN_MAP="emil-unemancipated-joya.ngrok-free.dev"

# 기존 ngrok 프로세스 종료
pkill -f "ngrok http" 2>/dev/null || true
sleep 1

# 대시보드 도메인
ngrok http 8080 --url "$DOMAIN_DASH" --log=stdout > /dev/null &
echo "대시보드: https://$DOMAIN_DASH"

# 지도 도메인
ngrok http 8080 --url "$DOMAIN_MAP" --log=stdout > /dev/null &
echo "수요지도: https://$DOMAIN_MAP/gyeonggi/"

echo "$DOMAIN_DASH" > "$DIR/.ngrok_url"
