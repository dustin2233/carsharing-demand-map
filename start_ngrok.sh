#!/bin/bash
# ngrok 터널 시작 + URL 자동 저장 + HTML 재생성
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
NGROK_URL_FILE="$DIR/.ngrok_url"

# 기존 ngrok 종료
pkill -f "ngrok http" 2>/dev/null || true
sleep 1

# ngrok 백그라운드 실행
ngrok http 8080 --log=stdout > /dev/null &
NGROK_PID=$!
echo "ngrok 시작 (PID: $NGROK_PID)..."
sleep 3

# ngrok API에서 public URL 추출
NGROK_URL=$(curl -s http://localhost:4040/api/tunnels | python3 -c "import sys,json; print(json.load(sys.stdin)['tunnels'][0]['public_url'])" 2>/dev/null)

if [ -z "$NGROK_URL" ]; then
    echo "❌ ngrok URL을 가져오지 못했습니다. ngrok 인증이 필요할 수 있습니다."
    echo "   실행: ngrok config add-authtoken <YOUR_TOKEN>"
    kill $NGROK_PID 2>/dev/null
    exit 1
fi

echo "$NGROK_URL" > "$NGROK_URL_FILE"
echo "✅ ngrok URL: $NGROK_URL"
echo "   저장: $NGROK_URL_FILE"

# HTML 재생성 (ngrok URL 반영)
echo "🔄 HTML 재생성 중..."
cd "$DIR"
python3 -c "from update import _build_html; _build_html()"

echo ""
echo "📋 다음 단계:"
echo "   1. server.py가 실행 중인지 확인 (python3 $DIR/server.py)"
echo "   2. git push로 GitHub Pages 배포"
echo "   3. GitHub Pages에서 시뮬레이션 테스트"
echo ""
echo "ngrok 종료: kill $NGROK_PID"
