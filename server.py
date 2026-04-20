#!/usr/bin/env python3
"""
경기도 카셰어링 잠재 수요 지도 — 로컬 서버
실행: python3 ~/carsharing_demand_map/server.py
접속: http://localhost:8080
"""

import http.server
import json
import os
import subprocess
from datetime import datetime

PORT = 8080
DIR = os.path.dirname(os.path.abspath(__file__))
LAST_UPDATE_DEMAND = os.path.join(DIR, '.last_update_demand')
LAST_UPDATE_ZONE = os.path.join(DIR, '.last_update_zone')
API_CONFIG_PATH = os.path.join(DIR, '.api_config.json')


def get_api_config():
    try:
        with open(API_CONFIG_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def get_last_update(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def deploy_to_github():
    """index.html을 GitHub Pages에 배포"""
    try:
        subprocess.run(['git', 'add', 'index.html', '*/index.html'], cwd=DIR, capture_output=True, timeout=10)
        subprocess.run(['git', 'commit', '-m', 'update'], cwd=DIR, capture_output=True, timeout=10)
        subprocess.run(['git', 'push'], cwd=DIR, capture_output=True, timeout=30)
        print(f"[{datetime.now()}] GitHub Pages 배포 완료")
    except Exception as e:
        print(f"[{datetime.now()}] GitHub Pages 배포 실패: {e}")


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def do_POST(self):
        if self.path == '/api/update-demand':
            self.handle_update_demand()
        elif self.path == '/api/update-zone':
            self.handle_update_zone()
        elif self.path == '/api/update':
            self.handle_update_demand()
        elif self.path == '/api/simulate':
            self.handle_simulate()
        elif self.path == '/api/simulate-eval':
            self.handle_simulate_eval()
        elif self.path == '/api/d2d-destinations':
            self.handle_d2d_destinations()
        elif self.path == '/api/dashboard-analyze':
            self.handle_dashboard_analyze()
        elif self.path == '/api/dashboard-region3':
            self.handle_dashboard_region3()
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == '/api/status':
            self.handle_status()
        elif self.path == '/api/ai-config':
            self.handle_ai_config()
        elif self.path.startswith('/api/dashboard'):
            self.handle_dashboard()
        elif self.path == '/' and 'dustin.ngrok.app' in self.headers.get('Host', ''):
            self.send_response(302)
            self.send_header('Location', '/dashboard.html')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
        else:
            super().do_GET()

    def do_OPTIONS(self):
        """CORS preflight"""
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def handle_status(self):
        self._send_json(200, {
            'last_update_demand': get_last_update(LAST_UPDATE_DEMAND),
            'last_update_zone': get_last_update(LAST_UPDATE_ZONE),
        })

    def handle_ai_config(self):
        cfg = get_api_config()
        self._send_json(200, {
            'configured': bool(cfg.get('api_key', '').strip()),
            'model': cfg.get('model', 'claude-sonnet-4-5-20251001'),
        })

    def handle_update_demand(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except Exception:
            body = {}
        team_id = body.get('team_id', 'gyeonggi')
        print(f"[{datetime.now()}] 수요 업데이트 시작 ({team_id})...")
        try:
            result = subprocess.run(
                ['python3', os.path.join(DIR, 'update.py'), '--demand', '--team', team_id],
                capture_output=True, text=True, timeout=600
            )
            today = datetime.now().strftime('%Y-%m-%d %H:%M')
            if result.returncode == 0:
                with open(LAST_UPDATE_DEMAND, 'w') as f:
                    f.write(today)
                deploy_to_github()
                print(f"[{datetime.now()}] 수요 업데이트 완료")
                self._send_json(200, {'success': True, 'last_update': today})
            else:
                print(f"[{datetime.now()}] 수요 업데이트 실패: {result.stderr[:300]}")
                self._send_json(500, {'error': result.stderr[:500]})
        except subprocess.TimeoutExpired:
            self._send_json(504, {'error': '업데이트 시간 초과 (10분)'})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    def handle_update_zone(self):
        """존/실적 업데이트 — 5분 쿨다운"""
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except Exception:
            body = {}
        team_id = body.get('team_id', 'gyeonggi')
        last = get_last_update(LAST_UPDATE_ZONE)
        if last:
            try:
                last_dt = datetime.strptime(last, '%Y-%m-%d %H:%M')
                elapsed = (datetime.now() - last_dt).total_seconds()
                if elapsed < 300:
                    remaining = int(300 - elapsed)
                    self._send_json(429, {
                        'error': f'존/실적 업데이트 쿨다운 중입니다. {remaining}초 후 다시 시도하세요.',
                        'last_update': last
                    })
                    return
            except ValueError:
                pass
        print(f"[{datetime.now()}] 존/실적 업데이트 시작 ({team_id})...")
        try:
            result = subprocess.run(
                ['python3', os.path.join(DIR, 'update.py'), '--zone', '--team', team_id],
                capture_output=True, text=True, timeout=600
            )
            today = datetime.now().strftime('%Y-%m-%d %H:%M')
            if result.returncode == 0:
                with open(LAST_UPDATE_ZONE, 'w') as f:
                    f.write(today)
                deploy_to_github()
                print(f"[{datetime.now()}] 존/실적 업데이트 완료")
                self._send_json(200, {'success': True, 'last_update': today})
            else:
                print(f"[{datetime.now()}] 존/실적 업데이트 실패: {result.stderr[:300]}")
                self._send_json(500, {'error': result.stderr[:500]})
        except subprocess.TimeoutExpired:
            self._send_json(504, {'error': '업데이트 시간 초과 (10분)'})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    def handle_simulate(self):
        """존 개설 시뮬레이션 — 실시간 BQ 쿼리"""
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            lat = float(body['lat'])
            lng = float(body['lng'])
            radius = float(body.get('radius', 1.0))
            team_id = body.get('team_id', 'gyeonggi')
            print(f"[{datetime.now()}] 시뮬레이션: ({lat:.5f}, {lng:.5f}), 반경 {radius}km, 팀: {team_id}")

            from update import simulate_zone
            result = simulate_zone(lat, lng, radius, team_id=team_id)
            self._send_json(200, result)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._send_json(500, {'error': str(e)})

    def handle_d2d_destinations(self):
        """부름 호출 목적지 조회"""
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
            zone_id = int(body['zone_id'])
            print(f"[{datetime.now()}] 부름호출지역 조회: zone_id={zone_id}")

            from update import query_d2d_destinations
            result = query_d2d_destinations(zone_id)
            self._send_json(200, result)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._send_json(500, {'error': str(e)})

    def handle_simulate_eval(self):
        """시뮬레이션 AI 평가 — GPT 5.2 + Claude 3.5 Sonnet"""
        try:
            length = int(self.headers.get('Content-Length', 0))
            sim_data = json.loads(self.rfile.read(length)) if length else {}
            team_id = sim_data.get('team_id', 'gyeonggi')
            print(f"[{datetime.now()}] AI 평가 요청: {sim_data.get('region2','')} {sim_data.get('region3','')} 팀: {team_id}")

            from update import evaluate_simulation_llm
            result = evaluate_simulation_llm(sim_data, team_id=team_id)
            self._send_json(200, result)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._send_json(500, {'error': str(e)})

    def handle_dashboard(self):
        """실적 대시보드 데이터 — 캐시 파일 조합"""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        team_id = qs.get('team_id', ['gyeonggi'])[0]
        cache_dir = os.path.join(DIR, '.cache', team_id)

        def load(name):
            p = os.path.join(cache_dir, f'{name}.json')
            if os.path.exists(p):
                with open(p) as f:
                    return json.load(f)
            return None

        zones = load('zones') or []
        profit = load('profit') or {}
        supply_demand = load('supply_demand') or {}
        dashboard_metrics = load('dashboard_metrics') or {}
        dashboard_region2 = load('dashboard_region2') or {}
        dashboard_region3 = load('dashboard_region3') or {}

        # 존+실적 병합
        profit_int = {int(k): v for k, v in profit.items()}
        zone_rows = []
        for z in zones:
            zid = int(z['zone_id'])
            p = profit_int.get(zid, {})
            zone_rows.append({
                'zone_id': zid,
                'zone_name': z.get('zone_name', ''),
                'region1': z.get('region1', ''),
                'region2': z.get('region2', ''),
                'region3': z.get('region3', ''),
                'car_count': int(z.get('car_count', 0)),
                'total_revenue': p.get('total_revenue', 0),
                'revenue_per_car_28d': p.get('revenue_per_car_28d', 0),
                'gp_per_car_28d': p.get('gp_per_car_28d', 0),
                'utilization_rate': p.get('utilization_rate', 0),
                'revenue_per_res': p.get('revenue_per_res', 0),
            })

        # 팀 전체 요약
        summary = {}
        for section in ('growth', 'decline'):
            for item in (supply_demand.get(section) or []):
                if '전체' in item.get('region2', ''):
                    summary = item
                    break
            if summary:
                break

        self._send_json(200, {
            'team_id': team_id,
            'summary': summary,
            'zones': zone_rows,
            'weekly': dashboard_metrics.get('weekly', []),
            'weekly_region2': dashboard_region2.get('weekly', []),
            'weekly_region3': dashboard_region3.get('weekly', []),
            'generated_at': dashboard_metrics.get('generated_at') or get_last_update(LAST_UPDATE_ZONE),
        })

    def handle_dashboard_analyze(self):
        """LLM 실적 분석 — 내부 LLM Gateway (LiteLLM) 호출"""
        import urllib.request as req_lib
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except Exception:
            self._send_json(400, {'error': '요청 파싱 실패'})
            return

        cfg = get_api_config()
        api_key = cfg.get('api_key', '').strip()
        gateway_url = cfg.get('gateway_url', 'https://litellm.ai.socarcorp.co.kr/v1/chat/completions')
        model = body.get('model') or cfg.get('model', 'dev/claude-4.6-sonnet')
        prompt = body.get('prompt', '')

        if not api_key:
            self._send_json(400, {'error': 'API 키가 서버에 설정되지 않았습니다. .api_config.json에 api_key를 입력하세요.'})
            return

        messages = body.get('messages')
        if not messages and not prompt:
            self._send_json(400, {'error': 'prompt 또는 messages가 필요합니다'})
            return
        if not messages:
            messages = [{'role': 'user', 'content': prompt}]

        try:
            # LiteLLM Gateway — OpenAI 호환 단일 포맷
            # messages 배열 직접 전달 지원 (대화 히스토리 포함)
            payload = json.dumps({
                'model': model,
                'messages': messages,
                'max_tokens': 16000,
                'temperature': 0.5,
            }).encode()
            request = req_lib.Request(
                gateway_url,
                data=payload,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}',
                },
            )
            with req_lib.urlopen(request, timeout=90) as resp:
                result = json.loads(resp.read())
            msg = result['choices'][0]['message']
            text = msg.get('content')
            if not text:
                # tool_call 응답이거나 content가 null인 경우
                print(f"[{datetime.now()}] LLM 응답 content 없음: {str(result)[:500]}")
                raise ValueError(f"LLM이 텍스트 응답을 반환하지 않았습니다. (finish_reason: {result['choices'][0].get('finish_reason')})")
            self._send_json(200, {'result': text})
        except Exception as e:
            self._send_json(500, {'error': str(e)})

    def handle_dashboard_region3(self):
        """특정 region2의 region3별 실적 조회 (캐시에서 필터링)"""
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except Exception:
            self._send_json(400, {'error': '요청 파싱 실패'})
            return
        team_id = body.get('team_id', 'gyeonggi')
        region2 = body.get('region2', '')
        cache_dir = os.path.join(DIR, '.cache', team_id)
        p = os.path.join(cache_dir, 'dashboard_region3.json')
        if not os.path.exists(p):
            self._send_json(404, {'error': 'region3 데이터 없음. 존/실적 업데이트를 먼저 실행하세요.'})
            return
        with open(p) as f:
            data = json.load(f)
        filtered = [r for r in data.get('weekly', []) if r.get('region2_parent', '') == region2]
        self._send_json(200, {'weekly': filtered})

    def log_message(self, format, *args):
        if '/api/' in str(args[0]):
            super().log_message(format, *args)


if __name__ == '__main__':
    server = http.server.HTTPServer(('', PORT), Handler)
    print(f"카셰어링 수요 지도 서버 시작: http://localhost:{PORT}")
    print(f"마지막 수요 업데이트: {get_last_update(LAST_UPDATE_DEMAND) or '없음'}")
    print(f"마지막 존/실적 업데이트: {get_last_update(LAST_UPDATE_ZONE) or '없음'}")
    print("종료: Ctrl+C")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버 종료")
        server.server_close()
