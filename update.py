#!/usr/bin/env python3
"""
경기도 카셰어링 잠재 수요 지도 - 주간 업데이트 스크립트
실행: python3 ~/carsharing_demand_map/update.py

index.html - 잠재 수요 지도 (앱 접속 + 예약 히트맵 + 존 마커 + Gap 분석 + 공급 분석)
"""

import json, math, os, subprocess, urllib.request, time
from datetime import datetime, timedelta

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_UPDATE_DEMAND_FILE = os.path.join(OUTPUT_DIR, '.last_update_demand')
LAST_UPDATE_ZONE_FILE = os.path.join(OUTPUT_DIR, '.last_update_zone')
NGROK_URL_FILE = os.path.join(OUTPUT_DIR, '.ngrok_url')

TODAY = datetime.now().strftime("%Y-%m-%d")


def _read_last_update(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        return '없음'
THREE_MONTHS_AGO = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
NEXT_DAY = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def run_bq(sql):
    """Execute BigQuery SQL via bq CLI and return rows."""
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=1000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ error: {result.stderr}")
    return json.loads(result.stdout)


def query_access():
    """앱 접속 위치 (광역 bounding box만, 서울/인천 제외는 Python 후처리)"""
    sql = f"""
    WITH markers_union AS (
      SELECT location.lat AS lat, location.lng AS lng
      FROM `socar-data.socar_server_3.GET_MARKERS_V2`
      WHERE timeMs >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
        AND timeMs < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
        AND location.lat BETWEEN 36.9 AND 38.1
        AND location.lng BETWEEN 126.3 AND 127.9
      UNION ALL
      SELECT location.lat AS lat, location.lng AS lng
      FROM `socar-data.socar_server_3.GET_MARKERS`
      WHERE timeMs >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
        AND timeMs < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
        AND location.lat BETWEEN 36.9 AND 38.1
        AND location.lng BETWEEN 126.3 AND 127.9
    )
    SELECT ROUND(lat,3) AS lat, ROUND(lng,3) AS lng, COUNT(*) AS access_count
    FROM markers_union WHERE lat IS NOT NULL AND lng IS NOT NULL
    GROUP BY 1,2 ORDER BY access_count DESC LIMIT 10000
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=10000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ error: {result.stderr}")
    rows = json.loads(result.stdout)
    for r in rows:
        r['lat'] = float(r['lat'])
        r['lng'] = float(r['lng'])
        r['access_count'] = int(r['access_count'])
    return rows


def query_reservation():
    """예약 생성 위치 (광역 bounding box만, 서울/인천 제외는 Python 후처리)"""
    sql = f"""
    SELECT ROUND(reservation_created_lat,3) AS lat, ROUND(reservation_created_lng,3) AS lng,
           COUNT(*) AS reservation_count
    FROM `socar-data.soda_store.reservation_v2`
    WHERE date >= '{THREE_MONTHS_AGO}' AND date <= '{TODAY}'
      AND region1 = '경기도' AND state = 3 AND member_imaginary IN (0,9)
      AND reservation_created_lat IS NOT NULL AND reservation_created_lng IS NOT NULL
      AND reservation_created_lat BETWEEN 36.9 AND 38.1
      AND reservation_created_lng BETWEEN 126.3 AND 127.9
    GROUP BY 1,2 ORDER BY reservation_count DESC LIMIT 10000
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=10000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ error: {result.stderr}")
    rows = json.loads(result.stdout)
    for r in rows:
        r['lat'] = float(r['lat'])
        r['lng'] = float(r['lng'])
        r['reservation_count'] = int(r['reservation_count'])
    return rows



def query_reservation_by_zone_region():
    """지역 존 소속 차량의 실제 예약 건수 (예약 생성 위치가 아닌, 차량 소속 지역 기준)"""
    sql = f"""
    SELECT cz.region2, COUNT(*) AS reservation_count
    FROM `socar-data.soda_store.reservation_v2` r
    JOIN `socar-data.tianjin_replica.car_info` c ON r.car_id = c.id
    JOIN `socar-data.tianjin_replica.carzone_info` cz ON c.zone_id = cz.id
    WHERE r.date >= '{THREE_MONTHS_AGO}' AND r.date <= '{TODAY}'
      AND cz.region1 = '경기도'
      AND cz.state = 1 AND cz.imaginary IN (0,3,5) AND cz.visibility = 1
      AND r.state = 3
      AND r.member_imaginary IN (0,9)
    GROUP BY cz.region2
    ORDER BY reservation_count DESC
    """
    return run_bq(sql)


def query_dtod():
    """부름 호출 위치 (경기도 영역)"""
    sql = f"""
    SELECT ROUND(d.start_lat, 4) AS lat, ROUND(d.start_lng, 4) AS lng,
           COUNT(*) AS call_count
    FROM `socar-data.tianjin_replica.reservation_dtod_info` d
    WHERE d.created_at >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
      AND d.created_at < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
      AND d.start_lat BETWEEN 36.9 AND 38.1
      AND d.start_lng BETWEEN 126.3 AND 127.9
      AND d.start_lat IS NOT NULL AND d.start_lng IS NOT NULL
    GROUP BY 1, 2
    ORDER BY call_count DESC
    LIMIT 3000
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=3000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ error: {result.stderr}")
    rows = json.loads(result.stdout)
    for r in rows:
        r['lat'] = float(r['lat'])
        r['lng'] = float(r['lng'])
        r['call_count'] = int(r['call_count'])
    return rows


def simulate_zone(lat, lng, radius_km=1.0):
    """존 개설 시뮬레이션 — 실시간 BQ 쿼리로 정확한 수요 산출"""
    # 위도/경도 → 반경 변환 (대략)
    dlat = radius_km / 111.0
    dlng = radius_km / (111.0 * abs(math.cos(math.radians(lat))))

    lat_min, lat_max = lat - dlat, lat + dlat
    lng_min, lng_max = lng - dlng, lng + dlng

    num_weeks = 90 / 7

    # 1) 반경 내 앱 접속수 (LIMIT 없이 정확한 수)
    access_sql = f"""
    WITH markers AS (
      SELECT location.lat AS lat, location.lng AS lng
      FROM `socar-data.socar_server_3.GET_MARKERS_V2`
      WHERE timeMs >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
        AND timeMs < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
        AND location.lat BETWEEN {lat_min} AND {lat_max}
        AND location.lng BETWEEN {lng_min} AND {lng_max}
      UNION ALL
      SELECT location.lat AS lat, location.lng AS lng
      FROM `socar-data.socar_server_3.GET_MARKERS`
      WHERE timeMs >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
        AND timeMs < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
        AND location.lat BETWEEN {lat_min} AND {lat_max}
        AND location.lng BETWEEN {lng_min} AND {lng_max}
    )
    SELECT COUNT(*) AS total_access FROM markers
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    """

    # 2) 반경 내 예약 생성수
    res_sql = f"""
    SELECT COUNT(*) AS total_res
    FROM `socar-data.soda_store.reservation_v2`
    WHERE date >= '{THREE_MONTHS_AGO}' AND date <= '{TODAY}'
      AND state = 3 AND member_imaginary IN (0,9)
      AND reservation_created_lat BETWEEN {lat_min} AND {lat_max}
      AND reservation_created_lng BETWEEN {lng_min} AND {lng_max}
    """

    # 3) 반경 내 부름 호출수
    dtod_sql = f"""
    SELECT COUNT(*) AS total_dtod
    FROM `socar-data.tianjin_replica.reservation_dtod_info`
    WHERE created_at >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
      AND created_at < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
      AND start_lat BETWEEN {lat_min} AND {lat_max}
      AND start_lng BETWEEN {lng_min} AND {lng_max}
    """

    # 4) 해당 지역의 region2/3 파악 (가장 가까운 존 기준)
    zones = _load_cache("zones") or []
    nearest_zone = None
    nearest_dist = float('inf')
    for z in zones:
        d = ((float(z['lat']) - lat)**2 + (float(z['lng']) - lng)**2) ** 0.5
        if d < nearest_dist:
            nearest_dist = d
            nearest_zone = z

    region2 = nearest_zone.get('region2', '') if nearest_zone else ''
    region3 = nearest_zone.get('region3', '') if nearest_zone else ''

    # 5) region3 / region2 전환율 (예약건수 기반, 접속 대비)
    # 가벼운 쿼리: region별 예약건수 + 차량수 (접속은 캐시 활용)
    conv_sql = f"""
    SELECT cz.region2, cz.region3,
           COUNT(*) AS res_cnt,
           COUNT(DISTINCT c.id) AS car_cnt
    FROM `socar-data.soda_store.reservation_v2` r
    JOIN `socar-data.tianjin_replica.car_info` c ON r.car_id = c.id
    JOIN `socar-data.tianjin_replica.carzone_info` cz ON c.zone_id = cz.id
    WHERE r.date >= '{THREE_MONTHS_AGO}' AND r.date <= '{TODAY}'
      AND cz.region1 = '경기도' AND cz.state = 1
      AND cz.imaginary IN (0,3,5) AND cz.visibility = 1
      AND r.state = 3 AND r.member_imaginary IN (0,9)
      AND cz.region2 = '{region2}'
    GROUP BY 1, 2
    ORDER BY cz.region3
    """

    # BQ 쿼리 실행
    def bq_single(sql):
        cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=100", sql]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            raise RuntimeError(f"BQ error: {r.stderr[:300]}")
        return json.loads(r.stdout)

    access_rows = bq_single(access_sql)
    res_rows = bq_single(res_sql)
    dtod_rows = bq_single(dtod_sql)

    total_access_90d = int(access_rows[0]['total_access']) if access_rows else 0
    total_res_90d = int(res_rows[0]['total_res']) if res_rows else 0
    total_dtod_90d = int(dtod_rows[0]['total_dtod']) if dtod_rows else 0

    weekly_access = round(total_access_90d / num_weeks)
    weekly_res = round(total_res_90d / num_weeks)
    weekly_dtod = round(total_dtod_90d / num_weeks)

    # 전환율 계산: 캐시된 접속 데이터에서 region별 접속수 집계
    cached_access = _load_cache("access") or []
    # region별 접속수: 각 접속 좌표를 가장 가까운 존의 region에 배정
    region_access = {}
    for z in zones:
        r2 = z.get('region2', '')
        r3 = z.get('region3', '')
        if r2 not in region_access:
            region_access[r2] = {'total': 0, 'by_r3': {}}
        if r3 and r3 not in region_access[r2]['by_r3']:
            region_access[r2]['by_r3'][r3] = 0
    # 존별 접속수를 zone의 region에 귀속 (간이 방식: 존 위치에서 가장 가까운 접속 격자)
    for a in cached_access:
        alat, alng, cnt = float(a['lat']), float(a['lng']), int(a['access_count'])
        best_d, best_r2, best_r3 = float('inf'), '', ''
        for z in zones:
            d = (float(z['lat']) - alat)**2 + (float(z['lng']) - alng)**2
            if d < best_d:
                best_d = d
                best_r2 = z.get('region2', '')
                best_r3 = z.get('region3', '')
        if best_r2:
            if best_r2 not in region_access:
                region_access[best_r2] = {'total': 0, 'by_r3': {}}
            region_access[best_r2]['total'] += cnt
            if best_r3:
                region_access[best_r2]['by_r3'][best_r3] = region_access[best_r2]['by_r3'].get(best_r3, 0) + cnt

    # 전환율 = 예약건수 / 접속수 (region3 → region2 fallback)
    conv_rows = bq_single(conv_sql)

    region3_conv = None
    region2_conv = None
    region2_res_total = sum(int(cr.get('res_cnt', 0)) for cr in conv_rows)
    region2_access_total = region_access.get(region2, {}).get('total', 0)

    if region2_access_total > 0:
        region2_conv = region2_res_total / region2_access_total

    # region3 전환율
    r3_access = region_access.get(region2, {}).get('by_r3', {}).get(region3, 0)
    r3_res = 0
    for cr in conv_rows:
        if cr.get('region3', '') == region3:
            r3_res = int(cr.get('res_cnt', 0))
    if r3_access > 0 and r3_res > 0:
        region3_conv = r3_res / r3_access

    # region3 전환율 우선, 없으면 region2
    if region3_conv is not None and region3_conv > 0:
        conv_rate = region3_conv
        conv_level = region3
    elif region2_conv is not None and region2_conv > 0:
        conv_rate = region2_conv
        conv_level = region2
    else:
        # 최종 fallback: 경기도 전체
        total_gg_access = sum(v.get('total', 0) for v in region_access.values())
        total_gg_res = sum(int(cr.get('res_cnt', 0)) for cr in conv_rows)
        conv_rate = total_gg_res / total_gg_access if total_gg_access > 0 else 0
        conv_level = '경기도'

    # 예상 주간 예약 (반경 내 실제 예약이 있으면 그대로, 없으면 접속 × 전환율)
    est_weekly_res = weekly_res if weekly_res > 0 else round(weekly_access * conv_rate)

    # 인근 존 실적 (캐시에서)
    profit_data = _load_cache("profit") or {}
    if profit_data:
        profit_data = {int(k): v for k, v in profit_data.items()}

    bench_zones = []
    bench_radius = 3.0
    for z in zones:
        zd = ((float(z['lat']) - lat)**2 + (float(z['lng']) - lng)**2) ** 0.5 * 111
        if zd <= bench_radius and int(z.get('car_count', 0)) > 0:
            zid = int(z['zone_id'])
            p = profit_data.get(zid, {})
            rev = p.get('revenue_per_car_28d', 0)
            gp = p.get('gp_per_car_28d', 0)
            util = p.get('utilization_rate', 0)
            if rev > 0:
                bench_zones.append({'dist': zd, 'rev': rev, 'gp': gp, 'util': util,
                                    'name': z.get('zone_name', ''), 'cars': int(z['car_count'])})

    bench_zones.sort(key=lambda x: x['dist'])

    # 가중평균 실적
    w_rev, w_gp, w_util, w_total = 0, 0, 0, 0
    for bz in bench_zones:
        w = 1 / max(bz['dist'], 0.1)
        w_rev += bz['rev'] * w
        w_gp += bz['gp'] * w
        w_util += bz['util'] * w
        w_total += w

    est_rev_per_car = round(w_rev / w_total) if w_total > 0 else 0
    est_gp_per_car = round(w_gp / w_total) if w_total > 0 else 0
    avg_util = w_util / w_total if w_total > 0 else 40

    # 추천 공급대수
    avg_hours_per_res = 8
    target_util = min(avg_util / 100, 0.7)
    if target_util > 0:
        cars_needed = est_weekly_res * avg_hours_per_res / (168 * target_util)
    else:
        cars_needed = est_weekly_res / 3 if est_weekly_res >= 3 else (1 if est_weekly_res >= 1 else 0)
    recommended_cars = max(0, round(cars_needed))

    is_recommend = recommended_cars >= 1 and est_rev_per_car >= 1000000

    return {
        'lat': lat, 'lng': lng, 'radius_km': radius_km,
        'region2': region2, 'region3': region3,
        'weekly_access': weekly_access,
        'weekly_res': weekly_res,
        'weekly_dtod': weekly_dtod,
        'total_access_90d': total_access_90d,
        'total_res_90d': total_res_90d,
        'conv_rate': round(conv_rate, 6),
        'conv_level': conv_level,
        'est_weekly_res': est_weekly_res,
        'bench_zone_count': len(bench_zones),
        'est_rev_per_car': est_rev_per_car,
        'est_gp_per_car': est_gp_per_car,
        'avg_util': round(avg_util, 1),
        'recommended_cars': recommended_cars,
        'is_recommend': is_recommend,
        'nearest_zone': nearest_zone.get('zone_name', '') if nearest_zone else '',
        'nearest_zone_dist_km': round(nearest_dist * 111, 1) if nearest_zone else None,
    }


def query_zones():
    """운영 존 + 차량 대수 + 부름 가능 여부 (is_d2d_car_exportable)"""
    sql = """
    SELECT cz.id AS zone_id, cz.zone_name, cz.name AS parking_name,
           cz.lat, cz.lng, cz.region2, cz.region3, cz.address,
           cz.is_d2d_car_exportable, cz.imaginary,
           COUNT(DISTINCT ci.id) AS car_count
    FROM `socar-data.tianjin_replica.carzone_info` cz
    LEFT JOIN `socar-data.tianjin_replica.car_info` ci
      ON cz.id = ci.zone_id AND ci.state = 5 AND ci.level = 1
    WHERE cz.state = 1 AND cz.region1 = '경기도'
      AND cz.imaginary IN (0,3,5) AND cz.visibility = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    HAVING NOT (cz.imaginary = 0 AND COUNT(DISTINCT ci.id) = 0)
    ORDER BY cz.region2, cz.zone_name
    """
    return run_bq(sql)


def query_socar_supply_by_region():
    """쏘카 지역별 실 운영 차량/존 수 (profit_socar_car_daily 기준, 최근 3개월)"""
    sql = f"""
    SELECT
        region2,
        COUNT(DISTINCT zone_id) AS socar_zones,
        COUNT(DISTINCT car_id) AS socar_cars
    FROM `socar-data.socar_biz_profit.profit_socar_car_daily`
    WHERE region1 = '경기도'
      AND car_state IN ('운영', '수리')
      AND car_sharing_type IN ('socar', 'zplus')
      AND date >= '{THREE_MONTHS_AGO}' AND date <= '{TODAY}'
    GROUP BY region2
    ORDER BY region2
    """
    rows = run_bq(sql)
    result = {}
    for r in rows:
        r2 = r.get('region2', '')
        if not r2:
            continue
        result[r2] = {
            'socar_zones': int(r.get('socar_zones', 0)),
            'socar_cars': int(r.get('socar_cars', 0)),
        }
    return result


def query_reservation_timeline():
    """경기도 존별 차량 예약 타임라인 (±1주), state != 0 (취소 제외)"""
    one_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    one_week_later = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    sql = f"""
    SELECT
        ci.zone_id,
        r.car_id,
        ci.car_name,
        ci.car_num,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', r.start_at, 'Asia/Seoul') AS start_at,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', r.end_at, 'Asia/Seoul') AS end_at,
        r.way,
        r.member_imaginary
    FROM `socar-data.tianjin_replica.reservation_info` r
    JOIN `socar-data.tianjin_replica.car_info` ci ON r.car_id = ci.id
    JOIN `socar-data.tianjin_replica.carzone_info` cz ON ci.zone_id = cz.id
    WHERE cz.region1 = '경기도'
      AND cz.state = 1 AND cz.imaginary IN (0,3,5) AND cz.visibility = 1
      AND r.state != 0
      AND r.end_at >= TIMESTAMP('{one_week_ago}', 'Asia/Seoul')
      AND r.start_at <= TIMESTAMP('{one_week_later}', 'Asia/Seoul')
    ORDER BY ci.zone_id, r.car_id, r.start_at
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=200000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        print(f"  [경고] 예약 타임라인 조회 실패: {result.stderr[:200]}")
        return {}
    rows = json.loads(result.stdout)
    # zone_id별로 그룹핑
    by_zone = {}
    for r in rows:
        zid = str(r['zone_id'])
        if zid not in by_zone:
            by_zone[zid] = []
        mi = int(r.get('member_imaginary', 0) or 0)
        by_zone[zid].append({
            'car_id': int(r['car_id']),
            'car_name': r.get('car_name', ''),
            'car_num': r.get('car_num', ''),
            'start': r['start_at'],
            'end': r['end_at'],
            'way': r.get('way', ''),
            'block': 1 if mi != 0 else 0,
        })
    return by_zone


def query_parking_contract():
    """존별 사업자명, 정산 방식, 대당 주차비"""
    sql = """
    SELECT
        z.legacy_zone_id AS zone_id,
        pr.name AS provider_name,
        CASE
            WHEN pp.settlement_type = 'PVSTP_BATCH' THEN '일괄정산'
            WHEN pp.settlement_type = 'PVSTP_INDIVIDUAL' THEN '개별정산'
            WHEN pp.settlement_type = 'PVSTP_RENT' THEN '임대'
            WHEN pp.settlement_type = 'PVSTP_FREE' THEN '무료'
            ELSE pp.settlement_type
        END AS settlement_type,
        CASE
            WHEN rp.payment_cycle = 'REPAC_MONTH' AND pp.settlement_type = 'PVSTP_RENT'
                THEN ROUND(SAFE_DIVIDE(rp.rent_price, IFNULL(COUNT(DISTINCT ci.id),1)),0)
            WHEN rp.payment_cycle = 'REPAC_QUARTER' AND pp.settlement_type = 'PVSTP_RENT'
                THEN ROUND(SAFE_DIVIDE(rp.rent_price, IFNULL(COUNT(DISTINCT ci.id),1))/3,0)
            WHEN rp.payment_cycle = 'REPAC_ONE_YEAR' AND pp.settlement_type = 'PVSTP_RENT'
                THEN ROUND(SAFE_DIVIDE(rp.rent_price, IFNULL(COUNT(DISTINCT ci.id),1))/12,0)
            WHEN rp.payment_cycle = 'REPAC_TWO_MONTHS' AND pp.settlement_type = 'PVSTP_RENT'
                THEN ROUND(SAFE_DIVIDE(rp.rent_price, IFNULL(COUNT(DISTINCT ci.id),1))/2,0)
            WHEN rp.payment_cycle = 'REPAC_HALF_YEAR' AND pp.settlement_type = 'PVSTP_RENT'
                THEN ROUND(SAFE_DIVIDE(rp.rent_price, IFNULL(COUNT(DISTINCT ci.id),1))/6,0)
            ELSE stp.price
        END AS price_per_car
    FROM `socar-data.socar_zone.zone` z
    LEFT JOIN `socar-data.socar_zone.parking` p ON p.id = z.parking_id
    LEFT JOIN `socar-data.socar_zone.parking_contract` c ON c.parking_id = p.id
    LEFT JOIN `socar-data.socar_zone.parking_settlement_policy` sp ON c.policy_id = sp.id
    LEFT JOIN `socar-data.socar_zone.parking_policy` pp ON sp.id = pp.policy_id
    LEFT JOIN `socar-data.socar_zone.settlement_type_policy` stp ON pp.id = stp.parking_policy_id
    LEFT JOIN `socar-data.socar_zone.rent_policy` rp ON pp.id = rp.parking_policy_id
    LEFT JOIN (
        SELECT ci.*, z2.parking_id
        FROM `socar-data.tianjin_replica.car_info` ci
        LEFT JOIN `socar-data.socar_zone.zone` z2 ON z2.legacy_zone_id = ci.zone_id
        WHERE ci.sharing_type = 'socar' AND ci.state IN (4,5) AND ci.level = 1
    ) ci ON ci.parking_id = z.parking_id
    LEFT JOIN `socar-data.socar_zone.parking_provider` pr ON c.provider_id = pr.id
    WHERE z.legacy_zone_id IS NOT NULL AND z.legacy_zone_id NOT IN (0)
        AND c.business_type <> 'CTRBT_CLEANING_BUSINESS'
        AND pp.settlement_type IS NOT NULL
        AND z.region_1 = '경기도'
    GROUP BY z.legacy_zone_id, pr.name, pp.settlement_type, rp.rent_price, stp.price, rp.payment_cycle
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=5000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ error: {result.stderr}")
    rows = json.loads(result.stdout)
    result = {}
    for r in rows:
        zid = int(r.get('zone_id', 0))
        result[zid] = {
            'provider_name': r.get('provider_name', ''),
            'settlement_type': r.get('settlement_type', ''),
            'price_per_car': int(float(r.get('price_per_car', 0) or 0)),
        }
    return result


def query_gcar_zones():
    """그린카 경기도 존 현황 (gcar_info_log 최근 날짜 기준, 존별 차량수)"""
    sql = """
    WITH latest_cars AS (
      SELECT zone_id, zone_name, region1, region2, lat, lng, sig_name,
             COUNT(DISTINCT car_id) AS total_cars
      FROM `socar-data.greencar.gcar_info_log`
      WHERE capture_date = (SELECT MAX(capture_date) FROM `socar-data.greencar.gcar_info_log`)
        AND region1 = '경기도'
      GROUP BY zone_id, zone_name, region1, region2, lat, lng, sig_name
    )
    SELECT * FROM latest_cars ORDER BY total_cars DESC
    """
    return run_bq(sql)


def query_zone_profit():
    """존별 실적 (profit_socar_car_daily + operation_per_car_daily_v2, 최근 3개월)
    대당 매출/GP = SUM(revenue or profit) * 28 / SUM(opr_day) → 28일 기준"""
    sql = f"""
    WITH profit AS (
        SELECT
            zone_id,
            ROUND(SUM(revenue)) AS total_revenue,
            ROUND(SAFE_DIVIDE(SUM(revenue) * 28, NULLIF(SUM(opr_day), 0))) AS revenue_per_car_28d,
            ROUND(SAFE_DIVIDE(SUM(profit) * 28, NULLIF(SUM(opr_day), 0))) AS gp_per_car_28d
        FROM `socar-data.socar_biz_profit.profit_socar_car_daily`
        WHERE region1 = '경기도'
            AND car_state IN ('운영', '수리')
            AND car_sharing_type IN ('socar', 'zplus')
            AND date >= '{THREE_MONTHS_AGO}' AND date <= '{TODAY}'
        GROUP BY zone_id
    ),
    util AS (
        SELECT
            zone_id,
            ROUND(SAFE_DIVIDE(SUM(op_min), SUM(dp_min) - SUM(bl_min)) * 100, 1) AS utilization_rate
        FROM `socar-data.socar_biz.operation_per_car_daily_v2`
        WHERE region1 = '경기도'
            AND date >= '{THREE_MONTHS_AGO}' AND date <= '{TODAY}'
        GROUP BY zone_id
    )
    SELECT p.*, COALESCE(u.utilization_rate, 0) AS utilization_rate
    FROM profit p
    LEFT JOIN util u ON p.zone_id = u.zone_id
    ORDER BY p.total_revenue DESC
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=2000", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"  [경고] 존 실적 조회 실패: {result.stderr[:200]}")
        return {}
    rows = json.loads(result.stdout)
    profit_by_zone = {}
    for r in rows:
        zid = int(r['zone_id'])
        profit_by_zone[zid] = {
            'total_revenue': int(float(r.get('total_revenue', 0) or 0)),
            'revenue_per_car_28d': int(float(r.get('revenue_per_car_28d', 0) or 0)),
            'gp_per_car_28d': int(float(r.get('gp_per_car_28d', 0) or 0)),
            'utilization_rate': float(r.get('utilization_rate', 0) or 0),
        }
    return profit_by_zone


def haversine_km(lat1, lng1, lat2, lng2):
    """두 좌표 간 거리 (km)"""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def _point_in_polygon(lat, lng, polygon):
    """Ray casting 알고리즘"""
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        lat_i, lng_i = polygon[i]
        lat_j, lng_j = polygon[j]
        if ((lng_i > lng) != (lng_j > lng)) and \
           (lat < (lat_j - lat_i) * (lng - lng_i) / (lng_j - lng_i) + lat_i):
            inside = not inside
        j = i
    return inside


SEOUL_POLY = [
    (37.695, 126.764), (37.701, 126.886), (37.690, 126.952),
    (37.700, 127.020), (37.690, 127.092), (37.612, 127.115),
    (37.580, 127.165), (37.555, 127.185), (37.530, 127.180),
    (37.505, 127.170), (37.478, 127.145), (37.460, 127.110),
    (37.448, 127.045),
    (37.430, 126.990), (37.440, 126.905), (37.460, 126.835),
    (37.505, 126.775), (37.550, 126.764), (37.600, 126.790),
]

INCHEON_POLY = [
    (37.620, 126.540), (37.620, 126.680), (37.610, 126.730),
    (37.595, 126.755), (37.575, 126.770), (37.540, 126.780),
    (37.510, 126.785), (37.480, 126.785), (37.450, 126.775),
    (37.420, 126.760), (37.395, 126.750), (37.370, 126.730),
    (37.350, 126.700), (37.340, 126.640), (37.350, 126.580),
    (37.380, 126.540), (37.420, 126.510), (37.470, 126.500),
    (37.530, 126.510), (37.580, 126.520),
]


def is_in_seoul(lat, lng):
    """서울 행정경계 내부인지 판별"""
    return _point_in_polygon(lat, lng, SEOUL_POLY)


def is_in_incheon(lat, lng):
    """인천 행정경계 내부인지 판별"""
    return _point_in_polygon(lat, lng, INCHEON_POLY)


def is_non_gyeonggi(lat, lng):
    """서울 또는 인천 내부인지 판별"""
    return is_in_seoul(lat, lng) or is_in_incheon(lat, lng)


def filter_non_gyeonggi(data, lat_key='lat', lng_key='lng', zone_coords=None, keep_dist_km=1.0):
    """서울+인천 데이터 제거 (히트맵용: 경기존 접경은 허용)."""
    result = []
    for r in data:
        lat, lng = float(r[lat_key]), float(r[lng_key])
        if is_non_gyeonggi(lat, lng):
            if zone_coords:
                near = any(
                    abs(lat - zl) < 0.02 and abs(lng - zn) < 0.02
                    and haversine_km(lat, lng, zl, zn) <= keep_dist_km
                    for zl, zn in zone_coords
                )
                if near:
                    result.append(r)
        else:
            result.append(r)
    return result


def filter_strict_gyeonggi(data, lat_key='lat', lng_key='lng'):
    """서울+인천 철저히 제외 (GAP/공급분석용: 접경 허용 없음)."""
    return [r for r in data if not is_non_gyeonggi(float(r[lat_key]), float(r[lng_key]))]


def _is_likely_gyeonggi(lat, lng):
    """경기도 범위 대략 판별 (강원/충남/인천 공항 등 제외)"""
    if is_non_gyeonggi(lat, lng):
        return False
    # 인천 공항/영종도 (lng < 126.55)
    if lng < 126.55:
        return False
    # 강원도 (lng > 127.55 and lat > 37.6)
    if lng > 127.55 and lat > 37.6:
        return False
    # 충청도 (lat < 37.0)
    if lat < 37.0:
        return False
    return True


def compute_gaps(access_data, reservation_data, zones_data):
    """앱 접속 있으나 반경 800m 내 존이 없는 지역 탐색 (서울/인천 철저 제외)"""
    # BQ에서 ROUND(lat,3) 단위로 이미 그루핑됨 → 같은 키로 매칭
    access_grid = {}
    for r in access_data:
        lat, lng = float(r['lat']), float(r['lng'])
        if not _is_likely_gyeonggi(lat, lng):
            continue
        key = f"{lat},{lng}"
        access_grid[key] = access_grid.get(key, 0) + int(r['access_count'])

    res_grid = {}
    for r in reservation_data:
        lat, lng = float(r['lat']), float(r['lng'])
        if not _is_likely_gyeonggi(lat, lng):
            continue
        key = f"{lat},{lng}"
        res_grid[key] = res_grid.get(key, 0) + int(r['reservation_count'])

    zone_coords = [(float(z['lat']), float(z['lng'])) for z in zones_data]

    # 접속 >= 90(3개월 누적) & 반경 800m 내 존 없는 곳 → 월평균으로 변환
    MIN_ACCESS = 90
    MIN_DIST_KM = 0.8
    num_months = 3
    gaps = []
    for key, access_count in access_grid.items():
        if access_count < MIN_ACCESS:
            continue
        lat, lng = map(float, key.split(','))
        min_dist = min(haversine_km(lat, lng, zl, zn) for zl, zn in zone_coords)
        if min_dist > MIN_DIST_KM:
            res_count = res_grid.get(key, 0)
            gaps.append({
                'lat': lat, 'lng': lng,
                'access_count': round(access_count / num_months),
                'reservation_count': round(res_count / num_months),
                'nearest_zone_km': round(min_dist, 2)
            })

    gaps.sort(key=lambda x: -x['access_count'])
    return gaps[:50]


def query_weekly_trends():
    """경기도 region2별 주간 접속/예약/공급 추이 (최근 12주)"""
    # 1) 접속: 주별 위치별 건수 → Python에서 region2 매핑
    access_sql = f"""
    WITH markers AS (
        SELECT DATE(timeMs, 'Asia/Seoul') AS dt,
            ROUND(location.lat, 2) AS lat, ROUND(location.lng, 2) AS lng
        FROM `socar-data.socar_server_3.GET_MARKERS_V2`
        WHERE timeMs >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
          AND timeMs < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
          AND location.lat BETWEEN 36.9 AND 38.1
          AND location.lng BETWEEN 126.3 AND 127.9
        UNION ALL
        SELECT DATE(timeMs, 'Asia/Seoul') AS dt,
            ROUND(location.lat, 2) AS lat, ROUND(location.lng, 2) AS lng
        FROM `socar-data.socar_server_3.GET_MARKERS`
        WHERE timeMs >= TIMESTAMP('{THREE_MONTHS_AGO}', 'Asia/Seoul')
          AND timeMs < TIMESTAMP('{NEXT_DAY}', 'Asia/Seoul')
          AND location.lat BETWEEN 36.9 AND 38.1
          AND location.lng BETWEEN 126.3 AND 127.9
    )
    SELECT FORMAT_DATE('%G-W%V', dt) AS week, lat, lng, COUNT(*) AS cnt
    FROM markers
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    GROUP BY week, lat, lng
    ORDER BY week, cnt DESC
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=200000", access_sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"BQ access error: {result.stderr}")
    access_rows = json.loads(result.stdout)

    # 2) 예약: 주별 region2별 (존 소속 기준)
    res_sql = f"""
    SELECT FORMAT_DATE('%G-W%V', r.date) AS week, cz.region2,
        COUNT(DISTINCT r.reservation_id) AS res_cnt
    FROM `socar-data.soda_store.reservation_v2` r
    JOIN `socar-data.tianjin_replica.carzone_info` cz ON r.zone_id = cz.id
    WHERE r.date >= '{THREE_MONTHS_AGO}' AND r.date <= '{TODAY}'
      AND r.state = 3 AND r.member_imaginary IN (0,9)
      AND cz.region1 = '경기도'
    GROUP BY week, cz.region2
    ORDER BY cz.region2, week
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=5000", res_sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ res error: {result.stderr}")
    res_rows = json.loads(result.stdout)

    # 3) 공급: 주별 region2별
    supply_sql = f"""
    SELECT FORMAT_DATE('%G-W%V', date) AS week, region2,
        COUNT(DISTINCT car_id) AS car_cnt
    FROM `socar-data.socar_biz_profit.profit_socar_car_daily`
    WHERE region1 = '경기도' AND car_state IN ('운영', '수리')
      AND car_sharing_type IN ('socar', 'zplus')
      AND date >= '{THREE_MONTHS_AGO}' AND date <= '{TODAY}'
    GROUP BY week, region2
    ORDER BY region2, week
    """
    cmd = ["bq", "query", "--use_legacy_sql=false", "--format=json", "--max_rows=5000", supply_sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"BQ supply error: {result.stderr}")
    supply_rows = json.loads(result.stdout)

    return {'access': access_rows, 'res': res_rows, 'supply': supply_rows}


def _assign_access_to_region(access_rows, zones_data):
    """접속 좌표를 가장 가까운 경기도 존의 region2에 할당 (그리드 캐시 사용)"""
    zone_coords = [(float(z['lat']), float(z['lng']), z.get('region2', '')) for z in zones_data]
    # 좌표→region2 캐시 (같은 ROUND(lat,2) 좌표는 같은 region)
    coord_cache = {}
    result = {}  # {(week, region2): count}
    for r in access_rows:
        lat, lng = float(r['lat']), float(r['lng'])
        coord_key = (lat, lng)
        if coord_key not in coord_cache:
            best_dist = float('inf')
            best_region = None
            for zl, zn, zr in zone_coords:
                if abs(lat - zl) > 0.15 or abs(lng - zn) > 0.15:
                    continue
                d = (lat - zl) ** 2 + (lng - zn) ** 2
                if d < best_dist:
                    best_dist = d
                    best_region = zr
            coord_cache[coord_key] = best_region
        region = coord_cache[coord_key]
        if region:
            key = (r['week'], region)
            result[key] = result.get(key, 0) + int(r['cnt'])
    return result


def _half_change(vals):
    """최근 절반 vs 이전 절반 평균 비교 (% 변화율)"""
    if len(vals) < 4:
        return 0
    mid = len(vals) // 2
    first_half = sum(vals[:mid]) / mid
    second_half = sum(vals[mid:]) / (len(vals) - mid)
    if first_half == 0:
        return 0
    return round((second_half - first_half) / first_half * 100, 1)


def compute_growth_analysis(weekly_data, zones_data=None):
    """주간 추이 데이터로 성장 지역 분석 + 공급 분류"""
    # 접속 데이터 → region2 매핑
    if zones_data and isinstance(weekly_data, dict) and 'access' in weekly_data:
        access_by_wr = _assign_access_to_region(weekly_data['access'], zones_data)
    else:
        access_by_wr = {}

    # region2별 주간 데이터 정리
    by_region = {}
    all_weeks = set()

    # 접속 데이터
    for (week, rg), cnt in access_by_wr.items():
        if rg not in by_region:
            by_region[rg] = {}
        if week not in by_region[rg]:
            by_region[rg][week] = {'access': 0, 'res': 0, 'cars': 0}
        by_region[rg][week]['access'] += cnt
        all_weeks.add(week)

    # 예약 데이터
    res_rows = weekly_data.get('res', []) if isinstance(weekly_data, dict) else weekly_data
    for r in res_rows:
        rg = r['region2']
        week = r['week']
        if rg not in by_region:
            by_region[rg] = {}
        if week not in by_region[rg]:
            by_region[rg][week] = {'access': 0, 'res': 0, 'cars': 0}
        by_region[rg][week]['res'] = int(r.get('res_cnt', 0))
        all_weeks.add(week)

    # 공급 데이터
    supply_rows = weekly_data.get('supply', []) if isinstance(weekly_data, dict) else []
    for r in supply_rows:
        rg = r['region2']
        week = r['week']
        if rg not in by_region:
            by_region[rg] = {}
        if week not in by_region[rg]:
            by_region[rg][week] = {'access': 0, 'res': 0, 'cars': 0}
        by_region[rg][week]['cars'] = int(r.get('car_cnt', 0))
        all_weeks.add(week)

    # 현재 진행 중인 불완전한 주 제외
    from datetime import datetime as _dt
    current_week = _dt.now().strftime('%G-W%V')

    # 경기도 전체 주간 합계 (시즈널리티 기준선)
    gg_total = {}  # {week: {access, res, cars}}
    for rg, weeks_dict in by_region.items():
        for w, vals in weeks_dict.items():
            if w == current_week:
                continue
            if w not in gg_total:
                gg_total[w] = {'access': 0, 'res': 0, 'cars': 0}
            gg_total[w]['access'] += vals['access']
            gg_total[w]['res'] += vals['res']
            gg_total[w]['cars'] += vals['cars']

    sorted_all_weeks = sorted(w for w in gg_total.keys())

    analysis = []
    decline = []
    for rg, weeks_dict in by_region.items():
        sorted_weeks = sorted(w for w in weeks_dict.keys() if w != current_week and w in gg_total)
        if len(sorted_weeks) < 4:
            continue

        # 지역의 경기도 대비 점유율(%) 시계열 계산
        # 경기도 전체 합이 0인 주차는 해당 지표 계산에서 제외
        access_share = []
        res_share = []
        car_share = []
        for w in sorted_weeks:
            rv = weeks_dict[w]
            gt = gg_total[w]
            if gt['access'] > 0:
                access_share.append(rv['access'] / gt['access'] * 100)
            if gt['res'] > 0:
                res_share.append(rv['res'] / gt['res'] * 100)
            if gt['cars'] > 0:
                car_share.append(rv['cars'] / gt['cars'] * 100)

        access_vals = [weeks_dict[w]['access'] for w in sorted_weeks]
        res_vals = [weeks_dict[w]['res'] for w in sorted_weeks]
        car_vals = [weeks_dict[w]['cars'] for w in sorted_weeks]
        # 양 끝 불완전 주차 0값 트림
        while access_vals and access_vals[-1] == 0:
            access_vals.pop(); res_vals.pop() if res_vals else None; car_vals.pop() if car_vals else None
        while access_vals and access_vals[0] == 0:
            access_vals.pop(0); res_vals.pop(0) if res_vals else None; car_vals.pop(0) if car_vals else None

        avg_access = sum(access_vals) / len(access_vals)
        avg_res = sum(res_vals) / len(res_vals)
        avg_cars = sum(car_vals) / len(car_vals) if car_vals else 0

        # 주평균
        access_weekly = round(avg_access)
        res_weekly = round(avg_res)
        cars_current = car_vals[-1] if car_vals else 0

        # 증감 = 경기도 대비 점유율의 후반기 vs 전반기 변화율
        access_growth = _half_change(access_share)
        res_growth = _half_change(res_share)
        car_growth = _half_change(car_share) if car_share else 0

        row = {
            'region2': rg,
            'access_weekly': access_weekly,
            'res_weekly': res_weekly,
            'cars': cars_current,
            'access_growth': access_growth,
            'res_growth': res_growth,
            'car_growth': car_growth,
            'access_trend': [round(v, 3) for v in access_share],
            'res_trend': [round(v, 3) for v in res_share],
            'car_trend': [round(v, 3) for v in car_share],
        }

        if res_growth > 0:
            # 수요 성장: 공급 분류
            if car_growth < -1:
                row['status'] = '점검 필요'
            elif car_growth > 1:
                row['status'] = '대응 진행 중'
            else:
                row['status'] = '증차 검토'
            analysis.append(row)
        else:
            # 수요 감소: 공급 분류 (반대 방향)
            if car_growth > 1:
                row['status'] = '점검 필요'
            elif car_growth < -1:
                row['status'] = '대응 진행 중'
            else:
                row['status'] = '감차 검토'
            decline.append(row)

    # 경기도 전체 시계열 (불완전 주차의 0값 제거)
    gg_access_vals = [gg_total[w]['access'] for w in sorted_all_weeks]
    gg_res_vals = [gg_total[w]['res'] for w in sorted_all_weeks]
    gg_car_vals = [gg_total[w]['cars'] for w in sorted_all_weeks]
    # 양 끝의 0 값 트림
    while gg_access_vals and gg_access_vals[-1] == 0:
        gg_access_vals.pop()
        if gg_res_vals: gg_res_vals.pop()
        if gg_car_vals: gg_car_vals.pop()
    while gg_access_vals and gg_access_vals[0] == 0:
        gg_access_vals.pop(0)
        if gg_res_vals: gg_res_vals.pop(0)
        if gg_car_vals: gg_car_vals.pop(0)
    gg_avg_access = sum(gg_access_vals) / len(gg_access_vals) if gg_access_vals else 0
    gg_avg_res = sum(gg_res_vals) / len(gg_res_vals) if gg_res_vals else 0
    gg_cars_current = gg_car_vals[-1] if gg_car_vals else 0

    gg_total_row = {
        'region2': '경기도 전체',
        'access_weekly': round(gg_avg_access),
        'res_weekly': round(gg_avg_res),
        'cars': gg_cars_current,
        'access_growth': _half_change(gg_access_vals),
        'res_growth': _half_change(gg_res_vals),
        'car_growth': _half_change(gg_car_vals) if gg_car_vals else 0,
        'status': '-',
        'access_trend': gg_access_vals,  # 경기도 전체는 절대값 트렌드
        'res_trend': gg_res_vals,
        'car_trend': gg_car_vals,
    }

    # 성장: 점검 필요 > 증차 검토 > 대응 진행 중 순, 같은 그룹 내에선 예약 성장률 높은 순
    status_order_growth = {'점검 필요': 0, '증차 검토': 1, '대응 진행 중': 2}
    analysis.sort(key=lambda x: (status_order_growth.get(x['status'], 9), -x['res_growth']))

    # 감소: 점검 필요 > 감차 검토 > 대응 진행 중 순, 같은 그룹 내에선 예약 감소율 큰 순
    status_order_decline = {'점검 필요': 0, '감차 검토': 1, '대응 진행 중': 2}
    decline.sort(key=lambda x: (status_order_decline.get(x['status'], 9), x['res_growth']))

    # 경기도 전체를 첫 번째 요소로 삽입
    analysis.insert(0, gg_total_row)
    decline.insert(0, gg_total_row)
    return {'growth': analysis, 'decline': decline}


def reverse_geocode(gaps):
    """Nominatim 역지오코딩으로 한글 주소 변환"""
    gg_cities = ['수원','성남','용인','부천','안산','안양','남양주','화성','평택','의정부',
                 '시흥','파주','김포','광명','광주','군포','하남','오산','이천','양주',
                 '구리','안성','포천','의왕','여주','동두천','과천','가평','양평','연천','고양']
    non_gg = ['춘천','당진','홍천','음성','충주','원주','천안','아산','세종','인천','서울']

    results = []
    for g in gaps:
        url = f"https://nominatim.openstreetmap.org/reverse?lat={g['lat']}&lon={g['lng']}&format=json&accept-language=ko&zoom=16"
        req = urllib.request.Request(url, headers={'User-Agent': 'socar-demand-map/1.0'})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                addr = data.get('address', {})
                city = addr.get('city', addr.get('county', addr.get('town', '')))
                district = addr.get('borough', addr.get('city_district', ''))
                suburb = addr.get('suburb', addr.get('quarter', addr.get('neighbourhood', '')))
                village = addr.get('village', addr.get('hamlet', ''))
                parts = [p for p in [city, district, suburb or village] if p]
                name = ' '.join(parts) if parts else f"({g['lat']:.3f}, {g['lng']:.3f})"
        except Exception:
            name = f"({g['lat']:.3f}, {g['lng']:.3f})"

        province = addr.get('province', addr.get('state', ''))
        is_gg = '경기' in province or any(c in name for c in gg_cities)
        is_non = any(c in name for c in non_gg)
        if is_gg and not is_non:
            results.append({**g, 'name': name})
        time.sleep(1.1)

    return results


# ── HTML Generation ────────────────────────────────────────────────────────

LEAFLET_CDN = """
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
"""

SHARED_STYLES = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1e2e; color: #e0e0e0; }
#map { position: absolute; top: 0; left: 240px; right: 0; bottom: 0; z-index: 0; }
.sidebar {
    position: fixed; top: 0; left: 0; bottom: 0; width: 240px; z-index: 1001;
    background: #1e2233; overflow-y: auto; display: flex; flex-direction: column;
    border-right: 1px solid #2a2f45;
}
.sidebar-header {
    padding: 16px 14px 10px; border-bottom: 1px solid #2a2f45;
}
.sidebar-header h1 { font-size: 14px; font-weight: 700; color: #fff; margin-bottom: 6px; }
.sidebar-header .date { font-size: 10px; color: #8890a4; line-height: 1.5; }
.stat-row {
    display: flex; flex-wrap: wrap; gap: 4px; padding: 8px 14px;
    border-bottom: 1px solid #2a2f45;
}
.stat-card {
    display: flex; flex-direction: column; align-items: center;
    background: #323850; border-radius: 5px; padding: 5px 8px; flex: 1; min-width: 65px;
}
.stat-card .label { font-size: 9px; color: #8890a4; white-space: nowrap; margin-bottom: 1px; }
.stat-card .value { font-size: 13px; font-weight: 700; color: #fff; }
.stat-card.socar .label { color: #42a5f5; }
.stat-card.gcar .label { color: #ffb74d; }
.sidebar-section {
    padding: 10px 14px 6px; border-bottom: 1px solid #2a2f45;
}
.sidebar-section-title {
    font-size: 9px; font-weight: 700; color: #6b7394; text-transform: uppercase;
    letter-spacing: 1px; margin-bottom: 6px;
}
.sidebar-section select {
    width: 100%; padding: 6px 8px; border-radius: 6px; border: 1px solid #3a3f55;
    background: #262b3e; color: #e0e0e0; font-size: 12px; cursor: pointer;
    margin-bottom: 4px;
}
.sidebar-section select:focus { outline: none; border-color: #5b6abf; }
.sidebar-btn {
    display: flex; align-items: center; gap: 8px; width: 100%;
    padding: 7px 10px; margin-bottom: 3px; border-radius: 6px; border: none;
    background: transparent; color: #c0c8e0; font-size: 12px; cursor: pointer;
    transition: background 0.15s;
    text-align: left;
}
.sidebar-btn:hover { background: #2a3050; }
.sidebar-btn.active { background: #303760; color: #fff; }
.sidebar-btn .dot {
    width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
}
.update-btn {
    display: flex; align-items: center; gap: 6px; width: 100%;
    padding: 8px 10px; margin-bottom: 3px; border-radius: 6px;
    border: 1px solid #3a3f55; background: #262b3e; color: #c0c8e0;
    font-size: 11px; cursor: pointer; transition: all 0.2s;
}
.update-btn:hover { background: #3a4060; color: #fff; border-color: #5b6abf; }
.update-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.update-btn.updating { animation: pulse 1.5s infinite; }
.update-time { font-size: 9px; color: #6b7394; padding: 1px 10px 4px; }
.analysis-tab { padding: 5px 14px; border-radius: 4px; border: 1px solid #3a3f55; background: #262b3e; color: #8890a4; font-size: 11px; cursor: pointer; transition: all 0.2s; }
.analysis-tab:hover { background: #3a4060; color: #fff; }
.analysis-tab.active { background: #3a4060; color: #fff; border-color: #5b6abf; }
@keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.4; } }
.legend-row {
    display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 14px;
    border-top: 1px solid #2a2f45; margin-top: auto;
}
.legend-item { display: flex; align-items: center; gap: 4px; font-size: 9px; color: #8890a4; }
.legend-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.gap-panel {
    position: fixed; top: 10px; right: 14px; z-index: 1000;
    background: rgba(30,34,51,0.96); border-radius: 10px;
    padding: 14px 16px; box-shadow: 0 2px 16px rgba(0,0,0,0.4);
    max-height: calc(100vh - 30px); overflow-y: auto; width: 320px;
    display: none; font-size: 12px; color: #e0e0e0;
    border: 1px solid #2a2f45;
}
.gap-panel h3 { font-size: 14px; margin-bottom: 8px; color: #fff; }
.gap-panel .gap-row {
    display: flex; justify-content: space-between; padding: 5px 0;
    border-bottom: 1px solid #2a2f45; cursor: pointer;
}
.gap-panel .gap-row:hover { background: #262b3e; }
.gap-panel .gap-name { color: #e0e0e0; flex: 1; }
.gap-panel .gap-cnt { color: #ffb74d; font-weight: 600; min-width: 70px; text-align: right; }
.gap-panel table { color: #e0e0e0; }
.gap-panel th { color: #8890a4; }
.gap-panel td { color: #e0e0e0; }
.leaflet-popup-content-wrapper { border-radius: 10px; background: #262b3e; color: #e0e0e0; box-shadow: 0 4px 20px rgba(0,0,0,0.4); }
.leaflet-popup-tip { background: #262b3e; }
.leaflet-popup-content { font-size: 12px; line-height: 1.7; min-width: 220px; }
.popup-title { font-weight: 700; font-size: 14px; margin-bottom: 6px; padding-bottom: 6px; color: #fff; border-bottom: 1px solid #3a3f55; }
.popup-row { display: flex; gap: 6px; align-items: center; padding: 1px 0; }
.popup-label { color: #8890a4; min-width: 72px; font-size: 11px; flex-shrink: 0; }
.popup-section { border-top: 1px solid #3a3f55; margin-top: 6px; padding-top: 6px; }
.popup-section-title { font-weight: 600; font-size: 11px; color: #6b7394; margin-bottom: 4px; }
.popup-badge {
    display: inline-block; padding: 1px 8px; border-radius: 10px;
    font-size: 11px; font-weight: 600; color: #fff;
}
.heatmap-scale {
    position: fixed; bottom: 24px; right: 14px; z-index: 1000;
    display: flex; gap: 10px; background: rgba(30,34,51,0.95);
    border-radius: 10px; padding: 10px 14px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.3); font-size: 11px;
    border: 1px solid #2a2f45; color: #e0e0e0;
}
"""

TILE_SETUP = """
    var vworldTile = L.tileLayer('https://xdworld.vworld.kr/2d/Base/service/{z}/{x}/{y}.png', {
        attribution: 'VWorld', maxZoom: 19
    });
    var osmTile = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors', maxZoom: 19
    });
    var baseTile = vworldTile;
    baseTile.on('tileerror', function() { map.removeLayer(vworldTile); osmTile.addTo(map); });
    baseTile.addTo(map);
"""

ZONE_JS = """
    function zoneColor(imaginary) {
        if (imaginary === 3) return 'rgba(230,126,34,0.7)';
        if (imaginary === 5) return 'rgba(142,68,173,0.7)';
        return 'rgba(39,174,96,0.55)';
    }
    function zoneLabel(imaginary) {
        if (imaginary === 3) return '스테이션';
        if (imaginary === 5) return '부름우선';
        return '일반';
    }
    function makeZoneIcon(z) {
        var color = zoneColor(z.imaginary);
        var size = Math.max(22, Math.min(38, 18 + z.car_count * 2));
        var label = z.car_count > 0 ? z.car_count : '';
        var html = '<div class="zone-pin" style="' +
            'width:' + size + 'px;height:' + size + 'px;' +
            'background:' + color + ';' +
            '">' + label + '</div>';
        return L.divIcon({
            className: 'zone-marker-wrap',
            html: html,
            iconSize: [size, size],
            iconAnchor: [size/2, size/2],
            popupAnchor: [0, -size/2]
        });
    }
    function fmtNum(n) {
        if (!n || n === 0) return '-';
        return Math.round(n).toLocaleString();
    }
    function makePopup(z) {
        var d2d = z.is_d2d_car_exportable === 'ABLE' ? '부름 가능' : '부름 불가';
        var d2dColor = z.is_d2d_car_exportable === 'ABLE' ? '#27ae60' : '#e74c3c';
        var contractHtml = '';
        if (z.provider_name || z.settlement_type || z.price_per_car) {
            contractHtml = '<div class="popup-section">' +
                '<div class="popup-section-title">거래처 정보</div>' +
                (z.provider_name ? '<div class="popup-row"><span class="popup-label">사업자</span><span>' + z.provider_name + '</span></div>' : '') +
                (z.settlement_type ? '<div class="popup-row"><span class="popup-label">정산방식</span><span>' + z.settlement_type + '</span></div>' : '') +
                (z.price_per_car ? '<div class="popup-row"><span class="popup-label">대당 주차비</span><span><b>' + fmtNum(z.price_per_car) + '원</b>/월</span></div>' : '') +
                '</div>';
        }
        var profitHtml = '';
        if (z.total_revenue > 0) {
            profitHtml = '<div class="popup-section">' +
                '<div class="popup-section-title">실적 <span style="font-weight:400;font-size:10px;color:#8890a4;margin-left:4px;">최근 4주</span></div>' +
                '<div class="popup-row"><span class="popup-label">총 매출</span><b>' + fmtNum(z.total_revenue) + '원</b></div>' +
                '<div class="popup-row"><span class="popup-label">대당 매출</span><b>' + fmtNum(z.revenue_per_car_28d) + '원</b></div>' +
                '<div class="popup-row"><span class="popup-label">대당 GP</span><b>' + fmtNum(z.gp_per_car_28d) + '원</b></div>' +
                '<div class="popup-row"><span class="popup-label">가동률</span><b>' + (z.utilization_rate || 0).toFixed(1) + '%</b></div>' +
                '</div>';
        }
        return '<div class="popup-title">' + z.zone_name + '</div>' +
            '<div class="popup-row"><span class="popup-label">존 ID</span><span style="font-family:monospace;color:#aab0c4">' + z.zone_id + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">주차장</span><span>' + z.parking_name + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">주소</span><span>' + z.address + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">차량</span><span><b>' + z.car_count + '</b>대</span></div>' +
            '<div class="popup-row"><span class="popup-label">유형</span><span class="popup-badge" style="background:' + zoneColor(z.imaginary) + '">' + zoneLabel(z.imaginary) + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">부름</span><span style="color:' + d2dColor + ';font-weight:600">' + d2d + '</span></div>' +
            contractHtml +
            profitHtml;
    }
"""


def jd(data):
    return json.dumps(data, ensure_ascii=False)


def _read_ngrok_url():
    try:
        with open(NGROK_URL_FILE) as f:
            return f.read().strip()
    except FileNotFoundError:
        return ''


def generate_index(access_data, reservation_data, zones_data, gaps, analysis=None, dtod_data=None, profit_data=None, profit_period='', gcar_data=None, socar_supply=None, parking_contract=None, timeline_data=None):
    """잠재 수요 지도 HTML 생성"""
    if analysis is None:
        analysis = {}
    # analysis가 dict(growth/decline)이면 분리, 아니면 하위호환
    if isinstance(analysis, dict):
        analysis_growth = analysis.get('growth', [])
        analysis_decline = analysis.get('decline', [])
    else:
        analysis_growth = analysis
        analysis_decline = []
    if dtod_data is None:
        dtod_data = []
    if profit_data is None:
        profit_data = {}
    if gcar_data is None:
        gcar_data = []
    if socar_supply is None:
        socar_supply = {}
    if parking_contract is None:
        parking_contract = {}
    if timeline_data is None:
        timeline_data = {}
    # profit + 주차 계약 데이터를 zones_data에 병합
    for z in zones_data:
        zid = int(z.get('zone_id', 0))
        p = profit_data.get(zid, {})
        z['total_revenue'] = p.get('total_revenue', 0)
        z['revenue_per_car_28d'] = p.get('revenue_per_car_28d', 0)
        z['gp_per_car_28d'] = p.get('gp_per_car_28d', 0)
        z['utilization_rate'] = p.get('utilization_rate', 0)
        pc = parking_contract.get(zid, {})
        z['provider_name'] = pc.get('provider_name', '')
        z['settlement_type'] = pc.get('settlement_type', '')
        z['price_per_car'] = pc.get('price_per_car', 0)
    # 주평균 계산 (90일 / 7)
    num_weeks = 90 / 7
    access_heat = [[float(r['lat']), float(r['lng']), round(int(r['access_count']) / num_weeks)] for r in access_data]
    res_heat = [[float(r['lat']), float(r['lng']), round(int(r['reservation_count']) / num_weeks)] for r in reservation_data]
    dtod_dots = [[float(r['lat']), float(r['lng']), round(int(r['call_count']) / num_weeks)] for r in dtod_data]

    # gcar 데이터 변환
    gcar_zones = []
    for g in gcar_data:
        gcar_zones.append({
            'zone_id': int(g.get('zone_id', 0)),
            'zone_name': g.get('zone_name', ''),
            'region2': g.get('region2', '').replace('\u3000', ' '),
            'lat': float(g.get('lat', 0)),
            'lng': float(g.get('lng', 0)),
            'total_cars': int(g.get('total_cars', 0)),
            'sig_name': g.get('sig_name', ''),
        })

    # Market share 분석: region2별 쏘카(profit_car_daily 기준) vs 그린카 차량수 비교
    gcar_by_region = {}
    for g in gcar_zones:
        r2 = g['region2'].replace(' ', '\u3000')
        gcar_by_region.setdefault(r2, {'cars': 0, 'zones': 0})
        gcar_by_region[r2]['cars'] += g['total_cars']
        gcar_by_region[r2]['zones'] += 1
    all_regions_ms = sorted(set(list(socar_supply.keys()) + list(gcar_by_region.keys())))
    market_share = []
    for r2 in all_regions_ms:
        s = socar_supply.get(r2, {'socar_cars': 0, 'socar_zones': 0})
        g = gcar_by_region.get(r2, {'cars': 0, 'zones': 0})
        sc = s.get('socar_cars', 0)
        sz = s.get('socar_zones', 0)
        gc = g['cars']
        gz = g['zones']
        total = sc + gc
        if total == 0:
            continue
        market_share.append({
            'region2': r2,
            'socar_cars': sc,
            'socar_zones': sz,
            'gcar_cars': gc,
            'gcar_zones': gz,
            'total_cars': total,
            'socar_share': round(sc / total * 100, 1) if total > 0 else 0,
        })
    market_share.sort(key=lambda x: x['socar_share'])

    total_a = round(sum(int(r['access_count']) for r in access_data) / num_weeks)
    total_r = round(sum(int(r['reservation_count']) for r in reservation_data) / num_weeks)
    total_d = round(sum(int(r['call_count']) for r in dtod_data) / num_weeks)
    total_z = len(zones_data)
    total_cars = sum(int(z.get('car_count', 0)) for z in zones_data)
    total_gcar_z = len(gcar_zones)
    total_gcar_cars = sum(g['total_cars'] for g in gcar_zones)

    regions = sorted(set(z.get('region2', '') for z in zones_data if z.get('region2')))

    LAST_UPDATE_DEMAND = _read_last_update(LAST_UPDATE_DEMAND_FILE)
    LAST_UPDATE_ZONE = _read_last_update(LAST_UPDATE_ZONE_FILE)
    ngrok_url = _read_ngrok_url()

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>경기도 카셰어링 잠재 수요 지도</title>
{LEAFLET_CDN}
<style>
{SHARED_STYLES}
.zone-marker-wrap {{ background: none !important; border: none !important; }}
.zone-pin {{
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    color: #fff; font-size: 10px; font-weight: 700;
    box-shadow: 0 1px 4px rgba(0,0,0,0.2);
    border: 1.5px solid rgba(255,255,255,0.7);
    transition: transform 0.15s, box-shadow 0.15s, opacity 0.15s;
    cursor: pointer;
}}
.zone-pin:hover {{
    transform: scale(1.2);
    opacity: 1 !important;
    box-shadow: 0 3px 10px rgba(0,0,0,0.35);
}}
.scale-col {{ display: flex; flex-direction: column; align-items: center; gap: 2px; }}
.scale-col .scale-title {{ font-weight: 700; font-size: 10px; margin-bottom: 2px; }}
.scale-col .scale-bar {{ width: 18px; height: 120px; border-radius: 4px; }}
.scale-col .scale-labels {{ display: flex; flex-direction: column; justify-content: space-between; height: 120px; font-size: 9px; color: #8890a4; }}
.scale-row {{ display: flex; gap: 4px; align-items: stretch; }}
.sim-ctx {{
    position: absolute; z-index: 2000; background: #262b3e; border: 1px solid #3a3f55;
    border-radius: 8px; padding: 4px 0; box-shadow: 0 4px 16px rgba(0,0,0,0.5); display: none;
}}
.sim-ctx-item {{
    padding: 8px 16px; font-size: 12px; color: #c0c8e0; cursor: pointer; white-space: nowrap;
}}
.sim-ctx-item:hover {{ background: #303760; color: #fff; }}
.sim-panel {{
    position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); z-index: 2000;
    background: #1e2233; border: 1px solid #3a3f55; border-radius: 12px;
    padding: 24px 28px; box-shadow: 0 8px 32px rgba(0,0,0,0.6); width: 380px;
    display: none; color: #e0e0e0; font-size: 12px;
}}
.sim-panel h3 {{ font-size: 15px; font-weight: 700; color: #fff; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 1px solid #3a3f55; }}
.sim-row {{ display: flex; justify-content: space-between; align-items: center; padding: 5px 0; }}
.sim-row .sim-label {{ color: #8890a4; font-size: 11px; }}
.sim-row .sim-value {{ font-weight: 700; color: #fff; font-size: 13px; }}
.sim-section {{ border-top: 1px solid #3a3f55; margin-top: 10px; padding-top: 10px; }}
.sim-section-title {{ font-size: 10px; font-weight: 700; color: #6b7394; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }}
.sim-result {{
    margin-top: 14px; padding: 12px 16px; border-radius: 8px; text-align: center;
    font-size: 14px; font-weight: 700;
}}
.sim-result.recommend {{ background: rgba(39,174,96,0.15); border: 1px solid #27ae60; color: #27ae60; }}
.sim-result.not-recommend {{ background: rgba(231,76,60,0.15); border: 1px solid #e74c3c; color: #e74c3c; }}
.sim-close {{
    position: absolute; top: 12px; right: 14px; background: none; border: none;
    color: #8890a4; font-size: 18px; cursor: pointer; line-height: 1;
}}
.sim-close:hover {{ color: #fff; }}
.sim-overlay {{
    position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.4);
    z-index: 1999; display: none;
}}
.sim-marker-pin {{
    width: 32px; height: 32px; border-radius: 50%; background: rgba(231,76,60,0.8);
    border: 2px dashed #fff; display: flex; align-items: center; justify-content: center;
    color: #fff; font-size: 14px; font-weight: 700; animation: sim-pulse 1.5s infinite;
}}
@keyframes sim-pulse {{ 0%,100% {{ box-shadow: 0 0 0 0 rgba(231,76,60,0.5); }} 50% {{ box-shadow: 0 0 0 12px rgba(231,76,60,0); }} }}
</style>
</head>
<body>

<div class="sidebar">
    <div class="sidebar-header">
        <h1>경기도 카셰어링 잠재 수요 지도</h1>
        <div class="date">{THREE_MONTHS_AGO} ~ {TODAY}</div>
        <div class="date">업데이트: {TODAY}</div>
    </div>
    <div class="stat-row">
        <div class="stat-card"><span class="label">접속/주</span><span class="value">{total_a:,}</span></div>
        <div class="stat-card"><span class="label">예약/주</span><span class="value">{total_r:,}</span></div>
        <div class="stat-card"><span class="label">부름/주</span><span class="value">{total_d:,}</span></div>
    </div>
    <div class="stat-row">
        <div class="stat-card socar"><span class="label">쏘카 존</span><span class="value">{total_z:,}</span></div>
        <div class="stat-card socar"><span class="label">쏘카 차량</span><span class="value">{total_cars:,}</span></div>
    </div>
    <div class="stat-row">
        <div class="stat-card gcar"><span class="label">그린카 존</span><span class="value">{total_gcar_z:,}</span></div>
        <div class="stat-card gcar"><span class="label">그린카 차량</span><span class="value">{total_gcar_cars:,}</span></div>
    </div>
    <div class="sidebar-section">
        <div class="sidebar-section-title">지역 검색</div>
        <div style="position:relative;">
            <input type="text" id="regionSearch" placeholder="지역명 또는 존 이름 검색" style="width:100%;padding:7px 10px;border-radius:6px;border:1px solid #3a3f55;background:#262b3e;color:#c0c8e0;font-size:11px;box-sizing:border-box;">
            <div id="searchResults" style="display:none;position:absolute;top:100%;left:0;right:0;max-height:200px;overflow-y:auto;background:#262b3e;border:1px solid #3a3f55;border-top:none;border-radius:0 0 6px 6px;z-index:9999;"></div>
        </div>
    </div>
    <div class="sidebar-section">
        <div class="sidebar-section-title">데이터 레이어</div>
        <button class="sidebar-btn" id="toggleAccess"><span class="dot" style="background:#fb8c00"></span>앱 접속</button>
        <button class="sidebar-btn" id="toggleRes"><span class="dot" style="background:#1e88e5"></span>예약 생성</button>
        <button class="sidebar-btn" id="toggleDtod"><span class="dot" style="background:#00bcd4"></span>부름 호출</button>
        <button class="sidebar-btn" id="toggleZones"><span class="dot" style="background:#27ae60"></span>운영 존</button>
        <button class="sidebar-btn" id="toggleGcar"><span class="dot" style="background:#ff6f00"></span>그린카 존</button>
    </div>
    <div class="sidebar-section">
        <div class="sidebar-section-title">분석</div>
        <button class="sidebar-btn" id="toggleGap"><span class="dot" style="background:#8e44ad"></span>미진출 지역 분석</button>
        <button class="sidebar-btn" id="toggleAnalysis"><span class="dot" style="background:#ef5350"></span>공급 분석</button>
        <button class="sidebar-btn" id="toggleMarketShare"><span class="dot" style="background:#ff7043"></span>Market Share</button>
    </div>
    <div class="sidebar-section">
        <div class="sidebar-section-title">업데이트</div>
        <button class="update-btn" id="updateDemandBtn" onclick="runUpdateDemand()">수요 업데이트</button>
        <div class="update-time" id="demandUpdateTime">마지막: {LAST_UPDATE_DEMAND}</div>
        <button class="update-btn" id="updateZoneBtn" onclick="runUpdateZone()">존/실적 업데이트</button>
        <div class="update-time" id="zoneUpdateTime">마지막: {LAST_UPDATE_ZONE}</div>
    </div>
    <div class="legend-row">
        <div class="legend-item"><span class="legend-dot" style="background:#27ae60"></span>일반</div>
        <div class="legend-item"><span class="legend-dot" style="background:#e67e22"></span>스테이션</div>
        <div class="legend-item"><span class="legend-dot" style="background:#8e44ad"></span>부름우선</div>
        <div class="legend-item"><span class="legend-dot" style="background:#ff6f00"></span>그린카</div>
    </div>
</div>

<div class="sim-ctx" id="simCtx"><div class="sim-ctx-item" id="simCtxBtn">존 개설 시뮬레이션</div></div>
<div class="sim-overlay" id="simOverlay"></div>
<div class="sim-panel" id="simPanel">
    <button class="sim-close" id="simClose">&times;</button>
    <h3>존 개설 시뮬레이션</h3>
    <div id="simContent"></div>
</div>

<div class="gap-panel" id="gapPanel">
    <h3>미진출 지역 분석</h3>
    <div style="font-size:11px;color:#6b7394;margin-bottom:8px;">앱 접속 월평균 30건 이상, 반경 800m 내 운영 존 없음 (서울/인천 제외)</div>
    <div id="gapList"></div>
</div>

<div class="gap-panel" id="analysisPanel" style="width:820px;">
    <h3>공급 분석</h3>
    <div style="display:flex;gap:4px;margin-bottom:10px;">
        <button id="tabGrowth" class="analysis-tab active" onclick="switchAnalysisTab('growth')">수요 성장</button>
        <button id="tabDecline" class="analysis-tab" onclick="switchAnalysisTab('decline')">수요 감소</button>
    </div>
    <div style="font-size:11px;color:#6b7394;margin-bottom:10px;" id="analysisDesc">예약 점유율 상승 추세 지역 | 증감·그래프 = 경기도 대비 점유율 변화 (경기도 전체 행은 절대값)</div>
    <table style="width:100%;border-collapse:collapse;font-size:11px;">
        <thead><tr>
            <th data-col="region2" style="text-align:left;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">지역</th>
            <th data-col="access_weekly" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">접속/주</th>
            <th style="padding:4px 6px;border-bottom:2px solid #3a3f55;color:#8890a4;">추이</th>
            <th data-col="access_growth" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">증감</th>
            <th data-col="res_weekly" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">예약/주</th>
            <th style="padding:4px 6px;border-bottom:2px solid #3a3f55;color:#8890a4;">추이</th>
            <th data-col="res_growth" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">증감</th>
            <th data-col="cars" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">차량</th>
            <th style="padding:4px 6px;border-bottom:2px solid #3a3f55;color:#8890a4;">추이</th>
            <th data-col="car_growth" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">증감</th>
            <th data-col="status" style="text-align:center;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">판정</th>
        </tr></thead>
        <tbody id="analysisBody"></tbody>
    </table>
</div>

<div class="gap-panel" id="marketSharePanel" style="width:580px;">
    <h3>Market Share 분석 — 쏘카 vs 그린카</h3>
    <div style="font-size:11px;color:#6b7394;margin-bottom:10px;">지역별 차량수 기준 점유율 비교 | 쏘카 점유율 낮은 순 정렬</div>
    <table style="width:100%;border-collapse:collapse;font-size:11px;">
        <thead><tr>
            <th data-ms="region2" style="text-align:left;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">지역</th>
            <th data-ms="socar_cars" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">쏘카 차량</th>
            <th data-ms="socar_zones" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">쏘카 존</th>
            <th data-ms="gcar_cars" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">그린카 차량</th>
            <th data-ms="gcar_zones" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">그린카 존</th>
            <th data-ms="socar_share" style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;cursor:pointer;color:#8890a4;">쏘카 점유율</th>
        </tr></thead>
        <tbody id="marketShareBody"></tbody>
    </table>
</div>

<div class="heatmap-scale" id="heatmapScale">
    <div class="scale-col">
        <span class="scale-title" style="color:#e65100">접속/주</span>
        <div class="scale-row">
            <div class="scale-bar" style="background:linear-gradient(to bottom, #e65100, #ef6c00, #fb8c00, #ffa726, #ffcc80, #ffe0b2, #fff3e0)"></div>
            <div class="scale-labels" id="accessScaleLabels"><span>-</span><span>-</span><span>-</span></div>
        </div>
    </div>
    <div class="scale-col">
        <span class="scale-title" style="color:#0d47a1">예약/주</span>
        <div class="scale-row">
            <div class="scale-bar" style="background:linear-gradient(to bottom, #0d47a1, #1565c0, #1e88e5, #42a5f5, #90caf9, #bbdefb, #e3f2fd)"></div>
            <div class="scale-labels" id="resScaleLabels"><span>-</span><span>-</span><span>-</span></div>
        </div>
    </div>
</div>

<div class="gap-panel" id="timelinePanel" style="width:720px;max-width:90vw;">
    <div style="display:flex;justify-content:space-between;align-items:center;">
        <h3 id="timelineTitle">예약 현황</h3>
        <span style="cursor:pointer;font-size:18px;color:#888;" onclick="document.getElementById('timelinePanel').style.display='none'">&times;</span>
    </div>
    <div style="font-size:11px;color:#6b7394;margin-bottom:8px;">차량별 예약 타임라인 (전후 1주)</div>
    <div id="timelineContent" style="overflow-x:auto;"></div>
</div>

<div id="map"></div>

<script>
var accessData = {jd(access_heat)};
var resData = {jd(res_heat)};
var dtodData = {jd(dtod_dots)};
var zonesData = {jd(zones_data)};
var gapsData = {jd(gaps)};
var analysisGrowth = {jd(analysis_growth)};
var analysisDecline = {jd(analysis_decline)};
var gcarData = {jd(gcar_zones)};
var marketShareData = {jd(market_share)};
var timelineData = {jd(timeline_data)};
var regions = {jd(regions)};
var lastUpdateDemand = '{LAST_UPDATE_DEMAND}';
var lastUpdateZone = '{LAST_UPDATE_ZONE}';
document.getElementById('demandUpdateTime').textContent = '마지막: ' + lastUpdateDemand;
document.getElementById('zoneUpdateTime').textContent = '마지막: ' + lastUpdateZone;
// 로컬 서버 실행 시 최신 값으로 갱신
fetch('/api/status').then(function(r){{return r.json();}}).then(function(d){{
    lastUpdateDemand = d.last_update_demand || lastUpdateDemand;
    lastUpdateZone = d.last_update_zone || lastUpdateZone;
    document.getElementById('demandUpdateTime').textContent = '마지막: ' + lastUpdateDemand;
    document.getElementById('zoneUpdateTime').textContent = '마지막: ' + lastUpdateZone;
}}).catch(function(){{}});

// 검색 기능
var searchInput = document.getElementById('regionSearch');
var searchResults = document.getElementById('searchResults');
var searchItemStyle = 'padding:6px 10px;cursor:pointer;font-size:11px;color:#c0c8e0;border-bottom:1px solid #2a2f45;';

function doSearch(query) {{
    searchResults.innerHTML = '';
    if (!query || query.length < 1) {{ searchResults.style.display = 'none'; return; }}
    var q = query.toLowerCase();
    var matches = [];
    // 지역(region2) 매칭
    regions.forEach(function(r) {{
        if (r.replace(/\\u3000/g, ' ').toLowerCase().indexOf(q) >= 0) {{
            matches.push({{ type: 'region', label: r.replace(/\\u3000/g, ' '), value: r }});
        }}
    }});
    // 존 이름 매칭
    zonesData.forEach(function(z) {{
        if (z.zone_name.toLowerCase().indexOf(q) >= 0 || z.parking_name.toLowerCase().indexOf(q) >= 0) {{
            matches.push({{ type: 'zone', label: z.zone_name + ' (' + z.region2.replace(/\\u3000/g, ' ') + ')', value: z }});
        }}
    }});
    if (matches.length === 0) {{
        searchResults.innerHTML = '<div style="' + searchItemStyle + 'color:#6b7394;">결과 없음</div>';
        searchResults.style.display = 'block';
        return;
    }}
    matches.slice(0, 20).forEach(function(m) {{
        var div = document.createElement('div');
        div.style.cssText = searchItemStyle;
        div.textContent = (m.type === 'region' ? '📍 ' : '🅿️ ') + m.label;
        div.addEventListener('mouseenter', function() {{ this.style.background = '#3a4060'; }});
        div.addEventListener('mouseleave', function() {{ this.style.background = 'transparent'; }});
        div.addEventListener('click', function() {{
            searchResults.style.display = 'none';
            if (m.type === 'region') {{
                filterByRegion(m.value);
                searchInput.value = m.label;
            }} else {{
                filterByRegion('');
                searchInput.value = m.label;
                map.setView([m.value.lat, m.value.lng], 16);
                allZoneMarkers.forEach(function(mk) {{
                    if (mk._zoneData.zone_id === m.value.zone_id) mk.openPopup();
                }});
            }}
        }});
        searchResults.appendChild(div);
    }});
    searchResults.style.display = 'block';
}}
searchInput.addEventListener('input', function() {{ doSearch(this.value); }});
searchInput.addEventListener('focus', function() {{ if (this.value) doSearch(this.value); }});
document.addEventListener('click', function(e) {{
    if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) searchResults.style.display = 'none';
}});
// 전체 보기 복원: 입력 비우면 리셋
searchInput.addEventListener('keydown', function(e) {{
    if (e.key === 'Escape') {{ this.value = ''; searchResults.style.display = 'none'; filterByRegion(''); }}
}});

function filterByRegion(region) {{
    zoneLayer.clearLayers();
    allZoneMarkers.forEach(function(m) {{
        if (!region || m._zoneData.region2 === region) zoneLayer.addLayer(m);
    }});
    if (region) {{
        var rz = zonesData.filter(function(z) {{ return z.region2 === region; }});
        if (rz.length > 0) {{
            var lats = rz.map(function(z) {{ return z.lat; }});
            var lngs = rz.map(function(z) {{ return z.lng; }});
            var pad = 0.03;
            map.fitBounds([
                [Math.min.apply(null, lats) - pad, Math.min.apply(null, lngs) - pad],
                [Math.max.apply(null, lats) + pad, Math.max.apply(null, lngs) + pad]
            ]);
        }}
    }} else {{
        map.setView([37.41, 127.0], 9);
    }}
}}

var map = L.map('map', {{ zoomControl: true }}).setView([37.41, 127.0], 9);
{TILE_SETUP}
{ZONE_JS}

var maxAccessVal = Math.max.apply(null, accessData.map(function(d) {{ return d[2]; }}));
var maxResVal = Math.max.apply(null, resData.map(function(d) {{ return d[2]; }}));

function formatCount(n) {{ if (n >= 10000) return (n/10000).toFixed(1) + '만'; if (n >= 1000) return (n/1000).toFixed(1) + '천'; return n.toString(); }}

// 접속 건수 → 색상 (주황 스펙트럼)
function accessColor(val) {{
    var ratio = Math.min(1, Math.log(val + 1) / Math.log(maxAccessVal + 1));
    var colors = [
        [255,243,224], [255,204,128], [255,167,38],
        [251,140,0], [239,108,0], [230,81,0]
    ];
    var idx = ratio * (colors.length - 1);
    var lo = Math.floor(idx), hi = Math.min(lo + 1, colors.length - 1);
    var t = idx - lo;
    var r = Math.round(colors[lo][0] + (colors[hi][0] - colors[lo][0]) * t);
    var g = Math.round(colors[lo][1] + (colors[hi][1] - colors[lo][1]) * t);
    var b = Math.round(colors[lo][2] + (colors[hi][2] - colors[lo][2]) * t);
    return 'rgb(' + r + ',' + g + ',' + b + ')';
}}

// 예약 건수 → 색상 (파랑 스펙트럼)
function resColor(val) {{
    var ratio = Math.min(1, Math.log(val + 1) / Math.log(maxResVal + 1));
    var colors = [
        [227,242,253], [144,202,249], [66,165,245],
        [30,136,229], [21,101,192], [13,71,161]
    ];
    var idx = ratio * (colors.length - 1);
    var lo = Math.floor(idx), hi = Math.min(lo + 1, colors.length - 1);
    var t = idx - lo;
    var r = Math.round(colors[lo][0] + (colors[hi][0] - colors[lo][0]) * t);
    var g = Math.round(colors[lo][1] + (colors[hi][1] - colors[lo][1]) * t);
    var b = Math.round(colors[lo][2] + (colors[hi][2] - colors[lo][2]) * t);
    return 'rgb(' + r + ',' + g + ',' + b + ')';
}}

function dotRadius(val, maxVal) {{
    return Math.max(4, Math.min(14, 3 + Math.sqrt(val / maxVal) * 11));
}}
function dotOpacity(val, maxVal) {{
    return Math.max(0.82, Math.min(0.97, 0.8 + (val / maxVal) * 0.17));
}}

var accessLayer = L.layerGroup();
accessData.forEach(function(d) {{
    L.circleMarker([d[0], d[1]], {{
        radius: dotRadius(d[2], maxAccessVal),
        fillColor: accessColor(d[2]),
        color: 'rgba(191,54,12,0.7)', weight: 1.5,
        fillOpacity: dotOpacity(d[2], maxAccessVal)
    }}).bindPopup('<div style="font-weight:600">앱 접속 지점</div><div>주평균 접속: <b>' + d[2].toLocaleString() + '</b></div>').addTo(accessLayer);
}});
accessLayer.addTo(map);

var resLayer = L.layerGroup();
resData.forEach(function(d) {{
    L.circleMarker([d[0], d[1]], {{
        radius: dotRadius(d[2], maxResVal),
        fillColor: resColor(d[2]),
        color: 'rgba(13,71,161,0.7)', weight: 1.5,
        fillOpacity: dotOpacity(d[2], maxResVal)
    }}).bindPopup('<div style="font-weight:600">예약 생성 지점</div><div>주평균 예약: <b>' + d[2].toLocaleString() + '</b></div>').addTo(resLayer);
}});
resLayer.addTo(map);

// 스케일 라벨 설정
document.getElementById('accessScaleLabels').innerHTML =
    '<span>' + formatCount(Math.round(maxAccessVal)) + '</span>' +
    '<span>' + formatCount(Math.round(maxAccessVal * 0.5)) + '</span>' +
    '<span>0</span>';
document.getElementById('resScaleLabels').innerHTML =
    '<span>' + formatCount(Math.round(maxResVal)) + '</span>' +
    '<span>' + formatCount(Math.round(maxResVal * 0.5)) + '</span>' +
    '<span>0</span>';

var zoneLayer = L.layerGroup();
var allZoneMarkers = [];
zonesData.forEach(function(z) {{
    var m = L.marker([z.lat, z.lng], {{ icon: makeZoneIcon(z) }}).bindPopup(makePopup(z)).on('click', function() {{ showTimeline(z.zone_id, z.zone_name); }});
    m._zoneData = z;
    allZoneMarkers.push(m);
    zoneLayer.addLayer(m);
}});
zoneLayer.addTo(map);

var dtodLayer = L.layerGroup();
var maxDtodVal = dtodData.reduce(function(m,d){{ return Math.max(m,d[2]); }}, 1);
dtodData.forEach(function(d) {{
    L.circleMarker([d[0], d[1]], {{
        radius: dotRadius(d[2], maxDtodVal), fillColor: '#ff4081', color: '#fff',
        weight: 1.5, fillOpacity: dotOpacity(d[2], maxDtodVal)
    }}).bindPopup('<div style="font-weight:600">부름 호출 지점</div><div>주평균 호출: <b>' + d[2].toLocaleString() + '</b></div>').addTo(dtodLayer);
}});

var gapLayer = L.layerGroup();
gapsData.forEach(function(g) {{
    var total = (g.access_count || 0) + (g.reservation_count || 0);
    L.circleMarker([g.lat, g.lng], {{
        radius: Math.max(6, Math.min(18, 5 + Math.log10(total + 1) * 2)),
        fillColor: '#e74c3c', color: '#c0392b', weight: 2, opacity: 0.9, fillOpacity: 0.5
    }}).bindPopup(
        '<div class="popup-title">' + (g.name || '(' + g.lat + ', ' + g.lng + ')') + '</div>' +
        '<div class="popup-row"><span class="popup-label">접속(월평균)</span><b>' + (g.access_count || 0).toLocaleString() + '</b></div>' +
        '<div class="popup-row"><span class="popup-label">예약(월평균)</span><b>' + (g.reservation_count || 0).toLocaleString() + '</b></div>' +
        '<div class="popup-row"><span class="popup-label">최근접 존</span><b>' + (g.nearest_zone_km || '-') + 'km</b></div>'
    ).addTo(gapLayer);
}});

var gapListDiv = document.getElementById('gapList');
gapsData.sort(function(a,b) {{ return (b.access_count + b.reservation_count) - (a.access_count + a.reservation_count); }});
gapsData.forEach(function(g) {{
    var row = document.createElement('div');
    row.className = 'gap-row';
    row.innerHTML = '<span class="gap-name">' + (g.name || '(' + g.lat.toFixed(3) + ', ' + g.lng.toFixed(3) + ')') + '</span>' +
        '<span class="gap-cnt" style="font-size:10px">접속 ' + (g.access_count||0).toLocaleString() + '/월 | 예약 ' + (g.reservation_count||0).toLocaleString() + '/월</span>';
    row.style.cursor = 'pointer';
    row.addEventListener('click', function() {{ map.setView([g.lat, g.lng], 15); }});
    gapListDiv.appendChild(row);
}});

// 그린카 존 레이어
var gcarLayer = L.layerGroup();
gcarData.forEach(function(g) {{
    var size = Math.max(18, Math.min(32, 16 + g.total_cars * 1.5));
    var html = '<div class="zone-pin" style="width:' + size + 'px;height:' + size + 'px;background:rgba(255,111,0,0.75);border-color:rgba(255,200,100,0.8);font-size:9px;">' + g.total_cars + '</div>';
    var icon = L.divIcon({{
        className: 'zone-marker-wrap',
        html: html,
        iconSize: [size, size],
        iconAnchor: [size/2, size/2],
        popupAnchor: [0, -size/2]
    }});
    L.marker([g.lat, g.lng], {{ icon: icon }}).bindPopup(
        '<div class="popup-title" style="color:#ff6f00">' + g.zone_name + '</div>' +
        '<div class="popup-row"><span class="popup-label">존 ID</span><span style="color:#666;font-family:monospace">' + g.zone_id + '</span></div>' +
        '<div class="popup-row"><span class="popup-label">지역</span><span>' + g.sig_name + '</span></div>' +
        '<div class="popup-row"><span class="popup-label">차량</span><b>' + g.total_cars + '대</b></div>' +
        '<div style="margin-top:4px;font-size:10px;color:#ff6f00;font-weight:600;">그린카</div>'
    ).addTo(gcarLayer);
}});

// Market Share 테이블
var msSortCol = 'socar_share', msSortAsc = true;
function renderMarketShare() {{
    var sorted = marketShareData.slice().sort(function(a, b) {{
        if (msSortCol === 'region2') return msSortAsc ? a.region2.localeCompare(b.region2) : b.region2.localeCompare(a.region2);
        return msSortAsc ? a[msSortCol] - b[msSortCol] : b[msSortCol] - a[msSortCol];
    }});
    var totSocar = 0, totGcar = 0;
    marketShareData.forEach(function(d) {{ totSocar += d.socar_cars; totGcar += d.gcar_cars; }});
    var avgShare = (totSocar + totGcar) > 0 ? (totSocar / (totSocar + totGcar) * 100).toFixed(1) : '0';
    var avgStyle = 'background:#262b3e;font-weight:700;color:#fff;';
    var html = '<tr style="' + avgStyle + '">' +
        '<td style="padding:4px 6px;border-bottom:2px solid #3a3f55;">경기도 전체</td>' +
        '<td style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;">' + totSocar + '</td>' +
        '<td style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;">-</td>' +
        '<td style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;">' + totGcar + '</td>' +
        '<td style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;">-</td>' +
        '<td style="text-align:right;padding:4px 6px;border-bottom:2px solid #3a3f55;">' + avgShare + '%</td></tr>';
    sorted.forEach(function(d) {{
        var shareColor = d.socar_share < 40 ? 'color:#c62828;font-weight:700' : d.socar_share < 50 ? 'color:#e65100;font-weight:600' : d.socar_share >= 70 ? 'color:#2e7d32;font-weight:600' : '';
        var barW = d.socar_share.toFixed(0);
        html += '<tr>' +
            '<td style="padding:4px 6px;border-bottom:1px solid #2a2f45">' + d.region2.replace(/\\u3000/g,' ') + '</td>' +
            '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid #2a2f45">' + d.socar_cars + '</td>' +
            '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid #2a2f45">' + d.socar_zones + '</td>' +
            '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid #2a2f45">' + d.gcar_cars + '</td>' +
            '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid #2a2f45">' + d.gcar_zones + '</td>' +
            '<td style="text-align:right;padding:4px 6px;border-bottom:1px solid #2a2f45;position:relative;' + shareColor + '">' +
                '<div style="position:absolute;top:2px;bottom:2px;left:0;width:' + barW + '%;background:#42a5f5;opacity:0.2;border-radius:3px"></div>' +
                d.socar_share.toFixed(1) + '%</td></tr>';
    }});
    document.getElementById('marketShareBody').innerHTML = html;
}}
document.querySelectorAll('#marketSharePanel th').forEach(function(th) {{
    th.addEventListener('click', function() {{
        var col = this.dataset.ms;
        if (msSortCol === col) msSortAsc = !msSortAsc;
        else {{ msSortCol = col; msSortAsc = col === 'socar_share'; }}
        renderMarketShare();
    }});
}});
renderMarketShare();

var showAccess = true, showRes = true, showZones = true, showGap = false, showAnalysis = false, showDtod = false, showGcar = false, showMarketShare = false;
function styleBtn(btn, active) {{
    if (active) btn.classList.add('active');
    else btn.classList.remove('active');
}}

document.getElementById('toggleAccess').addEventListener('click', function() {{
    showAccess = !showAccess;
    showAccess ? accessLayer.addTo(map) : map.removeLayer(accessLayer);
    styleBtn(this, showAccess);
}});
document.getElementById('toggleRes').addEventListener('click', function() {{
    showRes = !showRes;
    showRes ? resLayer.addTo(map) : map.removeLayer(resLayer);
    styleBtn(this, showRes);
}});
document.getElementById('toggleDtod').addEventListener('click', function() {{
    showDtod = !showDtod;
    showDtod ? dtodLayer.addTo(map) : map.removeLayer(dtodLayer);
    styleBtn(this, showDtod);
}});
document.getElementById('toggleZones').addEventListener('click', function() {{
    showZones = !showZones;
    showZones ? zoneLayer.addTo(map) : map.removeLayer(zoneLayer);
    styleBtn(this, showZones);
}});
document.getElementById('toggleGap').addEventListener('click', function() {{
    var wasOn = showGap;
    hidePanels();
    if (!wasOn) {{ showGap = true; document.getElementById('gapPanel').style.display = 'block'; gapLayer.addTo(map); }}
    styleBtn(this, showGap);
}});

// 공급 분석 테이블
var sortCol = 'status', sortAsc = true;
var analysisTab = 'growth';

function sparkSvg(vals, color) {{
    if (!vals || vals.length < 2) return '';
    var w = 60, h = 18, pad = 1;
    var mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals);
    var range = mx - mn || 1;
    var pts = vals.map(function(v, i) {{
        var x = pad + i * ((w - 2 * pad) / (vals.length - 1));
        var y = h - pad - (v - mn) / range * (h - 2 * pad);
        return x.toFixed(1) + ',' + y.toFixed(1);
    }});
    return '<svg width="' + w + '" height="' + h + '" style="vertical-align:middle">' +
        '<polyline points="' + pts.join(' ') + '" fill="none" stroke="' + color + '" stroke-width="1.5" stroke-linecap="round"/>' +
        '</svg>';
}}

function switchAnalysisTab(tab) {{
    analysisTab = tab;
    sortCol = 'status'; sortAsc = true;
    document.getElementById('tabGrowth').classList.toggle('active', tab === 'growth');
    document.getElementById('tabDecline').classList.toggle('active', tab === 'decline');
    document.getElementById('analysisDesc').textContent = tab === 'growth'
        ? '예약 점유율 상승 추세 지역 | 증감·그래프 = 경기도 대비 점유율 변화 (경기도 전체 행은 절대값)'
        : '예약 점유율 하락 추세 지역 | 증감·그래프 = 경기도 대비 점유율 변화 (경기도 전체 행은 절대값)';
    renderAnalysis();
}}

function renderAnalysis() {{
    var data = analysisTab === 'growth' ? analysisGrowth : analysisDecline;
    var statusOrd = analysisTab === 'growth'
        ? {{'점검 필요':0,'증차 검토':1,'대응 진행 중':2,'-':99}}
        : {{'점검 필요':0,'감차 검토':1,'대응 진행 중':2,'-':99}};
    var emptyMsg = analysisTab === 'growth' ? '수요 성장 지역 없음' : '수요 감소 지역 없음';
    var ggRow = null;
    var rest = [];
    data.forEach(function(d) {{
        if (d.region2 === '경기도 전체') ggRow = d;
        else rest.push(d);
    }});
    rest.sort(function(a, b) {{
        if (sortCol === 'region2') return sortAsc ? a.region2.localeCompare(b.region2) : b.region2.localeCompare(a.region2);
        if (sortCol === 'status') {{
            var d = (statusOrd[a.status]||9) - (statusOrd[b.status]||9);
            if (d !== 0) return sortAsc ? d : -d;
            return analysisTab === 'growth' ? b.res_growth - a.res_growth : a.res_growth - b.res_growth;
        }}
        return sortAsc ? a[sortCol] - b[sortCol] : b[sortCol] - a[sortCol];
    }});
    var sorted = ggRow ? [ggRow].concat(rest) : rest;

    var html = '';
    var statusColors = {{'점검 필요':'#e53935','증차 검토':'#ff9800','감차 검토':'#ff9800','대응 진행 중':'#43a047'}};
    sorted.forEach(function(d, idx) {{
        var sc = statusColors[d.status] || '#8890a4';
        var isGg = d.region2 === '경기도 전체';
        var rowStyle = isGg ? 'background:#1a1f33;font-weight:600;' : '';
        var borderStyle = isGg ? 'border-bottom:2px solid #3a3f55' : 'border-bottom:1px solid #2a2f45';
        var accColor = d.access_growth >= 0 ? '#4fc3f7' : '#ef5350';
        var resColor = d.res_growth >= 0 ? '#4fc3f7' : '#ef5350';
        var carColor = d.car_growth >= 0 ? '#66bb6a' : '#ef5350';
        html += '<tr style="' + rowStyle + '">' +
            '<td style="padding:4px 6px;' + borderStyle + ';white-space:nowrap">' + d.region2.replace(/\\u3000/g,' ') + '</td>' +
            '<td style="text-align:right;padding:4px 6px;' + borderStyle + '">' + d.access_weekly.toLocaleString() + '</td>' +
            '<td style="padding:2px 4px;' + borderStyle + '">' + sparkSvg(d.access_trend, accColor) + '</td>' +
            '<td style="text-align:right;padding:4px 6px;' + borderStyle + ';color:' + accColor + ';font-weight:600">' + (d.access_growth >= 0 ? '+' : '') + d.access_growth.toFixed(1) + '%</td>' +
            '<td style="text-align:right;padding:4px 6px;' + borderStyle + '">' + d.res_weekly.toLocaleString() + '</td>' +
            '<td style="padding:2px 4px;' + borderStyle + '">' + sparkSvg(d.res_trend, resColor) + '</td>' +
            '<td style="text-align:right;padding:4px 6px;' + borderStyle + ';color:' + resColor + ';font-weight:600">' + (d.res_growth >= 0 ? '+' : '') + d.res_growth.toFixed(1) + '%</td>' +
            '<td style="text-align:right;padding:4px 6px;' + borderStyle + '">' + d.cars + '</td>' +
            '<td style="padding:2px 4px;' + borderStyle + '">' + sparkSvg(d.car_trend, carColor) + '</td>' +
            '<td style="text-align:right;padding:4px 6px;' + borderStyle + ';color:' + carColor + '">' + (d.car_growth >= 0 ? '+' : '') + d.car_growth.toFixed(1) + '%</td>' +
            (isGg ? '<td style="text-align:center;padding:4px 6px;' + borderStyle + ';color:#6b7394">기준선</td>' :
            '<td style="text-align:center;padding:4px 6px;' + borderStyle + '"><span style="background:' + sc + ';color:#fff;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;white-space:nowrap;">' + d.status + '</span></td>') +
            '</tr>';
    }});
    if (sorted.length <= 1) html += '<tr><td colspan="11" style="padding:12px;text-align:center;color:#6b7394;">' + emptyMsg + '</td></tr>';
    document.getElementById('analysisBody').innerHTML = html;
}}
document.querySelectorAll('#analysisPanel th').forEach(function(th) {{
    th.addEventListener('click', function() {{
        var col = this.dataset.col;
        if (sortCol === col) sortAsc = !sortAsc;
        else {{ sortCol = col; sortAsc = false; }}
        renderAnalysis();
    }});
}});
renderAnalysis();

function hidePanels() {{
    ['gapPanel','analysisPanel','marketSharePanel'].forEach(function(id) {{ document.getElementById(id).style.display = 'none'; }});
    showGap = false; showAnalysis = false; showMarketShare = false;
    map.removeLayer(gapLayer);
    styleBtn(document.getElementById('toggleGap'), false);
    styleBtn(document.getElementById('toggleAnalysis'), false);
    styleBtn(document.getElementById('toggleMarketShare'), false);
}}

document.getElementById('toggleAnalysis').addEventListener('click', function() {{
    var wasOn = showAnalysis;
    hidePanels();
    if (!wasOn) {{ showAnalysis = true; document.getElementById('analysisPanel').style.display = 'block'; }}
    styleBtn(this, showAnalysis);
}});

document.getElementById('toggleGcar').addEventListener('click', function() {{
    showGcar = !showGcar;
    showGcar ? gcarLayer.addTo(map) : map.removeLayer(gcarLayer);
    styleBtn(this, showGcar);
}});

document.getElementById('toggleMarketShare').addEventListener('click', function() {{
    var wasOn = showMarketShare;
    hidePanels();
    if (!wasOn) {{ showMarketShare = true; document.getElementById('marketSharePanel').style.display = 'block'; }}
    styleBtn(this, showMarketShare);
}});

styleBtn(document.getElementById('toggleAccess'), true);
styleBtn(document.getElementById('toggleRes'), true);
styleBtn(document.getElementById('toggleDtod'), false);
styleBtn(document.getElementById('toggleZones'), true);
styleBtn(document.getElementById('toggleGap'), false);
styleBtn(document.getElementById('toggleAnalysis'), false);
styleBtn(document.getElementById('toggleGcar'), false);
styleBtn(document.getElementById('toggleMarketShare'), false);

// 업데이트 공통 함수
function doUpdate(endpoint, btn, label) {{
    if (!confirm(label + '를 실행하시겠습니까?\\nBigQuery 조회 후 페이지가 새로고침됩니다.')) return;
    btn.disabled = true;
    btn.classList.add('updating');
    btn.textContent = label + ' 중...';
    fetch(endpoint, {{ method: 'POST' }})
        .then(function(r) {{ return r.json(); }})
        .then(function(data) {{
            if (data.success) {{
                alert(label + ' 완료! 페이지를 새로고침합니다.');
                location.reload();
            }} else {{
                alert(label + ' 실패: ' + (data.error || '알 수 없는 오류'));
                btn.disabled = false; btn.classList.remove('updating'); btn.textContent = label;
            }}
        }})
        .catch(function(e) {{
            alert('서버에 연결할 수 없습니다.\\nserver.py가 실행 중인지 확인하세요.');
            btn.disabled = false; btn.classList.remove('updating'); btn.textContent = label;
        }});
}}
function runUpdateDemand() {{
    doUpdate('/api/update-demand', document.getElementById('updateDemandBtn'), '수요 업데이트');
}}
function runUpdateZone() {{
    doUpdate('/api/update-zone', document.getElementById('updateZoneBtn'), '존/실적 업데이트');
}}

// 예약 타임라인 렌더링
var wayColors = {{ 'round':'#5c6bc0', 'handle':'#5c6bc0', 'd2d_oneway':'#00897b', 'd2d_round':'#43a047', 'd2d_rev':'#43a047', 'z2d_oneway':'#00897b', 'block':'#555c6e' }};
var wayLabels = {{ 'round':'', 'handle':'', 'd2d_oneway':'부름', 'd2d_round':'부름', 'd2d_rev':'부름', 'z2d_oneway':'부름', 'block':'블락' }};
function showTimeline(zoneId, zoneName) {{
    var panel = document.getElementById('timelinePanel');
    var content = document.getElementById('timelineContent');
    document.getElementById('timelineTitle').textContent = zoneName + ' — 예약 현황';
    var reservations = timelineData[String(zoneId)] || [];
    if (reservations.length === 0) {{
        content.innerHTML = '<div style="color:#999;padding:20px;text-align:center">예약 데이터 없음</div>';
        panel.style.display = 'block';
        return;
    }}
    // 날짜 범위: 오늘 ±7일
    var now = new Date();
    var startDate = new Date(now); startDate.setDate(startDate.getDate() - 7); startDate.setHours(0,0,0,0);
    var endDate = new Date(now); endDate.setDate(endDate.getDate() + 7); endDate.setHours(23,59,59,999);
    var totalMs = endDate - startDate;
    var days = [];
    for (var d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {{
        days.push(new Date(d));
    }}
    // 차량별 그룹핑
    var cars = {{}};
    var carOrder = [];
    reservations.forEach(function(r) {{
        var key = r.car_id;
        if (!cars[key]) {{ cars[key] = {{ car_name: r.car_name, car_num: r.car_num, reservations: [] }}; carOrder.push(key); }}
        cars[key].reservations.push(r);
    }});
    // 요일 이름
    var dayNames = ['일','월','화','수','목','금','토'];
    // 테이블 생성
    var numDays = days.length;
    var html = '<table style="border-collapse:collapse;font-size:10px;width:100%;min-width:600px;color:#e0e0e0;table-layout:fixed;">';
    // colgroup으로 너비 고정
    html += '<colgroup><col style="width:110px;min-width:110px;">';
    for (var ci = 0; ci < numDays; ci++) html += '<col style="width:' + (100/numDays).toFixed(2) + '%;">';
    html += '</colgroup>';
    html += '<thead><tr><th style="padding:2px 4px;border:1px solid #2a2f45;position:sticky;left:0;background:#1e2233;z-index:1;color:#8890a4;">차량</th>';
    days.forEach(function(day) {{
        var mm = String(day.getMonth()+1).padStart(2,'0');
        var dd = String(day.getDate()).padStart(2,'0');
        var dn = dayNames[day.getDay()];
        var isToday = day.toDateString() === now.toDateString();
        var isWeekend = day.getDay() === 0 || day.getDay() === 6;
        var bg = isToday ? '#3a3520' : isWeekend ? '#35202a' : '#1e2233';
        html += '<th style="padding:2px 1px;border:1px solid #2a2f45;font-size:9px;background:' + bg + ';color:#8890a4;">' + mm + '-' + dd + '<br>(' + dn + ')</th>';
    }});
    html += '</tr></thead><tbody>';
    carOrder.forEach(function(carId) {{
        var car = cars[carId];
        html += '<tr><td style="padding:3px 4px;border:1px solid #2a2f45;white-space:nowrap;position:sticky;left:0;background:#1e2233;z-index:1;font-size:9px;"><b style="color:#fff">' + car.car_name + '</b><br><span style="color:#6b7394">' + car.car_num + '</span></td>';
        days.forEach(function(day, di) {{
            var dayStart = new Date(day); dayStart.setHours(0,0,0,0);
            var dayEnd = new Date(day); dayEnd.setHours(23,59,59,999);
            var isToday = day.toDateString() === now.toDateString();
            var isWeekend = day.getDay() === 0 || day.getDay() === 6;
            var bg = isToday ? '#2a2820' : isWeekend ? '#2a2025' : '#262b3e';
            html += '<td style="padding:0;border-top:1px solid #2a2f45;border-bottom:1px solid #2a2f45;border-left:1px solid #1e2538;border-right:1px solid #1e2538;height:28px;position:relative;overflow:visible;background:' + bg + '">';
            car.reservations.forEach(function(rv) {{
                var rs = new Date(rv.start.replace(' ', 'T'));
                var re = new Date(rv.end.replace(' ', 'T'));
                if (re < dayStart || rs > dayEnd) return;
                // 이 날이 예약의 시작일인 경우만 바를 그림 (연속 표시)
                var isFirstDay = rs >= dayStart;
                if (!isFirstDay && di > 0) return;  // 시작일이 아니면 스킵 (첫날 바가 overflow로 커버)
                var barStartPx = Math.max(0, (rs - dayStart) / 86400000) * 100;
                var totalSpanMs = Math.min(endDate.getTime(), re.getTime()) - Math.max(startDate.getTime(), rs.getTime());
                var oneDayMs = 86400000;
                var barWidthDays = totalSpanMs / oneDayMs;
                var barWidthPct = barWidthDays * 100;  // 100% = 1 day cell width
                if (barWidthPct < 5) barWidthPct = 5;
                var color = rv.block ? wayColors['block'] : (wayColors[rv.way] || '#90a4ae');
                var lbl = rv.block ? wayLabels['block'] : (wayLabels[rv.way] || '');
                var tipWay = rv.block ? '블락' : rv.way;
                html += '<div title="' + rv.start + ' ~ ' + rv.end + ' (' + tipWay + ')" style="position:absolute;top:2px;bottom:2px;left:' + barStartPx.toFixed(1) + '%;width:' + barWidthPct.toFixed(1) + '%;background:' + color + ';border-radius:2px;opacity:0.85;overflow:hidden;color:#fff;font-size:7px;line-height:24px;text-align:center;cursor:default;z-index:1;pointer-events:auto;">' + lbl + '</div>';
            }});
            html += '</td>';
        }});
        html += '</tr>';
    }});
    html += '</tbody></table>';
    // 범례
    html += '<div style="margin-top:8px;display:flex;gap:10px;font-size:10px;flex-wrap:wrap;color:#8890a4;">';
    [['handle','일반','#5c6bc0'],['d2d_round','부름왕복','#43a047'],['d2d_oneway','부름편도','#00897b'],['block','블락','#555c6e']].forEach(function(x) {{
        html += '<span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:' + x[2] + ';vertical-align:middle;margin-right:3px;"></span>' + x[1] + '</span>';
    }});
    html += '</div>';
    content.innerHTML = html;
    panel.style.display = 'block';
}}

// ── 존 개설 시뮬레이션 ──
(function() {{
    var simCtx = document.getElementById('simCtx');
    var simPanel = document.getElementById('simPanel');
    var simOverlay = document.getElementById('simOverlay');
    var simContent = document.getElementById('simContent');
    var simMarker = null;
    var simLat, simLng;

    // 우클릭 → 컨텍스트 메뉴
    map.on('contextmenu', function(e) {{
        e.originalEvent.preventDefault();
        simLat = e.latlng.lat;
        simLng = e.latlng.lng;
        var pt = map.latLngToContainerPoint(e.latlng);
        simCtx.style.left = pt.x + 'px';
        simCtx.style.top = pt.y + 'px';
        simCtx.style.display = 'block';
    }});

    map.on('click', function() {{ simCtx.style.display = 'none'; }});
    map.on('movestart', function() {{ simCtx.style.display = 'none'; }});

    document.getElementById('simCtxBtn').addEventListener('click', function() {{
        simCtx.style.display = 'none';
        runSimulation(simLat, simLng);
    }});

    document.getElementById('simClose').addEventListener('click', closeSimPanel);
    simOverlay.addEventListener('click', closeSimPanel);

    function closeSimPanel() {{
        simPanel.style.display = 'none';
        simOverlay.style.display = 'none';
        if (simMarker) {{ map.removeLayer(simMarker); simMarker = null; }}
    }}

    function runSimulation(lat, lng) {{
        // 시뮬레이션 마커
        if (simMarker) map.removeLayer(simMarker);
        simMarker = L.marker([lat, lng], {{
            icon: L.divIcon({{
                className: 'zone-marker-wrap',
                html: '<div class="sim-marker-pin">?</div>',
                iconSize: [32, 32], iconAnchor: [16, 16]
            }})
        }}).addTo(map);

        // 로딩 표시
        simContent.innerHTML = '<div style="text-align:center;padding:30px 0;"><div class="sim-marker-pin" style="display:inline-flex;width:40px;height:40px;font-size:16px;">?</div><div style="margin-top:12px;color:#8890a4;font-size:12px;">BQ 쿼리 실행 중...</div><div style="color:#6b7394;font-size:10px;margin-top:4px;">반경 1km 실시간 접속/예약 집계 중</div></div>';
        simPanel.style.display = 'block';
        simOverlay.style.display = 'block';

        // 서버 API 호출 (로컬이면 상대경로, 외부면 ngrok URL)
        var simApiBase = (location.hostname === 'localhost' || location.hostname === '127.0.0.1')
            ? '' : '{ngrok_url}';
        fetch(simApiBase + '/api/simulate', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ lat: lat, lng: lng, radius: 1.0 }})
        }})
        .then(function(resp) {{
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            return resp.json();
        }})
        .then(function(d) {{
            if (d.error) {{ simContent.innerHTML = '<div style="color:#e74c3c;padding:20px;text-align:center;">오류: ' + d.error + '</div>'; return; }}
            renderSimResult(d);
        }})
        .catch(function(err) {{
            simContent.innerHTML = '<div style="color:#e74c3c;padding:20px;text-align:center;">서버 연결 실패<br><span style="font-size:10px;color:#8890a4;">시뮬레이션 서버가 실행 중인지 확인하세요</span></div>';
        }});
    }}

    function renderSimResult(d) {{
        var html = '';
        html += '<div class="sim-row"><span class="sim-label">좌표</span><span class="sim-value" style="font-size:11px;font-family:monospace;">' + d.lat.toFixed(5) + ', ' + d.lng.toFixed(5) + '</span></div>';
        html += '<div class="sim-row"><span class="sim-label">지역</span><span class="sim-value" style="font-size:11px;">' + d.region2 + ' ' + d.region3 + '</span></div>';
        if (d.nearest_zone) {{
            html += '<div class="sim-row"><span class="sim-label">최근접 존</span><span class="sim-value" style="font-size:11px;">' + d.nearest_zone + ' (' + d.nearest_zone_dist_km + 'km)</span></div>';
        }}

        html += '<div class="sim-section"><div class="sim-section-title">반경 1km 수요 — 주간 (90일 기준)</div>';
        html += '<div class="sim-row"><span class="sim-label">앱 접속</span><span class="sim-value">' + d.weekly_access.toLocaleString() + '</span></div>';
        html += '<div class="sim-row"><span class="sim-label">예약 생성</span><span class="sim-value">' + d.weekly_res.toLocaleString() + '</span></div>';
        html += '<div class="sim-row"><span class="sim-label">부름 호출</span><span class="sim-value">' + d.weekly_dtod.toLocaleString() + '</span></div>';
        html += '<div class="sim-row"><span class="sim-label">전환율 (' + d.conv_level + ')</span><span class="sim-value">' + (d.conv_rate * 100).toFixed(2) + '%</span></div>';
        html += '</div>';

        html += '<div class="sim-section"><div class="sim-section-title">인근 벤치마크 (반경 3km, ' + d.bench_zone_count + '개 존)</div>';
        html += '<div class="sim-row"><span class="sim-label">평균 대당매출 (4주)</span><span class="sim-value">' + (d.est_rev_per_car > 0 ? d.est_rev_per_car.toLocaleString() + '원' : '-') + '</span></div>';
        html += '<div class="sim-row"><span class="sim-label">평균 대당GP (4주)</span><span class="sim-value">' + (d.est_gp_per_car > 0 ? d.est_gp_per_car.toLocaleString() + '원' : '-') + '</span></div>';
        html += '<div class="sim-row"><span class="sim-label">평균 가동률</span><span class="sim-value">' + (d.avg_util > 0 ? d.avg_util.toFixed(1) + '%' : '-') + '</span></div>';
        html += '</div>';

        html += '<div class="sim-section"><div class="sim-section-title">시뮬레이션 결과</div>';
        html += '<div class="sim-row"><span class="sim-label">예상 주간 예약</span><span class="sim-value">' + d.est_weekly_res.toLocaleString() + '건</span></div>';
        html += '<div class="sim-row"><span class="sim-label">추천 공급대수</span><span class="sim-value" style="color:#42a5f5">' + d.recommended_cars + '대</span></div>';
        html += '<div class="sim-row"><span class="sim-label">예상 대당매출 (4주)</span><span class="sim-value" style="color:#ffb74d">' + (d.est_rev_per_car > 0 ? d.est_rev_per_car.toLocaleString() + '원' : '-') + '</span></div>';
        html += '</div>';

        if (d.is_recommend) {{
            html += '<div class="sim-result recommend">존 개설 추천 O</div>';
        }} else {{
            var reasons = [];
            if (d.recommended_cars < 1) reasons.push('추천 공급대수 부족');
            if (d.est_rev_per_car < 1000000 && d.est_rev_per_car > 0) reasons.push('대당매출 100만원 미달');
            if (d.est_rev_per_car === 0) reasons.push('인근 실적 데이터 없음');
            html += '<div class="sim-result not-recommend">존 개설 추천 X</div>';
            html += '<div style="text-align:center;font-size:10px;color:#8890a4;margin-top:6px;">' + reasons.join(' · ') + '</div>';
        }}

        simContent.innerHTML = html;
    }}
}})();

// (검색 기능으로 대체됨)
</script>
</body>
</html>"""
    return html


def regenerate_from_cache():
    """캐시 데이터로 HTML만 재생성 (BQ 조회 없음, 서울 필터 적용)"""
    cache_dir = os.path.join(OUTPUT_DIR, ".cache")
    print(f"[{datetime.now()}] 캐시에서 HTML 재생성")

    def load_cache(name):
        path = os.path.join(cache_dir, f"{name}.json")
        with open(path) as f:
            return json.load(f)

    access = load_cache("access")
    reservation = load_cache("reservation")
    zones = load_cache("zones")
    gaps = load_cache("gaps")
    analysis = load_cache("supply_demand")
    dtod_path = os.path.join(cache_dir, "dtod.json")
    dtod = json.load(open(dtod_path)) if os.path.exists(dtod_path) else []

    # 서울+인천 데이터 제거 (히트맵: 접경 허용 / 분석: 철저 제외)
    zone_coords = [(float(z['lat']), float(z['lng'])) for z in zones]
    before_a, before_r, before_d = len(access), len(reservation), len(dtod)
    access = filter_non_gyeonggi(access, zone_coords=zone_coords, keep_dist_km=1.0)
    reservation = filter_non_gyeonggi(reservation, zone_coords=zone_coords, keep_dist_km=1.0)
    dtod = filter_non_gyeonggi(dtod, zone_coords=zone_coords, keep_dist_km=1.0)
    print(f"  서울+인천 필터: 접속 {before_a}->{len(access)}, 예약 {before_r}->{len(reservation)}, 부름 {before_d}->{len(dtod)}")

    # 공급 분석: 캐시에서 주간 추이 로드
    weekly_path = os.path.join(cache_dir, "weekly_trends.json")
    if os.path.exists(weekly_path):
        weekly_data = json.load(open(weekly_path))
        analysis = compute_growth_analysis(weekly_data, zones)
    else:
        analysis = load_cache("supply_demand") or {}  # fallback
    if isinstance(analysis, dict):
        print(f"  공급 분석: {len(analysis.get('growth',[]))} 성장 / {len(analysis.get('decline',[]))} 감소 지역")
    else:
        print(f"  공급 분석: {len(analysis)} 지역 (레거시)")

    # Gap 분석: 서울+인천 철저 제외 (compute_gaps 내부에서 is_non_gyeonggi 적용)
    gaps = compute_gaps(access, reservation, zones)
    print(f"  Gap 재계산: {len(gaps)} 지역 (역지오코딩 중...)")
    gaps = reverse_geocode(gaps)
    print(f"  Gap 역지오코딩 완료: {len(gaps)} 지역")

    # 실적 데이터 로드
    profit_path = os.path.join(cache_dir, "profit.json")
    profit_data = json.load(open(profit_path)) if os.path.exists(profit_path) else {}
    # profit 캐시의 키가 문자열이므로 int로 변환
    profit_data = {int(k): v for k, v in profit_data.items()} if profit_data else {}

    # 그린카 데이터 로드
    gcar_path = os.path.join(cache_dir, "gcar.json")
    gcar_data = json.load(open(gcar_path)) if os.path.exists(gcar_path) else []

    # 쏘카 지역별 실 운영 차량 로드
    socar_supply_path = os.path.join(cache_dir, "socar_supply.json")
    socar_supply = json.load(open(socar_supply_path)) if os.path.exists(socar_supply_path) else {}

    # 예약 타임라인 로드
    timeline_path = os.path.join(cache_dir, "timeline.json")
    timeline_data = json.load(open(timeline_path)) if os.path.exists(timeline_path) else {}

    html = generate_index(access, reservation, zones, gaps, analysis, dtod, profit_data,
                          gcar_data=gcar_data, socar_supply=socar_supply, timeline_data=timeline_data)
    path = os.path.join(OUTPUT_DIR, "index.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  -> {path} ({len(html):,} bytes)")
    print("[완료] HTML 재생성 완료")


def main():
    print(f"[{datetime.now()}] 경기도 잠재 수요 지도 업데이트 시작")
    print(f"  기간: {THREE_MONTHS_AGO} ~ {TODAY}")

    cache_dir = os.path.join(OUTPUT_DIR, ".cache")
    os.makedirs(cache_dir, exist_ok=True)

    print("  1/6 앱 접속 데이터 조회...")
    access = query_access()
    print(f"       {len(access)} rows, {sum(int(r['access_count']) for r in access):,} 건")

    print("  2/6 예약 생성 데이터 조회...")
    reservation = query_reservation()
    print(f"       {len(reservation)} rows, {sum(int(r['reservation_count']) for r in reservation):,} 건")

    print("  3/6 운영 존 + 차량 대수 조회...")
    zones = query_zones()
    print(f"       {len(zones)} 존, {sum(int(z.get('car_count',0)) for z in zones):,} 대")

    print("  4/6 지역별 실 예약 건수 조회 (존 소속 차량 기준)...")
    zone_reservations = query_reservation_by_zone_region()
    print(f"       {len(zone_reservations)} 지역")

    print("  5/6 부름 호출 위치 조회...")
    dtod = query_dtod()
    print(f"       {len(dtod)} rows, {sum(r['call_count'] for r in dtod):,} 건")

    print("  5.1/6 존별 실적 조회 (profit_socar_car_daily)...")
    profit_data = query_zone_profit()
    print(f"       {len(profit_data)} 존 실적")

    print("  5.2/6 그린카 존 현황 조회 (gcar_info_log)...")
    gcar_data = query_gcar_zones()
    print(f"       {len(gcar_data)} 존, {sum(int(g.get('total_cars',0)) for g in gcar_data):,} 대")

    print("  5.3/6 쏘카 지역별 실 운영 차량/존 수 (profit_car_daily)...")
    socar_supply = query_socar_supply_by_region()
    print(f"       {len(socar_supply)} 지역, {sum(v['socar_cars'] for v in socar_supply.values()):,} 대")

    zone_coords = [(float(z['lat']), float(z['lng'])) for z in zones]

    # 공급 분석: 주간 추이 기반 성장 지역 분석
    print("  5.5/6 주간 추이 조회 (접속/예약/공급)...")
    weekly_data = query_weekly_trends()
    access_cnt = len(weekly_data.get('access', []))
    res_cnt = len(weekly_data.get('res', []))
    supply_cnt = len(weekly_data.get('supply', []))
    print(f"       접속 {access_cnt}, 예약 {res_cnt}, 공급 {supply_cnt} rows")
    analysis = compute_growth_analysis(weekly_data, zones)
    print(f"       공급 분석: {len(analysis.get('growth',[]))} 성장 / {len(analysis.get('decline',[]))} 감소 지역")

    # Gap 분석: compute_gaps 내부에서 is_non_gyeonggi 적용
    print("  6/6 미충족 수요 분석 + 역지오코딩...")
    gaps = compute_gaps(access, reservation, zones)
    gaps_named = reverse_geocode(gaps)
    print(f"       {len(gaps_named)} 미충족 지역")

    # 히트맵 데이터 필터링: 경기도 존 3km 이내 + 서울/인천 제외 (접경 허용)
    def _near_zone(lat, lng, max_km=3.0):
        return any(haversine_km(lat, lng, zl, zn) <= max_km
                   for zl, zn in zone_coords
                   if abs(lat - zl) < 0.05 and abs(lng - zn) < 0.05
                   ) or min(haversine_km(lat, lng, zl, zn) for zl, zn in zone_coords) <= max_km

    access_before = len(access)
    access = [r for r in access if _near_zone(float(r['lat']), float(r['lng']))]
    access = filter_non_gyeonggi(access, zone_coords=zone_coords, keep_dist_km=1.0)
    access.sort(key=lambda x: -x['access_count'])
    access = access[:1000]
    print(f"       히트맵 필터: {access_before} -> {len(access)} (경기도 존 3km 이내, 서울+인천 접경허용, top 1000)")

    res_before = len(reservation)
    reservation = [r for r in reservation if _near_zone(float(r['lat']), float(r['lng']))]
    reservation = filter_non_gyeonggi(reservation, zone_coords=zone_coords, keep_dist_km=1.0)
    reservation.sort(key=lambda x: -x['reservation_count'])
    reservation = reservation[:1000]
    print(f"       예약 필터: {res_before} -> {len(reservation)}")

    # 부름 호출 데이터 필터링
    dtod_before = len(dtod)
    dtod = [r for r in dtod if _near_zone(r['lat'], r['lng'])]
    dtod = filter_non_gyeonggi(dtod, zone_coords=zone_coords, keep_dist_km=1.0)
    dtod.sort(key=lambda x: -x['call_count'])
    dtod = dtod[:1000]
    print(f"       부름 필터: {dtod_before} -> {len(dtod)}")

    print("  7/7 예약 타임라인 조회...")
    timeline_data = query_reservation_timeline()
    total_res = sum(len(v) for v in timeline_data.values())
    print(f"       {len(timeline_data)} 존, {total_res:,} 건")

    # Save cache
    for name, data in [("access", access), ("reservation", reservation),
                       ("zones", zones), ("gaps", gaps_named),
                       ("supply_demand", analysis), ("weekly_trends", weekly_data),
                       ("dtod", dtod),
                       ("profit", profit_data), ("gcar", gcar_data),
                       ("socar_supply", socar_supply), ("timeline", timeline_data)]:
        with open(os.path.join(cache_dir, f"{name}.json"), "w") as f:
            json.dump(data, f, ensure_ascii=False)

    print("  HTML 생성...")
    html1 = generate_index(access, reservation, zones, gaps_named, analysis, dtod, profit_data,
                           gcar_data=gcar_data, socar_supply=socar_supply, timeline_data=timeline_data)
    path1 = os.path.join(OUTPUT_DIR, "index.html")
    with open(path1, "w") as f:
        f.write(html1)
    print(f"  -> {path1} ({len(html1):,} bytes)")
    print(f"[완료] 대시보드가 생성되었습니다.")


def _load_cache(name):
    path = os.path.join(OUTPUT_DIR, ".cache", f"{name}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def _save_cache(name, data):
    cache_dir = os.path.join(OUTPUT_DIR, ".cache")
    os.makedirs(cache_dir, exist_ok=True)
    with open(os.path.join(cache_dir, f"{name}.json"), "w") as f:
        json.dump(data, f, ensure_ascii=False)


def _build_html():
    """캐시에서 모든 데이터 로드 → HTML 생성"""
    access = _load_cache("access") or []
    reservation = _load_cache("reservation") or []
    zones = _load_cache("zones") or []
    gaps = _load_cache("gaps") or []
    analysis = _load_cache("supply_demand") or []
    dtod = _load_cache("dtod") or []
    profit_data = _load_cache("profit") or {}
    if profit_data:
        profit_data = {int(k): v for k, v in profit_data.items()}
    gcar_data = _load_cache("gcar") or []
    socar_supply = _load_cache("socar_supply") or {}
    parking_contract = _load_cache("parking_contract") or {}
    if parking_contract:
        parking_contract = {int(k): v for k, v in parking_contract.items()}
    timeline_data = _load_cache("timeline") or {}

    html = generate_index(access, reservation, zones, gaps, analysis, dtod, profit_data,
                          gcar_data=gcar_data, socar_supply=socar_supply,
                          parking_contract=parking_contract, timeline_data=timeline_data)
    path = os.path.join(OUTPUT_DIR, "index.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  -> {path} ({len(html):,} bytes)")


def update_demand():
    """수요 데이터만 업데이트 (앱 접속, 예약 생성, 부름 호출)"""
    print(f"[{datetime.now()}] 수요 데이터 업데이트")
    print(f"  기간: {THREE_MONTHS_AGO} ~ {TODAY}")

    print("  1/3 앱 접속 데이터 조회...")
    access = query_access()
    print(f"       {len(access)} rows")

    print("  2/3 예약 생성 데이터 조회...")
    reservation = query_reservation()
    print(f"       {len(reservation)} rows")

    print("  3/3 부름 호출 위치 조회...")
    dtod = query_dtod()
    print(f"       {len(dtod)} rows")

    # 존 데이터 로드 (캐시)
    zones = _load_cache("zones") or []
    zone_coords = [(float(z['lat']), float(z['lng'])) for z in zones]

    # 공급 분석: 주간 추이 기반
    print("  3.5/3 주간 추이 조회...")
    weekly_data = query_weekly_trends()
    analysis = compute_growth_analysis(weekly_data, zones)
    print(f"       {len(analysis.get('growth',[]))} 성장 / {len(analysis.get('decline',[]))} 감소 지역")

    # Gap 분석
    gaps = compute_gaps(access, reservation, zones)
    gaps = reverse_geocode(gaps)

    # 히트맵 필터링
    def _near_zone(lat, lng, max_km=3.0):
        return any(haversine_km(lat, lng, zl, zn) <= max_km
                   for zl, zn in zone_coords
                   if abs(lat - zl) < 0.05 and abs(lng - zn) < 0.05
                   ) or (zone_coords and min(haversine_km(lat, lng, zl, zn) for zl, zn in zone_coords) <= max_km)

    if zone_coords:
        access = [r for r in access if _near_zone(float(r['lat']), float(r['lng']))]
        access = filter_non_gyeonggi(access, zone_coords=zone_coords, keep_dist_km=1.0)
        access.sort(key=lambda x: -x['access_count'])
        access = access[:1000]
        reservation = [r for r in reservation if _near_zone(float(r['lat']), float(r['lng']))]
        reservation = filter_non_gyeonggi(reservation, zone_coords=zone_coords, keep_dist_km=1.0)
        reservation.sort(key=lambda x: -x['reservation_count'])
        reservation = reservation[:1000]
        dtod = [r for r in dtod if _near_zone(r['lat'], r['lng'])]
        dtod = filter_non_gyeonggi(dtod, zone_coords=zone_coords, keep_dist_km=1.0)
        dtod.sort(key=lambda x: -x['call_count'])
        dtod = dtod[:1000]

    for name, data in [("access", access), ("reservation", reservation), ("dtod", dtod),
                       ("supply_demand", analysis), ("weekly_trends", weekly_data), ("gaps", gaps)]:
        _save_cache(name, data)

    _build_html()
    print("[완료] 수요 데이터 업데이트 완료")


def update_zone():
    """존/실적 데이터만 업데이트 (존 정보, 실적, 그린카, 예약 타임라인)"""
    print(f"[{datetime.now()}] 존/실적 데이터 업데이트")

    print("  1/6 운영 존 + 차량 대수 조회...")
    zones = query_zones()
    print(f"       {len(zones)} 존, {sum(int(z.get('car_count',0)) for z in zones):,} 대")

    print("  2/6 존별 실적 조회...")
    profit_data = query_zone_profit()
    print(f"       {len(profit_data)} 존 실적")

    print("  3/6 주차 계약 정보 조회...")
    parking_contract = query_parking_contract()
    print(f"       {len(parking_contract)} 존 계약")

    print("  4/6 그린카 존 현황 조회...")
    gcar_data = query_gcar_zones()
    print(f"       {len(gcar_data)} 존")

    print("  5/6 쏘카 지역별 실 운영 차량/존 수...")
    socar_supply = query_socar_supply_by_region()
    print(f"       {len(socar_supply)} 지역")

    print("  6/6 예약 타임라인 조회...")
    timeline_data = query_reservation_timeline()
    print(f"       {len(timeline_data)} 존, {sum(len(v) for v in timeline_data.values()):,} 건")

    for name, data in [("zones", zones), ("profit", profit_data), ("parking_contract", parking_contract),
                       ("gcar", gcar_data), ("socar_supply", socar_supply), ("timeline", timeline_data)]:
        _save_cache(name, data)

    _build_html()
    print("[완료] 존/실적 업데이트 완료")


if __name__ == "__main__":
    import sys
    if '--regen' in sys.argv:
        regenerate_from_cache()
    elif '--demand' in sys.argv:
        update_demand()
    elif '--zone' in sys.argv:
        update_zone()
    else:
        main()
