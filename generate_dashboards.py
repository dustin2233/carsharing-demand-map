#!/usr/bin/env python3
"""
Generate HTML dashboard for carsharing demand map project.
Reads JSON data files and produces a self-contained HTML dashboard.
"""

import json
import os

DATA_DIR = "/Users/dustin"
OUT_DIR = "/Users/dustin/carsharing_demand_map"


def load_json(filename):
    with open(os.path.join(DATA_DIR, filename), "r", encoding="utf-8") as f:
        return json.load(f)


def json_dump(data):
    return json.dumps(data, ensure_ascii=False)


# ── Shared HTML fragments ──────────────────────────────────────────────────

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
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
#map { position: absolute; top: 0; left: 0; right: 0; bottom: 0; z-index: 0; }

.header {
    position: fixed; top: 0; left: 0; right: 0; z-index: 1000;
    background: rgba(255,255,255,0.97); backdrop-filter: blur(8px);
    padding: 10px 20px; display: flex; align-items: center; gap: 18px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08); height: 56px;
}
.header h1 { font-size: 17px; font-weight: 700; color: #1a1a1a; white-space: nowrap; }
.header .date { font-size: 12px; color: #888; white-space: nowrap; }
.header .nav-link {
    font-size: 12px; color: #4a90d9; text-decoration: none;
    border: 1px solid #4a90d9; border-radius: 4px; padding: 3px 10px; white-space: nowrap;
}
.header .nav-link:hover { background: #4a90d9; color: #fff; }

.stat-card {
    display: inline-flex; flex-direction: column; align-items: center;
    background: #f7f8fa; border-radius: 6px; padding: 4px 12px; min-width: 90px;
}
.stat-card .label { font-size: 10px; color: #888; }
.stat-card .value { font-size: 14px; font-weight: 700; color: #333; }

.controls {
    position: fixed; top: 66px; left: 14px; z-index: 1000;
    display: flex; gap: 8px; flex-wrap: wrap;
}
.controls select, .controls button {
    padding: 7px 14px; border-radius: 8px; border: 1px solid #ddd;
    background: #fff; font-size: 13px; cursor: pointer;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}
.controls select:focus { outline: none; border-color: #4a90d9; }

.legend {
    position: fixed; bottom: 24px; left: 14px; z-index: 1000;
    background: rgba(255,255,255,0.95); border-radius: 10px;
    padding: 12px 16px; box-shadow: 0 2px 12px rgba(0,0,0,0.1);
    font-size: 12px; line-height: 1.8;
}
.legend-item { display: flex; align-items: center; gap: 8px; }
.legend-dot {
    width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0;
    border: 2px solid rgba(255,255,255,0.8);
}
.legend-bar { width: 40px; height: 8px; border-radius: 4px; flex-shrink: 0; }

.gap-panel {
    position: fixed; top: 66px; right: 14px; z-index: 1000;
    background: rgba(255,255,255,0.96); border-radius: 10px;
    padding: 14px 16px; box-shadow: 0 2px 16px rgba(0,0,0,0.12);
    max-height: calc(100vh - 100px); overflow-y: auto; width: 320px;
    display: none; font-size: 12px;
}
.gap-panel h3 { font-size: 14px; margin-bottom: 8px; color: #333; }
.gap-panel .gap-row {
    display: flex; justify-content: space-between; padding: 5px 0;
    border-bottom: 1px solid #f0f0f0;
}
.gap-panel .gap-row:hover { background: #f7f8fa; }
.gap-panel .gap-name { color: #333; flex: 1; }
.gap-panel .gap-cnt { color: #e67e22; font-weight: 600; min-width: 70px; text-align: right; }

.analysis-panel {
    position: fixed; top: 66px; right: 14px; z-index: 1000;
    background: rgba(255,255,255,0.97); border-radius: 10px;
    padding: 14px 16px; box-shadow: 0 2px 16px rgba(0,0,0,0.12);
    max-height: calc(100vh - 100px); overflow-y: auto; width: 520px;
    display: none; font-size: 11px;
}
.analysis-panel h3 { font-size: 14px; margin-bottom: 4px; color: #333; }
.analysis-panel .desc { font-size: 11px; color: #888; margin-bottom: 10px; }
.analysis-panel table { width: 100%; border-collapse: collapse; }
.analysis-panel th {
    text-align: left; padding: 4px 6px; border-bottom: 2px solid #ddd;
    font-size: 10px; color: #888; font-weight: 600; white-space: nowrap;
    cursor: pointer; user-select: none;
}
.analysis-panel th:hover { color: #333; }
.analysis-panel th.sorted { color: #1976d2; }
.analysis-panel td {
    padding: 4px 6px; border-bottom: 1px solid #f0f0f0; white-space: nowrap;
}
.analysis-panel tr:hover { background: #f7f8fa; }
.analysis-panel .bar-cell { position: relative; }
.analysis-panel .bar {
    position: absolute; top: 2px; bottom: 2px; left: 0;
    border-radius: 3px; opacity: 0.15;
}
.analysis-panel .high { color: #c62828; font-weight: 700; }
.analysis-panel .mid { color: #e65100; font-weight: 600; }

.leaflet-popup-content-wrapper { border-radius: 10px; }
.leaflet-popup-content { font-size: 13px; line-height: 1.6; min-width: 200px; }
.popup-title { font-weight: 700; font-size: 14px; margin-bottom: 4px; }
.popup-row { display: flex; gap: 6px; }
.popup-label { color: #888; min-width: 60px; }
.popup-badge {
    display: inline-block; padding: 1px 8px; border-radius: 10px;
    font-size: 11px; font-weight: 600; color: #fff;
}
"""

TILE_SETUP = """
    var vworldTile = L.tileLayer('https://xdworld.vworld.kr/2d/Base/service/{z}/{x}/{y}.png', {
        attribution: 'VWorld', maxZoom: 19
    });
    var osmTile = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors', maxZoom: 19
    });
    // Try VWorld first, fallback to OSM
    var baseTile = vworldTile;
    baseTile.on('tileerror', function() { map.removeLayer(vworldTile); osmTile.addTo(map); });
    baseTile.addTo(map);
"""


def make_zone_color_js():
    return """
    function zoneColor(imaginary) {
        if (imaginary === 3) return '#e67e22';
        if (imaginary === 5) return '#8e44ad';
        return '#27ae60';
    }
    function zoneLabel(imaginary) {
        if (imaginary === 3) return '스테이션';
        if (imaginary === 5) return '부름우선';
        return '일반';
    }
    function zoneBadgeColor(imaginary) {
        if (imaginary === 3) return '#e67e22';
        if (imaginary === 5) return '#8e44ad';
        return '#27ae60';
    }
    """


def make_popup_js():
    return """
    function makePopup(z) {
        var d2d = z.is_d2d_car_exportable === 'ABLE' ? '부름 가능' : '부름 불가';
        var d2dColor = z.is_d2d_car_exportable === 'ABLE' ? '#27ae60' : '#e74c3c';
        return '<div class="popup-title">' + z.zone_name + '</div>' +
            '<div class="popup-row"><span class="popup-label">주차장</span><span>' + z.parking_name + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">주소</span><span>' + z.address + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">차량</span><span><b>' + z.car_count + '</b>대</span></div>' +
            '<div class="popup-row"><span class="popup-label">유형</span><span class="popup-badge" style="background:' + zoneBadgeColor(z.imaginary) + '">' + zoneLabel(z.imaginary) + '</span></div>' +
            '<div class="popup-row"><span class="popup-label">부름</span><span style="color:' + d2dColor + ';font-weight:600">' + d2d + '</span></div>';
    }
    """


# ── Dashboard 1: index.html ───────────────────────────────────────────────

def generate_index():
    access = load_json("access_data_v4.json")
    reservation = load_json("reservation_data_v4.json")
    zones = load_json("zones_data_v6.json")
    gaps = load_json("gaps_geocoded_v4.json")
    analysis = load_json("supply_demand_analysis.json")

    total_access = sum(r["access_count"] for r in access)
    total_reservations = sum(r["reservation_count"] for r in reservation)
    total_zones = len(zones)
    total_cars = sum(z["car_count"] for z in zones)

    # Prepare heatmap data: [lat, lng, intensity]
    access_heat = [[r["lat"], r["lng"], r["access_count"]] for r in access]
    res_heat = [[r["lat"], r["lng"], r["reservation_count"]] for r in reservation]

    # Get unique regions from zones
    regions = sorted(set(z["region2"] for z in zones))

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>경기도 카셰어링 잠재 수요 지도</title>
{LEAFLET_CDN}
<style>
{SHARED_STYLES}
</style>
</head>
<body>

<div class="header">
    <h1>경기도 카셰어링 잠재 수요 지도</h1>
    <span class="date">2025-12-12 ~ 2026-03-12 (최근 3개월) | 업데이트: 2026-03-12</span>
    <div class="stat-card"><span class="label">앱 접속</span><span class="value">{total_access:,}</span></div>
    <div class="stat-card"><span class="label">예약 생성</span><span class="value">{total_reservations:,}</span></div>
    <div class="stat-card"><span class="label">운영 존</span><span class="value">{total_zones:,}</span></div>
    <div class="stat-card"><span class="label">차량 합계</span><span class="value">{total_cars:,}</span></div>
</div>

<div class="controls">
    <select id="regionSelect">
        <option value="">전체 지역</option>
    </select>
    <button id="toggleAccess" class="active" style="border-color:#e67e22;color:#e67e22">앱 접속 히트맵</button>
    <button id="toggleRes" class="active" style="border-color:#3498db;color:#3498db">예약 히트맵</button>
    <button id="toggleZones" class="active" style="border-color:#27ae60;color:#27ae60">운영 존</button>
    <button id="toggleGap" style="border-color:#8e44ad;color:#8e44ad">Gap 분석</button>
    <button id="toggleAnalysis" style="border-color:#c62828;color:#c62828">공급 분석</button>
</div>

<div class="legend">
    <div style="font-weight:700;margin-bottom:4px;">범례</div>
    <div class="legend-item"><span class="legend-bar" style="background:linear-gradient(90deg,#ffe0b2,#ff5722)"></span> 앱 접속</div>
    <div class="legend-item"><span class="legend-bar" style="background:linear-gradient(90deg,#bbdefb,#1565c0)"></span> 예약 생성</div>
    <div class="legend-item"><span class="legend-dot" style="background:#27ae60"></span> 일반 존</div>
    <div class="legend-item"><span class="legend-dot" style="background:#e67e22"></span> 스테이션</div>
    <div class="legend-item"><span class="legend-dot" style="background:#8e44ad"></span> 부름우선</div>
</div>

<div class="gap-panel" id="gapPanel">
    <h3>Gap 분석 — 존 미운영 고수요 지역</h3>
    <div id="gapList"></div>
</div>

<div class="analysis-panel" id="analysisPanel">
    <h3>공급-수요 분석</h3>
    <div class="desc">전환율 = 예약/접속 | 예약/차량이 높을수록 공급 부족</div>
    <table>
        <thead><tr>
            <th data-col="region2">지역</th>
            <th data-col="access">접속</th>
            <th data-col="reservation">예약</th>
            <th data-col="cars">차량</th>
            <th data-col="cvr">전환율</th>
            <th data-col="res_per_car">예약/차량</th>
        </tr></thead>
        <tbody id="analysisBody"></tbody>
    </table>
</div>

<div id="map"></div>

<script>
var accessData = {json_dump(access_heat)};
var resData = {json_dump(res_heat)};
var zonesData = {json_dump(zones)};
var gapsData = {json_dump(gaps)};
var analysisData = {json_dump(analysis)};
var regions = {json_dump(regions)};

// Populate region dropdown
var sel = document.getElementById('regionSelect');
regions.forEach(function(r) {{
    var o = document.createElement('option');
    o.value = r; o.textContent = r.replace(/\\u3000/g, ' ');
    sel.appendChild(o);
}});

// Map
var map = L.map('map', {{ zoomControl: true }}).setView([37.41, 127.0], 9);
{TILE_SETUP}

{make_zone_color_js()}
{make_popup_js()}

// Heatmap layers
var accessHeat = L.heatLayer(accessData, {{
    max: 0.6, radius: 30, minOpacity: 0.12, blur: 20,
    gradient: {{0.05:'#fff3e0',0.2:'#ffb74d',0.4:'#ff9800',0.6:'#f57c00',0.8:'#e65100',1:'#bf360c'}}
}}).addTo(map);

var resHeat = L.heatLayer(resData, {{
    max: 0.6, radius: 30, minOpacity: 0.12, blur: 20,
    gradient: {{0.05:'#e3f2fd',0.2:'#64b5f6',0.4:'#2196f3',0.6:'#1976d2',0.8:'#1565c0',1:'#0d47a1'}}
}}).addTo(map);

// Zone markers
var zoneLayer = L.layerGroup();
var allZoneMarkers = [];

zonesData.forEach(function(z) {{
    var m = L.circleMarker([z.lat, z.lng], {{
        radius: Math.max(4, Math.min(14, 3 + z.car_count * 1.0)),
        fillColor: zoneColor(z.imaginary),
        color: '#fff', weight: 1.5, opacity: 0.9, fillOpacity: 0.75
    }}).bindPopup(makePopup(z));
    m._zoneData = z;
    allZoneMarkers.push(m);
    zoneLayer.addLayer(m);
}});
zoneLayer.addTo(map);

// Gap markers
var gapLayer = L.layerGroup();
gapsData.forEach(function(g) {{
    var m = L.circleMarker([g.lat, g.lng], {{
        radius: Math.max(6, Math.min(18, 5 + Math.log10(g.count) * 2)),
        fillColor: '#e74c3c', color: '#c0392b', weight: 2, opacity: 0.9, fillOpacity: 0.5
    }}).bindPopup('<div class="popup-title">' + g.name + '</div><div>접속 수: <b>' + g.count.toLocaleString() + '</b></div>');
    gapLayer.addLayer(m);
}});

// Gap panel list
var gapListDiv = document.getElementById('gapList');
gapsData.sort(function(a,b) {{ return b.count - a.count; }});
gapsData.forEach(function(g) {{
    var row = document.createElement('div');
    row.className = 'gap-row';
    row.innerHTML = '<span class="gap-name">' + g.name + '</span><span class="gap-cnt">' + g.count.toLocaleString() + '</span>';
    row.style.cursor = 'pointer';
    row.addEventListener('click', function() {{ map.setView([g.lat, g.lng], 14); }});
    gapListDiv.appendChild(row);
}});

// Toggle buttons
var showAccess = true, showRes = true, showZones = true, showGap = false;

function styleBtn(btn, active) {{
    btn.style.background = active ? btn.style.borderColor : '#fff';
    btn.style.color = active ? '#fff' : btn.style.borderColor;
}}

document.getElementById('toggleAccess').addEventListener('click', function() {{
    showAccess = !showAccess;
    showAccess ? accessHeat.addTo(map) : map.removeLayer(accessHeat);
    styleBtn(this, showAccess);
}});
document.getElementById('toggleRes').addEventListener('click', function() {{
    showRes = !showRes;
    showRes ? resHeat.addTo(map) : map.removeLayer(resHeat);
    styleBtn(this, showRes);
}});
document.getElementById('toggleZones').addEventListener('click', function() {{
    showZones = !showZones;
    showZones ? zoneLayer.addTo(map) : map.removeLayer(zoneLayer);
    styleBtn(this, showZones);
}});
document.getElementById('toggleGap').addEventListener('click', function() {{
    showGap = !showGap;
    var panel = document.getElementById('gapPanel');
    panel.style.display = showGap ? 'block' : 'none';
    showGap ? gapLayer.addTo(map) : map.removeLayer(gapLayer);
    if (showGap) {{ document.getElementById('analysisPanel').style.display = 'none'; showAnalysis = false; styleBtn(document.getElementById('toggleAnalysis'), false); }}
    styleBtn(this, showGap);
}});

// Init button styles
styleBtn(document.getElementById('toggleAccess'), true);
styleBtn(document.getElementById('toggleRes'), true);
styleBtn(document.getElementById('toggleZones'), true);
styleBtn(document.getElementById('toggleGap'), false);
styleBtn(document.getElementById('toggleAnalysis'), false);

// Supply-demand analysis table
var showAnalysis = false;
var sortCol = 'res_per_car', sortAsc = false;

function renderAnalysis() {{
    var sorted = analysisData.slice().sort(function(a, b) {{
        var va = a[sortCol], vb = b[sortCol];
        return sortAsc ? va - vb : vb - va;
    }});
    if (sortCol === 'region2') {{
        sorted = analysisData.slice().sort(function(a, b) {{
            return sortAsc ? a.region2.localeCompare(b.region2) : b.region2.localeCompare(a.region2);
        }});
    }}
    var maxRpc = Math.max.apply(null, analysisData.map(function(d) {{ return d.res_per_car; }}));
    var html = '';
    sorted.forEach(function(d) {{
        var rpcPct = (d.res_per_car / maxRpc * 100).toFixed(0);
        var rpcClass = d.res_per_car >= 65 ? 'high' : d.res_per_car >= 60 ? 'mid' : '';
        var name = d.region2.replace(/\\u3000/g, ' ');
        html += '<tr>' +
            '<td>' + name + '</td>' +
            '<td style="text-align:right">' + (d.access > 0 ? d.access.toLocaleString() : '-') + '</td>' +
            '<td style="text-align:right">' + d.reservation.toLocaleString() + '</td>' +
            '<td style="text-align:right">' + d.cars + '</td>' +
            '<td style="text-align:right">' + (d.access > 0 ? d.cvr.toFixed(2) + '%' : '-') + '</td>' +
            '<td class="bar-cell ' + rpcClass + '" style="text-align:right">' +
                '<div class="bar" style="width:' + rpcPct + '%;background:#c62828"></div>' +
                d.res_per_car.toFixed(1) +
            '</td></tr>';
    }});
    document.getElementById('analysisBody').innerHTML = html;
    document.querySelectorAll('.analysis-panel th').forEach(function(th) {{
        th.className = th.dataset.col === sortCol ? 'sorted' : '';
    }});
}}

document.querySelectorAll('.analysis-panel th').forEach(function(th) {{
    th.addEventListener('click', function() {{
        var col = this.dataset.col;
        if (sortCol === col) sortAsc = !sortAsc;
        else {{ sortCol = col; sortAsc = false; }}
        renderAnalysis();
    }});
}});
renderAnalysis();

document.getElementById('toggleAnalysis').addEventListener('click', function() {{
    showAnalysis = !showAnalysis;
    document.getElementById('analysisPanel').style.display = showAnalysis ? 'block' : 'none';
    if (showAnalysis) {{ document.getElementById('gapPanel').style.display = 'none'; showGap = false; styleBtn(document.getElementById('toggleGap'), false); }}
    styleBtn(this, showAnalysis);
}});

// Region filter
sel.addEventListener('change', function() {{
    var region = this.value;

    // Filter zones
    zoneLayer.clearLayers();
    allZoneMarkers.forEach(function(m) {{
        if (!region || m._zoneData.region2 === region) {{
            zoneLayer.addLayer(m);
        }}
    }});

    // Filter heatmaps — we need original zone regions to decide if a point belongs.
    // For heatmaps, we filter by which data points fall within the region's bounding box.
    // Actually, access/reservation data has no region — so we filter zones only,
    // and keep heatmaps showing all data when no region, or approximate by bbox.
    // Per spec: "filters all layers" — but access/res data has no region2.
    // We'll filter by bounding box of zones in that region.
    if (region) {{
        var regionZones = zonesData.filter(function(z) {{ return z.region2 === region; }});
        if (regionZones.length > 0) {{
            var lats = regionZones.map(function(z) {{ return z.lat; }});
            var lngs = regionZones.map(function(z) {{ return z.lng; }});
            var pad = 0.03;
            var minLat = Math.min.apply(null, lats) - pad;
            var maxLat = Math.max.apply(null, lats) + pad;
            var minLng = Math.min.apply(null, lngs) - pad;
            var maxLng = Math.max.apply(null, lngs) + pad;

            var filteredAccess = accessData.filter(function(d) {{
                return d[0] >= minLat && d[0] <= maxLat && d[1] >= minLng && d[1] <= maxLng;
            }});
            var filteredRes = resData.filter(function(d) {{
                return d[0] >= minLat && d[0] <= maxLat && d[1] >= minLng && d[1] <= maxLng;
            }});

            map.removeLayer(accessHeat); map.removeLayer(resHeat);
            accessHeat = L.heatLayer(filteredAccess, {{
                max: 0.6, radius: 30, minOpacity: 0.12, blur: 20,
                gradient: {{0.05:'#fff3e0',0.2:'#ffb74d',0.4:'#ff9800',0.6:'#f57c00',0.8:'#e65100',1:'#bf360c'}}
            }});
            resHeat = L.heatLayer(filteredRes, {{
                max: 0.6, radius: 30, minOpacity: 0.12, blur: 20,
                gradient: {{0.05:'#e3f2fd',0.2:'#64b5f6',0.4:'#2196f3',0.6:'#1976d2',0.8:'#1565c0',1:'#0d47a1'}}
            }});
            if (showAccess) accessHeat.addTo(map);
            if (showRes) resHeat.addTo(map);

            map.fitBounds([[minLat, minLng], [maxLat, maxLng]]);
        }}
    }} else {{
        map.removeLayer(accessHeat); map.removeLayer(resHeat);
        accessHeat = L.heatLayer(accessData, {{
            max: 0.6, radius: 30, minOpacity: 0.12, blur: 20,
            gradient: {{0.05:'#fff3e0',0.2:'#ffb74d',0.4:'#ff9800',0.6:'#f57c00',0.8:'#e65100',1:'#bf360c'}}
        }});
        resHeat = L.heatLayer(resData, {{
            max: 0.6, radius: 30, minOpacity: 0.12, blur: 20,
            gradient: {{0.05:'#e3f2fd',0.2:'#64b5f6',0.4:'#2196f3',0.6:'#1976d2',0.8:'#1565c0',1:'#0d47a1'}}
        }});
        if (showAccess) accessHeat.addTo(map);
        if (showRes) resHeat.addTo(map);
        map.setView([37.41, 127.0], 9);
    }}
}});

</script>
</body>
</html>"""

    return html


# ── Main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)

    index_html = generate_index()
    with open(os.path.join(OUT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(index_html)
    print(f"Generated: {os.path.join(OUT_DIR, 'index.html')}")

    print("Done!")
