import base64
import json
import os
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo
from tuya_connector import TuyaOpenAPI

import historial

socket.setdefaulttimeout(20)

# ====== CONFIGURACIÓN (las claves vienen de GitHub, no se escriben acá) ======
ACCESS_ID  = os.environ["TUYA_ACCESS_ID"]
ACCESS_KEY = os.environ["TUYA_ACCESS_KEY"]
ENDPOINT   = "https://openapi.tuyaus.com"
UIDS       = [u.strip() for u in os.environ["TUYA_UIDS"].split(",") if u.strip()]
DIAS_LOG   = 7      # cuántos días atrás guarda Tuya los eventos (se suman al historial propio)
HORAS_BARRIDO = 20  # cada cuántas horas se piden los eventos de TODAS las cámaras
HILOS      = 5      # consultas en paralelo (si Tuya da error de límite, bajalo a 2)
LOGOS      = ["logo.png", "logo.svg", "logo.jpg", "logo.jpeg"]
# ==============================================================================

print("1. Conectando a Tuya...")
api = TuyaOpenAPI(ENDPOINT, ACCESS_ID, ACCESS_KEY)
api.connect()
print("2. Conectado.")

# ---------- Paso 1: lista de dispositivos ----------
items = []
for uid in UIDS:
    homes = api.get(f"/v1.0/users/{uid}/homes")
    if not homes.get("success"):
        print("Error en homes:", homes)
        continue
    for h in homes["result"]:
        resp = api.get(f"/v1.0/homes/{h['home_id']}/devices")
        if not resp.get("success"):
            print("Error en devices:", resp)
            continue
        for d in resp["result"]:
            items.append((d, h.get("name", "")))
print(f"3. Dispositivos encontrados: {len(items)}. Consultando detalle...")

if not items:
    raise SystemExit("No se encontró ningún dispositivo. Revisá las claves y los UID.")

# ---------- Paso 2: detalle de cada uno ----------
def a_ms(t):
    t = int(t or 0)
    if t == 0:
        return None
    return t if t > 10**12 else t * 1000

def leer_status(d):
    st = d.get("status")
    if isinstance(st, list) and st:
        return {s["code"]: s["value"] for s in st}
    r = api.get(f"/v1.0/devices/{d['id']}/status")
    if r.get("success"):
        return {s["code"]: s["value"] for s in r["result"]}
    return {}

AHORA = int(time.time() * 1000)
HIST = historial.cargar()
# Barrido completo (eventos de TODAS las cámaras) una vez por día; en la otra corrida
# solo las offline o las que cambiaron de estado, para no gastar cuota de la API de Tuya.
BARRIDO = AHORA - HIST.get("ultimo_barrido", 0) > HORAS_BARRIDO * 3600 * 1000
print(f"   Modo: {'barrido completo de eventos' if BARRIDO else 'solo offline / con cambios'}")

def traer_eventos(dev_id):
    """Eventos online(1)/offline(2) desde la última consulta (máx. DIAS_LOG días atrás)."""
    previo = HIST["dispositivos"].get(dev_id, {})
    ini = max(AHORA - DIAS_LOG * 86400 * 1000, previo.get("consultado", 0) - 3600 * 1000)
    eventos, fila = [], None
    for _ in range(20):                                   # paginado (100 por página)
        params = {"type": "1,2", "start_time": ini, "end_time": AHORA, "query_type": 1, "size": 100}
        if fila:
            params["start_row_key"] = fila
        r = api.get(f"/v1.0/devices/{dev_id}/logs", params)
        if not r.get("success"):
            return None
        res = r.get("result") or {}
        eventos += [(a_ms(l["event_time"]), int(l["event_id"])) for l in res.get("logs") or []
                    if l.get("event_id") in (1, 2, "1", "2")]
        fila = res.get("next_row_key")
        if not res.get("has_next") or not fila:
            break
    return eventos

def procesar(par):
    d, locacion = par
    online = bool(d.get("online"))
    st = leer_status(d)

    sd_total = sd_pct = None
    sd_estado = st.get("sd_status")
    nums = [int(x) for x in re.findall(r"\d+", str(st.get("sd_storge", "")))]
    if len(nums) >= 3 and nums[0] > 0:
        sd_total = round(nums[0] / 1024 / 1024, 1)       # asumo KB -> GB
        sd_pct = round(nums[1] * 100 / nums[0])

    tipo, detalle = "OK", ""
    if not online:
        tipo, detalle = "Internet", "Sin conexión (internet o energía del local)"
    elif sd_estado == 5:
        tipo, detalle = "SD", "Sin tarjeta SD"
    elif sd_estado == 2:
        tipo, detalle = "SD", "SD anormal / dañada"
    elif sd_estado == 3 or (sd_pct is not None and sd_pct >= 98):
        tipo, detalle = "SD", "SD llena"
    elif sd_estado == 4:
        tipo, detalle = "SD", "SD formateando"

    eventos = None
    previo = HIST["dispositivos"].get(d.get("id"), {})
    if BARRIDO or not online or previo.get("online") != online:
        try:
            eventos = traer_eventos(d["id"])
        except Exception:
            eventos = None

    return eventos, {
        "nombre": d.get("name", ""),
        "id": d.get("id", ""),
        "modelo": d.get("product_name") or d.get("model", ""),
        "locacion": locacion,
        "online": online,
        "ip": d.get("ip", ""),
        "ultima": a_ms(d.get("update_time")),
        "corte": None,
        "sd_total": sd_total,
        "sd_pct": sd_pct,
        "tipo": tipo,
        "detalle": detalle,
    }

camaras, consultas = [], 0
with ThreadPoolExecutor(max_workers=HILOS) as ex:
    for i, (eventos, c) in enumerate(ex.map(procesar, items), 1):
        dev = historial.sumar_eventos(HIST, c, eventos or [], AHORA)
        if eventos is not None:
            dev["consultado"] = AHORA
            dev.setdefault("cubierto", AHORA - DIAS_LOG * 86400 * 1000)   # desde cuándo hay datos
            consultas += 1
        dev["online"] = c["online"]
        if not c["online"]:
            cortes = [t for t, e in dev["ev"] if e == historial.OFFLINE]
            c["corte"] = max(cortes) if cortes else None
        camaras.append(c)
        if i % 50 == 0:
            print(f"   {i}/{len(items)}")
print(f"   Eventos consultados para {consultas} dispositivos.")
if BARRIDO:
    HIST["ultimo_barrido"] = AHORA
historial.guardar(HIST)
STATS = historial.calcular(HIST, camaras, AHORA)
print(f"Estadísticas: {STATS['total_cortes']} cortes en historial, {STATS['cam_patron']} cámaras con patrón.")

total = len(camaras)
on = sum(1 for c in camaras if c["online"])
off = total - on
sd = sum(1 for c in camaras if c["tipo"] == "SD")
pct = round(on * 100 / total, 1) if total else 0
print(f"Total: {total} | Online: {on} | Offline: {off} | Disponibilidad: {pct}% | Problemas de SD: {sd}")

# ---------- Logo ----------
logo_html = '<span class="logo-txt">gett</span>'
for nombre in LOGOS:
    if os.path.exists(nombre):
        ext = os.path.splitext(nombre)[1].lower().strip(".")
        mime = "svg+xml" if ext == "svg" else ("jpeg" if ext in ("jpg", "jpeg") else ext)
        with open(nombre, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        logo_html = f'<img src="data:image/{mime};base64,{b64}" alt="Gett">'
        print(f"Logo cargado: {nombre}")
        break
else:
    print("Aviso: no encontré logo.png en la carpeta, uso texto 'gett'.")

# ---------- Paso 3: generar HTML ----------
HTML = """<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8"><title>Gett Remoto</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
*{box-sizing:border-box}
body{margin:0;font-family:"Segoe UI",Arial,sans-serif;background:#f9fafc;color:#1b1b3a}
.top{position:fixed;top:0;left:0;right:0;height:60px;background:#1f1650;display:flex;align-items:center;justify-content:space-between;padding:0 24px;color:#cfcaf0;z-index:10;font-size:14px}
.brand{display:flex;align-items:center;gap:12px}
.brand img{height:34px}
.logo-txt{font-size:32px;font-weight:700;font-style:italic;color:#fff;letter-spacing:-1px}
.brand b{color:#ff7a00;font-size:16px}
.side{position:fixed;top:60px;bottom:0;left:0;width:256px;background:#1f1650;padding:12px 8px}
.side a{display:block;padding:10px 12px;border-radius:8px;color:#fff;cursor:pointer;margin-bottom:4px;font-size:15px}
.side a:hover{background:#2a1f6b}
.side a.act{background:#34296e;color:#ff7a00;font-weight:700}
main{margin-left:256px;padding:84px 40px 40px;max-width:1700px}
h1{margin:0;font-size:28px}
.sub{color:#4a5578;margin:2px 0 22px;font-size:15px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.kpi{background:#fff;border:1px solid #e6e8f0;border-radius:14px;padding:18px 20px}
.kpi .l{color:#4a5578;font-size:14px}
.kpi .v{font-size:36px;font-weight:700;margin-top:4px}
.kpi .bar{height:8px;background:#e9ebf3;border-radius:6px;margin-top:10px;overflow:hidden}
.kpi .bar i{display:block;height:100%;border-radius:6px}
.ok{color:#1b9a4a}.bad{color:#d32f2f}.warn{color:#e08a00}
h2{font-size:20px;margin:0 0 12px}
.filtros{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
input,select{padding:10px 14px;font-size:14px;background:#fff;color:#1b1b3a;border:1px solid #cfd3e3;border-radius:999px;outline:none}
input{flex:1;min-width:280px}
input:focus,select:focus{border-color:#ff7a00}
#cuenta{color:#4a5578;margin:8px 0}
.wrap{background:#fff;border:1px solid #e6e8f0;border-radius:14px;overflow:auto;max-height:68vh}
table{width:100%;border-collapse:collapse;font-size:14px}
th{background:#1f1650;color:#fff;text-align:left;padding:10px 12px;position:sticky;top:0;white-space:nowrap;cursor:pointer;user-select:none}
th:hover{background:#34296e}
th .fl{color:#ff7a00}
td{padding:9px 12px;border-bottom:1px solid #eef0f6;white-space:nowrap}
tr:hover td{background:#f4f2ff}
.chip{display:inline-block;padding:3px 10px;border-radius:999px;font-size:12.5px;font-weight:700}
.c-on{background:#e3f6e8;color:#1b7a3a}.c-off{background:#fde8e8;color:#c62828}.c-sd{background:#fff1d6;color:#a86400}
.mut{color:#8a93b2}
.mini{color:#8a93b2;font-size:12.5px;margin-top:6px}
.card{background:#fff;border:1px solid #e6e8f0;border-radius:14px;padding:18px 20px;margin-bottom:16px}
.card h3{margin:0 0 12px;font-size:16px}
.grid2{display:grid;grid-template-columns:3fr 2fr;gap:16px}
.ley{display:flex;gap:16px;font-size:13px;color:#4a5578;margin-bottom:8px}
.ley i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:6px}
.horas{display:flex;align-items:flex-end;gap:3px;height:180px;border-bottom:1px solid #e6e8f0}
.horas .h{flex:1;display:flex;gap:1px;align-items:flex-end;height:100%;position:relative}
.horas .h b{flex:1;border-radius:3px 3px 0 0;min-height:1px}
.horas .h span{position:absolute;bottom:-20px;left:0;right:0;text-align:center;font-size:11px;color:#8a93b2}
.horas{margin-bottom:24px}
.tr{display:flex;align-items:center;gap:10px;margin:7px 0;font-size:13.5px}
.tr .n{width:80px;color:#4a5578}.tr .b{flex:1;background:#f0f1f7;border-radius:6px;height:18px;overflow:hidden}
.tr .b i{display:block;height:100%;background:#3e299b;border-radius:6px}.tr .c{width:50px;text-align:right;font-weight:700}
.calor{display:grid;grid-template-columns:44px repeat(24,1fr);gap:2px;font-size:11px;color:#4a5578}
.calor div{height:22px;border-radius:3px;display:flex;align-items:center;justify-content:center}
.chip.c-pat{background:#efe9ff;color:#3e299b}
.lectura{border-left:5px solid #ff7a00}
.lectura p{margin:0 0 12px;line-height:1.55;font-size:14.5px;max-width:1100px}
.lectura p b.t{display:block;color:#1f1650;margin-bottom:2px}
.lectura p:last-child{margin-bottom:0}
@media(max-width:1100px){.grid2{grid-template-columns:1fr}}
@media(max-width:900px){.side{display:none}main{margin-left:0;padding:84px 16px}.kpis{grid-template-columns:repeat(2,1fr)}}
</style></head><body>

<div class="top">
  <div class="brand">__LOGO__<b>Remoto</b></div>
  <div>Datos al: __FECHA__</div>
</div>

<div class="side">
  <a id="v-todas" class="act" onclick="vista('todas')">Todas las cámaras</a>
  <a id="v-off" onclick="vista('off')">Offline</a>
  <a id="v-sd" onclick="vista('sd')">Problemas de SD</a>
  <a id="v-stats" onclick="vista('stats')">Estadísticas</a>
</div>

<main>
  <h1>Gett Remoto</h1>
  <p class="sub">Estado de las cámaras por punto de venta</p>

  <div class="kpis">
    <div class="kpi"><div class="l">Total de cámaras</div><div class="v" id="kTotal"></div></div>
    <div class="kpi"><div class="l">Online</div><div class="v ok" id="kOn"></div></div>
    <div class="kpi"><div class="l">Offline</div><div class="v bad" id="kOff"></div></div>
    <div class="kpi"><div class="l">Disponibilidad</div><div class="v" id="kPct"></div>
      <div class="bar"><i id="kBar"></i></div></div>
  </div>

  <section id="secCam">
  <h2>Cámaras</h2>
  <div class="filtros">
    <input id="q" placeholder="Buscar por nombre, ID, modelo o locación...">
    <select id="fEstado"><option value="">Estado: todos</option><option value="on">Online</option><option value="off">Offline</option></select>
    <select id="fTipo"><option value="">Error: todos</option><option value="OK">Sin error</option><option value="Internet">Internet</option><option value="SD">SD</option></select>
    <select id="fModelo"><option value="">Modelo: todos</option></select>
  </div>
  <div id="cuenta"></div>
  <div class="wrap"><table><thead><tr>
    <th data-k="nombre" onclick="ordenar('nombre')">Nombre<span class="fl"></span></th>
    <th data-k="id" onclick="ordenar('id')">ID dispositivo<span class="fl"></span></th>
    <th data-k="modelo" onclick="ordenar('modelo')">Modelo<span class="fl"></span></th>
    <th data-k="locacion" onclick="ordenar('locacion')">Locación (app)<span class="fl"></span></th>
    <th data-k="online" onclick="ordenar('online')">Estado<span class="fl"></span></th>
    <th data-k="ip" onclick="ordenar('ip')">Internet<span class="fl"></span></th>
    <th data-k="corte" onclick="ordenar('corte')">Sin conexión desde<span class="fl"></span></th>
    <th data-k="dias" onclick="ordenar('dias')">Días offline<span class="fl"></span></th>
    <th data-k="ultima" onclick="ordenar('ultima')">Última actividad<span class="fl"></span></th>
    <th data-k="sd_total" onclick="ordenar('sd_total')">Memoria SD<span class="fl"></span></th>
    <th data-k="tipo" onclick="ordenar('tipo')">Error<span class="fl"></span></th>
  </tr></thead><tbody id="tb"></tbody></table></div>
  </section>

  <section id="secStats" hidden>
  <h2>Estadísticas de conexión</h2>
  <p class="sub" id="sDesde"></p>
  <div class="card lectura"><h3>Lectura de los datos</h3><div id="sAnalisis"></div></div>
  <div class="kpis">
    <div class="kpi"><div class="l">Cortes reales (10 min o más)</div><div class="v" id="sCortes"></div><div class="mini" id="sVueltas"></div></div>
    <div class="kpi"><div class="l">Tiempo típico en volver (mediana)</div><div class="v" id="sMed"></div><div class="mini" id="sRap"></div></div>
    <div class="kpi"><div class="l">Cortes por horario del local</div><div class="v warn" id="sNoct"></div><div class="mini">se apaga de noche y vuelve al abrir</div></div>
    <div class="kpi"><div class="l">Cámaras con patrón repetido</div><div class="v" id="sPat"></div><div class="mini" id="sMicro"></div></div>
  </div>
  <div class="grid2">
    <div class="card"><h3>¿A qué hora se cortan y a qué hora vuelven? <span class="mut" style="font-weight:400;font-size:13px">(cortes de 10 min o más)</span></h3>
      <div class="ley"><span><i style="background:#d32f2f"></i>Se desconecta</span><span><i style="background:#1b9a4a"></i>Vuelve a conectarse</span></div>
      <div id="gHoras" class="horas"></div></div>
    <div class="card"><h3>¿Cuánto tardan en volver?</h3><div id="gTramos"></div></div>
  </div>
  <div class="card"><h3>Desconexiones por día y hora</h3><div id="gCalor"></div></div>
  <h2 style="margin-top:24px">Sucursales con problema de internet o energía</h2>
  <p class="sub">Sucursales donde las cámaras se caen <b>todas a la vez</b> (con menos de 3 minutos de diferencia). Cuando pasa seguido, el problema es del local y no de la cámara.</p>
  <div class="wrap" style="max-height:52vh"><table><thead><tr>
    <th>Locación</th><th>Cámaras</th><th>Cortes juntos</th><th>% de coincidencia</th>
    <th>En horario de atención</th><th>De noche</th><th>Micro-cortes juntos</th><th>Ahora</th><th>Diagnóstico</th>
  </tr></thead><tbody id="sucTb"></tbody></table></div>
  <h2 style="margin-top:24px">Por cámara</h2>
  <div class="filtros">
    <input id="sq" placeholder="Buscar por nombre o locación...">
    <select id="sPatF"><option value="">Patrón: todos</option><option value="1">Con patrón</option><option value="off">Offline ahora</option></select>
  </div>
  <div class="wrap"><table><thead><tr>
    <th data-s="nombre">Nombre<span class="fl"></span></th>
    <th data-s="loc">Locación<span class="fl"></span></th>
    <th data-s="dias">Días offline<span class="fl"></span></th>
    <th data-s="cortes">Cortes (10 min o más)<span class="fl"></span></th>
    <th data-s="micro">Micro-cortes<span class="fl"></span></th>
    <th data-s="vueltas">Volvió sola / reconectada<span class="fl"></span></th>
    <th data-s="med">Tiempo típico en volver<span class="fl"></span></th>
    <th data-s="max">Corte más largo<span class="fl"></span></th>
    <th data-s="hora">Hora más frecuente de corte<span class="fl"></span></th>
    <th data-s="noct">Cortes nocturnos<span class="fl"></span></th>
    <th data-s="patron">Patrón detectado<span class="fl"></span></th>
  </tr></thead><tbody id="stb"></tbody></table></div>
  </section>
</main>

<script>
const DATA = __DATA__;
const STATS = __STATS__;
const $ = id => document.getElementById(id);
const fmt = ms => ms ? new Date(ms).toLocaleString('es-AR', {hourCycle: 'h23'}) : '<span class="mut">—</span>';

// --- Contador de días offline (se calcula en vivo al abrir la página) ---
const FILA = {}; STATS.filas.forEach(f => FILA[f.id] = f);
function diasDesde(ms){ return ms ? (Date.now() - ms) / 86400000 : null; }
DATA.forEach(d => {
  const f = FILA[d.id] || {};
  const desde = d.online ? null : (d.corte || f.desde);
  d.dias = d.online ? null : diasDesde(desde);
  d.aprox = !d.online && !d.corte && !!f.aprox;
});
STATS.filas.forEach(f => f.dias = f.online ? null : diasDesde(f.desde));
function fmtDias(v, aprox){
  if (v === null || v === undefined) return '<span class="mut">—</span>';
  const d = Math.floor(v), h = Math.floor((v - d) * 24);
  const txt = (aprox ? '≥ ' : '') + (d ? d + ' d ' : '') + h + ' h';
  const cls = v >= 7 ? 'bad' : (v >= 2 ? 'warn' : '');
  return '<b class="' + cls + '">' + txt + '</b>';
}
function fmtMin(m){
  if (m === null || m === undefined) return '<span class="mut">—</span>';
  if (m < 60) return m + ' min';
  if (m < 1440) return (m / 60).toFixed(1).replace('.0', '') + ' h';
  return (m / 1440).toFixed(1).replace('.0', '') + ' días';
}

// --- KPIs (siempre sobre el total, no cambian con los filtros) ---
const total = DATA.length, on = DATA.filter(d => d.online).length, off = total - on;
const pct = total ? on * 100 / total : 0;
const col = pct >= 95 ? 'ok' : (pct >= 90 ? 'warn' : 'bad');
const colHex = pct >= 95 ? '#1b9a4a' : (pct >= 90 ? '#e08a00' : '#d32f2f');
$('kTotal').textContent = total;
$('kOn').textContent = on;
$('kOff').textContent = off;
$('kPct').textContent = pct.toFixed(1) + '%';
$('kPct').className = 'v ' + col;
$('kBar').style.width = pct + '%';
$('kBar').style.background = colHex;

// --- Filtros ---
[...new Set(DATA.map(d => d.modelo))].sort().forEach(m => {
  const o = document.createElement('option'); o.value = m; o.textContent = m; $('fModelo').appendChild(o);
});
// Orden por defecto: offline primero, cortes más recientes arriba
DATA.sort((a, b) => (a.online - b.online) || ((b.corte || b.ultima || 0) - (a.corte || a.ultima || 0)));

// --- Orden por columna (1er click A-Z, 2do click Z-A, 3er click vuelve al orden original) ---
let sortK = null, sortD = 1;
function ordenar(k){
  if (sortK !== k) { sortK = k; sortD = 1; }
  else if (sortD === 1) { sortD = -1; }
  else { sortK = null; sortD = 1; }
  pintar();
}
function vacio(v){ return v === null || v === undefined || v === ''; }
function cmp(a, b){
  const x = a[sortK], y = b[sortK];
  if (vacio(x) && vacio(y)) return 0;
  if (vacio(x)) return 1;     // los vacíos siempre al final
  if (vacio(y)) return -1;
  if (typeof x === 'string') return x.localeCompare(y, 'es', {numeric: true, sensitivity: 'base'}) * sortD;
  return (Number(x) - Number(y)) * sortD;
}

function marcar(n){
  ['todas','off','sd','stats'].forEach(x => $('v-' + x).classList.toggle('act', x === n));
}
function vista(n){
  $('secStats').hidden = n !== 'stats';
  $('secCam').hidden = n === 'stats';
  if (n === 'stats') { marcar(n); return; }
  $('q').value = ''; $('fModelo').value = '';
  $('fEstado').value = (n === 'off') ? 'off' : '';
  $('fTipo').value = (n === 'sd') ? 'SD' : '';
  marcar(n); pintar();
}

function pintar(){
  const t = $('q').value.toLowerCase(), e = $('fEstado').value, ti = $('fTipo').value, mo = $('fModelo').value;
  const r = DATA.filter(d =>
    (d.nombre+' '+d.id+' '+d.modelo+' '+d.locacion).toLowerCase().includes(t) &&
    (!e || (e === 'on') === d.online) && (!ti || d.tipo === ti) && (!mo || d.modelo === mo));
  if (sortK) r.sort(cmp);
  document.querySelectorAll('th[data-k]').forEach(th => {
    th.querySelector('.fl').textContent = th.dataset.k === sortK ? (sortD === 1 ? ' ▲' : ' ▼') : '';
  });
  $('cuenta').textContent = r.length + ' de ' + DATA.length + ' cámaras';
  $('tb').innerHTML = r.map(d => {
    const estado = d.online ? '<span class="chip c-on">Online</span>' : '<span class="chip c-off">Offline</span>';
    const internet = d.online ? 'Conectada' + (d.ip ? ' <span class="mut">(' + d.ip + ')</span>' : '') : '<span class="bad">Sin conexión</span>';
    const corte = d.online ? '<span class="mut">—</span>' : (d.corte ? fmt(d.corte) : '<span class="mut">sin dato en historial</span>');
    const sd = d.sd_total ? d.sd_total + ' GB (' + d.sd_pct + '% usado)' : '<span class="mut">sin dato</span>';
    const err = d.tipo === 'OK' ? '<span class="chip c-on">OK</span>'
      : '<span class="chip ' + (d.tipo === 'SD' ? 'c-sd' : 'c-off') + '">' + d.tipo + ': ' + d.detalle + '</span>';
    return `<tr><td>${d.nombre}</td><td>${d.id}</td><td>${d.modelo}</td><td>${d.locacion}</td>
      <td>${estado}</td><td>${internet}</td><td>${corte}</td><td>${fmtDias(d.dias, d.aprox)}</td><td>${fmt(d.ultima)}</td><td>${sd}</td><td>${err}</td></tr>`;
  }).join('');
}
['q','fEstado','fTipo','fModelo'].forEach(id => $(id).addEventListener('input', () => { marcar(null); pintar(); }));
pintar();

// ================= Estadísticas =================
const DIAS_SEM = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'];
$('sDesde').textContent = STATS.hist_desde
  ? 'Historial acumulado desde el ' + new Date(STATS.hist_desde).toLocaleDateString('es-AR') + '. Cuantos más días pasen, más confiables son los números.'
  : 'Todavía no hay historial: se empieza a juntar en la próxima actualización.';
$('sAnalisis').innerHTML = (STATS.analisis || []).map(([t, x]) => `<p><b class="t">${t}</b>${x}</p>`).join('');
$('sucTb').innerHTML = (STATS.sucursales || []).map(s => `<tr><td>${s.loc}</td><td>${s.camaras}</td>
  <td><b>${s.juntos}</b> <span class="mut">de ${s.eventos}</span></td><td>${s.pct}%</td>
  <td>${s.dia || '<span class="mut">0</span>'}</td><td>${s.noche || '<span class="mut">0</span>'}</td><td>${s.micro || '<span class="mut">0</span>'}</td>
  <td>${s.ahora_off ? '<span class="chip c-off">Todas offline</span>' : '<span class="chip c-on">Online</span>'}</td>
  <td><span class="chip ${s.diag.startsWith('Internet') ? 'c-off' : (s.diag.startsWith('Todas') ? 'c-sd' : 'c-pat')}">${s.diag}</span></td></tr>`).join('')
  || '<tr><td colspan="9" class="mut">Sin sucursales con cortes simultáneos repetidos.</td></tr>';
$('sCortes').textContent = STATS.total_cortes;
$('sVueltas').textContent = STATS.total_vueltas + ' volvieron a conectarse';
$('sMed').innerHTML = fmtMin(STATS.mediana);
$('sRap').textContent = STATS.pct_rapidas === null ? '' : STATS.pct_rapidas + '% vuelve en menos de 1 hora';
$('sNoct').textContent = STATS.nocturnos;
$('sPat').textContent = STATS.cam_patron;
$('sMicro').textContent = STATS.micro + ' micro-cortes (< 10 min)';

(function(){
  const mx = Math.max(1, ...STATS.hora_off, ...STATS.hora_on);
  $('gHoras').innerHTML = STATS.hora_off.map((v, h) =>
    `<div class="h" title="${String(h).padStart(2,'0')} h: ${v} cortes, ${STATS.hora_on[h]} vueltas">` +
    `<b style="height:${v*100/mx}%;background:#d32f2f"></b><b style="height:${STATS.hora_on[h]*100/mx}%;background:#1b9a4a"></b>` +
    `<span>${h % 3 === 0 ? String(h).padStart(2,'0') : ''}</span></div>`).join('');
  const mt = Math.max(1, ...STATS.tramos.map(t => t[1]));
  $('gTramos').innerHTML = STATS.tramos.map(([n, v]) =>
    `<div class="tr"><span class="n">${n}</span><span class="b"><i style="width:${v*100/mt}%"></i></span><span class="c">${v}</span></div>`).join('');
  const mc = Math.max(1, ...STATS.calor.flat());
  let html = '<div></div>' + [...Array(24).keys()].map(h => `<div>${h % 3 === 0 ? String(h).padStart(2,'0') : ''}</div>`).join('');
  STATS.calor.forEach((fila, i) => {
    html += `<div>${DIAS_SEM[i]}</div>` + fila.map((v, h) =>
      `<div title="${DIAS_SEM[i]} ${String(h).padStart(2,'0')} h: ${v} cortes" style="background:rgba(211,47,47,${v ? 0.08 + 0.92 * v / mc : 0.03})"></div>`).join('');
  });
  $('gCalor').innerHTML = '<div class="calor">' + html + '</div>';
})();

let sK = 'cortes', sD = -1;
document.querySelectorAll('th[data-s]').forEach(th => th.addEventListener('click', () => {
  const k = th.dataset.s;
  if (sK !== k) { sK = k; sD = -1; } else sD = -sD;
  pintarStats();
}));
function pintarStats(){
  const t = $('sq').value.toLowerCase(), p = $('sPatF').value;
  const r = STATS.filas.filter(f => (f.nombre + ' ' + f.loc).toLowerCase().includes(t) &&
    (!p || (p === '1' ? f.patron : !f.online)));
  r.sort((a, b) => {
    const x = a[sK], y = b[sK];
    if (vacio(x) && vacio(y)) return 0; if (vacio(x)) return 1; if (vacio(y)) return -1;
    return (typeof x === 'string' ? x.localeCompare(y, 'es', {numeric: true}) : x - y) * sD;
  });
  document.querySelectorAll('th[data-s]').forEach(th => th.querySelector('.fl').textContent = th.dataset.s === sK ? (sD === 1 ? ' ▲' : ' ▼') : '');
  $('stb').innerHTML = r.map(f => `<tr><td>${f.nombre}</td><td>${f.loc}</td><td>${fmtDias(f.dias, f.aprox)}</td>
    <td>${f.cortes}</td><td>${f.micro || '<span class="mut">0</span>'}</td><td>${f.vueltas}</td><td>${fmtMin(f.med)}</td><td>${fmtMin(f.max)}</td>
    <td>${f.hora === null ? '<span class="mut">—</span>' : String(f.hora).padStart(2,'0') + ' h <span class="mut">(' + f.hora_pct + '%)</span>'}</td>
    <td>${f.noct || '<span class="mut">0</span>'}</td>
    <td>${f.patron ? '<span class="chip c-pat">' + f.patron + '</span>' : '<span class="mut">—</span>'}</td></tr>`).join('');
}
['sq','sPatF'].forEach(id => $(id).addEventListener('input', pintarStats));
pintarStats();
</script></body></html>"""

fecha = datetime.now(ZoneInfo("America/Argentina/Cordoba")).strftime("%d/%m/%Y %H:%M")
salida = (HTML.replace("__STATS__", json.dumps(STATS, ensure_ascii=False))
              .replace("__DATA__", json.dumps(camaras, ensure_ascii=False))
              .replace("__FECHA__", fecha)
              .replace("__LOGO__", logo_html))
with open("buscador.html", "w", encoding="utf-8") as f:
    f.write(salida)

print("Listo: buscador.html generado")