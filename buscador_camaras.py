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

socket.setdefaulttimeout(20)

# ====== CONFIGURACIÓN (las claves vienen de GitHub, no se escriben acá) ======
ACCESS_ID  = os.environ["TUYA_ACCESS_ID"]
ACCESS_KEY = os.environ["TUYA_ACCESS_KEY"]
ENDPOINT   = "https://openapi.tuyaus.com"
UIDS       = [u.strip() for u in os.environ["TUYA_UIDS"].split(",") if u.strip()]
DIAS_LOG   = 7      # cuántos días atrás buscar el corte en el historial
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

def buscar_corte(dev_id):
    ahora = int(time.time() * 1000)
    ini = ahora - DIAS_LOG * 86400 * 1000
    r = api.get(f"/v1.0/devices/{dev_id}/logs",
                {"type": "1,2", "start_time": ini, "end_time": ahora,
                 "query_type": 1, "size": 50})
    if not r.get("success"):
        return None
    logs = (r.get("result") or {}).get("logs") or []
    cortes = [l["event_time"] for l in logs if l.get("event_id") == 2]
    return max(cortes) if cortes else None

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

    corte = None
    if not online:
        try:
            corte = buscar_corte(d["id"])
        except Exception:
            corte = None

    return {
        "nombre": d.get("name", ""),
        "id": d.get("id", ""),
        "modelo": d.get("product_name") or d.get("model", ""),
        "locacion": locacion,
        "online": online,
        "ip": d.get("ip", ""),
        "ultima": a_ms(d.get("update_time")),
        "corte": corte,
        "sd_total": sd_total,
        "sd_pct": sd_pct,
        "tipo": tipo,
        "detalle": detalle,
    }

camaras = []
with ThreadPoolExecutor(max_workers=HILOS) as ex:
    for i, c in enumerate(ex.map(procesar, items), 1):
        camaras.append(c)
        if i % 50 == 0:
            print(f"   {i}/{len(items)}")

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
    <th data-k="ultima" onclick="ordenar('ultima')">Última actividad<span class="fl"></span></th>
    <th data-k="sd_total" onclick="ordenar('sd_total')">Memoria SD<span class="fl"></span></th>
    <th data-k="tipo" onclick="ordenar('tipo')">Error<span class="fl"></span></th>
  </tr></thead><tbody id="tb"></tbody></table></div>
</main>

<script>
const DATA = __DATA__;
const $ = id => document.getElementById(id);
const fmt = ms => ms ? new Date(ms).toLocaleString('es-AR') : '<span class="mut">—</span>';

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
  ['todas','off','sd'].forEach(x => $('v-' + x).classList.toggle('act', x === n));
}
function vista(n){
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
      <td>${estado}</td><td>${internet}</td><td>${corte}</td><td>${fmt(d.ultima)}</td><td>${sd}</td><td>${err}</td></tr>`;
  }).join('');
}
['q','fEstado','fTipo','fModelo'].forEach(id => $(id).addEventListener('input', () => { marcar(null); pintar(); }));
pintar();
</script></body></html>"""

fecha = datetime.now(ZoneInfo("America/Argentina/Cordoba")).strftime("%d/%m/%Y %H:%M")
salida = (HTML.replace("__DATA__", json.dumps(camaras, ensure_ascii=False))
              .replace("__FECHA__", fecha)
              .replace("__LOGO__", logo_html))
with open("buscador.html", "w", encoding="utf-8") as f:
    f.write(salida)

print("Listo: buscador.html generado")