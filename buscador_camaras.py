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
HORAS_BARRIDO = 72  # cada cuántas horas se piden los eventos de TODAS las cámaras (Tuya guarda 7 días)
HILOS      = 5      # consultas en paralelo (si Tuya da error de límite, bajalo a 2)
LOGOS      = ["logo-blanco.png", "logo.png", "logo.svg", "logo.jpg", "logo.jpeg"]
# ==============================================================================

print("1. Conectando a Tuya...")
api = TuyaOpenAPI(ENDPOINT, ACCESS_ID, ACCESS_KEY)
api.connect()
print("2. Conectado.")

# Contador de consultas: el plan de Tuya tiene cuota limitada
import threading
_lock, CONSULTAS_API = threading.Lock(), [0]
_get = api.get
def _get_contado(*a, **k):
    with _lock:
        CONSULTAS_API[0] += 1
    return _get(*a, **k)
api.get = _get_contado

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
# Barrido completo (eventos de TODAS las cámaras) cada HORAS_BARRIDO; en las demás corridas
# solo las que cambiaron de estado o nunca se consultaron, para no gastar cuota de la API de Tuya.
BARRIDO = AHORA - HIST.get("ultimo_barrido", 0) > HORAS_BARRIDO * 3600 * 1000
print(f"   Modo: {'barrido completo de eventos' if BARRIDO else 'solo con cambios'}")

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
    if BARRIDO or previo.get("online") != online or not previo.get("consultado"):
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
print(f"   Consultas a la API de Tuya en esta corrida: {CONSULTAS_API[0]}")
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
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#1f1650">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#f4f5f9;--card:#fff;--ink:#16162e;--ink2:#51566e;--mut:#8f94aa;--line:#e6e7ef;--line2:#f0f1f6;
  --brand:#1f1650;--acc:#ff7a00;
  --ok:#16a34a;--ok-ink:#15803d;--ok-soft:#e7f6ec;
  --bad:#dc2626;--bad-ink:#b91c1c;--bad-soft:#fdeceb;
  --warn:#d97706;--warn-ink:#b45309;--warn-soft:#fdf3e1;
  --pat:#5b47c7;--pat-soft:#eeebfc;
  --r:14px;--sh:0 1px 2px rgba(22,22,46,.04),0 2px 6px rgba(22,22,46,.04);
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;font:14px/1.45 Inter,"Segoe UI",system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--ink);font-variant-numeric:tabular-nums}
b{font-weight:600}

/* ---------- Navegación ---------- */
.side{position:fixed;top:0;bottom:0;left:0;width:232px;background:var(--brand);display:flex;flex-direction:column;padding:22px 12px 16px;z-index:20}
.brand{display:flex;align-items:center;gap:10px;padding:2px 12px 28px}
.brand b{padding-bottom:6px}
.brand img{height:36px;display:block}
.logo-txt{font-size:28px;font-weight:700;font-style:italic;color:#fff;letter-spacing:-1px}
.brand b{color:var(--acc);font-weight:600;font-size:15px}
.nav{display:flex;flex-direction:column;gap:2px}
.nav a{display:flex;align-items:center;gap:12px;padding:10px 12px;border-radius:10px;color:#bdb8e3;cursor:pointer;font-weight:500;font-size:14px;transition:background .15s,color .15s;-webkit-tap-highlight-color:transparent}
.nav a svg{width:18px;height:18px;flex:none}
.nav a:hover{background:rgba(255,255,255,.06);color:#fff}
.nav a.act{background:rgba(255,255,255,.11);color:#fff}
.nav a.act svg{color:var(--acc)}
.nav .cnt{margin-left:auto;font-size:11.5px;font-weight:600;min-width:22px;text-align:center;padding:1px 7px;border-radius:999px;background:rgba(255,255,255,.12);color:#fff}
.nav .cnt.r{background:var(--bad)}
.nav .cnt.a{background:var(--acc)}
.nav .cnt:empty{display:none}
.nav .s{display:none}
.side .foot{margin-top:auto;padding:14px 12px 0;color:#8f89c4;font-size:12px;border-top:1px solid rgba(255,255,255,.08);line-height:1.5}
.side .foot b{color:#d9d5f5;font-weight:500;display:block}
.mtop{display:none}

/* ---------- Contenido ---------- */
main{margin-left:232px;padding:30px 36px 56px;max-width:1660px}
.head{display:flex;align-items:flex-end;justify-content:space-between;gap:12px 20px;margin-bottom:22px;flex-wrap:wrap}
h1{margin:0;font-size:24px;font-weight:700;letter-spacing:-.4px}
.sub{color:var(--ink2);margin:4px 0 0;font-size:14px;max-width:900px}
section > .sub{margin:-8px 0 18px}
.stamp{display:inline-flex;align-items:center;gap:8px;font-size:12.5px;color:var(--ink2);background:var(--card);border:1px solid var(--line);padding:6px 12px;border-radius:999px;white-space:nowrap}
.stamp i{width:7px;height:7px;border-radius:50%;background:var(--ok);box-shadow:0 0 0 3px var(--ok-soft)}
h2{font-size:16px;font-weight:600;margin:30px 0 12px;letter-spacing:-.1px}
h2 + .sub{margin:-6px 0 14px}

.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:22px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:var(--r);padding:16px 18px;box-shadow:var(--sh);min-width:0}
.kpi .l{color:var(--ink2);font-size:13px;font-weight:500}
.kpi .v{font-size:30px;font-weight:700;margin-top:6px;letter-spacing:-.6px;line-height:1.15}
.kpi .bar{height:6px;background:var(--line2);border-radius:6px;margin-top:12px;overflow:hidden}
.kpi .bar i{display:block;height:100%;border-radius:6px}
.ok{color:var(--ok-ink)}.bad{color:var(--bad-ink)}.warn{color:var(--warn-ink)}
.mut{color:var(--mut)}
.mini{color:var(--mut);font-size:12.5px;margin-top:6px}

.filtros{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
input,select{height:38px;padding:0 14px;font:inherit;font-size:13.5px;background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:10px;outline:none;transition:border-color .15s,box-shadow .15s}
input{flex:1;min-width:260px;padding-left:38px;background:var(--card) url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='none' stroke='%238f94aa' stroke-width='2' stroke-linecap='round' viewBox='0 0 24 24'%3E%3Ccircle cx='11' cy='11' r='7'/%3E%3Cpath d='m20 20-3.5-3.5'/%3E%3C/svg%3E") no-repeat 13px 50%}
select{-webkit-appearance:none;appearance:none;padding-right:34px;cursor:pointer;background:var(--card) url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='none' stroke='%2351566e' stroke-width='2.2' stroke-linecap='round' viewBox='0 0 24 24'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E") no-repeat right 12px center}
input:focus,select:focus{border-color:var(--acc);box-shadow:0 0 0 3px var(--acc-soft,#fff1e3)}
#cuenta{color:var(--mut);margin:4px 2px 10px;font-size:12.5px}

.wrap{background:var(--card);border:1px solid var(--line);border-radius:var(--r);overflow:auto;max-height:70vh;box-shadow:var(--sh)}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:13.5px}
th{background:#fafbfd;color:var(--ink2);font-weight:600;font-size:11.5px;text-transform:uppercase;letter-spacing:.45px;text-align:left;padding:11px 14px;position:sticky;top:0;z-index:1;white-space:nowrap;cursor:pointer;user-select:none;border-bottom:1px solid var(--line)}
th:hover{color:var(--ink)}
th .fl{color:var(--acc)}
td{padding:11px 14px;border-bottom:1px solid var(--line2);white-space:nowrap}
tbody tr:last-child td{border-bottom:0}
tbody tr:hover td{background:#fafaff}
td:first-child{font-weight:500}

.chip{display:inline-flex;align-items:center;gap:6px;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;line-height:1.6;white-space:nowrap}
.c-on{background:var(--ok-soft);color:var(--ok-ink)}
.c-off{background:var(--bad-soft);color:var(--bad-ink)}
.c-sd{background:var(--warn-soft);color:var(--warn-ink)}
.chip.c-pat{background:var(--pat-soft);color:var(--pat)}
.c-on::before,.c-off::before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor;flex:none}

.card{background:var(--card);border:1px solid var(--line);border-radius:var(--r);padding:18px 20px;margin-bottom:14px;box-shadow:var(--sh);min-width:0}
.card h3{margin:0 0 14px;font-size:14.5px;font-weight:600}
.grid2{display:grid;grid-template-columns:minmax(0,3fr) minmax(0,2fr);gap:14px}
.ley{display:flex;gap:16px;font-size:12.5px;color:var(--ink2);margin-bottom:10px;flex-wrap:wrap}
.ley i{display:inline-block;width:9px;height:9px;border-radius:3px;margin-right:6px}
.horas{display:flex;align-items:flex-end;gap:3px;height:180px;border-bottom:1px solid var(--line);margin-bottom:24px}
.horas .h{flex:1;display:flex;gap:1px;align-items:flex-end;height:100%;position:relative}
.horas .h b{flex:1;border-radius:3px 3px 0 0;min-height:1px}
.horas .h span{position:absolute;bottom:-20px;left:-4px;right:-4px;text-align:center;font-size:11px;color:var(--mut)}
.tr{display:flex;align-items:center;gap:10px;margin:8px 0;font-size:13px}
.tr .n{width:80px;color:var(--ink2);flex:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tr .b{flex:1;background:var(--line2);border-radius:6px;height:16px;overflow:hidden;min-width:0}
.tr .b i{display:block;height:100%;background:var(--pat);border-radius:6px}
.tr .c{width:44px;text-align:right;font-weight:600}
.calor{display:grid;grid-template-columns:38px repeat(24,minmax(0,1fr));gap:2px;font-size:11px;color:var(--ink2)}
.calor div{height:22px;border-radius:3px;display:flex;align-items:center;justify-content:center}
.lectura{border-left:4px solid var(--acc);background:linear-gradient(90deg,#fffaf5,var(--card) 240px)}
.lectura p{margin:0 0 12px;line-height:1.6;font-size:14px;max-width:1100px;color:#2c2f45}
.lectura p b.t{display:block;color:var(--ink);margin-bottom:2px}
.lectura p:last-child{margin-bottom:0}

/* ---------- Tablet ---------- */
@media(max-width:1200px){.grid2{grid-template-columns:minmax(0,1fr)}.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}}

/* ---------- Celular ---------- */
@media(max-width:860px){
  .mtop{display:flex;position:sticky;top:0;z-index:15;background:var(--brand);height:56px;padding:0 16px;align-items:center;justify-content:space-between;color:#bdb8e3;font-size:12px}
  .mtop .brand{padding:0;align-items:center}
  .mtop .brand img{height:26px}
  .side{top:auto;width:auto;right:0;flex-direction:row;padding:6px 4px calc(6px + env(safe-area-inset-bottom));box-shadow:0 -4px 16px rgba(22,22,46,.12)}
  .side > .brand,.side .foot{display:none}
  .nav{flex-direction:row;width:100%;gap:0}
  .nav a{flex:1;flex-direction:column;gap:4px;padding:7px 2px;font-size:10.5px;font-weight:500;position:relative;border-radius:8px;min-width:0}
  .nav a svg{width:21px;height:21px}
  .nav a.act{background:none}
  .nav a.act span.s{color:#fff;font-weight:600}
  .nav .l{display:none}.nav .s{display:block;white-space:nowrap}
  .nav .cnt{position:absolute;top:1px;left:calc(50% + 5px);margin:0;font-size:10px;min-width:18px;padding:0 5px;line-height:16px}
  main{margin-left:0;padding:20px 16px calc(96px + env(safe-area-inset-bottom))}
  .head .stamp{display:none}
  h1{font-size:21px}
  .kpis{gap:10px;margin-bottom:18px}
  .kpi{padding:13px 14px}
  .kpi .v{font-size:25px}
  .kpi .l{font-size:12.5px}
  .filtros input{min-width:0;flex-basis:100%}
  .filtros select{flex:1 1 40%;min-width:0}
  .card{padding:16px}
  .calor{grid-template-columns:30px repeat(24,minmax(0,1fr));gap:1px;font-size:9.5px}
  .calor div{height:16px}
  .tr .n{width:110px!important}
  /* las tablas pasan a tarjetas */
  .wrap{max-height:none;overflow:visible;background:none;border:0;box-shadow:none;border-radius:0}
  .wrap table,.wrap tbody,.wrap tr,.wrap td{display:block;width:100%}
  .wrap thead{display:none}
  .wrap tr{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:6px 14px;margin-bottom:10px;box-shadow:var(--sh)}
  .wrap td{display:flex;justify-content:space-between;align-items:baseline;gap:14px;padding:6px 0;border:0;white-space:normal!important;min-width:0!important;text-align:right;word-break:break-word}
  .wrap td::before{content:attr(data-l);color:var(--mut);font-size:12px;text-align:left;flex:none;max-width:45%}
  .wrap td:first-child{font-weight:600;font-size:15px;text-align:left;padding:8px 0;border-bottom:1px solid var(--line2);margin-bottom:4px}
  .wrap td:first-child::before{display:none}
  .wrap td > .v{min-width:0}
  .wrap td.largo{display:block;text-align:left}
  .wrap td.largo::before{display:block;max-width:none;margin-bottom:3px}
  .wrap .chip{white-space:normal;line-height:1.35;padding:3px 10px;text-align:left}
  #tb td:nth-child(2),#tb td:nth-child(6),#tb td:nth-child(9){display:none}
  .wrap td[colspan]{justify-content:center}.wrap td[colspan]::before{display:none}
  tbody tr:hover td{background:none}
}
</style></head><body>

<div class="mtop"><div class="brand">__LOGO__<b>Remoto</b></div><div>Datos al __FECHA__</div></div>

<div class="side">
  <div class="brand">__LOGO__<b>Remoto</b></div>
  <nav class="nav">
    <a id="v-todas" class="act" onclick="vista('todas')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m16 10 4.6-2.3a1 1 0 0 1 1.4.9v6.8a1 1 0 0 1-1.4.9L16 14"/><rect x="2" y="6" width="14" height="12" rx="2"/></svg><span class="l">Todas las cámaras</span><span class="s">Cámaras</span></a>
    <a id="v-off" onclick="vista('off')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 2l20 20M8.5 16.4a5 5 0 0 1 7 0M5 12.9a10 10 0 0 1 5.2-2.8M19 12.9a10 10 0 0 0-2.3-1.6M2 8.8a15 15 0 0 1 4.2-2.7M22 8.8A15 15 0 0 0 10.7 5M12 20h.01"/></svg><span class="l">Offline</span><span class="s">Offline</span><span class="cnt r" id="nOff"></span></a>
    <a id="v-sd" onclick="vista('sd')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 2h9l5 5v13a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2zM9 6v3M12 6v3M15 7v2"/></svg><span class="l">Problemas de SD</span><span class="s">SD</span><span class="cnt a" id="nSd"></span></a>
    <a id="v-stats" onclick="vista('stats')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18M8 17v-5M13 17V8M18 17v-3"/></svg><span class="l">Estadísticas</span><span class="s">Estadísticas</span></a>
  </nav>
  <div class="foot"><b>Datos al __FECHA__</b>Se actualiza a las 8:00, 11:30 y 14:00</div>
</div>

<main>
  <div class="head">
    <div><h1 id="titulo">Todas las cámaras</h1><p class="sub" id="subtit">Estado de cada cámara por punto de venta</p></div>
    <span class="stamp"><i></i>Datos al __FECHA__</span>
  </div>

  <section id="secCam">
  <div class="kpis">
    <div class="kpi"><div class="l">Total de cámaras</div><div class="v" id="kTotal"></div></div>
    <div class="kpi"><div class="l">Online</div><div class="v ok" id="kOn"></div></div>
    <div class="kpi"><div class="l">Offline</div><div class="v bad" id="kOff"></div></div>
    <div class="kpi"><div class="l">Disponibilidad</div><div class="v" id="kPct"></div>
      <div class="bar"><i id="kBar"></i></div></div>
  </div>
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
  <p class="sub" id="sDesde"></p>
  <div class="card lectura"><h3>Lectura de los datos</h3><div id="sAnalisis"></div></div>
  <div class="kpis">
    <div class="kpi"><div class="l">Cortes reales (10 min o más)</div><div class="v" id="sCortes"></div><div class="mini" id="sVueltas"></div></div>
    <div class="kpi"><div class="l">Tiempo típico en volver</div><div class="v" id="sMed"></div><div class="mini" id="sRap"></div></div>
    <div class="kpi"><div class="l">Cortes por horario del local</div><div class="v warn" id="sNoct"></div><div class="mini">se apaga de noche y vuelve al abrir</div></div>
    <div class="kpi"><div class="l">Cámaras con patrón repetido</div><div class="v" id="sPat"></div><div class="mini" id="sMicro"></div></div>
  </div>
  <div class="grid2">
    <div class="card"><h3>¿A qué hora se cortan y a qué hora vuelven? <span class="mut" style="font-weight:400;font-size:12.5px">(cortes de 10 min o más)</span></h3>
      <div class="ley"><span><i style="background:#dc2626"></i>Se desconecta</span><span><i style="background:#16a34a"></i>Vuelve a conectarse</span></div>
      <div id="gHoras" class="horas"></div></div>
    <div class="card"><h3>¿Cuánto tardan en volver?</h3><div id="gTramos"></div></div>
  </div>
  <div class="card"><h3>Desconexiones por día y hora</h3><div id="gCalor"></div></div>
  <h2>Sucursales con problema de internet o energía</h2>
  <p class="sub">Sucursales donde las cámaras se caen <b>todas a la vez</b> (con menos de 3 minutos de diferencia). Cuando pasa seguido, el problema es del local y no de la cámara.</p>
  <div class="wrap" style="max-height:52vh"><table><thead><tr>
    <th>Locación</th><th>Cámaras</th><th>Cortes juntos</th><th>% de coincidencia</th>
    <th>En horario de atención</th><th>De noche</th><th>Micro-cortes juntos</th><th>Ahora</th><th>Diagnóstico</th>
  </tr></thead><tbody id="sucTb"></tbody></table></div>
  <h2>Por cámara</h2>
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
// En el celular las tablas se ven como tarjetas: cada celda lleva el nombre de su columna
function etiquetar(tb){
  const ths = [...tb.closest('table').querySelectorAll('thead th')].map(th => th.childNodes[0].textContent.trim());
  tb.querySelectorAll('tr').forEach(tr => [...tr.children].forEach((td, i) => {
    td.dataset.l = ths[i] || '';
    if (td.childNodes.length > 1) td.innerHTML = '<span class="v">' + td.innerHTML + '</span>';
    if (td.textContent.length > 60) td.classList.add('largo');
  }));
}
document.querySelectorAll('tbody').forEach(tb => new MutationObserver(() => etiquetar(tb)).observe(tb, {childList: true}));
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
const colHex = pct >= 95 ? '#16a34a' : (pct >= 90 ? '#d97706' : '#dc2626');
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

const VISTAS = {
  todas: ['Todas las cámaras', 'Estado de cada cámara por punto de venta'],
  off:   ['Cámaras offline', 'Cámaras sin conexión en este momento'],
  sd:    ['Problemas de SD', 'Cámaras con la tarjeta de memoria faltante, llena o dañada'],
  stats: ['Estadísticas de conexión', 'Cortes, horarios y patrones de cada cámara y sucursal'],
};
$('nOff').textContent = DATA.filter(d => !d.online).length || '';
$('nSd').textContent = DATA.filter(d => d.tipo === 'SD').length || '';
function marcar(n){
  ['todas','off','sd','stats'].forEach(x => $('v-' + x).classList.toggle('act', x === n));
}
function vista(n){
  if (!VISTAS[n]) n = 'todas';
  $('titulo').textContent = VISTAS[n][0];
  $('subtit').textContent = VISTAS[n][1];
  try { history.replaceState(null, '', n === 'todas' ? location.pathname : '#' + n); } catch (e) {}
  window.scrollTo(0, 0);
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
    `<b style="height:${v*100/mx}%;background:#dc2626"></b><b style="height:${STATS.hora_on[h]*100/mx}%;background:#16a34a"></b>` +
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
vista(location.hash.slice(1));
</script></body></html>"""

fecha = datetime.now(ZoneInfo("America/Argentina/Cordoba")).strftime("%d/%m/%Y %H:%M")
salida = (HTML.replace("__STATS__", json.dumps(STATS, ensure_ascii=False))
              .replace("__DATA__", json.dumps(camaras, ensure_ascii=False))
              .replace("__FECHA__", fecha)
              .replace("__LOGO__", logo_html))
with open("buscador.html", "w", encoding="utf-8") as f:
    f.write(salida)

print("Listo: buscador.html generado")