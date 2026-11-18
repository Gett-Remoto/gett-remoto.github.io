"""Incidencias de monday (tablero "Incidencias Cámaras") cruzadas con las cámaras de Tuya.

Lee TODOS los tickets (abiertos y el historial de Resuelto) con la API de monday y los une a cada
sucursal / cámara del dashboard. Solo se piden columnas del ticket: nunca las de contraseña,
teléfono o mail del local.
Necesita el secret MONDAY_TOKEN. Si no está, el dashboard se genera igual, sin esta parte.
"""
import json
import os
import re
import unicodedata
import urllib.request
from collections import Counter
from datetime import date, datetime
from statistics import median

BOARD = 18244431313
COLS = {
    "text_mkwzja25": "reporte",          # Observación asesor de experiencia (lo que reporta el agente)
    "color_mkwz3r0v": "camara",          # Frente / Mostrador / Ambas
    "color_mm8j8zh7": "problema",        # Principal problema (DESCONECTADA / HARDWARE / INTERNET)
    "status": "estatus",
    "color_mm2dzfxw": "cliente",
    "date_mm4tpypm": "apertura",
    "date_mkx0cqy9": "resolucion",
    "text_mkwzs66m": "solucion",
    "lookup_mm2wd2ca": "posicion",       # mirror de BASE: coincide con el número de locación de Tuya
}
GRUPOS_FUERA = {"Bajas"}

# Categorías de lo que escriben los agentes de calidad (se buscan en el texto sin tildes)
CATEGORIAS = [
    ("Fuera de línea", ("fuera de linea", "fuera de line", "offline", "desconect", "no conecta")),
    ("No graba / nulla certe", ("nulla", "nella", "null", "no graba", "graba cortad", "graba 1", "no grababa")),
    ("Tarjeta SD", (" sd", "tarjeta", "almacenamiento", "memoria")),
    ("Inestabilidad de red", ("inestabilidad", "carga fallida", "se tilda", "congela", "transmision", "no carga")),
    ("No compartida en la app", ("compartid", "no fue conectada", "dispositivos compartidos")),
    ("Cámara tapada / movida", ("tapad", "tapando", "dada vuelta", "dando vuelta", "movida", "movieron")),
]


def _norm(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s)


def categorias(texto):
    t = " " + _norm(texto) + " "
    return [n for n, claves in CATEGORIAS if any(c in t for c in claves)]


# ---------------------------------------------------------------- lectura de monday
def _gql(token, query, variables=None):
    req = urllib.request.Request(
        "https://api.monday.com/v2",
        data=json.dumps({"query": query, "variables": variables or {}}).encode(),
        headers={"Authorization": token, "Content-Type": "application/json", "API-Version": "2025-04"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        res = json.loads(r.read())
    if res.get("errors"):
        raise RuntimeError(res["errors"])
    return res["data"]


_CAMPOS = """cursor items { id name created_at group { title }
  column_values(ids: %s) { id text ... on MirrorValue { display_value } } }""" % json.dumps(list(COLS))


def leer_monday(token):
    tickets, cursor = [], None
    data = _gql(token, "query { boards(ids: [%d]) { items_page(limit: 500) { %s } } }" % (BOARD, _CAMPOS))
    page = data["boards"][0]["items_page"]
    while True:
        for it in page["items"]:
            if it["group"]["title"] in GRUPOS_FUERA:
                continue
            t = {"id": it["id"], "nombre": it["name"], "grupo": it["group"]["title"], "creado": it["created_at"]}
            for cv in it["column_values"]:
                t[COLS[cv["id"]]] = cv.get("display_value") if cv["id"].startswith("lookup") else cv.get("text")
            tickets.append(t)
        cursor = page.get("cursor")
        if not cursor:
            break
        page = _gql(token, "query($c: String!) { next_items_page(limit: 500, cursor: $c) { %s } }" % _CAMPOS,
                    {"c": cursor})["next_items_page"]
    return tickets


def cargar_tickets():
    if os.environ.get("MONDAY_FIXTURE"):                    # pruebas locales
        with open(os.environ["MONDAY_FIXTURE"], encoding="utf-8") as f:
            return json.load(f)
    token = os.environ.get("MONDAY_TOKEN")
    if not token:
        print("Monday: falta MONDAY_TOKEN, el dashboard sale sin incidencias.")
        return None
    try:
        t = leer_monday(token)
        print(f"Monday: {len(t)} tickets leídos.")
        return t
    except Exception as e:                                   # monday caído: no frenar el dashboard
        print("Monday: no pude leer los tickets:", str(e)[:200])
        return None


# ---------------------------------------------------------------- unión ticket <-> sucursal / cámara
def _num_loc(loc):
    m = re.match(r"\s*(\d+)\s*[.\s]", loc or "")
    return m.group(1) if m else None


def tipo_camara(c):
    n = _norm(c["nombre"])
    if re.search(r"mostr|mostad|caja", n):
        return "Mostrador"
    if re.search(r"frente|\bfr\b|puerta", n):
        return "Frente"
    m = c.get("modelo", "")
    if "T62" in m or "T63" in m:
        return "Mostrador"
    if "T80" in m:
        return "Frente"
    return None


VACIAS = {"gr", "la", "luc", "lider", "bari", "tc", "suc", "sucursal", "y", "de", "del"}


def ubicar(t, locs, por_num):
    """Devuelve la locación de Tuya del ticket (o None)."""
    pos = re.sub(r"\D", "", str(t.get("posicion") or ""))
    if pos and pos in por_num:
        return por_num[pos]
    nom = _norm(t["nombre"])
    nums = re.findall(r"\d{4}", nom)
    if nums:
        cand = [l for l in locs if nums[0] in l]
        if len(cand) == 1:
            return cand[0]
    for k, v in ALIAS.items():
        nom = re.sub(r"\b" + k + r"\b", v, nom)
    todos = [w for w in re.findall(r"[a-z0-9]+", nom) if len(w) > 2 or w.isdigit()]
    for tokens in (todos, [w for w in todos if w not in VACIAS]):   # primero con todas las palabras
        if tokens:
            cand = [l for l in locs if all(re.search(r"\b" + w + r"\b", _norm(l)) for w in tokens)]
            if len(cand) == 1:
                return cand[0]
    return None


ALIAS = {"general": "gral", "cordoba": "cba", "paclin ruta 20": "paclin r20"}


def _fecha(s):
    try:
        return date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


def calcular(tickets, camaras, stats, hoy):
    if tickets is None:
        return None
    camaras = [c for c in camaras]
    locs = sorted({c["locacion"] for c in camaras})
    por_num = {}
    for l in locs:
        n = _num_loc(l)
        if n:
            por_num.setdefault(n, l)
    cams_loc = {}
    for c in camaras:
        cams_loc.setdefault(c["locacion"], []).append(c)

    por_cam = {}                # id de cámara -> lista de tickets
    por_loc = {}
    sin_ubicar = 0
    cat_total, prob_total, por_mes = Counter(), Counter(), Counter()
    dias_res = []
    for t in tickets:
        t["cats"] = categorias(t.get("reporte"))
        cat_total.update(t["cats"])
        if t.get("problema"):
            prob_total[t["problema"]] += 1
        ap = _fecha(t.get("apertura")) or _fecha(t["creado"])
        rs = _fecha(t.get("resolucion"))
        t["abierto"] = t["grupo"] != "Resuelto" and t.get("estatus") != "Listo"
        if ap:
            por_mes[ap.strftime("%Y-%m")] += 1
            if rs and rs >= ap:
                dias_res.append((rs - ap).days)
        loc = ubicar(t, locs, por_num)
        if not loc:
            sin_ubicar += 1
            continue
        por_loc.setdefault(loc, []).append(t)
        tipo = t.get("camara")
        for c in cams_loc.get(loc, []):
            tc = tipo_camara(c)
            if tipo in (None, "", "Ambas") or tc is None or tc == tipo:
                por_cam.setdefault(c["id"], []).append(t)

    # --- por cámara (se suma a la tabla de Estadísticas) ---
    for f in stats["filas"]:
        ts = por_cam.get(f["id"], [])
        f["tickets"] = len(ts)
        f["tickets_ab"] = sum(1 for t in ts if t["abierto"])

    # --- por sucursal ---
    conexion = {s["loc"]: s for s in stats.get("sucursales", [])}
    filas_cam = {f["id"]: f for f in stats["filas"]}
    sucs = []
    for loc, ts in por_loc.items():
        ts.sort(key=lambda t: t["creado"], reverse=True)
        cats = Counter(c for t in ts for c in t["cats"])
        res = [(_fecha(t.get("resolucion")) - (_fecha(t.get("apertura")) or _fecha(t["creado"]))).days
               for t in ts if _fecha(t.get("resolucion")) and (_fecha(t.get("apertura")) or _fecha(t["creado"]))]
        res = [d for d in res if d >= 0]
        cams = cams_loc.get(loc, [])
        cx = conexion.get(loc.strip()) or conexion.get(loc)
        micro = sum(filas_cam.get(c["id"], {}).get("micro", 0) for c in cams)
        ult = ts[0]
        sucs.append({
            "loc": loc.strip(), "cliente": next((t["cliente"] for t in ts if t.get("cliente")), ""),
            "tickets": len(ts), "abiertos": sum(1 for t in ts if t["abierto"]),
            "ult_fecha": (_fecha(ult.get("apertura")) or _fecha(ult["creado"])).isoformat(),
            "ult_texto": (ult.get("reporte") or "").strip()[:220],
            "cats": [[k, v] for k, v in cats.most_common(3)],
            "res_med": round(median(res)) if res else None,
            "off_ahora": sum(1 for c in cams if not c["online"]), "n_cams": len(cams),
            "conexion": cx["diag"] if cx else "", "micro": micro,
        })
    sucs.sort(key=lambda s: (-s["tickets"], -s["abiertos"]))

    # --- cruces para la lectura ---
    abiertos = [t for t in tickets if t["abierto"]]
    malas = {s["loc"] for s in stats.get("sucursales", []) if s["diag"].startswith("Internet")}
    fl = [t for t in abiertos if "Fuera de línea" in t["cats"]]
    fl_mala = 0
    for t in fl:
        loc = ubicar(t, locs, por_num)
        if loc and loc.strip() in malas:
            fl_mala += 1
    online_con_ticket = sum(1 for t in abiertos if "Fuera de línea" in t["cats"]
                            and _todas_online(t, locs, por_num, cams_loc))
    hoy_d = hoy.date() if isinstance(hoy, datetime) else hoy
    meses = sorted(por_mes.items())[-12:]
    sem = sum(1 for t in tickets if (hoy_d - (_fecha(t.get("apertura")) or _fecha(t["creado"]))).days < 7)

    return {
        "total": len(tickets), "abiertos": len(abiertos), "sin_ubicar": sin_ubicar,
        "res_mediana": round(median(dias_res)) if dias_res else None,
        "cats": cat_total.most_common(), "problemas": prob_total.most_common(),
        "meses": meses, "ultima_semana": sem,
        "sucursales": sucs, "fl_abiertos": len(fl), "fl_mala": fl_mala,
        "online_con_ticket": online_con_ticket,
        "desde": min((_fecha(t.get("apertura")) or _fecha(t["creado"])) for t in tickets).isoformat() if tickets else None,
    }


def _todas_online(t, locs, por_num, cams_loc):
    loc = ubicar(t, locs, por_num)
    if not loc:
        return False
    cams = [c for c in cams_loc.get(loc, []) if t.get("camara") in (None, "", "Ambas") or tipo_camara(c) == t.get("camara")]
    return bool(cams) and all(c["online"] for c in cams)


# ---------------------------------------------------------------- lectura escrita
def analisis(inc, stats):
    if not inc:
        return [["Sin datos de monday", "Falta conectar monday (secret MONDAY_TOKEN) para ver las incidencias."]]
    out = []
    out.append(["Panorama de incidencias",
        f"Hay <b>{inc['total']} tickets</b> en el historial (desde {inc['desde'][8:10]}/{inc['desde'][5:7]}/{inc['desde'][:4]}) "
        f"y <b>{inc['abiertos']} abiertos</b> ahora. En los últimos 7 días entraron <b>{inc['ultima_semana']}</b>. "
        + (f"Un ticket tarda típicamente <b>{inc['res_mediana']} días</b> en resolverse." if inc['res_mediana'] is not None else "")])
    if inc["cats"]:
        tot = sum(v for _, v in inc["cats"])
        top = ", ".join(f"<b>{k.lower()}</b> ({round(v * 100 / tot)}%)" for k, v in inc["cats"][:3])
        out.append(["Qué reportan los agentes de calidad",
            f"Lo que más reportan es {top}. "
            "\"Fuera de línea\" casi siempre es conexión (internet o energía del local); \"no graba / nulla certe\" "
            "y \"tarjeta SD\" son problemas de la memoria de la cámara, que se resuelven destrabando o cambiando la SD."])
    if inc["fl_abiertos"]:
        out.append(["Tickets de \"fuera de línea\" que en realidad son de internet",
            f"De los <b>{inc['fl_abiertos']}</b> tickets abiertos por cámara fuera de línea, <b>{inc['fl_mala']}</b> son de "
            "sucursales donde las cámaras se caen todas juntas (internet inestable). Ahí el reinicio de la cámara no "
            "alcanza: hay que pedir que revisen el internet o el router."
            + (f" Además, <b>{inc['online_con_ticket']}</b> tickets abiertos de \"fuera de línea\" ya tienen la cámara online: "
               "se pueden cerrar." if inc["online_con_ticket"] else "")])
    rep = [s for s in inc["sucursales"] if s["tickets"] >= 4]
    if rep:
        ej = ", ".join(f"{s['loc'].split('.', 1)[-1].strip()} ({s['tickets']})" for s in rep[:6])
        con_cx = sum(1 for s in rep if s["conexion"] or s["micro"] >= 10)
        out.append(["Sucursales que se repiten",
            f"<b>{len(rep)} sucursales</b> tienen 4 tickets o más en el historial: {ej}. "
            f"De esas, <b>{con_cx}</b> también muestran cortes de conexión en el dashboard: el problema de fondo "
            "es el internet del local, no la cámara. Conviene atacarlas con una solución definitiva (cable, router, "
            "repetidor o cambio de proveedor) en vez de reiniciar cada vez."])
    if inc["meses"] and len(inc["meses"]) >= 2:
        (m1, v1), (m2, v2) = inc["meses"][-2], inc["meses"][-1]
        out.append(["Tendencia",
            f"En {_mes(m1)} hubo {v1} tickets y en {_mes(m2)} van {v2}."])
    return out


MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def _mes(ym):
    return MESES[int(ym[5:7]) - 1]
