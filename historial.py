"""Historial de conexiones de las cámaras y estadísticas.

Cada corrida suma los eventos online/offline que devuelve Tuya (que solo guarda unos días)
a un historial propio, para tener datos de toda la vida de cada cámara.
El historial se guarda CIFRADO en historial.enc: el repo es público y tiene IDs y locales.
Clave: secret HIST_KEY (o, si no existe, DASH_PASSWORD).
"""
import base64
import json
import os
import time
import zlib
from collections import Counter
from datetime import datetime
from statistics import median
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

ARCHIVO = "historial.enc"
TZ = ZoneInfo("America/Argentina/Cordoba")
ONLINE, OFFLINE = 1, 2
SALT = b"gett-remoto-historial-v1"

# Corte "por horario del local": se apaga de noche y vuelve a la mañana (apertura).
NOCHE_INI = {19, 20, 21, 22, 23, 0, 1, 2, 3}
MANANA_FIN = {6, 7, 8, 9, 10, 11, 12}
MICRO_MIN = 10          # cortes de menos de 10 minutos = micro-corte


# ---------------------------------------------------------------- guardado
def _fernet():
    clave = os.environ.get("HIST_KEY") or os.environ.get("DASH_PASSWORD")
    if not clave:
        raise SystemExit("Falta HIST_KEY o DASH_PASSWORD para cifrar el historial.")
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=SALT, iterations=200_000)
    return Fernet(base64.urlsafe_b64encode(kdf.derive(clave.encode())))


def vacio():
    return {"version": 1, "desde": None, "ultimo_barrido": 0, "dispositivos": {}}


def cargar():
    if not os.path.exists(ARCHIVO):
        print("Historial: no existe todavía, arranco uno nuevo.")
        return vacio()
    try:
        with open(ARCHIVO, "rb") as f:
            datos = json.loads(zlib.decompress(_fernet().decrypt(f.read())))
        print(f"Historial: {len(datos['dispositivos'])} dispositivos cargados.")
        return datos
    except InvalidToken:
        # Si cambió la clave no se puede leer: NO se pisa, se guarda aparte y se arranca de cero.
        os.replace(ARCHIVO, ARCHIVO + f".ilegible-{int(time.time())}")
        print("AVISO: no pude descifrar el historial (¿cambió la clave?). Lo guardé aparte y arranco uno nuevo.")
        return vacio()


def guardar(hist):
    crudo = zlib.compress(json.dumps(hist, separators=(",", ":")).encode(), 9)
    with open(ARCHIVO, "wb") as f:
        f.write(_fernet().encrypt(crudo))
    print(f"Historial guardado ({os.path.getsize(ARCHIVO) // 1024} KB).")


# ---------------------------------------------------------------- carga de eventos
def sumar_eventos(hist, cam, eventos, ahora_ms):
    """eventos: lista de (ms, 1|2). Se deduplican y se ordenan."""
    d = hist["dispositivos"].setdefault(cam["id"], {"ev": [], "visto": ahora_ms})
    d.update(nombre=cam["nombre"], loc=cam["locacion"], modelo=cam["modelo"])
    previos = {(t, e) for t, e in d["ev"]}
    previos.update((int(t), int(e)) for t, e in eventos if e in (ONLINE, OFFLINE))
    d["ev"] = sorted(previos)
    # Primera vez que la vemos offline sin dato de Tuya: sirve como "offline desde al menos".
    if cam["online"]:
        d.pop("off_visto", None)
    else:
        d.setdefault("off_visto", ahora_ms)
    if d["ev"]:
        primero = d["ev"][0][0]
        if hist["desde"] is None or primero < hist["desde"]:
            hist["desde"] = primero
    return d


# ---------------------------------------------------------------- estadísticas
def _hora(ms):
    return datetime.fromtimestamp(ms / 1000, TZ)


def episodios(ev):
    """Cortes: cada OFFLINE seguido del siguiente ONLINE. Si sigue offline, fin = None."""
    eps, ini = [], None
    for t, e in ev:
        if e == OFFLINE and ini is None:
            ini = t
        elif e == ONLINE and ini is not None:
            eps.append((ini, t))
            ini = None
    if ini is not None:
        eps.append((ini, None))
    return eps


def es_nocturno(ini, fin):
    if fin is None:
        return False
    h0, h1 = _hora(ini).hour, _hora(fin).hour
    dur_h = (fin - ini) / 3_600_000
    return h0 in NOCHE_INI and h1 in MANANA_FIN and 4 <= dur_h <= 18


def calcular(hist, camaras, ahora_ms):
    filas = []
    hora_off, hora_on = [0] * 24, [0] * 24
    calor = [[0] * 24 for _ in range(7)]      # lunes=0 .. domingo=6
    duraciones, tramos = [], Counter()
    n_noct = n_micro = n_reales = 0
    por_loc = {}

    estado = {c["id"]: c for c in camaras}
    for dev_id, d in hist["dispositivos"].items():
        cam = estado.get(dev_id)
        if cam is None or not es_camara(cam):
            continue                            # dado de baja, o sensor / control de aire
        eps = episodios(d["ev"])
        por_loc.setdefault(cam["locacion"].strip(), {})[dev_id] = (cam, eps)
        todos = [(a, b, (b - a) / 60000) for a, b in eps if b is not None]
        micro = sum(1 for *_, m in todos if m < MICRO_MIN)
        # Corte "real" = duró 10 min o más (o sigue cortado). Los micro-cortes se cuentan aparte.
        reales = [(a, b) for a, b in eps if b is None or (b - a) / 60000 >= MICRO_MIN]
        cerrados = [(a, b) for a, b in reales if b is not None]
        mins = [(b - a) / 60000 for a, b in cerrados]
        noct = sum(1 for a, b in cerrados if es_nocturno(a, b))
        n_noct += noct
        n_micro += micro
        n_reales += len(reales)
        duraciones += mins
        for *_, m in todos:
            tramos[_tramo(m)] += 1
        horas = Counter()
        for a, b in reales:
            h = _hora(a)
            hora_off[h.hour] += 1
            calor[h.weekday()][h.hour] += 1
            horas[h.hour] += 1
            if b is not None:
                hora_on[_hora(b).hour] += 1
        tip = horas.most_common(1)
        n_off = len(reales)

        # Desde cuándo está offline ahora (para el contador de días)
        desde, aprox = None, False
        if not cam["online"]:
            if eps and eps[-1][1] is None:
                desde = eps[-1][0]
            elif cam.get("corte"):
                desde = cam["corte"]
            else:
                # Sin evento de corte en el historial: está caída desde ANTES de lo que cubre el historial.
                cotas = [x for x in (d.get("off_visto"), d.get("cubierto")) if x]
                desde, aprox = (min(cotas) if cotas else None), True

        patron = ""
        if len(cerrados) >= 3 and noct / len(cerrados) >= 0.5:
            patron = "Apaga de noche / vuelve al abrir"
        elif tip and n_off >= 4 and tip[0][1] / n_off >= 0.5:
            patron = f"Se corta casi siempre a las {tip[0][0]:02d} h"
        elif micro >= 10 and micro >= len(todos) * 0.6:
            patron = "Micro-cortes frecuentes (señal / wifi)"

        filas.append({
            "id": dev_id, "nombre": cam["nombre"], "loc": cam["locacion"], "modelo": cam["modelo"],
            "online": cam["online"], "cortes": n_off, "vueltas": len(cerrados),
            "prom": round(sum(mins) / len(mins)) if mins else None,
            "med": round(median(mins)) if mins else None,
            "max": round(max(mins)) if mins else None,
            "noct": noct, "micro": micro,
            "hora": tip[0][0] if tip else None,
            "hora_pct": round(tip[0][1] * 100 / n_off) if tip else None,
            "patron": patron, "desde": desde, "aprox": aprox,
        })

    stats = {
        "generado": ahora_ms,
        "hist_desde": hist["desde"],
        "filas": filas,
        "hora_off": hora_off, "hora_on": hora_on, "calor": calor,
        "tramos": [[k, tramos.get(k, 0)] for k in TRAMOS],
        "total_cortes": n_reales, "total_vueltas": len(duraciones),
        "mediana": round(median(duraciones)) if duraciones else None,
        "pct_rapidas": round(sum(1 for m in duraciones if m < 60) * 100 / len(duraciones)) if duraciones else None,
        "nocturnos": n_noct, "micro": n_micro,
        "cam_patron": sum(1 for f in filas if f["patron"]),
    }
    stats["sucursales"] = sucursales(por_loc)
    stats["analisis"] = analisis(stats, camaras, ahora_ms)
    return stats


# ---------------------------------------------------------------- sucursales
JUNTOS_MIN = 3          # dos cámaras que se cortan con menos de 3 min de diferencia = corte de la sucursal
DIA = set(range(9, 20))  # 9 a 19 h: horario en que los locales suelen estar abiertos


def sucursales(por_loc):
    """Busca cortes en los que TODAS las cámaras de la sucursal se caen juntas.
    Si pasa seguido, el problema es del local (internet o energía) y no de una cámara."""
    res = []
    for loc, cams in por_loc.items():
        if len(cams) < 2:
            continue
        inicios = sorted((a, dev, b) for dev, (_, eps) in cams.items() for a, b in eps)
        grupos, actual = [], []
        for a, dev, b in inicios:
            if actual and a - actual[0][0] > JUNTOS_MIN * 60000:
                grupos.append(actual)
                actual = []
            actual.append((a, dev, b))
        if actual:
            grupos.append(actual)
        juntos = [g for g in grupos if len({dev for _, dev, _ in g}) >= 2]
        ahora_off = all(not c["online"] for c, _ in cams.values())
        if not grupos and not ahora_off:
            continue
        dia = noche = micro = 0
        for g in juntos:
            a = g[0][0]
            fines = [b for *_, b in g if b]
            dur = (max(fines) - a) / 60000 if len(fines) == len(g) else None
            if dur is not None and dur < MICRO_MIN:
                micro += 1
            elif _hora(a).hour in DIA:
                dia += 1
            else:
                noche += 1
        pct = round(len(juntos) * 100 / len(grupos)) if grupos else 0
        diag = ""
        if len(juntos) >= 3 and pct >= 40:
            if micro >= max(dia, noche) and micro >= 3:
                diag = "Internet inestable del local (micro-cortes en todas las cámaras a la vez)"
            elif dia >= noche:
                diag = "Internet del local se cae en horario de atención"
            else:
                diag = "Se corta la luz / internet del local de noche"
        elif ahora_off:
            diag = "Todas las cámaras offline ahora (revisar internet o energía)"
        if not diag:
            continue
        res.append({"loc": loc, "camaras": len(cams), "eventos": len(grupos), "juntos": len(juntos),
                    "pct": pct, "dia": dia, "noche": noche, "micro": micro,
                    "ahora_off": ahora_off, "diag": diag})
    res.sort(key=lambda r: (-r["juntos"], -r["pct"]))
    return res


# ---------------------------------------------------------------- análisis escrito
def _fmt_min(m):
    if m is None:
        return "sin dato"
    if m < 60:
        return f"{round(m)} minutos"
    if m < 1440:
        h = round(m / 60)
        return f"{h} horas" if h >= 2 else "1 hora"
    return f"{m / 1440:.1f} días".replace(".0 ", " ")


def _franja(horas, n=3):
    top = sorted(range(24), key=lambda h: -horas[h])[:n]
    return ", ".join(f"{h:02d} h" for h in sorted(top))


def analisis(s, camaras, ahora_ms):
    """Lectura en lenguaje simple. Devuelve una lista de [título, texto] (el texto puede tener <b>)."""
    F = s["filas"]
    out = []
    dias_hist = (ahora_ms - s["hist_desde"]) / 86400000 if s["hist_desde"] else 0
    total_cam = len(F)
    on = sum(1 for f in F if f["online"])
    todos_cortes = s["total_cortes"] + s["micro"]

    if dias_hist < 1 or not todos_cortes:
        return [["Todavía no hay datos suficientes",
                 "El historial se empezó a juntar recién. En unos días aparece acá la lectura de los datos."]]

    out.append(["Panorama general",
        f"Hay <b>{total_cam} cámaras</b> y ahora están online <b>{on}</b> ({round(on * 100 / total_cam)}%). "
        f"En los últimos <b>{dias_hist:.0f} días</b> hubo <b>{todos_cortes} caídas</b>, pero "
        f"<b>{round(s['micro'] * 100 / todos_cortes)}% fueron micro-cortes</b> de menos de 10 minutos: "
        f"la cámara se cae y vuelve sola casi enseguida. Sacando eso quedan <b>{s['total_cortes']} cortes reales</b>; "
        f"la mitad vuelve en <b>{_fmt_min(s['mediana'])}</b> o menos, y solo el {s['pct_rapidas']}% vuelve en menos de una hora."])

    off_noche = sum(s["hora_off"][h] for h in NOCHE_INI) / max(1, sum(s["hora_off"]))
    on_manana = sum(s["hora_on"][h] for h in MANANA_FIN) / max(1, sum(s["hora_on"]))
    lectura = (" Es decir: muchas cámaras se apagan cuando cierra el local y vuelven cuando abre, no porque fallen."
               if off_noche >= 0.4 and on_manana >= 0.4 else
               " No se ve un patrón claro de cierre y apertura: los cortes están repartidos en el día.")
    out.append(["¿Cuándo se cortan y cuándo vuelven?",
        f"Los cortes reales se concentran a las <b>{_franja(s['hora_off'])}</b> y las vueltas a las "
        f"<b>{_franja(s['hora_on'])}</b>. El {round(off_noche * 100)}% de los cortes empieza de noche (19 a 3 h) y "
        f"el {round(on_manana * 100)}% de las vueltas es a la mañana (6 a 12 h).{lectura} "
        f"Cuando un corte empieza a la noche y termina a la mañana lo marco como <b>corte por horario del local</b>: "
        f"hubo {s['nocturnos']} así."])

    noct = [f for f in F if f["patron"].startswith("Apaga")]
    if noct:
        ej = ", ".join(sorted({f["loc"].split(".", 1)[-1].strip() for f in noct})[:8])
        out.append(["Locales que apagan de noche",
            f"<b>{len(noct)} cámaras</b> se apagan casi todas las noches y vuelven a la mañana (por ejemplo: {ej}). "
            "Probablemente cortan la luz o el router al cerrar. No es una falla de la cámara, pero mientras "
            "están apagadas <b>no graban</b>: si se quiere grabación nocturna hay que pedirle al local que deje "
            "el router y las cámaras enchufadas."])

    suc = s["sucursales"]
    inet = [x for x in suc if x["diag"].startswith("Internet")]
    if inet:
        ej = ", ".join(x["loc"].split(".", 1)[-1].strip() for x in inet[:6])
        out.append(["Sucursales con internet inestable",
            f"En <b>{len(inet)} sucursales</b> las cámaras se caen <b>todas a la vez</b> una y otra vez "
            f"(por ejemplo: {ej}). Cuando las dos cámaras se cortan juntas el problema no es de la cámara: "
            "es el <b>internet o el wifi del local</b>. Ahí no sirve reiniciar la cámara; conviene hablar con el "
            "local por su proveedor de internet, revisar el router o cablear con RJ45."])

    micro = sorted([f for f in F if f["patron"].startswith("Micro")], key=lambda f: -f["micro"])
    if micro:
        ej = ", ".join(f"{f['nombre'].strip()} ({f['micro']})" for f in micro[:4])
        out.append(["Cámaras que se reconectan todo el tiempo",
            f"<b>{len(micro)} cámaras</b> tienen micro-cortes muy seguidos, sobre todo {ej}. "
            "Si solo le pasa a una cámara del local, suele ser <b>mala señal de wifi</b> en ese punto "
            "(lejos del router, paredes, metal). Un repetidor o un cable suele solucionarlo. "
            "Ojo: estas cámaras pueden figurar Online y aun así tener huecos en la grabación."])

    off = [f for f in F if not f["online"]]
    off7 = [f for f in off if f["desde"] and (ahora_ms - f["desde"]) / 86400000 >= 7]
    off2 = [f for f in off if f["desde"] and (ahora_ms - f["desde"]) / 86400000 >= 2]
    if off:
        out.append(["Cámaras caídas hace días",
            f"Hay <b>{len(off)} cámaras offline</b>; <b>{len(off2)}</b> llevan más de 2 días y "
            f"<b>{len(off7)}</b> más de una semana. Las que pasan los 2 días casi nunca vuelven solas: "
            "son las que hay que priorizar con un contacto al local (el contador está en la columna Días offline)."])

    dias = [sum(x) for x in s["calor"]]
    if sum(dias):
        nom = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
        out.append(["Días de la semana",
            f"El día con más cortes es el <b>{nom[dias.index(max(dias))]}</b> y el más tranquilo el "
            f"<b>{nom[dias.index(min(dias))]}</b>."])

    out.append(["Qué haría",
        "1) Contactar primero las cámaras con más de 2 días offline. "
        "2) En las sucursales con internet inestable, no mandar el paso a paso de reinicio: pedir que revisen el "
        "internet o el router. 3) En las que apagan de noche, avisar que así no graban de noche. "
        "4) Las que se reconectan todo el tiempo, revisar la señal de wifi o cablearlas."])
    if dias_hist < 14:
        out.append(["Ojo con los números",
            f"Por ahora hay solo {dias_hist:.0f} días de historial. Los patrones se vuelven confiables a partir de "
            "2 o 3 semanas; el historial se sigue sumando solo en cada actualización."])
    return out


# Dispositivos que no son cámaras (sensores de temperatura, controles de aire): no entran en las estadísticas.
NO_CAMARA_PALABRAS = {"sensor", "temp", "temp.", "temperatura", "temperature", "aire", "air"}
NO_CAMARA_TEXTO = ("空调", "温湿度", "传感器", "遥控")


def es_camara(cam):
    txt = (cam.get("nombre", "") + " " + cam.get("modelo", "")).lower()
    if any(t in txt for t in NO_CAMARA_TEXTO):
        return False
    return not (set(txt.replace("-", " ").split()) & NO_CAMARA_PALABRAS)


TRAMOS = ["< 10 min", "10-60 min", "1-6 h", "6-24 h", "1-3 días", "3-7 días", "> 7 días"]


def _tramo(m):
    for lim, nom in ((10, TRAMOS[0]), (60, TRAMOS[1]), (360, TRAMOS[2]), (1440, TRAMOS[3]),
                     (4320, TRAMOS[4]), (10080, TRAMOS[5])):
        if m < lim:
            return nom
    return TRAMOS[6]
