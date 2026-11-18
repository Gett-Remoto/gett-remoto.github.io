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

    estado = {c["id"]: c for c in camaras}
    for dev_id, d in hist["dispositivos"].items():
        cam = estado.get(dev_id)
        if cam is None or not es_camara(cam):
            continue                            # dado de baja, o sensor / control de aire
        eps = episodios(d["ev"])
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

    return {
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
