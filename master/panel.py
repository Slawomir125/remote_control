"""Panel administracyjny Master Center (serwer HTTP + endpointy API)."""

import hashlib
import json
import shlex
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

from master.config import (
    PLIK_PANELU, POLECENIA_SLAVES, PANEL_ADMIN_COMMANDS,
    ENDPOINTY, MACRO_DIR, PANEL_PORT, PORT,
    SLAVE_PUBLIC_PORT, SLAVE_PUBLIC_HTTPS, PLIKI_MAX_B,
    endpoint,
)
from master.logika import (
    polacz_slave, rozlacz_slave, wydaj_polecenie,
    znajdz_slaves, wczytaj_makro,
)
from master.widoki import snapshot
from master.baza import historia_slaves


# ─── narzędzia ───────────────────────────────────────────────────────────────

@dataclass
class Zapytanie:
    sciezka: str
    params: dict
    dane: Optional[dict] = None          # sparsowany JSON z POST (None dla strumien=True)
    naglowki: Optional[object] = None
    rfile: Optional[object] = None       # surowy strumień dla endpointów strumien=True
    wfile: Optional[object] = None       # surowy strumień do zapisu odpowiedzi (gdy handler sam odpowiada)
    content_length: int = 0


def odpowiedz_json(obiekt, kod=200):
    return kod, "application/json; charset=utf-8", json.dumps(obiekt).encode()


# ─── endpointy ───────────────────────────────────────────────────────────────

@endpoint("/")
@endpoint("/index.html")
def ep_strona(stan, zapyt):
    if PLIK_PANELU.exists():
        return 200, "text/html; charset=utf-8", PLIK_PANELU.read_bytes()
    return 500, "text/plain; charset=utf-8", b"Brak pliku master_center.html obok master.py"


@endpoint("/api/stan")
def ep_stan(stan, zapyt):
    try:
        since = int(zapyt.params.get("since", ["0"])[0])
    except ValueError:
        return odpowiedz_json({"blad": "since"}, 400)
    return odpowiedz_json(snapshot(stan, since))


@endpoint("/api/meta")
def ep_meta(stan, zapyt):
    """Stałe dane dla panelu: polecenia, makra, token rejestracji i publiczny port Slaves."""
    macros = sorted(p.stem for p in MACRO_DIR.glob("*.sh") if p.is_file())
    pub_port = SLAVE_PUBLIC_PORT or PORT
    pub_https = SLAVE_PUBLIC_HTTPS if SLAVE_PUBLIC_HTTPS is not None else (pub_port == 443)
    return odpowiedz_json({
        "polecenia": POLECENIA_SLAVES,
        "macros": macros,
        "token_rejestracji": stan.token_rejestracji,
        "port_slaves": pub_port,
        "https_slaves": pub_https,
    })


@endpoint("/api/slaves")
def ep_slaves(stan, zapyt):
    with stan.lock:
        slaves = sorted(
            ({"id": slave_id, "nazwa": slave.nazwa} for slave_id, slave in stan.slaves.items()),
            key=lambda slave: (0, int(slave["id"])) if slave["id"].isdigit() else (1, slave["id"]),
        )
    return odpowiedz_json({"slaves": slaves})


@endpoint("/api/slave/", prefix=True)
def ep_slave(stan, zapyt):
    try:
        with stan.lock:
            slave_id = znajdz_slaves(stan, urllib.parse.unquote(zapyt.sciezka.rsplit("/", 1)[1]))
    except ValueError as e:
        return odpowiedz_json({"blad": str(e)}, 404)
    return odpowiedz_json(historia_slaves(stan, slave_id))


@endpoint("/api/slave/polacz", metoda="POST")
def ep_slave_polacz(stan, zapyt):
    """Otwiera sesję wybranego Slave'a (czekamy aż przyjdzie z pollingiem)."""
    dane = zapyt.dane or {}
    slave_id = str(dane.get("id", "")).strip()
    if not slave_id:
        raise ValueError("Podaj ID Slave'a.")
    with stan.lock:
        slave_id = znajdz_slaves(stan, slave_id)
    polacz_slave(stan, slave_id)
    return odpowiedz_json({"ok": True, "id": slave_id})


@endpoint("/api/slave/rozlacz", metoda="POST")
def ep_slave_rozlacz(stan, zapyt):
    """Zamyka sesję wybranego Slave'a — kolejne pakiety dostaną 503."""
    dane = zapyt.dane or {}
    slave_id = str(dane.get("id", "")).strip()
    if not slave_id:
        raise ValueError("Podaj ID Slave'a.")
    with stan.lock:
        slave_id = znajdz_slaves(stan, slave_id)
    rozlacz_slave(stan, slave_id)
    return odpowiedz_json({"ok": True, "id": slave_id})


@endpoint("/api/polecenie", metoda="POST")
def ep_polecenie(stan, zapyt):
    """{"cel": ID | nazwa, "polecenie": "pauza", "args": [...]}"""
    dane = zapyt.dane or {}
    args = dane.get("args", [])
    if not isinstance(args, list):
        raise ValueError("args musi być tablicą")
    polecenie_panelu = str(dane.get("polecenie", "")).strip().lower()
    if polecenie_panelu not in PANEL_ADMIN_COMMANDS:
        raise ValueError("Użyj terminala Slaves; ten endpoint obsługuje tylko pauza, wznow i stop.")
    p = wydaj_polecenie(stan, str(dane.get("cel", "")), polecenie_panelu, args)
    return odpowiedz_json({"ok": True, "id": p.id, "cel": p.cel, "adresaci": p.adresaci})


def parsuj_komende_terminalowa(tekst):
    if not isinstance(tekst, str) or len(tekst) > 1024:
        raise ValueError("Polecenie jest puste albo za długie.")
    try:
        czesci = shlex.split(tekst)
    except ValueError:
        raise ValueError("Nieprawidłowa składnia.") from None
    if not czesci:
        raise ValueError("Wpisz jedną dozwoloną komendę.")
    nazwa = czesci[0].lower()
    if nazwa == "status":
        if len(czesci) != 1:
            raise ValueError(f"'{nazwa}' nie przyjmuje argumentów.")
        return nazwa, []
    if nazwa == "makro":
        if len(czesci) != 2:
            raise ValueError("Użycie: makro <nazwa> (plik macros/<nazwa>.sh).")
        return nazwa, wczytaj_makro(czesci[1])
    return "exec", czesci


# ─── transfer plików: slave ↔ operator ──────────────────────────────────────

@endpoint("/api/slave/wyslij", metoda="POST", strumien=True)
def ep_slave_wyslij(stan, zapyt):
    """Operator wgrywa plik na Mastera. Query: ?id=<slave>&zdalny=<ścieżka_na_slave>.
    Body = surowe bajty (Content-Length wymagany). Master kolejkuje polecenie `pobierz` do slave'a."""
    slave_id = (zapyt.params.get("id", [""])[0] or "").strip()
    zdalny = (zapyt.params.get("zdalny", [""])[0] or "").strip()
    if not slave_id:
        return 400, "application/json", json.dumps({"blad": "Podaj id slave'a."}).encode()
    if not zdalny:
        return 400, "application/json", json.dumps({"blad": "Podaj ścieżkę docelową."}).encode()
    if zapyt.content_length <= 0:
        return 411, "application/json", json.dumps({"blad": "Wymagany Content-Length."}).encode()
    if zapyt.content_length > PLIKI_MAX_B:
        return 413, "application/json", json.dumps(
            {"blad": f"Plik większy niż limit {PLIKI_MAX_B} B."}).encode()
    try:
        with stan.lock:
            slave_id = znajdz_slaves(stan, slave_id)
    except ValueError as e:
        return 404, "application/json", json.dumps({"blad": str(e)}).encode()

    hasher = hashlib.sha256()
    buf = bytearray()
    pozostalo = zapyt.content_length
    while pozostalo > 0:
        chunk = zapyt.rfile.read(min(1024 * 1024, pozostalo))
        if not chunk:
            break
        buf.extend(chunk); hasher.update(chunk); pozostalo -= len(chunk)
    if len(buf) != zapyt.content_length:
        return 400, "application/json", json.dumps({"blad": "niekompletne body"}).encode()

    sha = hasher.hexdigest()
    p = wydaj_polecenie(stan, slave_id, "pobierz", [zdalny, sha, str(len(buf))])
    with stan.lock:
        stan.pliki_do_slave[p.id] = {
            "bytes": bytes(buf), "zdalna": zdalny, "sha256": sha, "slave_id": slave_id,
            "ts": time.time(),
        }
    return 200, "application/json", json.dumps(
        {"ok": True, "polecenie_id": p.id, "bajtow": len(buf), "sha256": sha,
         "cel": zdalny, "slave": slave_id}).encode()


@endpoint("/api/slave/pobierz", metoda="GET", strumien=True)
def ep_slave_pobierz(stan, zapyt):
    """Operator pobiera plik ze slave'a. Query: ?id=<slave>&zdalny=<ścieżka>&nazwa=<nazwa_pliku>&timeout=<s>.
    Master kolejkuje polecenie `wyslij <zdalny>` do slave'a i czeka aż pliki_od_slave się napełni."""
    slave_id = (zapyt.params.get("id", [""])[0] or "").strip()
    zdalny = (zapyt.params.get("zdalny", [""])[0] or "").strip()
    nazwa = (zapyt.params.get("nazwa", [""])[0] or "").strip() or Path(zdalny).name or "plik.bin"
    try:
        timeout = float(zapyt.params.get("timeout", ["120"])[0])
    except ValueError:
        timeout = 120.0

    def blad_json(kod, msg):
        tresc = json.dumps({"blad": msg}).encode()
        reason = {400: "Bad Request", 404: "Not Found", 500: "Internal Server Error",
                  504: "Gateway Timeout"}.get(kod, "Error")
        try:
            zapyt.wfile.write(f"HTTP/1.1 {kod} {reason}\r\n".encode("ascii"))
            zapyt.wfile.write(b"Content-Type: application/json; charset=utf-8\r\n")
            zapyt.wfile.write(f"Content-Length: {len(tresc)}\r\nConnection: close\r\n\r\n".encode("ascii"))
            zapyt.wfile.write(tresc)
        except (BrokenPipeError, ConnectionResetError):
            pass

    if not slave_id or not zdalny:
        blad_json(400, "Podaj id slave'a i ścieżkę zdalną.")
        return None
    try:
        with stan.lock:
            slave_id = znajdz_slaves(stan, slave_id)
    except ValueError as e:
        blad_json(404, str(e))
        return None

    event = threading.Event()
    with stan.lock:
        p = wydaj_polecenie(stan, slave_id, "wyslij", [zdalny])
        stan.pliki_od_slave[p.id] = {
            "buf": bytearray(), "final": False, "event": event,
            "slave_id": slave_id, "ts": time.time(),
        }

    # Czekamy aż slave wszystko wyśle i oznaczy final=1.
    event.wait(timeout=timeout)
    with stan.lock:
        wpis = stan.pliki_od_slave.get(p.id)
        if wpis is None or not wpis["final"]:
            # Timeout — ale zostaw wpis, slave może jeszcze dociągnąć.
            blad_json(504, f"timeout {timeout:g}s: slave nie dokończył wysyłki")
            return None
        bajty = bytes(wpis["buf"])
        stan.pliki_od_slave.pop(p.id, None)

    bezpieczna = nazwa.encode("ascii", "ignore").decode("ascii").replace('"', "") or "plik.bin"
    try:
        zapyt.wfile.write(b"HTTP/1.1 200 OK\r\n")
        zapyt.wfile.write(b"Content-Type: application/octet-stream\r\n")
        zapyt.wfile.write(f'Content-Disposition: attachment; filename="{bezpieczna}"\r\n'.encode("ascii"))
        zapyt.wfile.write(f"Content-Length: {len(bajty)}\r\n".encode("ascii"))
        zapyt.wfile.write(b"Cache-Control: no-store\r\nConnection: close\r\n\r\n")
        zapyt.wfile.write(bajty)
    except (BrokenPipeError, ConnectionResetError):
        pass
    return None


@endpoint("/api/terminal", metoda="POST")
def ep_terminal(stan, zapyt):
    dane = zapyt.dane or {}
    cel = str(dane.get("cel", "")).strip()
    if not cel:
        raise ValueError("Podaj ID lub nazwę Slave'a.")
    nazwa, args = parsuj_komende_terminalowa(dane.get("komenda"))
    granice = cel.split("-", 1)
    if len(granice) == 2 and all(c.strip().isdigit() for c in granice):
        poczatek, koniec = (int(c.strip()) for c in granice)
        if koniec < poczatek:
            raise ValueError("Koniec zakresu musi być większy lub równy początkowi.")
        with stan.lock:
            cele = sorted(
                (slave_id for slave_id in stan.slaves
                 if slave_id.isdigit() and poczatek <= int(slave_id) <= koniec),
                key=int,
            )
        if not cele:
            raise ValueError(f"Brak zarejestrowanych Slaves w zakresie {poczatek}-{koniec}.")
    else:
        cele = [cel]
    polecenia = [wydaj_polecenie(stan, slave_id, nazwa, args) for slave_id in cele]
    adresaci = sum(p.adresaci for p in polecenia)
    return odpowiedz_json({
        "ok": True,
        "id": polecenia[0].id,
        "cel": cel,
        "cele": cele,
        "adresaci": adresaci,
        "polecenia": [{"id": p.id, "cel": p.cel, "adresaci": p.adresaci} for p in polecenia],
        "komenda": dane["komenda"],
    })


# ─── serwer HTTP panelu ─────────────────────────────────────────────────────

def znajdz_endpoint(metoda, sciezka):
    """Zwraca (funkcja, strumien) albo (None, False). Exact match wygrywa z prefix match."""
    best_prefix = None
    for m, wzorzec, prefix, strumien, funkcja in ENDPOINTY:
        if m != metoda:
            continue
        if sciezka == wzorzec:
            return funkcja, strumien
        if prefix and sciezka.startswith(wzorzec):
            best_prefix = (funkcja, strumien)
    return best_prefix or (None, False)


class SerwerPanelu(ThreadingHTTPServer):
    request_queue_size = 128


class Panel(BaseHTTPRequestHandler):
    """Klej HTTP: szuka funkcji w rejestrze ENDPOINTY i wywołuje ją."""

    def log_message(self, *a):
        pass

    def _wczytaj_json(self):
        dl = int(self.headers.get("Content-Length") or 0)
        if dl > 10_000:
            raise ValueError("za duże żądanie")
        dane = json.loads(self.rfile.read(dl) or b"{}")
        if not isinstance(dane, dict):
            raise ValueError("oczekiwano obiektu JSON")
        return dane

    def _obsluz(self, metoda):
        sciezka, _, zapytanie = self.path.partition("?")
        stan, (funkcja, strumien) = self.server.stan, znajdz_endpoint(metoda, sciezka)
        wynik_wlasna_odp = False
        if funkcja is None:
            kod, typ, tresc = 404, "text/plain; charset=utf-8", b"404"
        else:
            try:
                params = urllib.parse.parse_qs(zapytanie)
                if strumien:
                    dl = int(self.headers.get("Content-Length") or 0)
                    zapyt = Zapytanie(sciezka, params, None, self.headers,
                                      rfile=self.rfile, wfile=self.wfile, content_length=dl)
                    wynik = funkcja(stan, zapyt)
                    if wynik is None:
                        wynik_wlasna_odp = True        # handler sam zapisał odpowiedź
                        kod = typ = tresc = None
                    else:
                        kod, typ, tresc = wynik
                else:
                    dane = self._wczytaj_json() if metoda == "POST" else None
                    kod, typ, tresc = funkcja(stan, Zapytanie(sciezka, params, dane, self.headers))
            except ValueError as e:
                kod, typ, tresc = odpowiedz_json({"blad": str(e)}, 400)
            except Exception as e:
                kod, typ, tresc = 500, "text/plain; charset=utf-8", f"blad: {e}".encode()
        if wynik_wlasna_odp:
            return
        try:
            self.send_response(kod)
            self.send_header("Content-Type", typ)
            self.send_header("Content-Length", str(len(tresc)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(tresc)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self._obsluz("GET")

    def do_POST(self):
        self._obsluz("POST")
