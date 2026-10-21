"""Panel administracyjny Master Center (serwer HTTP + endpointy API)."""

import hmac
import json
import shlex
import time
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from master.config import (
    PLIK_PANELU, PLIK_SLAVE, POLECENIA_SLAVES, PANEL_ADMIN_COMMANDS,
    CELE_WSZYSTKIE, ENDPOINTY, MACRO_DIR, PANEL_PORT,
    endpoint, TOKEN_JEDNORAZOWY_WAZNY_S,
)
from master.logika import (
    uruchom_master, wstrzymaj_master, wydaj_polecenie,
    znajdz_slaves, wygeneruj_token_jednorazowy, wygeneruj_kod_startowy,
    wczytaj_makro,
)
from master.widoki import snapshot
from master.baza import historia_slaves


# ─── narzędzia ───────────────────────────────────────────────────────────────

@dataclass
class Zapytanie:
    sciezka: str
    params: dict
    dane: Optional[dict] = None          # sparsowany JSON z POST
    naglowki: Optional[object] = None


def odpowiedz_json(obiekt, kod=200):
    return kod, "application/json; charset=utf-8", json.dumps(obiekt).encode()


def token_poprawny(stan, naglowki):
    podany = (naglowki.get("X-Token") or "").encode()
    return bool(stan.token_panelu) and hmac.compare_digest(podany, stan.token_panelu.encode())


# ─── endpointy ───────────────────────────────────────────────────────────────

@endpoint("/")
@endpoint("/index.html")
def ep_strona(stan, zapyt):
    if PLIK_PANELU.exists():
        return 200, "text/html; charset=utf-8", PLIK_PANELU.read_bytes()
    return 500, "text/plain; charset=utf-8", b"Brak pliku master_center.html obok master.py"


@endpoint("/api/stan", chroniony=True)
def ep_stan(stan, zapyt):
    try:
        since = int(zapyt.params.get("since", ["0"])[0])
    except ValueError:
        return odpowiedz_json({"blad": "since"}, 400)
    return odpowiedz_json(snapshot(stan, since))


@endpoint("/api/meta")
def ep_meta(stan, zapyt):
    """Stałe dane dla panelu: jakie polecenia zna Master (nazwa -> opis)."""
    macros = sorted(p.stem for p in MACRO_DIR.glob("*.sh") if p.is_file())
    return odpowiedz_json({"polecenia": POLECENIA_SLAVES, "macros": macros})


@endpoint("/api/slaves", chroniony=True)
def ep_slaves(stan, zapyt):
    with stan.lock:
        slaves = sorted(
            ({"id": slave_id, "nazwa": slave.nazwa} for slave_id, slave in stan.slaves.items()),
            key=lambda slave: (0, int(slave["id"])) if slave["id"].isdigit() else (1, slave["id"]),
        )
    return odpowiedz_json({"slaves": slaves})


@endpoint("/api/slave/", prefix=True, chroniony=True)
def ep_slave(stan, zapyt):
    try:
        with stan.lock:
            slave_id = znajdz_slaves(stan, urllib.parse.unquote(zapyt.sciezka.rsplit("/", 1)[1]))
    except ValueError as e:
        return odpowiedz_json({"blad": str(e)}, 404)
    return odpowiedz_json(historia_slaves(stan, slave_id))


@endpoint("/api/master", metoda="POST", chroniony=True)
def ep_master(stan, zapyt):
    """{"akcja": "start" | "stop"}"""
    akcja = (zapyt.dane or {}).get("akcja")
    if akcja == "start":
        uruchom_master(stan)
    elif akcja == "stop":
        wstrzymaj_master(stan)
    else:
        raise ValueError("akcja musi być 'start' albo 'stop'")
    return odpowiedz_json({"ok": True, "pracuje": stan.pracuje})


@endpoint("/api/token-jednorazowy", metoda="POST", chroniony=True)
def ep_token_jednorazowy(stan, zapyt):
    """Generuje jednorazowy token rejestracyjny + gotowe polecenie uruchomienia slave.py."""
    dane = zapyt.dane or {}
    nazwa = str(dane.get("nazwa", "")).strip()[:12]
    domena = str(dane.get("domena", "")).strip()
    if not domena:
        raise ValueError("podaj domenę/adres, pod którym Slave ma znaleźć Master")
    https = bool(dane.get("https", True))
    port = dane.get("port")

    kod, token, wygasa = wygeneruj_kod_startowy(stan, domena, https, port, nazwa)

    polecenie = ["python3", "slave.py", "--master", shlex.quote(domena)]
    if not https:
        polecenie += ["--no-https"]
    if port:
        polecenie += ["--port", str(int(port))]
    polecenie += ["--token-rejestracji", shlex.quote(token)]
    if nazwa:
        polecenie += ["--nazwa", shlex.quote(nazwa)]

    panel_port = PANEL_PORT
    port_str = "" if panel_port == 80 else f":{panel_port}"
    curl = f"curl -s http://{domena}{port_str}/join/{kod} | python3"

    return odpowiedz_json({
        "token": token,
        "kod": kod,
        "curl": curl,
        "wygasa_za_s": TOKEN_JEDNORAZOWY_WAZNY_S,
        "wygasa_ts": wygasa,
        "polecenie": " ".join(polecenie),
    })


@endpoint("/join/", prefix=True)
def ep_join(stan, zapyt):
    """Serwuje slave.py z wstrzykniętą konfiguracją — curl | python3 bootstrap."""
    kod = zapyt.sciezka[6:]
    if not kod:
        return 404, "text/plain", b"brak kodu"
    with stan.lock:
        wpis = stan.kody_startowe.get(kod)
    if not wpis or wpis["wygasa"] <= time.time():
        return 404, "text/plain", b"kod nie istnieje lub wygasl"
    with stan.lock:
        del stan.kody_startowe[kod]
    if not PLIK_SLAVE.exists():
        return 500, "text/plain", b"brak pliku slave.py"
    tresc = PLIK_SLAVE.read_text(encoding="utf-8")
    https_v = "1" if wpis["https"] else "0"
    preamb = (
        "import os as _os\n"
        f"_os.environ.setdefault('SLAVE_MASTER',{wpis['master']!r})\n"
        f"_os.environ.setdefault('SLAVE_TOKEN',{wpis['token']!r})\n"
    )
    if wpis.get("nazwa"):
        preamb += f"_os.environ.setdefault('SLAVE_NAME',{wpis['nazwa']!r})\n"
    preamb += (
        f"_os.environ.setdefault('SLAVE_HTTPS',{https_v!r})\n"
        "del _os\n"
    )
    return 200, "text/x-python; charset=utf-8", (preamb + tresc).encode()


@endpoint("/api/polecenie", metoda="POST", chroniony=True)
def ep_polecenie(stan, zapyt):
    """{"cel": "wszystkie" | ID | nazwa, "polecenie": "pauza", "args": [...]}"""
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


@endpoint("/api/terminal", metoda="POST", chroniony=True)
def ep_terminal(stan, zapyt):
    dane = zapyt.dane or {}
    cel = str(dane.get("cel", "")).strip()
    if not cel:
        raise ValueError("Podaj ID, nazwę Slaves, zakres ID (np. 1-12) albo *.")
    nazwa, args = parsuj_komende_terminalowa(dane.get("komenda"))
    if cel.lower() in CELE_WSZYSTKIE:
        polecenia = [wydaj_polecenie(stan, "wszystkie", nazwa, args)]
        cele = ["wszystkie"]
    else:
        granice = cel.split("-", 1)
        if len(granice) == 2 and all(c.strip().isdigit() for c in granice):
            poczatek, koniec = (int(c.strip()) for c in granice)
            if koniec < poczatek:
                raise ValueError("Koniec zakresu musi być większy lub równy początkowi.")
            with stan.lock:
                cele = sorted(
                    (slave_id for slave_id in stan.slaves if slave_id.isdigit() and poczatek <= int(slave_id) <= koniec),
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
    """-> (funkcja, chroniony) albo None."""
    for m, wzorzec, prefix, chroniony, funkcja in ENDPOINTY:
        if m == metoda and (sciezka == wzorzec or (prefix and sciezka.startswith(wzorzec))):
            return funkcja, chroniony
    return None


class SerwerPanelu(ThreadingHTTPServer):
    request_queue_size = 128


class Panel(BaseHTTPRequestHandler):
    """Klej HTTP: szuka funkcji w rejestrze ENDPOINTY, pilnuje tokenu przy zapisach."""

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
        stan, wpis = self.server.stan, znajdz_endpoint(metoda, sciezka)
        if wpis is None:
            kod, typ, tresc = 404, "text/plain; charset=utf-8", b"404"
        elif wpis[1] and not token_poprawny(stan, self.headers):
            time.sleep(0.5)                          # spowalnia zgadywanie tokenu
            kod, typ, tresc = odpowiedz_json({"blad": "zły lub brak tokenu"}, 403)
        else:
            try:
                dane = self._wczytaj_json() if metoda == "POST" else None
                kod, typ, tresc = wpis[0](stan, Zapytanie(sciezka, urllib.parse.parse_qs(zapytanie), dane, self.headers))
            except ValueError as e:
                kod, typ, tresc = odpowiedz_json({"blad": str(e)}, 400)
            except Exception as e:
                kod, typ, tresc = 500, "text/plain; charset=utf-8", f"blad: {e}".encode()
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
