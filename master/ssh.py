"""Zarządzanie sesjami SSH i zdalne wykonywanie poleceń przez ControlMaster."""

import json
import os
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from master.config import KATALOG_DANYCH, endpoint

PLIK_HOSTOW = KATALOG_DANYCH / "ssh_hosts.json"
KATALOG_SESJI = Path("/tmp/master_ssh")
SSH_TIMEOUT = 30
SSH_CONNECT_TIMEOUT = 10


# ─── struktury danych ────────────────────────────────────────────────────────

@dataclass
class HostSSH:
    id: str
    nazwa: str
    host: str
    port: int = 22
    uzytkownik: str = ""
    klucz: str = ""
    sesja: str = "rozłączony"        # rozłączony / łączenie / połączony / błąd
    blad: str = ""
    ostatnio: Optional[float] = None


@dataclass
class StanSSH:
    hosty: dict = field(default_factory=dict)
    licznik_hostow: int = 0
    licznik_zadan: int = 0
    feed: deque = field(default_factory=lambda: deque(maxlen=500))
    seq: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


# ─── persystencja ────────────────────────────────────────────────────────────

def wczytaj_hosty():
    stan = StanSSH()
    if PLIK_HOSTOW.exists():
        try:
            dane = json.loads(PLIK_HOSTOW.read_text(encoding="utf-8"))
            for h in dane.get("hosty", []):
                host = HostSSH(
                    id=h["id"], nazwa=h["nazwa"], host=h["host"],
                    port=h.get("port", 22),
                    uzytkownik=h.get("uzytkownik", ""),
                    klucz=h.get("klucz", ""),
                )
                stan.hosty[host.id] = host
            stan.licznik_hostow = dane.get("licznik", len(stan.hosty))
        except (json.JSONDecodeError, KeyError, OSError):
            pass
    return stan


def _zapisz_hosty(stan_ssh):
    dane = {
        "licznik": stan_ssh.licznik_hostow,
        "hosty": [
            {"id": h.id, "nazwa": h.nazwa, "host": h.host, "port": h.port,
             "uzytkownik": h.uzytkownik, "klucz": h.klucz}
            for h in stan_ssh.hosty.values()
        ],
    }
    KATALOG_DANYCH.mkdir(parents=True, exist_ok=True)
    tymczasowy = PLIK_HOSTOW.with_name(f".{PLIK_HOSTOW.name}.{os.getpid()}.tmp")
    try:
        tymczasowy.write_text(json.dumps(dane, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tymczasowy, PLIK_HOSTOW)
    finally:
        try:
            tymczasowy.unlink()
        except FileNotFoundError:
            pass


# ─── feed ────────────────────────────────────────────────────────────────────

def _feed(stan_ssh, **wpis):
    with stan_ssh.lock:
        stan_ssh.seq += 1
        stan_ssh.feed.append({"seq": stan_ssh.seq, "ts": time.time(), **wpis})


# ─── zarządzanie hostami ────────────────────────────────────────────────────

def dodaj_host(stan_ssh, nazwa, host, port=22, uzytkownik="", klucz=""):
    with stan_ssh.lock:
        stan_ssh.licznik_hostow += 1
        host_id = str(stan_ssh.licznik_hostow)
        h = HostSSH(id=host_id, nazwa=nazwa, host=host, port=port,
                     uzytkownik=uzytkownik, klucz=klucz)
        stan_ssh.hosty[host_id] = h
        _zapisz_hosty(stan_ssh)
    adres = f"{uzytkownik}@{host}" if uzytkownik else host
    _feed(stan_ssh, typ="sys", tekst=f"+ Dodano host #{host_id} '{nazwa}' ({adres}:{port})")
    return h


def usun_host(stan_ssh, host_id):
    with stan_ssh.lock:
        if host_id not in stan_ssh.hosty:
            raise ValueError(f"Nie znaleziono hosta SSH #{host_id}")
        h = stan_ssh.hosty.pop(host_id)
        _zapisz_hosty(stan_ssh)
    _zamknij_sesje(h)
    _feed(stan_ssh, typ="sys", tekst=f"- Usunięto host #{host_id} '{h.nazwa}'")


def znajdz_host(stan_ssh, cel):
    cel = cel.strip().lstrip("#")
    if cel in stan_ssh.hosty:
        return [cel]
    trafione = [h.id for h in stan_ssh.hosty.values() if h.nazwa == cel]
    if len(trafione) == 1:
        return trafione
    if trafione:
        raise ValueError(f"Nazwa '{cel}' jest niejednoznaczna (ID: {', '.join(trafione)})")
    raise ValueError(f"Nie znaleziono hosta SSH '{cel}'")


# ─── sesje SSH (ControlMaster) ──────────────────────────────────────────────

def _sciezka_socketu(host_id):
    KATALOG_SESJI.mkdir(mode=0o700, parents=True, exist_ok=True)
    return str(KATALOG_SESJI / f"s{host_id}")


def _cel_ssh(h):
    return f"{h.uzytkownik}@{h.host}" if h.uzytkownik else h.host


def _bazowe_args(h):
    args = ["-o", "StrictHostKeyChecking=accept-new",
            "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
            "-o", "BatchMode=yes"]
    if h.port != 22:
        args += ["-p", str(h.port)]
    if h.klucz:
        args += ["-i", h.klucz]
    return args


def polacz_host(stan_ssh, host_id):
    """Nawiązuje persistent sesję SSH (ControlMaster w tle)."""
    with stan_ssh.lock:
        h = stan_ssh.hosty.get(host_id)
        if h is None:
            return False, "Nie znaleziono hosta"
        h.sesja = "łączenie"
        host_nazwa = h.nazwa

    socket = _sciezka_socketu(host_id)
    cel = _cel_ssh(h)

    # Sprawdź czy sesja już aktywna
    try:
        r = subprocess.run(["ssh", "-S", socket, "-O", "check", cel],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            with stan_ssh.lock:
                if host_id in stan_ssh.hosty:
                    stan_ssh.hosty[host_id].sesja = "połączony"
                    stan_ssh.hosty[host_id].blad = ""
            _feed(stan_ssh, typ="sys", tekst=f"~ #{host_id} '{host_nazwa}': sesja już aktywna")
            return True, "Sesja już aktywna"
    except (subprocess.TimeoutExpired, OSError):
        pass

    # Nowy ControlMaster: -M master, -N no command, -f go background
    args = ["ssh", "-MNf",
            "-o", f"ControlPath={socket}",
            "-o", "ControlMaster=yes",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3"]
    args += _bazowe_args(h)
    args.append(cel)

    try:
        wynik = subprocess.run(args, capture_output=True, text=True,
                               timeout=SSH_CONNECT_TIMEOUT + 5)
        tekst = (wynik.stdout + wynik.stderr).strip()
        ok = wynik.returncode == 0
    except subprocess.TimeoutExpired:
        ok, tekst = False, "timeout połączenia"
    except OSError as e:
        ok, tekst = False, str(e)

    with stan_ssh.lock:
        if host_id in stan_ssh.hosty:
            stan_ssh.hosty[host_id].sesja = "połączony" if ok else "błąd"
            stan_ssh.hosty[host_id].blad = "" if ok else tekst[:200]
            stan_ssh.hosty[host_id].ostatnio = time.time()

    info = "sesja nawiązana" if ok else f"błąd: {tekst}"
    _feed(stan_ssh, typ="sys", tekst=f"{'[OK]' if ok else '[!]'} #{host_id} '{host_nazwa}': {info}")
    return ok, tekst if not ok else "Sesja nawiązana"


def _zamknij_sesje(h):
    """Zamyka ControlMaster (helper, nie wymaga locka na stan)."""
    socket = _sciezka_socketu(h.id)
    cel = _cel_ssh(h)
    try:
        subprocess.run(["ssh", "-S", socket, "-O", "exit", cel],
                       capture_output=True, text=True, timeout=5)
    except (subprocess.TimeoutExpired, OSError):
        pass
    try:
        Path(socket).unlink(missing_ok=True)
    except OSError:
        pass


def rozlacz_host(stan_ssh, host_id):
    """Zamyka persistent sesję SSH."""
    with stan_ssh.lock:
        h = stan_ssh.hosty.get(host_id)
        if h is None:
            return False, "Nie znaleziono hosta"
        host_nazwa = h.nazwa

    _zamknij_sesje(h)

    with stan_ssh.lock:
        if host_id in stan_ssh.hosty:
            stan_ssh.hosty[host_id].sesja = "rozłączony"
            stan_ssh.hosty[host_id].blad = ""

    _feed(stan_ssh, typ="sys", tekst=f"~ #{host_id} '{host_nazwa}': sesja zamknięta")
    return True, "Sesja zamknięta"


# ─── wykonywanie poleceń ────────────────────────────────────────────────────

def wykonaj_ssh(stan_ssh, host_id, komenda, zadanie_id=None, timeout=SSH_TIMEOUT):
    """Wykonuje polecenie SSH — przez sesję (szybko) lub bezpośrednio (wolniej)."""
    with stan_ssh.lock:
        h = stan_ssh.hosty.get(host_id)
        if h is None:
            return False, f"Nie znaleziono hosta #{host_id}"
        host_nazwa = h.nazwa

    socket = _sciezka_socketu(host_id)
    cel = _cel_ssh(h)
    args = ["ssh"]

    if Path(socket).exists():
        args += ["-o", f"ControlPath={socket}", "-o", "ControlMaster=no"]

    args += _bazowe_args(h)
    args += [cel, komenda]

    try:
        wynik = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        tekst = (wynik.stdout + wynik.stderr).strip()
        ok = wynik.returncode == 0
    except subprocess.TimeoutExpired:
        ok, tekst = False, f"timeout ({timeout}s)"
    except OSError as e:
        ok, tekst = False, f"błąd: {e}"

    teraz = time.time()
    with stan_ssh.lock:
        if host_id in stan_ssh.hosty:
            stan_ssh.hosty[host_id].ostatnio = teraz
            stan_ssh.hosty[host_id].blad = "" if ok else tekst[:200]
            if not ok and stan_ssh.hosty[host_id].sesja == "połączony":
                if not Path(socket).exists():
                    stan_ssh.hosty[host_id].sesja = "rozłączony"

    _feed(stan_ssh, typ="wynik", zadanie_id=zadanie_id,
          host_id=host_id, host_nazwa=host_nazwa,
          komenda=komenda, ok=ok, wynik=tekst[:4000])
    return ok, tekst


def _wykonaj_w_tle(stan_ssh, host_ids, komenda, zadanie_id):
    for hid in host_ids:
        threading.Thread(target=wykonaj_ssh, daemon=True,
                         args=(stan_ssh, hid, komenda, zadanie_id)).start()


# ─── widoki ──────────────────────────────────────────────────────────────────

def _widok_hosta(h, teraz):
    return {
        "id": h.id, "nazwa": h.nazwa, "host": h.host, "port": h.port,
        "uzytkownik": h.uzytkownik, "klucz": bool(h.klucz),
        "sesja": h.sesja, "blad": h.blad,
        "ostatnio": (teraz - h.ostatnio) if h.ostatnio else None,
    }


def snapshot_ssh(stan_ssh, since=0):
    with stan_ssh.lock:
        teraz = time.time()
        for h in stan_ssh.hosty.values():
            if h.sesja == "połączony" and not Path(_sciezka_socketu(h.id)).exists():
                h.sesja = "rozłączony"
        return {
            "hosty": [_widok_hosta(h, teraz) for h in stan_ssh.hosty.values()],
            "feed": [e for e in stan_ssh.feed if e["seq"] > since][-100:],
            "seq": stan_ssh.seq,
        }


# ─── endpointy API ──────────────────────────────────────────────────────────

def _json(obiekt, kod=200):
    return kod, "application/json; charset=utf-8", json.dumps(obiekt).encode()


@endpoint("/api/ssh/stan", chroniony=True)
def ep_ssh_stan(stan, zapyt):
    try:
        since = int(zapyt.params.get("since", ["0"])[0])
    except ValueError:
        return _json({"blad": "since"}, 400)
    return _json(snapshot_ssh(stan.ssh, since))


@endpoint("/api/ssh/dodaj", metoda="POST", chroniony=True)
def ep_ssh_dodaj(stan, zapyt):
    dane = zapyt.dane or {}
    nazwa = str(dane.get("nazwa", "")).strip()[:30]
    host = str(dane.get("host", "")).strip()
    if not nazwa or not host:
        raise ValueError("Podaj nazwę i adres hosta.")
    port = int(dane.get("port", 22) or 22)
    uzytkownik = str(dane.get("uzytkownik", "")).strip()
    klucz = str(dane.get("klucz", "")).strip()
    h = dodaj_host(stan.ssh, nazwa, host, port, uzytkownik, klucz)
    return _json({"ok": True, "id": h.id, "nazwa": h.nazwa})


@endpoint("/api/ssh/usun", metoda="POST", chroniony=True)
def ep_ssh_usun(stan, zapyt):
    dane = zapyt.dane or {}
    host_id = str(dane.get("id", "")).strip()
    if not host_id:
        raise ValueError("Podaj ID hosta do usunięcia.")
    usun_host(stan.ssh, host_id)
    return _json({"ok": True})


@endpoint("/api/ssh/polacz", metoda="POST", chroniony=True)
def ep_ssh_polacz(stan, zapyt):
    """Nawiązuje persistent sesję SSH (lub wszystkie)."""
    dane = zapyt.dane or {}
    host_id = str(dane.get("id", "")).strip()
    if not host_id:
        raise ValueError("Podaj ID hosta lub * dla wszystkich.")
    if host_id == "*":
        with stan.ssh.lock:
            host_ids = list(stan.ssh.hosty.keys())
        if not host_ids:
            raise ValueError("Brak skonfigurowanych hostów.")
        for hid in host_ids:
            threading.Thread(target=polacz_host, daemon=True,
                             args=(stan.ssh, hid)).start()
        return _json({"ok": True, "hosty": len(host_ids)})
    if host_id not in stan.ssh.hosty:
        raise ValueError(f"Nie znaleziono hosta #{host_id}")
    threading.Thread(target=polacz_host, daemon=True,
                     args=(stan.ssh, host_id)).start()
    return _json({"ok": True, "id": host_id})


@endpoint("/api/ssh/rozlacz", metoda="POST", chroniony=True)
def ep_ssh_rozlacz(stan, zapyt):
    """Zamyka sesję SSH (lub wszystkie)."""
    dane = zapyt.dane or {}
    host_id = str(dane.get("id", "")).strip()
    if not host_id:
        raise ValueError("Podaj ID hosta lub * dla wszystkich.")
    if host_id == "*":
        with stan.ssh.lock:
            host_ids = list(stan.ssh.hosty.keys())
        for hid in host_ids:
            rozlacz_host(stan.ssh, hid)
        return _json({"ok": True, "hosty": len(host_ids)})
    if host_id not in stan.ssh.hosty:
        raise ValueError(f"Nie znaleziono hosta #{host_id}")
    ok, msg = rozlacz_host(stan.ssh, host_id)
    return _json({"ok": ok, "tekst": msg})


@endpoint("/api/ssh/wykonaj", metoda="POST", chroniony=True)
def ep_ssh_wykonaj(stan, zapyt):
    dane = zapyt.dane or {}
    komenda = str(dane.get("komenda", "")).strip()
    cel = str(dane.get("cel", "")).strip()
    if not komenda:
        raise ValueError("Podaj komendę do wykonania.")
    if not cel:
        raise ValueError("Podaj ID hosta lub * dla wszystkich.")

    with stan.ssh.lock:
        if cel in ("*", "wszystkie"):
            host_ids = list(stan.ssh.hosty.keys())
            if not host_ids:
                raise ValueError("Brak skonfigurowanych hostów SSH.")
        else:
            host_ids = znajdz_host(stan.ssh, cel)
        stan.ssh.licznik_zadan += 1
        zadanie_id = stan.ssh.licznik_zadan

    ile = len(host_ids)
    adresat = f"wszystkie ({ile})" if ile > 1 else f"#{host_ids[0]}"
    _feed(stan.ssh, typ="sys",
          tekst=f"> [{zadanie_id}] {komenda}  →  {adresat}")
    _wykonaj_w_tle(stan.ssh, host_ids, komenda, zadanie_id)
    return _json({"ok": True, "zadanie_id": zadanie_id, "hosty": ile, "komenda": komenda})


@endpoint("/api/ssh/testuj", metoda="POST", chroniony=True)
def ep_ssh_testuj(stan, zapyt):
    dane = zapyt.dane or {}
    host_id = str(dane.get("id", "")).strip()
    if not host_id or host_id not in stan.ssh.hosty:
        raise ValueError("Podaj prawidłowe ID hosta.")

    with stan.ssh.lock:
        stan.ssh.licznik_zadan += 1
        zadanie_id = stan.ssh.licznik_zadan

    _feed(stan.ssh, typ="sys", tekst=f"> [{zadanie_id}] test połączenia  →  #{host_id}")
    threading.Thread(target=wykonaj_ssh, daemon=True,
                     args=(stan.ssh, host_id, "echo ok", zadanie_id,
                           SSH_CONNECT_TIMEOUT + 5)).start()
    return _json({"ok": True, "zadanie_id": zadanie_id})
