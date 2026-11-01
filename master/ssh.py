"""Zarządzanie sesjami SSH i zdalne wykonywanie poleceń przez ControlMaster."""

import json
import os
import shutil
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import shlex

from master.config import KATALOG_DANYCH, PLIKI_MAX_B, PLIKI_CHUNK_B, endpoint
from master.stan import log

PLIK_HOSTOW = KATALOG_DANYCH / "ssh_hosts.json"
KATALOG_SESJI = KATALOG_DANYCH / "ssh_sessions"
PLIK_KNOWN_HOSTS = KATALOG_DANYCH / "known_hosts"
KATALOG_KLUCZY = KATALOG_DANYCH / "keys"
PLIK_KLUCZA_PRYWATNEGO = KATALOG_KLUCZY / "master_id_ed25519"
PLIK_KLUCZA_PUBLICZNEGO = KATALOG_KLUCZY / "master_id_ed25519.pub"
SSH_TIMEOUT = 30
SSH_CONNECT_TIMEOUT = 10
PROMOCJA_TIMEOUT = 180


# ─── struktury danych ────────────────────────────────────────────────────────

@dataclass
class HostSSH:
    id: str
    nazwa: str
    host: str
    port: int = 22
    uzytkownik: str = ""
    klucz: str = ""
    haslo: str = ""
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
                    haslo=h.get("haslo", ""),
                )
                stan.hosty[host.id] = host
            stan.licznik_hostow = dane.get("licznik", len(stan.hosty))
        except (json.JSONDecodeError, KeyError, OSError):
            kopia = PLIK_HOSTOW.with_name(f"{PLIK_HOSTOW.name}.broken-{int(time.time())}")
            try:
                PLIK_HOSTOW.rename(kopia)
            except OSError:
                pass
    return stan


def _zapisz_hosty(stan_ssh):
    dane = {
        "licznik": stan_ssh.licznik_hostow,
        "hosty": [
            {"id": h.id, "nazwa": h.nazwa, "host": h.host, "port": h.port,
             "uzytkownik": h.uzytkownik, "klucz": h.klucz, "haslo": h.haslo}
            for h in stan_ssh.hosty.values()
        ],
    }
    KATALOG_DANYCH.mkdir(parents=True, exist_ok=True)
    tymczasowy = PLIK_HOSTOW.with_name(f".{PLIK_HOSTOW.name}.{os.getpid()}.tmp")
    try:
        tymczasowy.write_text(json.dumps(dane, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tymczasowy, PLIK_HOSTOW)
        try:
            os.chmod(PLIK_HOSTOW, 0o600)
        except OSError:
            pass
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

def dodaj_host(stan_ssh, nazwa, host, port=22, uzytkownik="", klucz="", haslo=""):
    with stan_ssh.lock:
        stan_ssh.licznik_hostow += 1
        host_id = str(stan_ssh.licznik_hostow)
        h = HostSSH(id=host_id, nazwa=nazwa, host=host, port=port,
                     uzytkownik=uzytkownik, klucz=klucz, haslo=haslo)
        stan_ssh.hosty[host_id] = h
        _zapisz_hosty(stan_ssh)
    adres = f"{uzytkownik}@{host}" if uzytkownik else host
    auth = "hasło" if haslo else ("klucz" if klucz else "domyślne")
    _feed(stan_ssh, typ="sys",
          tekst=f"+ Dodano host #{host_id} '{nazwa}' ({adres}:{port}, auth: {auth})")
    return h


def edytuj_host(stan_ssh, host_id, **pola):
    """Aktualizuje wybrane pola hosta (uzytkownik/klucz/haslo/port/nazwa/host)."""
    dozwolone = {"nazwa", "host", "port", "uzytkownik", "klucz", "haslo"}
    with stan_ssh.lock:
        h = stan_ssh.hosty.get(host_id)
        if h is None:
            raise ValueError(f"Nie znaleziono hosta SSH #{host_id}")
        for k, v in pola.items():
            if k in dozwolone and v is not None:
                setattr(h, k, v)
        _zapisz_hosty(stan_ssh)
    _feed(stan_ssh, typ="sys", tekst=f"~ #{host_id} '{h.nazwa}': zaktualizowano")
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
            "-o", f"UserKnownHostsFile={PLIK_KNOWN_HOSTS}",
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
            "-o", "BatchMode=no" if h.haslo else "BatchMode=yes",
            "-o", "PubkeyAuthentication=" + ("no" if h.haslo else "yes"),
            "-o", "PasswordAuthentication=" + ("yes" if h.haslo else "no"),
            "-o", "KbdInteractiveAuthentication=" + ("yes" if h.haslo else "no")]
    if h.port != 22:
        args += ["-p", str(h.port)]
    if h.klucz:
        args += ["-i", h.klucz]
    return args


BRAK_SSHPASS = (
    "sshpass niezainstalowany na hoście Mastera — zainstaluj pakiet 'sshpass' "
    "(apt/pacman), użyj klucza SSH zamiast hasła, albo uruchom Mastera w Dockerze."
)
BRAK_SSHKEYGEN = (
    "ssh-keygen niezainstalowany na hoście Mastera — zainstaluj pakiet "
    "'openssh-client' albo uruchom Mastera w Dockerze."
)


def _zapewnij_klucz_mastera():
    """Zwraca (ścieżka_prywatnego, zawartość_publicznego). Tworzy keypair ed25519 raz."""
    KATALOG_KLUCZY.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(KATALOG_KLUCZY, 0o700)
    except OSError:
        pass
    if not PLIK_KLUCZA_PRYWATNEGO.exists():
        if shutil.which("ssh-keygen") is None:
            raise FileNotFoundError(BRAK_SSHKEYGEN)
        for p in (PLIK_KLUCZA_PRYWATNEGO, PLIK_KLUCZA_PUBLICZNEGO):
            try: p.unlink()
            except FileNotFoundError: pass
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-N", "", "-q",
             "-f", str(PLIK_KLUCZA_PRYWATNEGO),
             "-C", "master@remote-control"],
            check=True, capture_output=True, timeout=15,
        )
    try:
        os.chmod(PLIK_KLUCZA_PRYWATNEGO, 0o600)
    except OSError:
        pass
    pub = PLIK_KLUCZA_PUBLICZNEGO.read_text(encoding="utf-8").strip()
    if not pub.startswith("ssh-"):
        raise ValueError(f"Niepoprawny klucz publiczny w {PLIK_KLUCZA_PUBLICZNEGO}")
    return str(PLIK_KLUCZA_PRYWATNEGO), pub


def _probuj_adres(priv_sciezka, uzytkownik, host, port, timeout=5):
    """Szybki probe: ssh -o BatchMode=yes 'echo ok'. Zwraca (ok, komunikat)."""
    cel = f"{uzytkownik}@{host}" if uzytkownik else host
    argv = ["ssh",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", f"UserKnownHostsFile={PLIK_KNOWN_HOSTS}",
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", f"ConnectTimeout={timeout}",
            "-o", "BatchMode=yes",
            "-o", "PubkeyAuthentication=yes",
            "-o", "PasswordAuthentication=no",
            "-i", priv_sciezka,
            "-p", str(port),
            cel, "echo ok"]
    try:
        wynik = subprocess.run(argv, capture_output=True, text=True, timeout=timeout + 5)
        tekst = (wynik.stdout + wynik.stderr).strip()
        return wynik.returncode == 0, tekst[:120]
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, str(e)[:120]


def _wybierz_adres(priv_sciezka, uzytkownik, port, local_ips, public_ip, hostname,
                   wpis_fn, prefix):
    """Próbuje po kolei local_ips → public_ip → hostname. Zwraca (host, źródło) lub (None, None)."""
    kandydaci = []
    for ip in local_ips or []:
        ip = str(ip).strip()
        if ip and ip not in [k[0] for k in kandydaci]:
            kandydaci.append((ip, "local"))
    pub = (public_ip or "").strip()
    if pub and pub not in [k[0] for k in kandydaci]:
        kandydaci.append((pub, "public"))
    hn = (hostname or "").strip()
    if hn and hn not in [k[0] for k in kandydaci]:
        kandydaci.append((hn, "hostname"))
    if not kandydaci:
        return None, None
    for host_cand, zrodlo in kandydaci:
        wpis_fn(f"[i] {prefix}: próba SSH do {host_cand}:{port} ({zrodlo})…")
        ok, msg = _probuj_adres(priv_sciezka, uzytkownik, host_cand, port)
        if ok:
            wpis_fn(f"[OK] {prefix}: {host_cand} ({zrodlo}) odpowiedział")
            return host_cand, zrodlo
        wpis_fn(f"[i] {prefix}: {host_cand} ({zrodlo}) niedostępny — {msg[:80]}")
    return None, None


def _parsuj_promocje(przyklad):
    """Z wyniku polecenia `promuj_ssh` wyciąga dict z JSON po prefiksie 'PROMOCJA '."""
    if not przyklad or not przyklad.startswith("PROMOCJA "):
        return None
    try:
        info = json.loads(przyklad[len("PROMOCJA "):])
        return info if isinstance(info, dict) else None
    except (ValueError, TypeError):
        return None


def _zaraportuj_sshd(wpis_fn, prefix, info):
    """Loguje do feedu kroki ensure-sshd zgłoszone przez slave'a."""
    for krok in info.get("sshd_akcje") or []:
        wpis_fn(f"[i] {prefix}: {krok}")
    if info.get("sshd_blad"):
        wpis_fn(f"[!] {prefix}: {info['sshd_blad']}")
    status = info.get("sshd_status")
    if status:
        wpis_fn(f"[i] {prefix}: sshd status={status}, port={info.get('port', '?')}")


def promuj_slave_do_ssh(stan, slave_id, nazwa, host, port=22, uzytkownik="",
                        timeout=PROMOCJA_TIMEOUT):
    """Przez kanał poleceń Slave: zainstaluj klucz Mastera i zapewnij działający sshd,
    po potwierdzeniu dodaj hosta do listy SSH i spróbuj nawiązać sesję.

    Działa asynchronicznie: wraca od razu z `Polecenie`, promocja kończy się w tle
    i wpisuje status do feedu SSH.
    """
    from master.logika import wydaj_polecenie

    priv_sciezka, pub = _zapewnij_klucz_mastera()
    p = wydaj_polecenie(stan, slave_id, "promuj_ssh", [pub])

    def _wpis(tekst):
        _feed(stan.ssh, typ="sys", tekst=tekst)
        log(stan, tekst, True)

    _wpis(f"> promocja slave #{slave_id} → SSH '{nazwa}': wysłano klucz "
          f"(polecenie #{p.id}), czekam na potwierdzenie")

    def _dokoncz():
        start = time.time()
        ok, przyklad = False, ""
        while time.time() - start < timeout:
            with stan.lock:
                if p.wykonane > 0:
                    ok = p.bledy == 0
                    przyklad = p.przyklad
                    break
            time.sleep(0.3)
        else:
            _wpis(f"[!] promocja slave #{slave_id} → SSH '{nazwa}': "
                  f"timeout {timeout}s — slave nie potwierdził")
            return
        if not ok:
            _wpis(f"[!] promocja slave #{slave_id} → SSH '{nazwa}': "
                  f"promuj_ssh nie powiodło się — {przyklad[:200]}")
            return
        prefix = f"promocja #{slave_id} '{nazwa}'"
        info = _parsuj_promocje(przyklad) or {}
        _zaraportuj_sshd(_wpis, prefix, info)
        sshd_status = info.get("sshd_status")
        wykryty = info.get("sshd_port_wykryty") or info.get("port")
        if port == 22 and isinstance(wykryty, int) and 0 < wykryty < 65536 and wykryty != 22:
            _wpis(f"[i] {prefix}: slave wykrył sshd na porcie {wykryty}, "
                  f"ale formularz podał 22 — używam 22 (zmień w panelu jeśli trzeba)")
        try:
            h = dodaj_host(stan.ssh, nazwa, host, port, uzytkownik, priv_sciezka, "")
        except Exception as e:
            _wpis(f"[!] {prefix}: nie udało się dodać hosta: {e}")
            return
        _wpis(f"[OK] {prefix} → SSH: host #{h.id} '{nazwa}' dodany")
        if sshd_status and sshd_status not in ("running", "started"):
            _wpis(f"[!] {prefix}: sshd na slave nie działa (status={sshd_status}) — "
                  "pomijam SSH-connect. Napraw ręcznie i kliknij 'Połącz'.")
            return
        polacz_host(stan.ssh, h.id)

    threading.Thread(target=_dokoncz, daemon=True).start()
    return p


def _komenda_i_env(h, ssh_args):
    """Zwraca (argv, env) do subprocess.run — z prefiksem sshpass gdy host ma hasło.

    Rzuca FileNotFoundError (z opisowym komunikatem) jeśli host wymaga hasła
    a sshpass nie jest zainstalowany.
    """
    if h.haslo:
        if shutil.which("sshpass") is None:
            raise FileNotFoundError(BRAK_SSHPASS)
        argv = ["sshpass", "-e", "ssh", *ssh_args]
        env = {**os.environ, "SSHPASS": h.haslo}
        return argv, env
    return ["ssh", *ssh_args], None


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
    ssh_args = ["-MNf",
                "-o", f"ControlPath={socket}",
                "-o", "ControlMaster=yes",
                "-o", "ServerAliveInterval=30",
                "-o", "ServerAliveCountMax=3"]
    ssh_args += _bazowe_args(h)
    ssh_args.append(cel)

    try:
        argv, env = _komenda_i_env(h, ssh_args)
        wynik = subprocess.run(argv, capture_output=True, text=True,
                               timeout=SSH_CONNECT_TIMEOUT + 5, env=env)
        tekst = (wynik.stdout + wynik.stderr).strip()
        ok = wynik.returncode == 0
        if not ok and not tekst:
            tekst = f"ssh zakończył się kodem {wynik.returncode}"
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
    ssh_args = []

    uzyto_sesji = Path(socket).exists()
    if uzyto_sesji:
        ssh_args += ["-o", f"ControlPath={socket}", "-o", "ControlMaster=no"]

    ssh_args += _bazowe_args(h)
    ssh_args += [cel, komenda]

    try:
        # Istniejący ControlMaster uwierzytelnił się już wcześniej — nie potrzeba sshpass.
        if uzyto_sesji:
            argv, env = ["ssh", *ssh_args], None
        else:
            argv, env = _komenda_i_env(h, ssh_args)
        wynik = subprocess.run(argv, capture_output=True, text=True,
                               timeout=timeout, env=env)
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


# ─── transfer plików ────────────────────────────────────────────────────────
# Streaming przez istniejący ControlMaster + `cat` na zdalnej stronie.
# Dlaczego nie scp: pure streaming (brak tempfile), 8-bit clean, pokazuje
# edukacyjnie jak kanał shella niesie dowolne bajty.

def _ssh_args_sesji(host_id, h):
    """Argumenty ssh do reużycia istniejącej sesji ControlMaster. Zwraca None gdy brak sesji."""
    socket = _sciezka_socketu(host_id)
    if not Path(socket).exists():
        return None
    cel = _cel_ssh(h)
    args = ["-o", f"ControlPath={socket}", "-o", "ControlMaster=no"]
    args += _bazowe_args(h)
    args.append(cel)
    return args


def wyslij_ssh(stan_ssh, host_id, zdalna_sciezka, rfile, rozmiar, zadanie_id=None):
    """Streamuje `rozmiar` bajtów z rfile do pliku `zdalna_sciezka` na hoście SSH.
    Zwraca (ok, komunikat, wyslano_bajtow)."""
    with stan_ssh.lock:
        h = stan_ssh.hosty.get(host_id)
        if h is None:
            return False, f"Nie znaleziono hosta #{host_id}", 0
        host_nazwa = h.nazwa
    if rozmiar <= 0 or rozmiar > PLIKI_MAX_B:
        return False, f"Rozmiar poza zakresem (0, {PLIKI_MAX_B} B)", 0
    args = _ssh_args_sesji(host_id, h)
    if args is None:
        return False, "Brak aktywnej sesji ControlMaster — kliknij najpierw Połącz.", 0
    # Zdalnie: `cat > <path>`. shlex.quote chroni ścieżkę przed shell-injection.
    zdalna_komenda = f"cat > {shlex.quote(zdalna_sciezka)}"
    argv = ["ssh", *args, zdalna_komenda]
    proces = subprocess.Popen(argv, stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wyslano = 0
    try:
        pozostalo = rozmiar
        while pozostalo > 0:
            chunk = rfile.read(min(PLIKI_CHUNK_B, pozostalo))
            if not chunk:
                break
            proces.stdin.write(chunk)
            wyslano += len(chunk)
            pozostalo -= len(chunk)
        try:
            proces.stdin.close()
        except (BrokenPipeError, OSError):
            pass
        try:
            _, stderr = proces.communicate(timeout=SSH_TIMEOUT)
        except subprocess.TimeoutExpired:
            proces.kill()
            return False, "timeout zapisu zdalnego", wyslano
        ok = proces.returncode == 0 and wyslano == rozmiar
        tekst = ("OK" if ok else
                 (stderr.decode("utf-8", "replace").strip() or
                  f"ssh zakończył się kodem {proces.returncode}"))[:200]
    except OSError as e:
        proces.kill()
        return False, f"błąd I/O: {e}", wyslano
    _feed(stan_ssh, typ="plik", kierunek="wyslij", zadanie_id=zadanie_id,
          host_id=host_id, host_nazwa=host_nazwa,
          zdalna=zdalna_sciezka, bajtow=wyslano, ok=ok, wynik=tekst)
    return ok, tekst, wyslano


def pobierz_ssh(stan_ssh, host_id, zdalna_sciezka, wfile, zadanie_id=None):
    """Streamuje plik `zdalna_sciezka` z hosta SSH do wfile.
    Zwraca (ok, komunikat, pobrano_bajtow). Wołający odpowiedzialny za wysłanie nagłówków przed wywołaniem."""
    with stan_ssh.lock:
        h = stan_ssh.hosty.get(host_id)
        if h is None:
            return False, f"Nie znaleziono hosta #{host_id}", 0
        host_nazwa = h.nazwa
    args = _ssh_args_sesji(host_id, h)
    if args is None:
        return False, "Brak aktywnej sesji ControlMaster — kliknij najpierw Połącz.", 0
    zdalna_komenda = f"cat {shlex.quote(zdalna_sciezka)}"
    argv = ["ssh", *args, zdalna_komenda]
    proces = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pobrano = 0
    try:
        while pobrano < PLIKI_MAX_B:
            chunk = proces.stdout.read(PLIKI_CHUNK_B)
            if not chunk:
                break
            wfile.write(chunk)
            pobrano += len(chunk)
        try:
            _, stderr = proces.communicate(timeout=SSH_TIMEOUT)
        except subprocess.TimeoutExpired:
            proces.kill()
            return False, "timeout odczytu zdalnego", pobrano
        ok = proces.returncode == 0
        tekst = ("OK" if ok else
                 (stderr.decode("utf-8", "replace").strip() or
                  f"ssh zakończył się kodem {proces.returncode}"))[:200]
    except (OSError, BrokenPipeError) as e:
        proces.kill()
        return False, f"błąd I/O: {e}", pobrano
    _feed(stan_ssh, typ="plik", kierunek="pobierz", zadanie_id=zadanie_id,
          host_id=host_id, host_nazwa=host_nazwa,
          zdalna=zdalna_sciezka, bajtow=pobrano, ok=ok, wynik=tekst)
    return ok, tekst, pobrano


# ─── widoki ──────────────────────────────────────────────────────────────────

def _widok_hosta(h, teraz):
    return {
        "id": h.id, "nazwa": h.nazwa, "host": h.host, "port": h.port,
        "uzytkownik": h.uzytkownik,
        "klucz": bool(h.klucz),
        "has_haslo": bool(h.haslo),
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


@endpoint("/api/ssh/stan")
def ep_ssh_stan(stan, zapyt):
    try:
        since = int(zapyt.params.get("since", ["0"])[0])
    except ValueError:
        return _json({"blad": "since"}, 400)
    return _json(snapshot_ssh(stan.ssh, since))


@endpoint("/api/ssh/dodaj", metoda="POST")
def ep_ssh_dodaj(stan, zapyt):
    dane = zapyt.dane or {}
    nazwa = str(dane.get("nazwa", "")).strip()[:30]
    host = str(dane.get("host", "")).strip()
    if not nazwa or not host:
        raise ValueError("Podaj nazwę i adres hosta.")
    port = int(dane.get("port", 22) or 22)
    uzytkownik = str(dane.get("uzytkownik", "")).strip()
    klucz = str(dane.get("klucz", "")).strip()
    haslo = str(dane.get("haslo", ""))
    h = dodaj_host(stan.ssh, nazwa, host, port, uzytkownik, klucz, haslo)
    return _json({"ok": True, "id": h.id, "nazwa": h.nazwa})


@endpoint("/api/ssh/edytuj", metoda="POST")
def ep_ssh_edytuj(stan, zapyt):
    dane = zapyt.dane or {}
    host_id = str(dane.get("id", "")).strip()
    if not host_id:
        raise ValueError("Podaj ID hosta do edycji.")
    pola = {}
    for klucz in ("nazwa", "host", "uzytkownik", "klucz", "haslo"):
        if klucz in dane:
            pola[klucz] = str(dane[klucz])
    if "port" in dane and dane["port"] not in (None, ""):
        pola["port"] = int(dane["port"])
    edytuj_host(stan.ssh, host_id, **pola)
    return _json({"ok": True, "id": host_id})


@endpoint("/api/ssh/usun", metoda="POST")
def ep_ssh_usun(stan, zapyt):
    dane = zapyt.dane or {}
    host_id = str(dane.get("id", "")).strip()
    if not host_id:
        raise ValueError("Podaj ID hosta do usunięcia.")
    usun_host(stan.ssh, host_id)
    return _json({"ok": True})


@endpoint("/api/ssh/polacz", metoda="POST")
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


@endpoint("/api/ssh/rozlacz", metoda="POST")
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


@endpoint("/api/ssh/wykonaj", metoda="POST")
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


def promuj_slave_auto(stan, slave_id, nazwa=None, port=22, timeout=PROMOCJA_TIMEOUT):
    """Jednoprzyciskowa promocja: slave sam instaluje klucz i raportuje user/IP,
    Master automatycznie dodaje hosta i otwiera sesję.

    Preferowany adres: public_ip > pierwszy local_ip > hostname.
    """
    from master.logika import wydaj_polecenie

    priv_sciezka, pub = _zapewnij_klucz_mastera()
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            raise ValueError(f"Nie znaleziono Slave #{slave_id}")
        nazwa_slave = slave.nazwa
    nazwa_hosta = (nazwa or nazwa_slave)[:30] or f"slave-{slave_id}"

    p = wydaj_polecenie(stan, slave_id, "promuj_ssh", [pub])

    def _wpis(tekst):
        """Równolegle: feed SSH w panelu + log konsoli (żeby było widać w `docker compose logs`)."""
        _feed(stan.ssh, typ="sys", tekst=tekst)
        log(stan, tekst, True)

    _wpis(f"> auto-promocja slave #{slave_id} '{nazwa_slave}' → SSH: wysłano klucz "
          f"(polecenie #{p.id}), czekam na raport")

    def _dokoncz():
        start = time.time()
        ok, przyklad = False, ""
        while time.time() - start < timeout:
            with stan.lock:
                if p.wykonane > 0:
                    ok = p.bledy == 0
                    przyklad = p.przyklad
                    break
            time.sleep(0.3)
        else:
            _wpis(f"[!] auto-promocja #{slave_id}: timeout {timeout}s — slave nie odpowiedział")
            return
        if not ok:
            _wpis(f"[!] auto-promocja #{slave_id}: slave zgłosił błąd — {przyklad[:200]}")
            return
        info = _parsuj_promocje(przyklad)
        if not info:
            _wpis(f"[!] auto-promocja #{slave_id}: slave nie zwrócił poprawnego raportu — "
                  f"{przyklad[:200]}")
            return
        prefix = f"auto-promocja #{slave_id}"
        _zaraportuj_sshd(_wpis, prefix, info)
        local_ips = info.get("local_ips") or []
        uzytkownik = str(info.get("user", "") or "").strip()
        try:
            port_zdalny = int(info.get("sshd_port_wykryty") or info.get("port") or port or 22)
        except (TypeError, ValueError):
            port_zdalny = port or 22
        sshd_status = info.get("sshd_status")
        if sshd_status and sshd_status not in ("running", "started"):
            # sshd nie działa — nie ma sensu próbować SSH; nie dodawaj martwego hosta.
            _wpis(f"[!] {prefix}: sshd na slave nie działa (status={sshd_status}) — "
                  "nie dodaję hosta. Napraw sshd (patrz powyższe wpisy) i powtórz promocję.")
            return
        host, zrodlo = _wybierz_adres(priv_sciezka, uzytkownik, port_zdalny,
                                       local_ips, info.get("public_ip"),
                                       info.get("hostname"), _wpis, prefix)
        if not host:
            _wpis(f"[!] {prefix}: żaden adres slave'a nie odpowiedział po SSH — "
                  f"local={local_ips} public={info.get('public_ip') or '–'} "
                  f"hostname={info.get('hostname') or '–'}. "
                  "Shift+klik SSH w panelu → wpisz dostępny host/port ręcznie.")
            return
        try:
            h = dodaj_host(stan.ssh, nazwa_hosta, host, port_zdalny,
                           uzytkownik, priv_sciezka, "")
        except Exception as e:
            _wpis(f"[!] {prefix}: nie udało się dodać hosta: {e}")
            return
        _wpis(f"[OK] {prefix} → SSH host #{h.id} "
              f"{(uzytkownik + '@') if uzytkownik else ''}{host}:{port_zdalny} "
              f"(źródło: {zrodlo})")
        ok_sesja, tekst_sesja = polacz_host(stan.ssh, h.id)
        if ok_sesja:
            _wpis(f"[OK] {prefix}: sesja SSH otwarta → host #{h.id}")
        else:
            _wpis(f"[!] {prefix}: SSH-connect do {host}:{port_zdalny} "
                  f"nie powiódł się — {tekst_sesja[:200]}")

    threading.Thread(target=_dokoncz, daemon=True).start()
    return p


@endpoint("/api/ssh/z_slave_auto", metoda="POST")
def ep_ssh_z_slave_auto(stan, zapyt):
    """Auto-promocja połączonego Slave'a do SSH — bez pytania o host/user."""
    dane = zapyt.dane or {}
    slave_id = str(dane.get("slave_id", "")).strip()
    if not slave_id:
        raise ValueError("Podaj slave_id.")
    nazwa = str(dane.get("nazwa", "")).strip() or None
    try:
        port = int(dane.get("port", 22) or 22)
    except (TypeError, ValueError):
        raise ValueError("Port musi być liczbą.")
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            raise ValueError(f"Nie znaleziono Slave #{slave_id}")
        if slave.sesja != "połączony":
            raise ValueError(
                f"Slave #{slave_id} nie jest aktualnie połączony "
                "(stan: " + slave.sesja + ") — otwórz sesję najpierw."
            )
    p = promuj_slave_auto(stan, slave_id, nazwa=nazwa, port=port)
    return _json({"ok": True, "polecenie_id": p.id})


@endpoint("/api/ssh/z_slave", metoda="POST")
def ep_ssh_z_slave(stan, zapyt):
    """Promuj połączonego Slave'a do SSH: wyślij klucz Mastera przez jego kanał poleceń,
    dodaj do listy hostów SSH i nawiąż sesję."""
    dane = zapyt.dane or {}
    slave_id = str(dane.get("slave_id", "")).strip()
    nazwa = str(dane.get("nazwa", "")).strip()[:30]
    host = str(dane.get("host", "")).strip()
    if not slave_id or not nazwa or not host:
        raise ValueError("Podaj slave_id, nazwę i host.")
    try:
        port = int(dane.get("port", 22) or 22)
    except (TypeError, ValueError):
        raise ValueError("Port musi być liczbą.")
    uzytkownik = str(dane.get("uzytkownik", "")).strip()
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            raise ValueError(f"Nie znaleziono Slave #{slave_id}")
        if slave.sesja != "połączony":
            raise ValueError(
                f"Slave #{slave_id} nie jest aktualnie połączony "
                "(stan: " + slave.sesja + ") — otwórz sesję najpierw."
            )
    p = promuj_slave_do_ssh(stan, slave_id, nazwa, host, port, uzytkownik)
    return _json({"ok": True, "polecenie_id": p.id})


@endpoint("/api/ssh/testuj", metoda="POST")
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


# ─── transfer plików: endpointy ─────────────────────────────────────────────

@endpoint("/api/ssh/wyslij", metoda="POST", strumien=True)
def ep_ssh_wyslij(stan, zapyt):
    """Streamuje ciało żądania (surowe bajty) do pliku na zdalnym hoście.
    Query: ?id=<host_id>&zdalny=<ścieżka>. Nagłówek: Content-Length."""
    host_id = (zapyt.params.get("id", [""])[0] or "").strip()
    zdalna = (zapyt.params.get("zdalny", [""])[0] or "").strip()
    if not host_id or host_id not in stan.ssh.hosty:
        return 400, "application/json", json.dumps({"blad": "Podaj prawidłowe ID hosta."}).encode()
    if not zdalna:
        return 400, "application/json", json.dumps({"blad": "Podaj zdalną ścieżkę."}).encode()
    if zapyt.content_length <= 0:
        return 411, "application/json", json.dumps({"blad": "Wymagany Content-Length."}).encode()
    if zapyt.content_length > PLIKI_MAX_B:
        return 413, "application/json", json.dumps(
            {"blad": f"Plik większy niż limit {PLIKI_MAX_B} B."}).encode()

    with stan.ssh.lock:
        stan.ssh.licznik_zadan += 1
        zadanie_id = stan.ssh.licznik_zadan
    _feed(stan.ssh, typ="sys",
          tekst=f"> [{zadanie_id}] wyślij plik → #{host_id}:{zdalna} ({zapyt.content_length} B)")
    ok, tekst, wyslano = wyslij_ssh(stan.ssh, host_id, zdalna,
                                     zapyt.rfile, zapyt.content_length, zadanie_id)
    kod = 200 if ok else 500
    return kod, "application/json", json.dumps(
        {"ok": ok, "zadanie_id": zadanie_id, "bajtow": wyslano, "wynik": tekst}).encode()


@endpoint("/api/ssh/pobierz", metoda="GET", strumien=True)
def ep_ssh_pobierz(stan, zapyt):
    """Streamuje plik ze zdalnego hosta jako attachment.
    Query: ?id=<host_id>&zdalny=<ścieżka>&nazwa=<nazwa_pliku>.
    Zwraca None (handler sam zapisał odpowiedź)."""
    host_id = (zapyt.params.get("id", [""])[0] or "").strip()
    zdalna = (zapyt.params.get("zdalny", [""])[0] or "").strip()
    nazwa = (zapyt.params.get("nazwa", [""])[0] or "").strip() or Path(zdalna).name or "plik"

    def blad_json(kod, msg):
        tresc = json.dumps({"blad": msg}).encode()
        reason = {400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}.get(kod, "Error")
        try:
            zapyt.wfile.write(f"HTTP/1.1 {kod} {reason}\r\n".encode("ascii"))
            zapyt.wfile.write(b"Content-Type: application/json; charset=utf-8\r\n")
            zapyt.wfile.write(f"Content-Length: {len(tresc)}\r\nConnection: close\r\n\r\n".encode("ascii"))
            zapyt.wfile.write(tresc)
        except (BrokenPipeError, ConnectionResetError):
            pass

    if not host_id or host_id not in stan.ssh.hosty:
        blad_json(400, "Podaj prawidłowe ID hosta.")
        return None
    if not zdalna:
        blad_json(400, "Podaj zdalną ścieżkę.")
        return None

    # Sprawdź sesję ControlMaster PRZED wysłaniem nagłówków — inaczej błąd ląduje
    # w ciele pliku zamiast jako HTTP error.
    with stan.ssh.lock:
        h = stan.ssh.hosty.get(host_id)
    if h is None or _ssh_args_sesji(host_id, h) is None:
        blad_json(400, "Brak aktywnej sesji ControlMaster — kliknij najpierw Połącz.")
        return None

    with stan.ssh.lock:
        stan.ssh.licznik_zadan += 1
        zadanie_id = stan.ssh.licznik_zadan
    _feed(stan.ssh, typ="sys",
          tekst=f"> [{zadanie_id}] pobierz plik ← #{host_id}:{zdalna}")

    # Nagłówki chunked — rozmiar nieznany z góry (strumień z `cat`).
    try:
        bezpieczna_nazwa = nazwa.encode("ascii", "ignore").decode("ascii") or "plik.bin"
        bezpieczna_nazwa = bezpieczna_nazwa.replace('"', "")
        zapyt.wfile.write(b"HTTP/1.1 200 OK\r\n")
        zapyt.wfile.write(b"Content-Type: application/octet-stream\r\n")
        zapyt.wfile.write(f'Content-Disposition: attachment; filename="{bezpieczna_nazwa}"\r\n'.encode("ascii"))
        zapyt.wfile.write(b"Transfer-Encoding: chunked\r\n")
        zapyt.wfile.write(b"Cache-Control: no-store\r\n")
        zapyt.wfile.write(b"Connection: close\r\n\r\n")
    except (BrokenPipeError, ConnectionResetError):
        return None

    class ChunkedWriter:
        def __init__(self, wfile):
            self.wfile = wfile
        def write(self, data):
            if not data:
                return
            self.wfile.write(f"{len(data):x}\r\n".encode("ascii"))
            self.wfile.write(data)
            self.wfile.write(b"\r\n")

    cw = ChunkedWriter(zapyt.wfile)
    try:
        pobierz_ssh(stan.ssh, host_id, zdalna, cw, zadanie_id)
        zapyt.wfile.write(b"0\r\n\r\n")
    except (BrokenPipeError, ConnectionResetError):
        pass
    return None
