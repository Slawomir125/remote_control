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
import json
import os
import random
import shlex
import sqlite3
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
MIN_ODPYTYWANIE_S = 1.0
MAX_ODPYTYWANIE_S = 5.0
DEFAULT_MASTER_HOST = "localhost"
DEFAULT_HTTPS = True

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
    pakiet = {"typ": "praca",
              "odpowiedz": None if bez_odpowiedzi else postep.odpowiedz,
              "zadanie_id": None if bez_odpowiedzi else postep.zadanie_id}
    if postep.pauza or bez_odpowiedzi:
        pakiet["pauza"] = True                  # Master nie wyda nowego zadania
    if postep.potwierdzenia:
        pakiet["potwierdzenia"] = list(postep.potwierdzenia)
    return pakiet


def zastosuj_werdykt(postep, werdykt):
    """Aktualizuje liczniki po werdykcie Master ('ok' / 'zle' / 'nieaktualne' / None)."""
    if werdykt == "ok":
        postep.dobre += 1
    elif werdykt == "zle":
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
        try:
            funkcja = POLECENIA[p.get("polecenie")]
            ok, wynik = True, funkcja(konfig, postep, [str(a) for a in p.get("args", [])])
        except KeyError:
            ok, wynik = False, f"nieznane polecenie: {p.get('polecenie')!r}"
        except (IndexError, ValueError, ZeroDivisionError) as e:
            ok = False
            wynik = str(e) if p.get("polecenie") == "makro" else f"błąd polecenia: {e!r}"
        wynik = str(wynik)
        if p.get("polecenie") != "makro":
            wynik = wynik[:1000]
        postep.potwierdzenia.append({"id": cid, "rozgl": bool(p.get("rozgl")), "ok": ok, "wynik": wynik})


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
    """Wysyła żądanie JSON; HTTPS weryfikuje certyfikat hosta przez systemowy magazyn CA."""
    scheme = "https" if lacze.https else "http"
    authority = lacze.domena or lacze.host
    if ":" in authority and not authority.startswith("["):
        authority = f"[{authority}]"
    default_port = 443 if lacze.https else 80
    if lacze.port and lacze.port != default_port:
        authority = f"{authority}:{lacze.port}"
    path = "/api/slave/register" if slave_id == "0" else f"/api/slave/{slave_id}"
    request = urllib.request.Request(
        f"{scheme}://{authority}{path}",
        data=json.dumps(obiekt).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}",
                 "User-Agent": "gru-minionki-slave/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
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
    return konfig.plik_stanu or KATALOG / "slave_state" / f"{quote(konfig.nazwa, safe='')}.json"


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
        legacy_paths = (KATALOG / "minion_stan" / f"{nazwa}.json",
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
        odp = zapytaj(lacze, "0", token_rejestracji, {"nazwa": konfig.nazwa})
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
    loguj(konfig, f"Zadanie #{odp_master['zadanie_id']}: operacja {odp_master['operacja']!r} "
                  f"na {odp_master['liczby']} = {wynik}   (dotąd OK: {postep.dobre}, błędy: {postep.zle})",
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

    if "przerwa_domyslna" in odp:
        nowa = float(odp["przerwa_domyslna"])
        if nowa != postep.opoznienie:
            loguj(konfig, f"→ Master: nowa domyślna przerwa {postep.opoznienie:g}s → {nowa:g}s",
                  wazne=False)
            postep.opoznienie = nowa

    werdykt = odp.get("poprzednie")
    zastosuj_werdykt(postep, werdykt)
    if werdykt == "nieaktualne":
        loguj(konfig, "(odpowiedź nieaktualna - dostaję zadanie od nowa)", wazne=False)

    wykonaj_polecenia(konfig, postep, odp.get("polecenia") or [])
    if postep.stop:                              # polecenie 'stop': ostatni pakiet tylko z potwierdzeniem
        zapytaj(lacze, stan["id"], token, zbuduj_pakiet_pracy(postep, bez_odpowiedzi=True))
        return 0

    if "operacja" in odp:
        try:
            wynik = rozwiaz(odp["operacja"], odp["liczby"])
        except (KeyError, ValueError, ZeroDivisionError) as e:
            loguj(konfig, "Nie umiem tego policzyć:", repr(e))
            return 1
        for hak in HAKI_PO_ZADANIU:
            hak(konfig, postep, odp, wynik)
        stop.wait(postep.opoznienie)
        postep.odpowiedz, postep.zadanie_id = wynik, odp["zadanie_id"]
    else:                                        # Master nie wydał zadania; czekaj na kolejne polecenia
        postep.odpowiedz = postep.zadanie_id = None
        stop.wait(random.uniform(MIN_ODPYTYWANIE_S, MAX_ODPYTYWANIE_S))
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
    raise SystemExit(uruchom(parsuj_argumenty()))


if __name__ == "__main__":
    main()