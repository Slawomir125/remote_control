"""Master v7 - serwer poleceń Slaves HTTP/HTTPS + baza wyników + panel "Master Center".

Kod to małe funkcje: logika (czyste funkcje) oddzielona od stanu (dataclassy) i od
wejścia/wyjścia (sieć, baza, konsola). Punkty rozbudowy to REJESTRY wypełniane dekoratorami:

    OPERACJE.add("%")                   nowy symbol działania (dodaj obliczanie też w slave.py)
    @typ_pakietu("telemetria")          nowy typ wiadomości od Slaves
  @komenda("nazwa", "opis")           nowa komenda w konsoli Master
  @endpoint("/api/cos")               nowy adres w panelu (HTTP); funkcja (stan, zapytanie);
                                      opcje: metoda="POST", chroniony=True (wymaga tokenu panelu)
  POLECENIA_SLAVES["nazwa"] = ...  nowe polecenie dla Slaves (+ @polecenie w slave.py)

Slaves łączą się przez POST JSON do /api/slave; TLS kończy Cloudflare Tunnel albo reverse proxy.
Rejestracja używa tokenu bootstrapowego, a po niej każdy Slave ma osobny token bearer.

Skala: połączenia Slaves obsługuje asyncio (jeden wątek, dziesiątki tysięcy połączeń),
zapis do bazy idzie w osobnym wątku. Limit to deskryptory plików (ulimit -n) i CPU.

Panel (Master Center) ma sterowanie: START/STOP Master, polecenia dla wszystkich i dla jednego Slaves.
API odczytu i sterowania wymaga tokenu (nagłówek X-Token) - Master wypisuje go przy starcie i trzyma
w master_panel_token.txt (albo zmienna MASTER_PANEL_TOKEN). Panel nasłuchuje lokalnie; zdalne połączenie
prowadź przez tunel SSH, aby token i dane nie leciały jawnym HTTP po sieci.

Uruchom:  python master.py     (konsola: wpisz 'pomoc'; panel: http://127.0.0.1:8080)
"""
import asyncio
import heapq
import hmac
import json
import os
import queue
import secrets
import shlex
import sqlite3
import threading
import time
import urllib.parse
from collections import Counter, deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

# ================================================================ KONFIGURACJA ==

KATALOG = Path(__file__).parent
KATALOG_DANYCH = Path(os.environ.get("MASTER_DATA_DIR", str(KATALOG)))
HOST = os.environ.get("MASTER_HOST", "127.0.0.1")
PORT = int(os.environ.get("MASTER_PORT", "9000"))
PANEL_HOST = os.environ.get("MASTER_PANEL_HOST", "127.0.0.1")
PANEL_PORT = int(os.environ.get("MASTER_PANEL_PORT", "8080"))
PLIK_BAZY = KATALOG_DANYCH / "master.db"
PLIK_TOKEN_REJESTRACJI = KATALOG_DANYCH / "master_registration_token.txt"
PLIK_PANELU = KATALOG / "master_center.html"
PLIK_TOKENU = KATALOG_DANYCH / "master_panel_token.txt"
MACRO_DIR = KATALOG / "macros"
STARY_STAN = KATALOG_DANYCH / "master_stan.json"
LEGACY_STARY_STAN = KATALOG_DANYCH / "gru_stan.json"
AKTYWNY_DO_S = 15                            # Slave bez kontaktu dłużej = nieaktywny
CISZA_ROZLACZ_S = 60                         # timeout żądania HTTP Slaves
MAX_LINIA = 4 * 1024 * 1024                  # maksymalny rozmiar treści HTTP (4 MiB)
BACKLOG = 4096                               # kolejka połączeń oczekujących na accept()
POLECENIE_TTL_S = 300                        # nieodebrane polecenie wygasa po tylu sekundach
MAX_POLECEN = 200                            # ile ostatnich poleceń pamięta Master (do komendy 'polecenia')
LOG_ZADAN_NA_S = 20                          # limit linii "zadanie rozwiązane" w konsoli na sekundę
RAPORT_CO_S = 2                              # co ile s Master zbiera rejestracje/odrzucenia w jedną linię


# ===================================================================== REJESTRY ==
# Tu lądują funkcje oznaczone dekoratorami. Reszta kodu tylko czyta te rejestry.

OPERACJE = {"+", "-", "*", "//"}
TYPY_PAKIETOW = {}   # typ -> (funkcja(stan, slave_id, dane) -> dict | None, anonimowy)
KOMENDY = {}         # nazwa/alias -> funkcja(stan, arg) -> str | None | KONIEC
OPISY_KOMEND = []    # (nazwa, opis) - do komendy 'pomoc'
ENDPOINTY = []       # (metoda, sciezka, prefix, chroniony, funkcja(stan, zapytanie) -> (kod, typ, bajty))
KONIEC = object()    # zwróć z komendy, żeby zakończyć Master

# Polecenia, które Master potrafi wydać Slaves (nazwa -> opis). Wykonuje je slave.py (@polecenie).
POLECENIA_SLAVES = {
    "exec": "<program> [flagi...] - uruchamia program bez shella",
    "makro": "sekwencja z pliku .sh w katalogu macros/ (bez shella)",
    "pauza": "wstrzymuje pobieranie zadań",
    "wznow": "wznawia pracę po pauzie",
    "status": "Slave odsyła swoje liczniki",
    "stop": "Slave kończy pracę",
}
PANEL_ADMIN_COMMANDS = {"pauza", "wznow", "stop"}


def typ_pakietu(nazwa, anonimowy=False):
    """anonimowy=True: pakiet idzie z ID "0" i tokenem bootstrapowym (np. rejestracja)."""
    def dekorator(f):
        TYPY_PAKIETOW[nazwa] = (f, anonimowy)
        return f
    return dekorator


def komenda(nazwa, opis, aliasy=()):
    def dekorator(f):
        OPISY_KOMEND.append((nazwa, opis))
        for n in (nazwa, *aliasy):
            KOMENDY[n] = f
        return f
    return dekorator


def endpoint(sciezka, prefix=False, metoda="GET", chroniony=False):
    """chroniony=True: wymaga tokenu panelu w nagłówku X-Token (dla zapisów/sterowania)."""
    def dekorator(f):
        ENDPOINTY.append((metoda, sciezka, prefix, chroniony, f))
        return f
    return dekorator


# =============================================================== ZADANIA (CZYSTE) ==
# Czyste funkcje: bez stanu, bez I/O - łatwe do testowania i zabawy.

def rowne(a, b):
    if type(a) is int and type(b) is int:
        return a == b
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return False


def ocen_odpowiedz(zadanie, dane, teraz):
    """Porównuje odpowiedź Slaves z zadaniem w toku.
    Zwraca (werdykt, szczegoly): werdykt = None (to prośba o pierwsze zadanie),
    "ok", "zle" albo "nieaktualne" (odpowiedź na inne zadanie niż to w toku)."""
    odp = dane.get("odpowiedz")
    if odp is None:
        return None, {}
    if not zadanie or dane.get("zadanie_id") != zadanie["id"]:
        return "nieaktualne", {}
        ok = rowne(odp, zadanie["wynik"])
    return ("ok" if ok else "zle"), {"ok": ok, "czas": teraz - zadanie["t0"], "odp_txt": str(odp)[:30]}


# ====================================================== OKNA CZASOWE (CZYSTE) ==
# Liczenie "ile w ostatniej minucie" bez trzymania całej historii w pamięci.

def przytnij_okno(timestampy, teraz, okno=60):
    """Usuwa z początku kolejki znaczniki starsze niż `okno` s (kolejka jest rosnąca)."""
    while timestampy and teraz - timestampy[0] > okno:
        timestampy.popleft()


def ile_w_oknie(timestampy, teraz, okno=60):
    """Ile znaczników mieści się w ostatnich `okno` s. Idziemy od końca i kończymy po pierwszym zbyt starym."""
    n = 0
    for t in reversed(timestampy):
        if teraz - t > okno:
            break
        n += 1
    return n


def zlicz_sekunde(sekundy, teraz, zachowaj=120):
    """Globalny licznik tempa: kolejka [sekunda, ile]. Pamięć stała niezależnie od tempa."""
    s = int(teraz)
    if sekundy and sekundy[-1][0] == s:
        sekundy[-1][1] += 1
    else:
        sekundy.append([s, 1])
    while sekundy and sekundy[0][0] < s - zachowaj:
        sekundy.popleft()


def seria_tempa(sekundy, teraz, okno=60):
    """Ile zadań ukończono w każdej z ostatnich `okno` sekund (najstarsza pierwsza)."""
    seria, s = [0] * okno, int(teraz)
    for sek, ile in reversed(sekundy):
        wiek = s - sek
        if wiek >= okno:
            break
        if wiek >= 0:
            seria[okno - 1 - wiek] += ile
    return seria


# ==================================================================== TOKENY ==

def wczytaj_token_rejestracji():
    env = os.environ.get("MASTER_REGISTRATION_TOKEN")
    if env:
        return env.strip()
    if PLIK_TOKEN_REJESTRACJI.exists():
        return PLIK_TOKEN_REJESTRACJI.read_text(encoding="utf-8").strip()
    token = secrets.token_urlsafe(32)
    PLIK_TOKEN_REJESTRACJI.write_text(token, encoding="utf-8")
    try:
        os.chmod(PLIK_TOKEN_REJESTRACJI, 0o600)
    except OSError:
        pass
    return token


# ======================================================================== STAN ==
# Dane bez logiki. Funkcje poniżej dostają `stan` jako pierwszy argument.

@dataclass
class Polecenie:
    id: int
    nazwa: str
    args: list
    ts: float
    cel: str                       # "wszystkie" albo ID Slaves
    adresaci: int = 0
    wykonane: int = 0              # ile Slaves potwierdziło
    bledy: int = 0                 # ile z nich zgłosiło błąd
    przyklad: str = ""             # wynik z pierwszego potwierdzenia


@dataclass
class Slave:
    nazwa: str
    token: str                                   # token bearer Slaves
    dobre: int = 0
    zle: int = 0
    suma_czasu: float = 0.0
    ostatnio: Optional[float] = None
    zadanie: Optional[dict] = None               # zadanie w toku
    pauza: bool = True
    polecenia: list = field(default_factory=list)   # polecenia czekające na potwierdzenie
    rozgl_ack: int = 0                           # ostatnie potwierdzone polecenie "do wszystkich"
    ts: deque = field(default_factory=deque)     # czasy ukończeń z ostatniej minuty


@dataclass
class Stan:
    db: sqlite3.Connection
    token_rejestracji: str
    slaves: dict = field(default_factory=dict)  # id -> Slave
    pracuje: bool = False
    log_wlaczony: bool = True
    licznik_zadan: int = 0
    sesja_zadan: int = 0
    polaczenia: int = 0
    dobre_razem: int = 0
    zle_razem: int = 0
    suma_czasu_razem: float = 0.0
    seq: int = 0                                 # numer ostatniego zdarzenia w feedzie
    start_ts: float = field(default_factory=time.time)
    feed: deque = field(default_factory=lambda: deque(maxlen=500))
    tempo_sekundy: deque = field(default_factory=deque)           # [sekunda, ile] - wykres tempa
    kolejka: queue.Queue = field(default_factory=queue.Queue)     # wyniki czekające na zapis
    lock: threading.RLock = field(default_factory=threading.RLock)
    db_lock: threading.Lock = field(default_factory=threading.Lock)
    licznik_polecen: int = 0
    polecenia: dict = field(default_factory=dict)                 # id -> Polecenie (ostatnie MAX_POLECEN)
    opozn_domyslne: float = 1.0                                   # domyślna przerwa Slaves między zadaniami
    rozgloszenia: deque = field(default_factory=lambda: deque(maxlen=MAX_POLECEN))
    ostatnie_rozgl_id: int = 0
    nowi: list = field(default_factory=list)                      # rejestracje od ostatniego raportu
    odrzucone: Counter = field(default_factory=Counter)           # powód -> ile pakietów
    log_sekunda: int = 0
    log_licznik: int = 0
    log_pominiete: int = 0
    token_panelu: str = ""                        # chroni sterowanie z panelu
    jednorazowe: dict = field(default_factory=dict)  # token -> (wygasa_ts, etykieta)


def log(stan, tekst, zawsze=False):
    if zawsze or stan.log_wlaczony:
        print(f"[{time.strftime('%H:%M:%S')}] {tekst}", flush=True)


def log_zadania(stan, linia):
    """Linia 'zadanie rozwiązane' z limitem na sekundę - przy tysiącach Slaves nie zalewa konsoli."""
    if not stan.log_wlaczony:
        return
    teraz = int(time.time())
    if stan.log_sekunda != teraz:
        if stan.log_pominiete:
            print(f"[{time.strftime('%H:%M:%S')}] ... pominięto {stan.log_pominiete} linii "
                  f"(limit {LOG_ZADAN_NA_S}/s; komenda 'log' wyłącza podgląd)", flush=True)
        stan.log_sekunda, stan.log_licznik, stan.log_pominiete = teraz, 0, 0
    if stan.log_licznik < LOG_ZADAN_NA_S:
        stan.log_licznik += 1
        print(f"[{time.strftime('%H:%M:%S')}] {linia}", flush=True)
    else:
        stan.log_pominiete += 1


# ======================================================================== BAZA ==

SCHEMAT = """
CREATE TABLE IF NOT EXISTS slaves(
    id TEXT PRIMARY KEY, nazwa TEXT NOT NULL, token TEXT NOT NULL, zarejestrowany REAL NOT NULL);
CREATE TABLE IF NOT EXISTS zadania(
    nr INTEGER PRIMARY KEY AUTOINCREMENT, zadanie_id INTEGER, slave_id TEXT NOT NULL,
    ts REAL NOT NULL, dzialanie TEXT, poprawna TEXT, odpowiedz TEXT,
    ok INTEGER NOT NULL, czas REAL NOT NULL, operacja TEXT, liczby TEXT);
CREATE INDEX IF NOT EXISTS ix_zad_slave ON zadania(slave_id, ts);
CREATE INDEX IF NOT EXISTS ix_zad_ts ON zadania(ts);
"""
INSERT_ZADANIE = ("INSERT INTO zadania(zadanie_id, slave_id, ts, dzialanie, poprawna, odpowiedz,"
                  " ok, czas, operacja, liczby) VALUES (?,?,?,?,?,?,?,?,?,?)")


def db_polacz(zapis=True):
    c = sqlite3.connect(PLIK_BAZY, timeout=10, check_same_thread=False)
    if zapis:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
    return c


def migracja_nazw_plikow():
    KATALOG_DANYCH.mkdir(parents=True, exist_ok=True)
    legacy_db = KATALOG_DANYCH / "gru.db"
    if legacy_db.exists() and not PLIK_BAZY.exists():
        for suffix in ("", "-wal", "-shm"):
            source = Path(str(legacy_db) + suffix)
            if source.exists():
                os.replace(source, Path(str(PLIK_BAZY) + suffix))
    for old_name, new_path in (
        ("minion_registration_token.txt", PLIK_TOKEN_REJESTRACJI),
        ("panel_token.txt", PLIK_TOKENU),
        ("gru_stan.json", STARY_STAN),
    ):
        old_path = KATALOG_DANYCH / old_name
        if old_path.exists() and not new_path.exists():
            os.replace(old_path, new_path)


def migruj_nazwy_schematu(db):
    """Rename legacy database tables and columns without dropping stored records."""
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "miniony" in tables:
        if "slaves" not in tables:
            db.execute("ALTER TABLE miniony RENAME TO slaves")
        else:
            old_columns = {row[1] for row in db.execute("PRAGMA table_info(miniony)")}
            new_columns = {row[1] for row in db.execute("PRAGMA table_info(slaves)")}
            old_token = "token" if "token" in old_columns else "klucz"
            new_token = "token" if "token" in new_columns else "klucz"
            db.execute(
                f"INSERT OR IGNORE INTO slaves(id, nazwa, {new_token}, zarejestrowany) "
                f"SELECT id, nazwa, {old_token}, zarejestrowany FROM miniony"
            )
            db.execute("DROP TABLE miniony")

    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "slaves" in tables:
        columns = {row[1] for row in db.execute("PRAGMA table_info(slaves)")}
        if "klucz" in columns and "token" not in columns:
            db.execute("ALTER TABLE slaves RENAME COLUMN klucz TO token")
    if "zadania" in tables:
        columns = {row[1] for row in db.execute("PRAGMA table_info(zadania)")}
        if "minion_id" in columns and "slave_id" not in columns:
            db.execute("ALTER TABLE zadania RENAME COLUMN minion_id TO slave_id")
        elif "minion_id" in columns and "slave_id" in columns:
            db.execute("UPDATE zadania SET slave_id = minion_id WHERE slave_id IS NULL")
        db.execute("DROP INDEX IF EXISTS ix_zad_minion")
    db.commit()


def migruj_schemat(db):
    """Baza z v3 nie ma kolumn operacja/liczby - dodajemy je, stare wiersze zostają."""
    kolumny = {w[1] for w in db.execute("PRAGMA table_info(zadania)")}
    for nazwa in ("operacja", "liczby"):
        if nazwa not in kolumny:
            db.execute(f"ALTER TABLE zadania ADD COLUMN {nazwa} TEXT")
    db.commit()


def importuj_stary_stan(db):
    """v2 trzymała Slaves w master_stan.json - przenosimy je do bazy (raz)."""
    legacy_state = STARY_STAN if STARY_STAN.exists() else LEGACY_STARY_STAN
    if legacy_state.exists() and not db.execute("SELECT 1 FROM slaves LIMIT 1").fetchone():
        for slave_id, record in json.loads(legacy_state.read_text()).items():
            token = record.get("token", record.get("klucz"))
            db.execute("INSERT INTO slaves VALUES (?,?,?,?)", (slave_id, record["nazwa"], token, time.time()))
        db.commit()
        legacy_state.rename(legacy_state.with_name(legacy_state.name + ".migrated"))
        print(f"[Master] Zaimportowano Slave state z {legacy_state.name} do bazy.")


def inicjuj_baze(db):
    migruj_nazwy_schematu(db)
    db.executescript(SCHEMAT)
    migruj_schemat(db)
    importuj_stary_stan(db)


def wczytaj_slaves(db):
    """Slaves z bazy razem z historycznymi licznikami (statystyki przeżywają restart)."""
    slaves = {slave_id: Slave(nazwa, token) for slave_id, nazwa, token in db.execute(
        "SELECT id, nazwa, token FROM slaves ORDER BY CAST(id AS INTEGER)")}
    for slave_id, dobre, zle, suma in db.execute(
            "SELECT slave_id, SUM(ok), SUM(1 - ok), SUM(czas) FROM zadania GROUP BY slave_id"):
        if slave_id in slaves:
            slave = slaves[slave_id]
            slave.dobre, slave.zle, slave.suma_czasu = dobre, zle, suma
    return slaves


def utworz_stan():
    migracja_nazw_plikow()
    db = db_polacz()
    inicjuj_baze(db)
    stan = Stan(db=db, token_rejestracji=wczytaj_token_rejestracji())
    stan.slaves = wczytaj_slaves(db)
    stan.dobre_razem = sum(slave.dobre for slave in stan.slaves.values())
    stan.zle_razem = sum(slave.zle for slave in stan.slaves.values())
    stan.suma_czasu_razem = sum(slave.suma_czasu for slave in stan.slaves.values())
    stan.licznik_zadan = db.execute("SELECT COALESCE(MAX(zadanie_id), 0) FROM zadania").fetchone()[0]
    stan.token_panelu = wczytaj_token_panelu()
    return stan


def pisarz(stan):
    """Wątek zapisu: wyniki z kolejki trafiają do bazy paczkami (max 0,5 s / 200 wierszy),
    więc praca Slaves nie czeka na dysk. None w kolejce = koniec."""
    while True:
        paczka = [stan.kolejka.get()]
        koniec = paczka[0] is None
        limit = time.monotonic() + 0.5
        while not koniec and len(paczka) < 200:
            pozostalo = limit - time.monotonic()
            if pozostalo <= 0:
                break
            try:
                w = stan.kolejka.get(timeout=pozostalo)
            except queue.Empty:
                break
            paczka.append(w)
            koniec = w is None
        wiersze = [w for w in paczka if w is not None]
        if wiersze:
            with stan.db_lock:
                stan.db.executemany(INSERT_ZADANIE, wiersze)
                stan.db.commit()
        if koniec:
            return


def start_pisarza(stan):
    watek = threading.Thread(target=pisarz, args=(stan,), daemon=True)
    watek.start()
    return watek


def zamknij_pisarza(stan, watek):
    stan.kolejka.put(None)
    watek.join(timeout=5)


def historia_slaves(stan, slave_id):
    """Statystyki z bazy + ostatnie 50 zadań jednego Slaves (okno szczegółów w panelu)."""
    slave = stan.slaves.get(slave_id)
    if not slave:
        return None
    c = db_polacz(zapis=False)
    try:
        agg = c.execute(
            "SELECT COUNT(*), COALESCE(SUM(ok), 0), AVG(czas), MIN(czas), MAX(czas), MIN(ts), MAX(ts)"
            " FROM zadania WHERE slave_id=?", (slave_id,)).fetchone()
        wiersze = c.execute(
            "SELECT ts, dzialanie, poprawna, odpowiedz, ok, czas FROM zadania"
            " WHERE slave_id=? ORDER BY nr DESC LIMIT 50", (slave_id,)).fetchall()
        rej = c.execute("SELECT zarejestrowany FROM slaves WHERE id=?", (slave_id,)).fetchone()[0]
    finally:
        c.close()
    return {
        "id": slave_id, "nazwa": slave.nazwa, "zarejestrowany": rej,
        "razem": agg[0], "dobre": agg[1], "sr_czas": agg[2], "min_czas": agg[3],
        "max_czas": agg[4], "pierwsze": agg[5], "ostatnie": agg[6],
        "zadania": [{"ts": r[0], "dzialanie": r[1], "poprawna": r[2], "odpowiedz": r[3],
                     "ok": bool(r[4]), "czas": r[5]} for r in wiersze],
    }


def info_bazy(stan):
    with stan.db_lock:
        z = stan.db.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM zadania").fetchone()
        n = stan.db.execute("SELECT COUNT(*) FROM slaves").fetchone()[0]
    rozmiar = PLIK_BAZY.stat().st_size / 1024 if PLIK_BAZY.exists() else 0
    fmt = lambda t: time.strftime("%Y-%slave-%d %H:%M:%S", time.localtime(t)) if t else "-"
    return (f"Baza {PLIK_BAZY.name}: {rozmiar:.0f} KB | Slaves: {n} | "
            f"zadań zapisanych: {z[0]} | od {fmt(z[1])} do {fmt(z[2])}")


# ============================================================== LOGIKA MASTER ==

def dodaj_zdarzenie(stan, typ, **dane):
    """Wpis do live feedu (panel czyta go przez /api/stan)."""
    with stan.lock:
        stan.seq += 1
        stan.feed.append({"seq": stan.seq, "ts": time.time(), "typ": typ, **dane})


def zarejestruj(stan, nazwa):
    with stan.lock:
        for slave_id, slave in stan.slaves.items():
            if slave.nazwa == nazwa:
                return slave_id, slave.token
        slave_id = str(len(stan.slaves) + 1)
        token = secrets.token_urlsafe(32)
        slave = stan.slaves[slave_id] = Slave(nazwa, token)
        slave.rozgl_ack = stan.ostatnie_rozgl_id        # nowy Slave nie dostaje starych poleceń "do wszystkich"
        stan.nowi.append((slave_id, nazwa))
        with stan.db_lock:
            stan.db.execute("INSERT INTO slaves VALUES (?,?,?,?)", (slave_id, nazwa, token, time.time()))
            stan.db.commit()
    return slave_id, token


def odrzuc(stan, powod):
    """Liczy odrzucony pakiet; raport zbiorczy wypisuje raporter() - bez zalewania konsoli."""
    with stan.lock:
        stan.odrzucone[powod] += 1


TOKEN_JEDNORAZOWY_WAZNY_S = 600                  # jednorazowy kod startowy wygasa po tylu sekundach


def wygeneruj_token_jednorazowy(stan, etykieta="", wazny_s=TOKEN_JEDNORAZOWY_WAZNY_S):
    """Tworzy token rejestracyjny do jednorazowego użytku (wygasa czasem albo po zużyciu)."""
    teraz = time.time()
    token = secrets.token_urlsafe(24)
    with stan.lock:
        # przy okazji sprząta stare/wygasłe, żeby słownik nie rósł bez końca
        for stary, (wygasa, _) in list(stan.jednorazowe.items()):
            if wygasa <= teraz:
                del stan.jednorazowe[stary]
        stan.jednorazowe[token] = (teraz + wazny_s, etykieta)
    dodaj_zdarzenie(stan, "sys", tekst=f"Wygenerowano jednorazowy kod startowy"
                     + (f" dla '{etykieta}'" if etykieta else ""))
    return token, teraz + wazny_s


def token_jednorazowy_zuzyj(stan, token):
    """Sprawdza jednorazowy token i od razu go usuwa (zużywa się przy pierwszym poprawnym użyciu)."""
    teraz = time.time()
    with stan.lock:
        wpis = stan.jednorazowe.pop(token, None)
    if wpis is None:
        return False
    wygasa, _etykieta = wpis
    return wygasa > teraz


def uruchom_master(stan):
    """Master zaczyna odpowiadać Slaves (wspólne dla konsoli i panelu)."""
    stan.pracuje = True
    dodaj_zdarzenie(stan, "sys", tekst="START - Master słucha Slaves")
    log(stan, "START - Master słucha Slaves", True)


def wstrzymaj_master(stan):
    """Master znów ignoruje wszystkich (Slaves czekają i ponawiają)."""
    stan.pracuje = False
    dodaj_zdarzenie(stan, "sys", tekst="STOP - Master ignoruje Slaves")
    log(stan, "STOP - Master ignoruje Slaves", True)


def zapisz_wynik(stan, slave_id, slave, szczegoly, teraz):
    """Pod blokadą: liczniki, kolejka do bazy, feed. Zwraca linię do logu konsoli."""
    z = slave.zadanie
    ok, czas, odp_txt = szczegoly["ok"], szczegoly["czas"], szczegoly["odp_txt"]
    if ok:
        slave.dobre += 1
        stan.dobre_razem += 1
    else:
        slave.zle += 1
        stan.zle_razem += 1
    slave.suma_czasu += czas
    stan.suma_czasu_razem += czas
    slave.ts.append(teraz)
    przytnij_okno(slave.ts, teraz)
    zlicz_sekunde(stan.tempo_sekundy, teraz)
    stan.sesja_zadan += 1
    poprawna = str(z["wynik"])
    stan.kolejka.put((z["id"], slave_id, teraz, z["tekst"], poprawna, odp_txt,
                      1 if ok else 0, czas, z["operacja"], json.dumps(z["liczby"])))
    dodaj_zdarzenie(stan, "zad", id=slave_id, nazwa=slave.nazwa, tekst=z["tekst"], odp=odp_txt,
                    poprawna=poprawna, ok=ok, czas=czas)
    slave.zadanie = None
    if ok:
        ocena = "OK"
    else:
        ocena = f"ZLE (poprawna: {z['wynik']})"
    return f"#{slave_id:<3}{slave.nazwa:<12} {z['tekst']:<24} = {odp_txt:<8} {ocena:<22} {czas:.2f}s"


# ---- polecenia dla Slaves ------------------------------------------------

CELE_WSZYSTKIE = ("wszystkie", "wszyscy", "wszystkim", "all", "*")


def znajdz_slaves(stan, cel):
    """ID albo nazwa Slaves -> ID. Wołać pod stan.lock. Rzuca ValueError (brak / nazwa niejednoznaczna)."""
    cel = cel.strip().lstrip("#")
    if cel in stan.slaves:
        return cel
    trafione = [slave_id for slave_id, slave in stan.slaves.items() if slave.nazwa == cel]
    if len(trafione) == 1:
        return trafione[0]
    if trafione:
        raise ValueError(f"nazwa '{cel}' jest niejednoznaczna (ID: {', '.join(trafione[:5])}...) - podaj ID")
    raise ValueError(f"nie ma Slaves '{cel}'")


def wydaj_polecenie(stan, cel, nazwa, args):
    """Kolejkuje polecenie dla jednego Slaves (cel = ID lub nazwa) albo wszystkich (cel = "wszystkie").
    Rzuca ValueError z czytelnym komunikatem."""
    nazwa = nazwa.strip().lower()
    if nazwa not in POLECENIA_SLAVES:
        raise ValueError(f"nieznane polecenie '{nazwa}'. Znane: {', '.join(POLECENIA_SLAVES)}")
    if not isinstance(args, list) or (nazwa != "makro" and (len(args) > 50 or any(len(str(a)) > 200 for a in args))):
        raise ValueError("Argumenty polecenia są za długie albo jest ich za dużo.")
    args = [str(a) for a in args]
    with stan.lock:
        cel = "wszystkie" if cel.strip().lower() in CELE_WSZYSTKIE else znajdz_slaves(stan, cel)
        stan.licznik_polecen += 1
        p = Polecenie(stan.licznik_polecen, nazwa, args, time.time(), cel)
        if cel == "wszystkie":
            p.adresaci = len(stan.slaves)
            stan.rozgloszenia.append(p)              # jedna kopia dla wszystkich, niezależnie od liczby Slaves
            stan.ostatnie_rozgl_id = p.id
        else:
            p.adresaci = 1
            stan.slaves[cel].polecenia.append(p)
        stan.polecenia[p.id] = p
        while len(stan.polecenia) > MAX_POLECEN:
            stan.polecenia.pop(next(iter(stan.polecenia)))
    tresc = f"{nazwa} {' '.join(args)}".strip()
    adresat = "do wszystkich" if cel == "wszystkie" else f"do #{cel}"
    dodaj_zdarzenie(stan, "sys", tekst=f"> polecenie #{p.id} '{tresc}' {adresat}")
    return p


def zbierz_polecenia(stan, slave, teraz):
    """Polecenia do wysłania Slavesi w tej odpowiedzi (ścieżka zwykła jest O(1))."""
    wyslij = []
    if slave.polecenia:
        slave.polecenia = [p for p in slave.polecenia if teraz - p.ts <= POLECENIE_TTL_S]
        wyslij.extend(slave.polecenia)
    if slave.rozgl_ack < stan.ostatnie_rozgl_id:
        swieze = [p for p in stan.rozgloszenia if p.id > slave.rozgl_ack and teraz - p.ts <= POLECENIE_TTL_S]
        if swieze:
            wyslij.extend(swieze)
        else:
            slave.rozgl_ack = stan.ostatnie_rozgl_id     # nic do odebrania (wygasłe) - nie skanuj ponownie
    wyslij.sort(key=lambda p: p.id)
    return [{"id": p.id, "polecenie": p.nazwa, "args": p.args, "rozgl": p.cel == "wszystkie"}
            for p in wyslij[:20]]


def przyjmij_potwierdzenia(stan, slave_id, slave, potwierdzenia):
    """Slave odsyła wyniki poleceń. Powtórzone potwierdzenia (ponowiony pakiet) są ignorowane."""
    if not isinstance(potwierdzenia, list):
        return
    for p in potwierdzenia[:50]:
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        # ── pętla: Slave sam zapętlił polecenie; wyniki wracają jako "petla:N" ──
        pol = stan.polecenia.get(pid)
        if pol is None:
            continue
        if p.get("rozgl"):
            if pol.cel != "wszystkie" or pol.id <= slave.rozgl_ack:
                continue
            slave.rozgl_ack = pol.id
        else:
            if not any(x is pol for x in slave.polecenia):
                continue
            slave.polecenia = [x for x in slave.polecenia if x is not pol]
        ok, wynik = bool(p.get("ok")), str(p.get("wynik", ""))
        if pol.nazwa != "makro":
            wynik = wynik[:1000]
        pierwsze_potwierdzenie = pol.wykonane == 0
        pol.wykonane += 1
        pol.bledy += 0 if ok else 1
        pol.przyklad = pol.przyklad or wynik[:200]
        if pol.cel != "wszystkie" or pierwsze_potwierdzenie or pol.nazwa in {"exec", "makro"}:
            tekst = f"< #{slave_id} {slave.nazwa}: {pol.nazwa} -> {wynik}" + ("" if ok else "  [BŁĄD]")
            dodaj_zdarzenie(stan, "sys", tekst=tekst, command_id=pol.id,
                            slave_id=slave_id, slave_nazwa=slave.nazwa, wynik=wynik, ok=ok)
            wpis_logu = (f"< #{slave_id} {slave.nazwa}: makro -> wynik {len(wynik)} znaków"
                         if pol.nazwa == "makro" else tekst)
            log(stan, wpis_logu, True)


def przetworz_prace(stan, slave_id, dane):
    """Jeden pakiet 'praca': potwierdzenia poleceń i odpytywanie o kolejne polecenia."""
    linia_logu = None
    with stan.lock:
        slave = stan.slaves[slave_id]
        teraz = time.time()
        slave.ostatnio = teraz
        slave.pauza = bool(dane.get("pauza"))
        przyjmij_potwierdzenia(stan, slave_id, slave, dane.get("potwierdzenia"))
        werdykt, szczegoly = ocen_odpowiedz(slave.zadanie, dane, teraz)
        if werdykt in ("ok", "zle"):
            linia_logu = zapisz_wynik(stan, slave_id, slave, szczegoly, teraz)
        odpowiedz = {"poprzednie": werdykt}
        odpowiedz["przerwa_domyslna"] = stan.opozn_domyslne
        polecenia = zbierz_polecenia(stan, slave, teraz)
        if polecenia:
            odpowiedz["polecenia"] = polecenia
    if linia_logu:
        log_zadania(stan, linia_logu)                # poza blokadą
    return odpowiedz


# ============================================================ WIDOKI (CZYSTE) ==

def widok_slaves(slave_id, slave, teraz):
    n = slave.dobre + slave.zle
    return {
        "id": slave_id, "nazwa": slave.nazwa, "dobre": slave.dobre, "zle": slave.zle,
        "sr_czas": (slave.suma_czasu / n) if n else None,
        "na_min": ile_w_oknie(slave.ts, teraz),
        "ostatnio": (teraz - slave.ostatnio) if slave.ostatnio else None,
        "zadanie": slave.zadanie["tekst"] if slave.zadanie else None,
        "pauza": slave.pauza,
    }


def top_slaves(stan, ile, teraz):
    """`ile` najbardziej pracowitych Slaves (przy tysiącach nie wysyłamy wszystkich)."""
    najlepsze = heapq.nlargest(ile, stan.slaves.items(), key=lambda kv: kv[1].dobre + kv[1].zle)
    return [widok_slaves(slave_id, slave, teraz) for slave_id, slave in najlepsze]


def widok_polecenia(p, teraz):
    return {"id": p.id, "polecenie": f"{p.nazwa} {' '.join(p.args)}".strip(), "cel": p.cel,
            "adresaci": p.adresaci, "wykonane": p.wykonane, "bledy": p.bledy,
            "przyklad": p.przyklad, "wiek": teraz - p.ts}


def snapshot(stan, since):
    """Wszystko, czego panel potrzebuje w jednym zapytaniu."""
    with stan.lock:
        teraz = time.time()
        seria = seria_tempa(stan.tempo_sekundy, teraz)
        n = stan.dobre_razem + stan.zle_razem
        aktywne = sum(1 for slave in stan.slaves.values() if slave.ostatnio is not None and teraz - slave.ostatnio < AKTYWNY_DO_S)
        return {
            "pracuje": stan.pracuje, "uptime": teraz - stan.start_ts,
            "slaves": len(stan.slaves), "polaczenia": stan.polaczenia, "aktywne": aktywne,
            "zadan_razem": n, "sesja": stan.sesja_zadan, "dobre": stan.dobre_razem, "zle": stan.zle_razem,
            "sr_czas": (stan.suma_czasu_razem / n) if n else None,
            "tempo": sum(seria[-10:]) / 10, "seria": seria,
            "polecenia": [widok_polecenia(p, teraz) for p in list(stan.polecenia.values())[-10:][::-1]],
            "feed": [e for e in stan.feed if e["seq"] > since][-100:], "seq": stan.seq,
        }


def tabela_slaves(stan, ile=20):
    with stan.lock:
        widoki = top_slaves(stan, ile, time.time())
        razem = len(stan.slaves)
    if not widoki:
        return "Brak zarejestrowanych Slaves."
    linie = [f"{'ID':>5}  {'Nazwa':<12} {'OK':>6} {'Błędy':>6} {'Śr.czas':>8} {'Zad/min':>8} {'Ostatnio':>10}  Zadanie w toku"]
    for w in widoki:
        sr = f"{w['sr_czas']:.2f}s" if w["sr_czas"] is not None else "-"
        ost = f"{w['ostatnio']:.0f}s temu" if w["ostatnio"] is not None else "-"
        linie.append(f"{w['id']:>5}  {w['nazwa']:<12} {w['dobre']:>6} {w['zle']:>6} {sr:>8} "
                     f"{w['na_min']:>8} {ost:>10}  {'PAUZA' if w['pauza'] else (w['zadanie'] or '-')}")
    if razem > len(widoki):
        linie.append(f"... pokazano {len(widoki)} najaktywniejszych z {razem} ('slaves wszystkie' pokaże resztę)")
    return "\n".join(linie)


def tabela_polecen(stan, ile=10):
    with stan.lock:
        teraz = time.time()
        widoki = [widok_polecenia(p, teraz) for p in list(stan.polecenia.values())[-ile:]]
    if not widoki:
        return "Brak poleceń. Użyj: polec <id|wszystkie> <polecenie> [arg...]"
    linie = [f"{'#':>4} {'Polecenie':<28} {'Cel':<10} {'Adresaci':>8} {'Potw.':>7} {'Błędy':>6} {'Wiek':>6}  Przykładowy wynik"]
    for w in widoki:
        linie.append(f"{w['id']:>4} {w['polecenie'][:28]:<28} {w['cel']:<10} {w['adresaci']:>8} {w['wykonane']:>7} "
                     f"{w['bledy']:>6} {w['wiek']:>5.0f}s  {w['przyklad']}")
    return "\n".join(linie)


# ==================================================== PAKIETY OD SLAVES ==

@typ_pakietu("rejestracja", anonimowy=True)
def pakiet_rejestracja(stan, slave_id, dane):
    nazwa = str(dane.get("nazwa", "?"))[:12]
    nowe_id, token = zarejestruj(stan, nazwa)
    return {"id": nowe_id, "token": token}


@typ_pakietu("praca")
def pakiet_praca(stan, slave_id, dane):
    return przetworz_prace(stan, slave_id, dane)


def token_rowny(a, b):
    return hmac.compare_digest(str(a).encode("utf-8"), str(b).encode("utf-8"))


def obsluz_zadanie_http(stan, sciezka, authorization, dane):
    """Obsługuje endpointy rejestracji i pracy; TLS zapewnia Cloudflare Tunnel/proxy."""
    token = authorization.removeprefix("Bearer ").strip() if authorization.startswith("Bearer ") else ""
    if sciezka == "/api/slave/register":
        if token_rowny(token, stan.token_rejestracji):
            return 200, pakiet_rejestracja(stan, "0", dane)
        if token and token_jednorazowy_zuzyj(stan, token):
            return 200, pakiet_rejestracja(stan, "0", dane)
        return 401, {"blad": "nieprawidłowy token rejestracji"}

    prefix = "/api/slave/"
    if not sciezka.startswith(prefix):
        return 404, {"blad": "nie znaleziono"}
    slave_id = urllib.parse.unquote(sciezka[len(prefix):]).strip()
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            odrzuc(stan, "nieznane ID")
            return 401, {"blad": "nieznany Slave"}
        if not token_rowny(token, slave.token):
            odrzuc(stan, "zły token Slaves")
            return 401, {"blad": "nieprawidłowy token Slaves"}
        if not stan.pracuje:
            return 503, {"blad": "Master wstrzymany"}
    if not isinstance(dane, dict) or dane.get("typ") != "praca":
        return 400, {"blad": "nieprawidłowy pakiet pracy"}
    return 200, pakiet_praca(stan, slave_id, dane)


# ================================================== SERWER HTTP SLAVES ==
# Minimalny HTTP/1.1 endpoint; TLS kończy Cloudflare, Cloudflare Tunnel lub reverse proxy.

async def obsluz_polaczenie(stan, reader, writer):
    stan.polaczenia += 1
    status, response = 400, {"blad": "nieprawidłowe żądanie"}
    try:
        request_line = await asyncio.wait_for(reader.readline(), CISZA_ROZLACZ_S)
        method, target, _ = request_line.decode("ascii").strip().split(" ", 2)
        naglowki = {}
        rozmiar_naglowkow = 0
        while True:
            line = await asyncio.wait_for(reader.readline(), CISZA_ROZLACZ_S)
            rozmiar_naglowkow += len(line)
            if rozmiar_naglowkow > 65536:
                status, response = 431, {"blad": "nagłówki za duże"}
                break
            if line in (b"\r\n", b"\n", b""):
                break
            key, separator, value = line.decode("latin-1").partition(":")
            if separator:
                naglowki[key.strip().lower()] = value.strip()
        if status == 400:
            if method != "POST":
                status, response = 405, {"blad": "wymagana metoda POST"}
            elif naglowki.get("transfer-encoding", "").lower() == "chunked":
                status, response = 400, {"blad": "chunked nieobsługiwany"}
            else:
                dl = int(naglowki.get("content-length", "0"))
                if dl <= 0 or dl > MAX_LINIA:
                    status, response = 413, {"blad": "nieprawidłowy rozmiar żądania"}
                else:
                    body = await asyncio.wait_for(reader.readexactly(dl), CISZA_ROZLACZ_S)
                    dane = json.loads(body)
                    if not isinstance(dane, dict):
                        status, response = 400, {"blad": "oczekiwano obiektu JSON"}
                    else:
                        status, response = obsluz_zadanie_http(
                            stan, urllib.parse.urlsplit(target).path,
                            naglowki.get("authorization", ""), dane,
                        )
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, UnicodeError, ValueError, json.JSONDecodeError):
        status, response = 400, {"blad": "nieprawidłowe żądanie HTTP/JSON"}
    except (ConnectionError, OSError):
        pass
    except Exception as e:
        log(stan, f"Błąd obsługi żądania Slaves: {e}", True)
        status, response = 500, {"blad": "wewnętrzny błąd Master"}
    finally:
        stan.polaczenia -= 1
        try:
            body = json.dumps(response, ensure_ascii=False).encode("utf-8")
            reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
                      405: "Method Not Allowed", 413: "Payload Too Large", 431: "Request Header Fields Too Large",
                      500: "Internal Server Error",
                      503: "Service Unavailable"}.get(status, "Error")
            writer.write(f"HTTP/1.1 {status} {reason}\r\n".encode("ascii"))
            writer.write(b"Content-Type: application/json; charset=utf-8\r\n")
            writer.write(f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii"))
            writer.write(body)
            await writer.drain()
            writer.close()
        except Exception:
            pass


def uruchom_serwer_slaves(stan):
    """Startuje endpoint HTTP w osobnym wątku z własną pętlą asyncio."""
    petla = asyncio.new_event_loop()
    gotowe, wynik = threading.Event(), {}

    def dzialaj():
        asyncio.set_event_loop(petla)
        try:
            wynik["serwer"] = petla.run_until_complete(asyncio.start_server(
                lambda r, w: obsluz_polaczenie(stan, r, w), HOST, PORT, backlog=BACKLOG, limit=MAX_LINIA))
        except Exception as e:
            wynik["blad"] = e
            gotowe.set()
            return
        gotowe.set()
        petla.run_forever()
        wynik["serwer"].close()

    threading.Thread(target=dzialaj, daemon=True).start()
    gotowe.wait()
    if "blad" in wynik:
        raise wynik["blad"]

    async def koniec():
        wynik["serwer"].close()
        zadania = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in zadania:                           # rozłącz Slaves zamiast zostawiać wiszące zadania
            t.cancel()
        await asyncio.gather(*zadania, return_exceptions=True)
        petla.stop()

    def zatrzymaj():
        try:
            asyncio.run_coroutine_threadsafe(koniec(), petla).result(timeout=5)
        except Exception:
            pass
    return zatrzymaj


def podnies_limit_plikow():
    """Każde połączenie to deskryptor pliku. Podnosi miękki limit (ulimit -n), ile się da."""
    try:
        import resource
    except ImportError:                             # Windows
        return None
    miekki, twardy = resource.getrlimit(resource.RLIMIT_NOFILE)
    gora = 1_048_576 if twardy == resource.RLIM_INFINITY else twardy
    for cel in (gora, 1_048_576, 262_144, 65_536, 32_768, 10_240):
        if miekki < cel <= gora:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (cel, twardy))
                return cel
            except (ValueError, OSError):
                continue
    return miekki


def raporter(stan):
    """Co RAPORT_CO_S zbiera rejestracje i odrzucone pakiety w jedną linię (zamiast tysięcy)."""
    while True:
        time.sleep(RAPORT_CO_S)
        with stan.lock:
            nowi, stan.nowi = stan.nowi, []
            odrzucone, stan.odrzucone = stan.odrzucone, Counter()
            razem = len(stan.slaves)
        if 0 < len(nowi) <= 5:
            for slave_id, nazwa in nowi:
                log(stan, f"+ #{slave_id} {nazwa} zarejestrowany", True)
        elif nowi:
            log(stan, f"+ {len(nowi)} nowych Slaves (razem {razem})", True)
        if odrzucone:
            log(stan, "! odrzucone pakiety: " + ", ".join(f"{p}: {n}" for p, n in odrzucone.items()), True)


# ============================================================ PANEL (HTTP) ==
# Endpoint = funkcja (stan, zapytanie) -> (kod, typ, bajty), rejestrowana dekoratorem @endpoint.
# Zapisy (POST) są chronione tokenem w nagłówku X-Token. Własny nagłówek wymusza zapytanie
# "preflight", którego serwer nie obsługuje - obca strona w przeglądarce nie wyśle sterowania.
# Rzucony ValueError = odpowiedź 400 z komunikatem.

@dataclass
class Zapytanie:
    sciezka: str
    params: dict
    dane: Optional[dict] = None          # sparsowany JSON z POST
    naglowki: Optional[object] = None


def odpowiedz_json(obiekt, kod=200):
    return kod, "application/json; charset=utf-8", json.dumps(obiekt).encode()


def wczytaj_token_panelu():
    env = os.environ.get("MASTER_PANEL_TOKEN")
    if env:
        return env.strip()
    if PLIK_TOKENU.exists():
        return PLIK_TOKENU.read_text().strip()
    token = secrets.token_urlsafe(12)
    PLIK_TOKENU.write_text(token)
    try:
        os.chmod(PLIK_TOKENU, 0o600)
    except OSError:
        pass
    return token


def token_poprawny(stan, naglowki):
    podany = (naglowki.get("X-Token") or "").encode()
    return bool(stan.token_panelu) and hmac.compare_digest(podany, stan.token_panelu.encode())


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
    """{"akcja": "start" | "stop"} - to samo co komendy start/stop w konsoli."""
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
    """{"nazwa": opcjonalna nazwa Slaves, "domena": host Master, "https": bool, "port": int}
    -> jednorazowy token rejestracyjny + gotowe polecenie uruchomienia slave.py."""
    dane = zapyt.dane or {}
    nazwa = str(dane.get("nazwa", "")).strip()[:12]
    domena = str(dane.get("domena", "")).strip()
    if not domena:
        raise ValueError("podaj domenę/adres, pod którym Slave ma znaleźć Master")
    https = bool(dane.get("https", True))
    port = dane.get("port")

    token, wygasa = wygeneruj_token_jednorazowy(stan, etykieta=nazwa)

    polecenie = ["python3", "slave.py", "--master", shlex.quote(domena)]
    if not https:
        polecenie += ["--no-https"]
    if port:
        polecenie += ["--port", str(int(port))]
    polecenie += ["--token-rejestracji", shlex.quote(token)]
    if nazwa:
        polecenie += ["--nazwa", shlex.quote(nazwa)]

    return odpowiedz_json({
        "token": token,
        "wygasa_za_s": TOKEN_JEDNORAZOWY_WAZNY_S,
        "wygasa_ts": wygasa,
        "polecenie": " ".join(polecenie),
    })


@endpoint("/api/polecenie", metoda="POST", chroniony=True)
def ep_polecenie(stan, zapyt):
    """{"cel": "wszystkie" | ID | nazwa, "polecenie": "pauza", "args": [...]} - jak komenda `polec`."""
    dane = zapyt.dane or {}
    args = dane.get("args", [])
    if not isinstance(args, list):
        raise ValueError("args musi być tablicą")
    polecenie_panelu = str(dane.get("polecenie", "")).strip().lower()
    if polecenie_panelu not in PANEL_ADMIN_COMMANDS:
        raise ValueError("Użyj terminala Slaves; ten endpoint obsługuje tylko pauza, wznow i stop.")
    p = wydaj_polecenie(stan, str(dane.get("cel", "")), polecenie_panelu, args)
    return odpowiedz_json({"ok": True, "id": p.id, "cel": p.cel, "adresaci": p.adresaci})


def parsuj_linie_macros(linia):
    try:
        args = shlex.split(linia, comments=True)
    except ValueError:
        raise ValueError("Nieprawidłowa składnia w pliku macros.") from None
    operatory = {"|", "||", "&", "&&", ";", ">", ">>", "<", "<<", "2>&1"}
    if any(arg in operatory for arg in args):
        raise ValueError("Makra nie obsługują potoków, przekierowań ani operatorów shella.")
    return args


def wczytaj_makro(nazwa):
    nazwa = str(nazwa).strip()
    if nazwa.endswith(".sh"):
        nazwa = nazwa[:-3]
    if not nazwa or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in nazwa):
        raise ValueError("Nazwa macros może zawierać tylko litery ASCII, cyfry, '_' i '-'.")
    sciezka = MACRO_DIR / f"{nazwa}.sh"
    try:
        tresc = sciezka.read_bytes()
    except OSError:
        raise ValueError(f"Nie znaleziono macros '{nazwa}' w katalogu macros/.") from None
    try:
        linie = tresc.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        raise ValueError("Plik macros musi być zapisany jako UTF-8.") from None
    komendy = []
    for linia in linie:
        if linia.lstrip().startswith("#!"):
            continue
        args = parsuj_linie_macros(linia)
        if args:
            komendy.append(shlex.join(args))
    if not komendy:
        raise ValueError("Makro nie zawiera żadnych poleceń.")
    return komendy


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


def znajdz_endpoint(metoda, sciezka):
    """-> (funkcja, chroniony) albo None."""
    for slave, wzorzec, prefix, chroniony, funkcja in ENDPOINTY:
        if slave == metoda and (sciezka == wzorzec or (prefix and sciezka.startswith(wzorzec))):
            return funkcja, chroniony
    return None


class SerwerPanelu(ThreadingHTTPServer):
    request_queue_size = 128                       # domyślne 5 gubi zapytania, gdy Master jest zajęty


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
            except ValueError as e:                  # zły JSON, nieznany cel, nieznane polecenie...
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


# ============================================================== KONSOLA ==
# Komenda = funkcja (stan, arg) -> tekst do wypisania | None | KONIEC.

@komenda("start", "Master zaczyna odpowiadać Slaves (do tej pory: cisza)")
def k_start(stan, arg):
    uruchom_master(stan)


@komenda("stop", "Master znów ignoruje wszystkich (Slaves czekają i ponawiają)")
def k_stop(stan, arg):
    wstrzymaj_master(stan)


@komenda("status", "podsumowanie")
def k_status(stan, arg):
    return (f"Praca: {'TAK' if stan.pracuje else 'NIE'} | Slaves: {len(stan.slaves)} | "
            f"połączeń: {stan.polaczenia} | rozwiązane: {stan.dobre_razem + stan.zle_razem} "
            f"(dobre: {stan.dobre_razem}, błędne: {stan.zle_razem}) | "
            f"podgląd zadań: {'ON' if stan.log_wlaczony else 'OFF'} | "
            f"domyślna przerwa: {stan.opozn_domyslne:g}s")


@komenda("domyslna_przerwa",
         "domyslna_przerwa <sek> - zmień domyślny czas przerwy dla WSZYSTKICH Slaves "
         "(obowiązuje od następnego pakietu; indywidualne opoznienie go nadpisuje)")
def k_domyslna_przerwa(stan, arg):
    try:
        sek = max(0.0, float(arg))
    except ValueError:
        return f"użycie: domyslna_przerwa <sekundy>  (aktualnie: {stan.opozn_domyslne:g}s)"
    stara = stan.opozn_domyslne
    stan.opozn_domyslne = sek
    log(stan, f"domyślna przerwa: {stara:g}s → {sek:g}s (Slaves zaktualizują się przy następnym pakiecie)", True)


@komenda("slaves", "slaves [ile|wszystkie] - tabela Slaves (domyślnie 20 najaktywniejszych)")
def k_slaves(stan, arg):
    if arg.lower() == "wszystkie":
        ile = len(stan.slaves)
    else:
        ile = int(arg) if arg.isdigit() else 20
    return tabela_slaves(stan, ile)


@komenda("polec", "polec <id|nazwa|wszystkie> <polecenie> [arg...] - wyślij polecenie Slavesi (lub wszystkim)")
def k_polec(stan, arg):
    czesci = arg.split()
    if len(czesci) < 2:
        opisy = "\n".join(f"    {n:<11} {o}" for n, o in POLECENIA_SLAVES.items())
        return ("Użycie: polec <id|wszystkie> <program|polecenie> [argumenty]\n"
                "  np.  polec 3 ls -la   |   polec wszystkie pauza   |   polec 12 status\n"
                f"  Polecenia:\n{opisy}")
    nazwa = czesci[1].lower()
    args = czesci[2:]
    if nazwa not in POLECENIA_SLAVES:
        args = [czesci[1], *args]
        nazwa = "exec"
    try:
        p = wydaj_polecenie(stan, czesci[0], nazwa, args)
    except ValueError as e:
        return f"Błąd: {e}"
    return (f"Polecenie #{p.id} '{p.nazwa}' w kolejce | adresatów: {p.adresaci} | "
            f"Slave odbiera je przy najbliższym kontakcie. Wyniki: komenda 'polecenia'.")


@komenda("wszystkim", "wszystkim <polecenie> [arg...] - skrót: polecenie dla wszystkich Slaves")
def k_wszystkim(stan, arg):
    return k_polec(stan, "wszystkie " + arg)


@komenda("polecenia", "ostatnie polecenia: do ilu dotarło, ile potwierdziło, przykładowy wynik")
def k_polecenia(stan, arg):
    return tabela_polecen(stan)


@komenda("macros", "lista plików .sh dostępnych w katalogu macros/")
def k_macros(stan, arg):
    nazwy = sorted(p.stem for p in MACRO_DIR.glob("*.sh") if p.is_file())
    return "Dostępne macros: " + (", ".join(nazwy) if nazwy else "brak plików .sh w katalogu macros/")


@komenda("makro", "makro <id|nazwa|wszystkie> <nazwa.sh> - uruchom stos poleceń z pliku")
def k_makro(stan, arg):
    czesci = arg.split()
    if len(czesci) != 2:
        return "Użycie: makro <id|nazwa|wszystkie> <nazwa.sh>"
    try:
        p = wydaj_polecenie(stan, czesci[0], "makro", wczytaj_makro(czesci[1]))
    except ValueError as e:
        return f"Błąd: {e}"
    return (f"Makro #{p.id} w kolejce | adresatów: {p.adresaci} | "
            "wyniki pojawią się w terminalu panelu lub komendzie 'polecenia'.")


@komenda("baza", "informacje o bazie wyników")
def k_baza(stan, arg):
    return info_bazy(stan)


@komenda("log", "włącz/wyłącz podgląd rozwiązanych zadań w konsoli")
def k_log(stan, arg):
    stan.log_wlaczony = not stan.log_wlaczony
    return "Podgląd zadań: " + ("ON" if stan.log_wlaczony else "OFF")


@komenda("pomoc", "ta lista", aliasy=("help", "?"))
def k_pomoc(stan, arg):
    linie = ["Komendy:"] + [f"  {n:<10} - {o}" for n, o in OPISY_KOMEND]
    return "\n".join(linie + [f"Panel w przeglądarce: http://localhost:{PANEL_PORT}"])


@komenda("wyjdz", "koniec", aliasy=("exit", "quit"))
def k_wyjdz(stan, arg):
    return KONIEC


def konsola(stan):
    print("Master gotowy. Wpisz 'pomoc'. Dopóki nie wpiszesz 'start' - ignoruję Slaves.")
    while True:
        try:
            linia = input("master> ").strip()
        except (EOFError, KeyboardInterrupt):
            return
        if not linia:
            continue
        nazwa, _, arg = linia.partition(" ")
        funkcja = KOMENDY.get(nazwa.lower())
        if funkcja is None:
            print("Nieznana komenda. Wpisz 'pomoc'.")
            continue
        wynik = funkcja(stan, arg.strip())
        if wynik is KONIEC:
            return
        if wynik:
            print(wynik)


# ================================================================== START ==

def uruchom_w_tle(serwer, stan):
    serwer.stan = stan                             # handlery biorą stan z serwera
    threading.Thread(target=serwer.serve_forever, daemon=True).start()
    return serwer


def main():
    KATALOG_DANYCH.mkdir(parents=True, exist_ok=True)
    limit = podnies_limit_plikow()
    stan = utworz_stan()
    pisarz_watek = start_pisarza(stan)
    zatrzymaj_serwer = uruchom_serwer_slaves(stan)
    threading.Thread(target=raporter, args=(stan,), daemon=True).start()
    panel = None
    try:
        panel = uruchom_w_tle(SerwerPanelu((PANEL_HOST, PANEL_PORT), Panel), stan)
        log(stan, f"Master Center: http://127.0.0.1:{PANEL_PORT} (zdalnie użyj tunelu SSH)", True)
        log(stan, f"Token sterowania panelu: {stan.token_panelu}  (plik {PLIK_TOKENU.name})", True)
    except OSError as e:
        log(stan, f"! Master Center nie wystartował (port {PANEL_PORT}: {e})", True)
    log(stan, f"Slave HTTP origin: {HOST}:{PORT} | zarejestrowanych: {len(stan.slaves)} | "
              f"limit plików: {limit} | {info_bazy(stan)}", True)
    log(stan, f"Token rejestracyjny Slaves: {stan.token_rejestracji} "
              "(ustaw po stronie Slaves jako SLAVE_REGISTRATION_TOKEN)", True)
    try:
        konsola(stan)
    finally:
        zatrzymaj_serwer()
        if panel:
            panel.shutdown()
        zamknij_pisarza(stan, pisarz_watek)


if __name__ == "__main__":
    main()