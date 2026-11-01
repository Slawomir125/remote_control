"""Struktury danych (Stan, Slave, Polecenie) i pomocnicze funkcje logowania."""

import queue
import time
import threading
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Optional

from master.config import LOG_ZADAN_NA_S, MAX_POLECEN


@dataclass
class Polecenie:
    id: int
    nazwa: str
    args: list
    ts: float
    cel: str                       # ID Slaves
    adresaci: int = 0
    wykonane: int = 0              # ile Slaves potwierdziło
    bledy: int = 0                 # ile z nich zgłosiło błąd
    przyklad: str = ""             # wynik z pierwszego potwierdzenia


@dataclass
class Slave:
    nazwa: str
    token: str                                   # token bearer Slaves
    sesja: str = "łączenie"                      # rozłączony / łączenie / połączony / błąd
    dobre: int = 0
    zle: int = 0
    suma_czasu: float = 0.0
    ostatnio: Optional[float] = None
    zadanie: Optional[dict] = None               # zadanie w toku
    pauza: bool = True
    polecenia: list = field(default_factory=list)   # polecenia czekające na potwierdzenie
    ts: deque = field(default_factory=deque)     # czasy ukończeń z ostatniej minuty


@dataclass
class Stan:
    db: object                                   # sqlite3.Connection
    token_rejestracji: str
    slaves: dict = field(default_factory=dict)   # id -> Slave
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
    nowi: list = field(default_factory=list)                      # rejestracje od ostatniego raportu
    odrzucone: Counter = field(default_factory=Counter)           # powód -> ile pakietów
    log_sekunda: int = 0
    log_licznik: int = 0
    log_pominiete: int = 0
    ssh: Optional[object] = None                                  # StanSSH (ustawiane w main.py po wczytaj_hosty)
    # Transfer plików: staging per polecenie_id. Czyszczony po konsumpcji.
    pliki_do_slave: dict = field(default_factory=dict)    # pol_id -> {bytes, zdalna, sha256, slave_id}
    pliki_od_slave: dict = field(default_factory=dict)    # pol_id -> {buf: bytearray, final: bool, event, slave_id}


def log(stan, tekst, zawsze=False):
    if zawsze or stan.log_wlaczony:
        print(f"[{time.strftime('%H:%M:%S')}] {tekst}", flush=True)


def log_zadania(stan, linia):
    """Linia 'zadanie rozwiązane' z limitem na sekundę — przy tysiącach Slaves nie zalewa konsoli."""
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
