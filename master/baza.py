"""Warstwa bazodanowa: schemat, migracje, odczyt/zapis, wątek pisarza."""

import json
import os
import queue
import sqlite3
import time
import threading
from pathlib import Path

from master.config import (
    KATALOG_DANYCH, PLIK_BAZY, PLIK_TOKEN_REJESTRACJI,
    STARY_STAN, LEGACY_STARY_STAN,
    wczytaj_token_rejestracji,
)
from master.stan import Slave, Stan

# ─── schemat ─────────────────────────────────────────────────────────────────

SCHEMAT = """
CREATE TABLE IF NOT EXISTS slaves(
    id TEXT PRIMARY KEY, nazwa TEXT NOT NULL, token TEXT NOT NULL,
    zarejestrowany REAL NOT NULL,
    sesja TEXT NOT NULL DEFAULT 'rozłączony');
CREATE TABLE IF NOT EXISTS zadania(
    nr INTEGER PRIMARY KEY AUTOINCREMENT, zadanie_id INTEGER, slave_id TEXT NOT NULL,
    ts REAL NOT NULL, dzialanie TEXT, poprawna TEXT, odpowiedz TEXT,
    ok INTEGER NOT NULL, czas REAL NOT NULL, operacja TEXT, liczby TEXT);
CREATE INDEX IF NOT EXISTS ix_zad_slave ON zadania(slave_id, ts);
CREATE INDEX IF NOT EXISTS ix_zad_ts ON zadania(ts);
"""
INSERT_ZADANIE = ("INSERT INTO zadania(zadanie_id, slave_id, ts, dzialanie, poprawna, odpowiedz,"
                  " ok, czas, operacja, liczby) VALUES (?,?,?,?,?,?,?,?,?,?)")


# ─── połączenie ──────────────────────────────────────────────────────────────

def db_polacz(zapis=True):
    c = sqlite3.connect(PLIK_BAZY, timeout=10, check_same_thread=False)
    if zapis:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
    return c


# ─── migracje ────────────────────────────────────────────────────────────────

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
    """Dodaje brakujące kolumny w istniejących tabelach (stare bazy)."""
    kolumny = {w[1] for w in db.execute("PRAGMA table_info(zadania)")}
    for nazwa in ("operacja", "liczby"):
        if nazwa not in kolumny:
            db.execute(f"ALTER TABLE zadania ADD COLUMN {nazwa} TEXT")
    kolumny_slaves = {w[1] for w in db.execute("PRAGMA table_info(slaves)")}
    if "sesja" not in kolumny_slaves:
        db.execute("ALTER TABLE slaves ADD COLUMN sesja TEXT NOT NULL DEFAULT 'rozłączony'")
    db.commit()


def importuj_stary_stan(db):
    """v2 trzymała Slaves w master_stan.json — przenosimy je do bazy (raz)."""
    legacy_state = STARY_STAN if STARY_STAN.exists() else LEGACY_STARY_STAN
    if legacy_state.exists() and not db.execute("SELECT 1 FROM slaves LIMIT 1").fetchone():
        for slave_id, record in json.loads(legacy_state.read_text()).items():
            token = record.get("token", record.get("klucz"))
            db.execute("INSERT INTO slaves VALUES (?,?,?,?,?)",
                       (slave_id, record["nazwa"], token, time.time(), "rozłączony"))
        db.commit()
        legacy_state.rename(legacy_state.with_name(legacy_state.name + ".migrated"))
        print(f"[Master] Zaimportowano Slave state z {legacy_state.name} do bazy.")


def inicjuj_baze(db):
    migruj_nazwy_schematu(db)
    db.executescript(SCHEMAT)
    migruj_schemat(db)
    importuj_stary_stan(db)


# ─── odczyt ──────────────────────────────────────────────────────────────────

def wczytaj_slaves(db):
    """Slaves z bazy razem z historycznymi licznikami (statystyki przeżywają restart).

    Master trzyma sesję za wszelką cenę: jedyną sesją, która przeżywa restart
    w stanie "rozłączony", jest ta, którą admin explicitnie zamknął. Każdy inny
    stan (połączony/łączenie/błąd) startuje jako "łączenie" — otwarta, czekamy
    aż Slave się odezwie i pierwszy udany pakiet przełączy go na "połączony".
    """
    slaves = {}
    for slave_id, nazwa, token, sesja_db in db.execute(
            "SELECT id, nazwa, token, sesja FROM slaves ORDER BY CAST(id AS INTEGER)"):
        sesja = "rozłączony" if sesja_db == "rozłączony" else "łączenie"
        slaves[slave_id] = Slave(nazwa, token, sesja=sesja)
    for slave_id, dobre, zle, suma in db.execute(
            "SELECT slave_id, SUM(ok), SUM(1 - ok), SUM(czas) FROM zadania GROUP BY slave_id"):
        if slave_id in slaves:
            slave = slaves[slave_id]
            slave.dobre, slave.zle, slave.suma_czasu = dobre, zle, suma
    return slaves


def utworz_stan():
    """Tworzy kompletny obiekt Stan: baza, tokeny, slaves, liczniki."""
    migracja_nazw_plikow()
    db = db_polacz()
    inicjuj_baze(db)
    stan = Stan(db=db, token_rejestracji=wczytaj_token_rejestracji())
    stan.slaves = wczytaj_slaves(db)
    stan.dobre_razem = sum(slave.dobre for slave in stan.slaves.values())
    stan.zle_razem = sum(slave.zle for slave in stan.slaves.values())
    stan.suma_czasu_razem = sum(slave.suma_czasu for slave in stan.slaves.values())
    stan.licznik_zadan = db.execute("SELECT COALESCE(MAX(zadanie_id), 0) FROM zadania").fetchone()[0]
    return stan


# ─── wątek zapisu ────────────────────────────────────────────────────────────

def pisarz(stan):
    """Wyniki z kolejki trafiają do bazy paczkami (max 0,5 s / 200 wierszy)."""
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


# ─── historia i statystyki ───────────────────────────────────────────────────

def historia_slaves(stan, slave_id):
    """Statystyki z bazy + ostatnie 50 zadań jednego Slave (okno szczegółów w panelu)."""
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
    fmt = lambda t: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) if t else "-"
    return (f"Baza {PLIK_BAZY.name}: {rozmiar:.0f} KB | Slaves: {n} | "
            f"zadań zapisanych: {z[0]} | od {fmt(z[1])} do {fmt(z[2])}")
