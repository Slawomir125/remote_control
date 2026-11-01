"""Logika koordynatora: rejestracja, sesje, polecenia, przetwarzanie pracy, makra."""

import json
import secrets
import time

from master.config import (
    MACRO_DIR, POLECENIE_TTL_S, MAX_POLECEN, POLECENIA_SLAVES,
)
from master.stan import Slave, Polecenie, log, log_zadania
from master.zadania import ocen_odpowiedz, przytnij_okno, zlicz_sekunde


# ─── zdarzenia i rejestracja ─────────────────────────────────────────────────

def dodaj_zdarzenie(stan, typ, **dane):
    """Wpis do live feedu (panel czyta go przez /api/stan)."""
    with stan.lock:
        stan.seq += 1
        stan.feed.append({"seq": stan.seq, "ts": time.time(), "typ": typ, **dane})


def zarejestruj(stan, nazwa):
    with stan.lock:
        for slave_id, slave in stan.slaves.items():
            if slave.nazwa == nazwa:
                # Jeżeli admin wcześniej rozłączył slave'a, re-rejestracja nie
                # może tego nadpisać — intencja Rozłącz musi przeżyć restart slave'a.
                if slave.sesja != "rozłączony":
                    slave.sesja = "połączony"
                return slave_id, slave.token
        slave_id = str(len(stan.slaves) + 1)
        token = secrets.token_urlsafe(32)
        slave = stan.slaves[slave_id] = Slave(nazwa, token, sesja="połączony")
        stan.nowi.append((slave_id, nazwa))
        with stan.db_lock:
            stan.db.execute("INSERT INTO slaves VALUES (?,?,?,?,?)",
                            (slave_id, nazwa, token, time.time(), "połączony"))
            stan.db.commit()
    return slave_id, token


def odrzuc(stan, powod):
    """Liczy odrzucony pakiet; raport zbiorczy wypisuje raporter()."""
    with stan.lock:
        stan.odrzucone[powod] += 1


# ─── sesje per Slave ────────────────────────────────────────────────────────

def _persistuj_sesje(stan, slave_id, sesja):
    """Zapisuje intencję admina do DB — dzięki temu rozłączenie/otwarcie przeżywa restart."""
    with stan.db_lock:
        stan.db.execute("UPDATE slaves SET sesja=? WHERE id=?", (sesja, slave_id))
        stan.db.commit()


def polacz_slave(stan, slave_id):
    """Admin otwiera sesję — czeka aż Slave się odezwie (następny poll)."""
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            raise ValueError(f"Nie znaleziono Slave #{slave_id}")
        slave.sesja = "łączenie"
        nazwa = slave.nazwa
    _persistuj_sesje(stan, slave_id, "łączenie")
    dodaj_zdarzenie(stan, "sys", tekst=f"~ #{slave_id} '{nazwa}': sesja otwarta (łączenie)")
    log(stan, f"~ #{slave_id} '{nazwa}': sesja otwarta (łączenie)", True)


def rozlacz_slave(stan, slave_id):
    """Admin zamyka sesję — Slave dostaje 503 przy kolejnych pollach."""
    with stan.lock:
        slave = stan.slaves.get(slave_id)
        if slave is None:
            raise ValueError(f"Nie znaleziono Slave #{slave_id}")
        slave.sesja = "rozłączony"
        nazwa = slave.nazwa
    _persistuj_sesje(stan, slave_id, "rozłączony")
    dodaj_zdarzenie(stan, "sys", tekst=f"~ #{slave_id} '{nazwa}': sesja zamknięta")
    log(stan, f"~ #{slave_id} '{nazwa}': sesja zamknięta", True)


# ─── zapis wyników ───────────────────────────────────────────────────────────

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


# ─── polecenia dla Slaves ────────────────────────────────────────────────────

def znajdz_slaves(stan, cel):
    """ID albo nazwa Slave -> ID. Wołać pod stan.lock."""
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
    """Kolejkuje polecenie dla jednego Slave (bez broadcastu — każdy osobno)."""
    nazwa = nazwa.strip().lower()
    if nazwa not in POLECENIA_SLAVES:
        raise ValueError(f"nieznane polecenie '{nazwa}'. Znane: {', '.join(POLECENIA_SLAVES)}")
    if not isinstance(args, list) or (nazwa != "makro" and (len(args) > 50 or any(len(str(a)) > 200 for a in args))):
        raise ValueError("Argumenty polecenia są za długie albo jest ich za dużo.")
    args = [str(a) for a in args]
    with stan.lock:
        cel_id = znajdz_slaves(stan, cel)
        stan.licznik_polecen += 1
        p = Polecenie(stan.licznik_polecen, nazwa, args, time.time(), cel_id)
        p.adresaci = 1
        stan.slaves[cel_id].polecenia.append(p)
        stan.polecenia[p.id] = p
        while len(stan.polecenia) > MAX_POLECEN:
            stan.polecenia.pop(next(iter(stan.polecenia)))
    tresc = f"{nazwa} {' '.join(args)}".strip()
    dodaj_zdarzenie(stan, "sys", tekst=f"> polecenie #{p.id} '{tresc}' do #{cel_id}")
    return p


def zbierz_polecenia(stan, slave, teraz):
    """Polecenia do wysłania Slave w tej odpowiedzi (ścieżka zwykła O(1))."""
    if not slave.polecenia:
        return []
    slave.polecenia = [p for p in slave.polecenia if teraz - p.ts <= POLECENIE_TTL_S]
    wyslij = sorted(slave.polecenia, key=lambda p: p.id)
    return [{"id": p.id, "cmd": p.nazwa, "args": p.args}
            for p in wyslij[:20]]


def przyjmij_potwierdzenia(stan, slave_id, slave, potwierdzenia):
    """Slave odsyła wyniki poleceń. Powtórzone (ponowiony pakiet) są ignorowane."""
    if not isinstance(potwierdzenia, list):
        return
    for p in potwierdzenia[:50]:
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        pol = stan.polecenia.get(pid)
        if pol is None:
            continue
        if not any(x is pol for x in slave.polecenia):
            continue
        slave.polecenia = [x for x in slave.polecenia if x is not pol]
        ok, wynik = bool(p.get("ok")), str(p.get("output", ""))
        if pol.nazwa != "makro":
            wynik = wynik[:1000]
        pol.wykonane += 1
        pol.bledy += 0 if ok else 1
        pol.przyklad = pol.przyklad or wynik[:2000 if pol.nazwa == "promuj_ssh" else 200]
        tekst = f"< #{slave_id} {slave.nazwa}: {pol.nazwa} -> {wynik}" + ("" if ok else "  [BŁĄD]")
        dodaj_zdarzenie(stan, "sys", tekst=tekst, command_id=pol.id,
                        slave_id=slave_id, slave_nazwa=slave.nazwa, wynik=wynik, ok=ok)
        wpis_logu = (f"< #{slave_id} {slave.nazwa}: makro -> wynik {len(wynik)} znaków"
                     if pol.nazwa == "makro" else tekst)
        log(stan, wpis_logu, True)


# ─── przetwarzanie pracy ────────────────────────────────────────────────────

SESJA_ROZLACZONA = object()  # sentinel — serwer_slaves zamienia na HTTP 503


def przetworz_prace(stan, slave_id, dane):
    """Jeden pakiet 'praca': potwierdzenia poleceń i odpytywanie o kolejne.

    Zwraca SESJA_ROZLACZONA, gdy admin zamknął sesję — serwer odpowiada 503.
    """
    linia_logu = None
    with stan.lock:
        slave = stan.slaves[slave_id]
        if slave.sesja == "rozłączony":
            return SESJA_ROZLACZONA
        teraz = time.time()
        if slave.sesja != "połączony":
            slave.sesja = "połączony"
        slave.ostatnio = teraz
        slave.pauza = bool(dane.get("idle"))
        przyjmij_potwierdzenia(stan, slave_id, slave, dane.get("acks"))
        werdykt, szczegoly = ocen_odpowiedz(slave.zadanie, dane, teraz)
        if werdykt in ("ok", "fail"):
            linia_logu = zapisz_wynik(stan, slave_id, slave, szczegoly, teraz)
        odpowiedz = {"prev": werdykt}
        odpowiedz["interval"] = stan.opozn_domyslne
        polecenia = zbierz_polecenia(stan, slave, teraz)
        if polecenia:
            odpowiedz["tasks"] = polecenia
    if linia_logu:
        log_zadania(stan, linia_logu)                # poza blokadą
    return odpowiedz


# ─── makra ───────────────────────────────────────────────────────────────────

def wczytaj_makro(nazwa):
    """Wczytuje plik macros/<nazwa>.sh i zwraca listę linii do wykonania przez shell."""
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
        linia = linia.strip()
        if not linia or linia.startswith("#"):
            continue
        komendy.append(linia)
    if not komendy:
        raise ValueError("Makro nie zawiera żadnych poleceń.")
    return komendy
