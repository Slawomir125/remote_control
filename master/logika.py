"""Logika koordynatora: rejestracja, polecenia, przetwarzanie pracy, makra."""

import json
import secrets
import time
from pathlib import Path

from master.config import (
    MACRO_DIR, POLECENIE_TTL_S, MAX_POLECEN, POLECENIA_SLAVES,
    CELE_WSZYSTKIE, TOKEN_JEDNORAZOWY_WAZNY_S,
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
                return slave_id, slave.token
        slave_id = str(len(stan.slaves) + 1)
        token = secrets.token_urlsafe(32)
        slave = stan.slaves[slave_id] = Slave(nazwa, token)
        slave.rozgl_ack = stan.ostatnie_rozgl_id        # nowy Slave nie dostaje starych poleceń
        stan.nowi.append((slave_id, nazwa))
        with stan.db_lock:
            stan.db.execute("INSERT INTO slaves VALUES (?,?,?,?)",
                            (slave_id, nazwa, token, time.time()))
            stan.db.commit()
    return slave_id, token


def odrzuc(stan, powod):
    """Liczy odrzucony pakiet; raport zbiorczy wypisuje raporter()."""
    with stan.lock:
        stan.odrzucone[powod] += 1


# ─── tokeny jednorazowe ─────────────────────────────────────────────────────

def wygeneruj_token_jednorazowy(stan, etykieta="", wazny_s=TOKEN_JEDNORAZOWY_WAZNY_S):
    """Tworzy token rejestracyjny do jednorazowego użytku."""
    teraz = time.time()
    token = secrets.token_urlsafe(24)
    with stan.lock:
        for stary, (wygasa, _) in list(stan.jednorazowe.items()):
            if wygasa <= teraz:
                del stan.jednorazowe[stary]
        stan.jednorazowe[token] = (teraz + wazny_s, etykieta)
    dodaj_zdarzenie(stan, "sys", tekst="Wygenerowano jednorazowy kod startowy"
                     + (f" dla '{etykieta}'" if etykieta else ""))
    return token, teraz + wazny_s


def wygeneruj_kod_startowy(stan, master_addr, https, port, nazwa):
    """Generuje krótki 6-znakowy kod + token jednorazowy do curl | python3."""
    kod = secrets.token_urlsafe(4)
    token, wygasa = wygeneruj_token_jednorazowy(stan, etykieta=nazwa)
    teraz = time.time()
    with stan.lock:
        for stary, dane in list(stan.kody_startowe.items()):
            if dane["wygasa"] <= teraz:
                del stan.kody_startowe[stary]
        stan.kody_startowe[kod] = {
            "token": token, "master": master_addr,
            "https": https, "port": port, "nazwa": nazwa, "wygasa": wygasa,
        }
    return kod, token, wygasa


def token_jednorazowy_zuzyj(stan, token):
    """Sprawdza jednorazowy token i od razu go usuwa."""
    teraz = time.time()
    with stan.lock:
        wpis = stan.jednorazowe.pop(token, None)
    if wpis is None:
        return False
    wygasa, _etykieta = wpis
    return wygasa > teraz


# ─── sterowanie Master ───────────────────────────────────────────────────────

def uruchom_master(stan):
    """Master zaczyna odpowiadać Slaves."""
    stan.pracuje = True
    dodaj_zdarzenie(stan, "sys", tekst="START - Master słucha Slaves")
    log(stan, "START - Master słucha Slaves", True)


def wstrzymaj_master(stan):
    """Master znów ignoruje wszystkich (Slaves czekają i ponawiają)."""
    stan.pracuje = False
    dodaj_zdarzenie(stan, "sys", tekst="STOP - Master ignoruje Slaves")
    log(stan, "STOP - Master ignoruje Slaves", True)


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
    """Kolejkuje polecenie dla jednego Slave albo wszystkich."""
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
            stan.rozgloszenia.append(p)
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
    """Polecenia do wysłania Slave w tej odpowiedzi (ścieżka zwykła O(1))."""
    wyslij = []
    if slave.polecenia:
        slave.polecenia = [p for p in slave.polecenia if teraz - p.ts <= POLECENIE_TTL_S]
        wyslij.extend(slave.polecenia)
    if slave.rozgl_ack < stan.ostatnie_rozgl_id:
        swieze = [p for p in stan.rozgloszenia if p.id > slave.rozgl_ack and teraz - p.ts <= POLECENIE_TTL_S]
        if swieze:
            wyslij.extend(swieze)
        else:
            slave.rozgl_ack = stan.ostatnie_rozgl_id
    wyslij.sort(key=lambda p: p.id)
    return [{"id": p.id, "polecenie": p.nazwa, "args": p.args, "rozgl": p.cel == "wszystkie"}
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


# ─── przetwarzanie pracy ────────────────────────────────────────────────────

def przetworz_prace(stan, slave_id, dane):
    """Jeden pakiet 'praca': potwierdzenia poleceń i odpytywanie o kolejne."""
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
