"""Widoki: snapshoty i tabele dla panelu i konsoli."""

import heapq
import time

from master.config import AKTYWNY_DO_S
from master.zadania import ile_w_oknie, seria_tempa


def widok_slaves(slave_id, slave, teraz):
    n = slave.dobre + slave.zle
    return {
        "id": slave_id, "nazwa": slave.nazwa, "dobre": slave.dobre, "zle": slave.zle,
        "sr_czas": (slave.suma_czasu / n) if n else None,
        "na_min": ile_w_oknie(slave.ts, teraz),
        "ostatnio": (teraz - slave.ostatnio) if slave.ostatnio else None,
        "zadanie": slave.zadanie["tekst"] if slave.zadanie else None,
        "pauza": slave.pauza,
        "sesja": slave.sesja,
    }


def top_slaves(stan, ile, teraz):
    """`ile` najbardziej pracowitych Slaves."""
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
        aktywne = sum(1 for slave in stan.slaves.values()
                      if slave.sesja == "połączony")
        return {
            "uptime": teraz - stan.start_ts,
            "slaves_tabela": [widok_slaves(sid, s, teraz) for sid, s in stan.slaves.items()],
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
    linie = [f"{'ID':>5}  {'Nazwa':<12} {'Sesja':<11} {'OK':>6} {'Błędy':>6} {'Śr.czas':>8} {'Zad/min':>8} {'Ostatnio':>10}  Zadanie w toku"]
    for w in widoki:
        sr = f"{w['sr_czas']:.2f}s" if w["sr_czas"] is not None else "-"
        ost = f"{w['ostatnio']:.0f}s temu" if w["ostatnio"] is not None else "-"
        linie.append(f"{w['id']:>5}  {w['nazwa']:<12} {w['sesja']:<11} {w['dobre']:>6} {w['zle']:>6} {sr:>8} "
                     f"{w['na_min']:>8} {ost:>10}  {'PAUZA' if w['pauza'] else (w['zadanie'] or '-')}")
    if razem > len(widoki):
        linie.append(f"... pokazano {len(widoki)} najaktywniejszych z {razem} ('slaves wszystkie' pokaże resztę)")
    return "\n".join(linie)


def tabela_polecen(stan, ile=10):
    with stan.lock:
        teraz = time.time()
        widoki = [widok_polecenia(p, teraz) for p in list(stan.polecenia.values())[-ile:]]
    if not widoki:
        return "Brak poleceń. Użyj: polec <id> <polecenie> [arg...]"
    linie = [f"{'#':>4} {'Polecenie':<28} {'Cel':<10} {'Adresaci':>8} {'Potw.':>7} {'Błędy':>6} {'Wiek':>6}  Przykładowy wynik"]
    for w in widoki:
        linie.append(f"{w['id']:>4} {w['polecenie'][:28]:<28} {w['cel']:<10} {w['adresaci']:>8} {w['wykonane']:>7} "
                     f"{w['bledy']:>6} {w['wiek']:>5.0f}s  {w['przyklad']}")
    return "\n".join(linie)
