"""Zunifikowany widok znanych hostów — SSH + Slaves w jednej liście."""

import json
import time

from master.config import AKTYWNY_DO_S, endpoint


def _widok_slave(slave_id, slave, teraz):
    ostatnio_s = (teraz - slave.ostatnio) if slave.ostatnio else None
    aktywny = ostatnio_s is not None and ostatnio_s < AKTYWNY_DO_S
    polaczony = aktywny and not slave.pauza
    return {
        "id": slave_id,
        "typ": "slave",
        "nazwa": slave.nazwa,
        "adres": "",
        "aktywny": aktywny,
        "polaczony": polaczony,
        "ostatnio_s": ostatnio_s,
        "info": (slave.zadanie["tekst"] if slave.zadanie else ""),
    }


def _widok_ssh(host, teraz):
    ostatnio_s = (teraz - host.ostatnio) if host.ostatnio else None
    polaczony = host.sesja == "połączony"
    aktywny = polaczony or (ostatnio_s is not None and ostatnio_s < AKTYWNY_DO_S)
    adres = (f"{host.uzytkownik}@{host.host}" if host.uzytkownik else host.host)
    if host.port != 22:
        adres += f":{host.port}"
    return {
        "id": host.id,
        "typ": "ssh",
        "nazwa": host.nazwa,
        "adres": adres,
        "aktywny": aktywny,
        "polaczony": polaczony,
        "ostatnio_s": ostatnio_s,
        "info": host.sesja,
    }


@endpoint("/api/hosty-znane", chroniony=True)
def ep_hosty_znane(stan, zapyt):
    teraz = time.time()
    with stan.lock:
        slaves = [_widok_slave(sid, s, teraz) for sid, s in stan.slaves.items()]
    with stan.ssh.lock:
        ssh = [_widok_ssh(h, teraz) for h in stan.ssh.hosty.values()]
    tresc = {
        "hosty": slaves + ssh,
        "licznik_slaves": len(slaves),
        "licznik_ssh": len(ssh),
    }
    return 200, "application/json; charset=utf-8", json.dumps(tresc).encode()
