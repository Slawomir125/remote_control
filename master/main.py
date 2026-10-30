"""Punkt startowy koordynatora Master — montaż i uruchomienie."""

import threading

from master.config import (
    KATALOG_DANYCH, HOST, PORT, PANEL_HOST, PANEL_PORT, PLIK_TOKENU,
)
from master.stan import log
from master.baza import utworz_stan, start_pisarza, zamknij_pisarza, info_bazy
from master.serwer_slaves import uruchom_serwer_slaves, podnies_limit_plikow, raporter
from master.panel import SerwerPanelu, Panel
from master.konsola import konsola  # import rejestruje @komenda
from master.ssh import wczytaj_hosty  # import rejestruje @endpoint SSH
from master import hosty  # noqa: F401  — import rejestruje @endpoint /api/hosty-znane


def uruchom_w_tle(serwer, stan):
    serwer.stan = stan
    threading.Thread(target=serwer.serve_forever, daemon=True).start()
    return serwer


def main():
    KATALOG_DANYCH.mkdir(parents=True, exist_ok=True)
    limit = podnies_limit_plikow()
    stan = utworz_stan()
    stan.ssh = wczytaj_hosty()
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
