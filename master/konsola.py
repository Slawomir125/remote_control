"""Konsola interaktywna Master."""

import sys
import threading

from master.config import (
    PANEL_PORT, MACRO_DIR, POLECENIA_SLAVES, OPISY_KOMEND, KOMENDY, KONIEC,
    komenda,
)
from master.stan import log
from master.logika import (
    polacz_slave, rozlacz_slave, wydaj_polecenie, wczytaj_makro,
    znajdz_slaves,
)
from master.widoki import tabela_slaves, tabela_polecen
from master.baza import info_bazy


@komenda("status", "podsumowanie")
def k_status(stan, arg):
    with stan.lock:
        aktywne = sum(1 for s in stan.slaves.values() if s.sesja == "połączony")
    return (f"Slaves: {len(stan.slaves)} (sesje połączone: {aktywne}) | "
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


@komenda("polec", "polec <id|nazwa> <polecenie> [arg...] - wyślij polecenie do jednego Slave'a")
def k_polec(stan, arg):
    czesci = arg.split()
    if len(czesci) < 2:
        opisy = "\n".join(f"    {n:<11} {o}" for n, o in POLECENIA_SLAVES.items())
        return ("Użycie: polec <id|nazwa> <program|polecenie> [argumenty]\n"
                "  np.  polec 3 ls -la   |   polec slave-1 pauza   |   polec 12 status\n"
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
    return (f"Polecenie #{p.id} '{p.nazwa}' w kolejce dla #{p.cel} | "
            f"Slave odbiera je przy najbliższym kontakcie. Wyniki: komenda 'polecenia'.")


@komenda("polacz", "polacz <id|nazwa> - otwórz sesję z wybranym Slave'em")
def k_polacz(stan, arg):
    if not arg.strip():
        return "Użycie: polacz <id|nazwa>"
    try:
        with stan.lock:
            slave_id = znajdz_slaves(stan, arg.strip())
        polacz_slave(stan, slave_id)
    except ValueError as e:
        return f"Błąd: {e}"
    return f"Sesja Slave #{slave_id} otwarta (łączenie)."


@komenda("rozlacz", "rozlacz <id|nazwa> - zamknij sesję z wybranym Slave'em")
def k_rozlacz(stan, arg):
    if not arg.strip():
        return "Użycie: rozlacz <id|nazwa>"
    try:
        with stan.lock:
            slave_id = znajdz_slaves(stan, arg.strip())
        rozlacz_slave(stan, slave_id)
    except ValueError as e:
        return f"Błąd: {e}"
    return f"Sesja Slave #{slave_id} zamknięta."


@komenda("polecenia", "ostatnie polecenia: do ilu dotarło, ile potwierdziło, przykładowy wynik")
def k_polecenia(stan, arg):
    return tabela_polecen(stan)


@komenda("macros", "lista plików .sh dostępnych w katalogu macros/")
def k_macros(stan, arg):
    nazwy = sorted(p.stem for p in MACRO_DIR.glob("*.sh") if p.is_file())
    return "Dostępne macros: " + (", ".join(nazwy) if nazwy else "brak plików .sh w katalogu macros/")


@komenda("makro", "makro <id|nazwa> <nazwa.sh> - uruchom stos poleceń z pliku dla jednego Slave'a")
def k_makro(stan, arg):
    czesci = arg.split()
    if len(czesci) != 2:
        return "Użycie: makro <id|nazwa> <nazwa.sh>"
    try:
        p = wydaj_polecenie(stan, czesci[0], "makro", wczytaj_makro(czesci[1]))
    except ValueError as e:
        return f"Błąd: {e}"
    return (f"Makro #{p.id} w kolejce dla #{p.cel} | "
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
    # Bez TTY (np. docker compose up bez `tty: true`) input() natychmiast rzuca
    # EOFError, konsola kończy się, main() wychodzi, Docker restartuje kontener
    # i pętla trwa. W tym trybie trzymamy wątek main żywy, serwery HTTP/asyncio
    # pracują w tle do SIGTERM/SIGINT.
    if not sys.stdin.isatty():
        print("Master gotowy (tryb nieinteraktywny — brak TTY). "
              "Panel i serwer slave'ów pracują; wyślij SIGTERM aby zatrzymać.")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            return
        return

    print("Master gotowy. Wpisz 'pomoc'. Nasłuchiwanie Slaves jest zawsze włączone.")
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
