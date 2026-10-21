"""Konsola interaktywna Master."""

from master.config import (
    PANEL_PORT, MACRO_DIR, POLECENIA_SLAVES, OPISY_KOMEND, KOMENDY, KONIEC,
    komenda,
)
from master.stan import log
from master.logika import (
    uruchom_master, wstrzymaj_master, wydaj_polecenie, wczytaj_makro,
)
from master.widoki import tabela_slaves, tabela_polecen
from master.baza import info_bazy


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
